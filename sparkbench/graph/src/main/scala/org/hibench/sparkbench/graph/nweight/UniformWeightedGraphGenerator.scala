/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hibench.sparkbench.graph.nweight

import java.nio.{ByteBuffer,ByteOrder}
import java.security.MessageDigest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object UniformWeightedGraphGenerator {
  val Universe=2401080
  case class Feature(id: Long,value: Double)
  case class Profile(pairs: Long,directedEdges: Long,universe: Int,seed: Long,partitions: Int,modelSha256: String,algorithm: String)
  def main(args: Array[String]): Unit = {
    require(args.length==5,"Usage: UniformWeightedGraphGenerator OUTPUT PAIRS PARTITIONS SEED MODEL_URI")
    val spark=SparkSession.builder().appName("NWeight uniform weighted graph generator").getOrCreate()
    var candidates: org.apache.spark.sql.DataFrame=null
    var features: org.apache.spark.sql.DataFrame=null
    try {
      import spark.implicits._
      val count=args(1).toLong;val partitions=args(2).toInt;val seed=args(3).toLong
      require(count>0 && count<=4250000000L && partitions>0 && seed>=0 && seed<Long.MaxValue,"Invalid graph profile")
      val bytes=spark.read.format("binaryFile").load(args(4)).select("content").as[Array[Byte]].head()
      require(bytes.length==Universe*4,"Model must contain exactly 2,401,080 big-endian float features")
      val sha=MessageDigest.getInstance("SHA-256").digest(bytes).map(b=>f"${b&255}%02x").mkString
      features=Seq(bytes).toDS().flatMap { blob =>
        val buffer=ByteBuffer.wrap(blob).order(ByteOrder.BIG_ENDIAN)
        Iterator.range(0,Universe).map { id =>
          val value=buffer.getFloat().toDouble
          require(java.lang.Double.isFinite(value),"Nonfinite model feature")
          Feature(id.toLong,value)
        }
      }.toDF().repartition(partitions).persist(StorageLevel.MEMORY_AND_DISK)
      // Exact Long row budgeting avoids the old floor-to-zero retry loop.
      val proposed=(BigDecimal(count)*BigDecimal("1.2")).setScale(0,BigDecimal.RoundingMode.CEILING).toLong
      candidates=spark.range(0,proposed,1,partitions)
        // Spark offsets RNG seeds by partition ID. Adjacent field seeds would
        // share streams across adjacent partitions, creating artificial graph paths.
        .select(floor(rand(seed)*Universe).cast("long").as("src"),floor(rand(seed ^ 0x5DEECE66DL)*Universe).cast("long").as("dst"))
        .filter(col("src")=!=col("dst")).dropDuplicates("src","dst")
        .withColumn("bucket",pmod(xxhash64(col("src"),col("dst")),lit(partitions)))
        .persist(StorageLevel.MEMORY_AND_DISK)
      require(candidates.count()>=count,"Oversampling produced insufficient unique pairs")
      val counts=candidates.groupBy("bucket").count()
      val offsets=counts.withColumn("offset",coalesce(sum(col("count")).over(Window.orderBy("bucket")
        .rowsBetween(Window.unboundedPreceding,-1)),lit(0L))).select("bucket","offset")
      val selected=candidates.withColumn("position",sum(lit(1L)).over(Window.partitionBy("bucket")
        .orderBy(xxhash64(col("dst"),col("src"),lit(seed)),col("src"),col("dst"))
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)))
        .join(broadcast(offsets),Seq("bucket")).filter(col("position")+col("offset")<=count).select("src","dst")
      val weighted=selected.join(features.select(col("id").as("src"),col("value").as("sw")),Seq("src"))
        .join(features.select(col("id").as("dst"),col("value").as("dw")),Seq("dst"))
        .select(col("src"),col("dst"),format_string("%.4f",col("sw")*col("dw")).cast("double").as("weight"))
      val directed=weighted.union(weighted.select(col("dst").as("src"),col("src").as("dst"),col("weight")))
      directed.write.mode("errorifexists").parquet(args(0))
      Seq(Profile(count,Math.multiplyExact(count,2L),Universe,seed,partitions,sha,"uniform-feature-product-v1"))
        .toDS().write.parquet(args(0)+"/_generator_profile")
    } finally {if(candidates!=null)candidates.unpersist();if(features!=null)features.unpersist();spark.stop()}
  }
}
