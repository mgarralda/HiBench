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

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/** weighted-path-topk-v1: immutable recurrence, duplicate path contributions sum.
  * Legacy shared mutable vertex queues are deliberately not reproduced.
  */
object DataFrameNWeight {
  def top(frame: DataFrame, limit: Int): DataFrame = frame
    .withColumn("position",row_number().over(Window.partitionBy("src").orderBy(col("weight").desc,col("dst").desc)))
    .filter(col("position")<=limit).drop("position")

  def compute(raw: DataFrame, degree: Int, limit: Int): DataFrame = {
    require(degree>=1 && limit>=1,"NWeight degree and top-K must be positive")
    val edges=raw.select("src","dst","weight").persist(StorageLevel.MEMORY_AND_DISK)
    var state: DataFrame=null
    try {
      require(edges.filter(isnan(col("weight")) || abs(col("weight"))===lit(Double.PositiveInfinity)).count()==0,"Nonfinite edge weight")
      state=top(edges.groupBy("src","dst").agg(sum("weight").as("weight")),limit).persist(StorageLevel.MEMORY_AND_DISK)
      state.count()
      var step=2
      while(step<=degree) {
        val previous=state
        val candidates=edges.as("e").join(previous.as("s"),col("e.dst")===col("s.src"))
          .select(col("e.src").as("src"),col("s.dst").as("dst"),(col("e.weight")*col("s.weight")).as("weight"))
          .filter(col("src")=!=col("dst"))
          .groupBy("src","dst").agg(sum("weight").as("weight"))
        state=top(candidates,limit).persist(StorageLevel.MEMORY_AND_DISK)
        state.count();previous.unpersist();step+=1
      }
      val vertices=edges.select(col("src").as("id")).union(edges.select(col("dst").as("id"))).distinct()
      val results=state.groupBy("src").agg(sort_array(collect_list(struct(col("dst"),col("weight")))).as("targets"))
      // Materialize the compact result before releasing recurrence caches.
      val output=vertices.join(results,vertices("id")===results("src"),"left")
        .select(col("id"),coalesce(col("targets"),array().cast("array<struct<dst:bigint,weight:double>>")).as("targets"))
        .persist(StorageLevel.MEMORY_AND_DISK)
      output.count();output
    } finally {if(state!=null)state.unpersist();edges.unpersist()}
  }
  def main(args: Array[String]): Unit = {
    require(args.length==4,"Usage: DataFrameNWeight INPUT OUTPUT DEGREE TOP_K")
    val spark=SparkSession.builder().appName("NWeight weighted-path-topk-v1").getOrCreate()
    var output: DataFrame=null
    try {
      output=compute(spark.read.parquet(args(0)),args(2).toInt,args(3).toInt)
      output.write.mode("errorifexists").parquet(args(1))
      println(s"NWeight algorithm=weighted-path-topk-v1 degree=${args(2)} topK=${args(3)} vertices=${output.count()}")
    } finally {if(output!=null)output.unpersist();spark.stop()}
  }
}
