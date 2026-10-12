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

package org.hibench.sparkbench.websearch

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.hibench.sparkbench.common.datagen.{LegacyZipfian,WebLinkGenerator}

object PageRankDataGenerator {
  case class Edge(src: Long,dst: Long)
  case class Profile(pages: Long,partitions: Int,seed: Long,zipfExponent: Double,meanLinks: Double)
  def edges(spark: SparkSession,pages: Long,partitions: Int,seed: Long): DataFrame = {
    require(pages>=2 && pages<=Long.MaxValue/40 && partitions>0,"Invalid PageRank shape")
    require(seed>=1 && seed+partitions-1<=Int.MaxValue,"Invalid PageRank slot seed")
    val zipf=new LegacyZipfian(pages,.5);zipf.setupZipf(pages*40,.1)
    val kernel=zipf.createLegacyZipfCore();val slotSize=(pages-1)/partitions+1
    import spark.implicits._
    spark.range(0,partitions,1,partitions).as[Long].flatMap { partition =>
      val generator=new WebLinkGenerator((seed+partition).toInt,kernel)
      val start=(BigInt(slotSize)*partition).min(BigInt(pages)).toLong
      val end=(BigInt(start)+slotSize).min(BigInt(pages)).toLong
      new Iterator[Long] {
        private var id=start
        def hasNext: Boolean=id<end
        def next(): Long = {if(!hasNext) throw new NoSuchElementException("End of PageRank slot");val result=id;id+=1;result}
      }.flatMap(id=>generator.nextLinks().iterator.map(dst=>Edge(id,dst)))
    }.toDF()
  }
  def main(args: Array[String]): Unit = {
    require(args.length==4,"Usage: PageRankDataGenerator OUTPUT PAGES PARTITIONS SEED")
    val spark=SparkSession.builder().appName("PageRankDataGenerator").getOrCreate()
    try {
      val n=args(1).toLong;val p=args(2).toInt;val seed=args(3).toLong
      edges(spark,n,p,seed).write.mode("errorifexists").parquet(args(0)+"/edges")
      spark.range(0,n,1,p).write.mode("errorifexists").parquet(args(0)+"/vertices")
      import spark.implicits._
      Seq(Profile(n,p,seed,.5,40)).toDS().write.parquet(args(0)+"/_generator_profile")
    } finally spark.stop()
  }
}
