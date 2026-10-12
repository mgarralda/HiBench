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
import org.apache.spark.sql.functions._

object DataFramePageRank {
  def ranks(rawEdges: DataFrame,iterations: Int): DataFrame = {
    val edges=rawEdges.select("src","dst").distinct()
    val degree=edges.groupBy("src").agg(count(lit(1)).as("degree"))
    iterate(edges.join(degree,Seq("src")),iterations)
  }
  def iterate(links: DataFrame,iterations: Int): DataFrame = {
    require(iterations>=0,"PageRank iterations must be nonnegative")
    var result=links.select(col("src").as("id")).distinct().withColumn("rank",lit(1.0))
    for(i<-1 to iterations) {
      val current=result.select(col("id").as("src"),col("rank"))
      result=links.join(current,Seq("src")).groupBy("dst")
        .agg(sum(col("rank")/col("degree")).as("contribution"))
        .select(col("dst").as("id"),(lit(.15)+lit(.85)*col("contribution")).as("rank"))
    }
    result
  }
  def main(args: Array[String]): Unit = {
    require(args.length==3,"Usage: DataFramePageRank INPUT_EDGES OUTPUT ITERATIONS")
    val spark=SparkSession.builder().appName("DataFramePageRank").getOrCreate()
    var links: DataFrame=null
    try {
      val edges=spark.read.parquet(args(0)).select("src","dst").distinct()
      val degree=edges.groupBy("src").agg(count(lit(1)).as("degree"))
      links=edges.join(degree,Seq("src")).cache()
      iterate(links,args(2).toInt).write.mode("errorifexists").parquet(args(1))
    } finally {if(links!=null) links.unpersist();spark.stop()}
  }
}
