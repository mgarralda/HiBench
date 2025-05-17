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


package com.intel.hibench.sparkbench.micro

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ScalaWordCount{
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: ScalaWordCountDF <INPUT_HDFS> <OUTPUT_HDFS>")

    // bootstrap SparkContext + SparkSession
    val sparkConf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    import spark.implicits._

    // load as DataFrame of a single "value" column
    val io = new IOCommon(sc)
    val df = io.loadDF(args(0))

    // split→explode→group→count
    val countsDF = df
      .withColumn("word", explode(split($"value", "\\s+")))
      .groupBy("word")
      .count()

    // write out as Snappy‑compressed Parquet
    io.saveDF(args(1), countsDF)

    spark.stop()
  }
}



