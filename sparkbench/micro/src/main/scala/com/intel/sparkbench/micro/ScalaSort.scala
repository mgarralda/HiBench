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
import org.apache.spark.sql.functions._

object ScalaSort {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: ScalaSortDF <INPUT_HDFS> <OUTPUT_HDFS>")

    // spark context + session
    val conf = new SparkConf().setAppName("ScalaSort")
    val sc   = new SparkContext(conf)
//    val spark = SparkSession.builder().config(conf).getOrCreate()

    // fetch default parallelism & desired shuffle partitions
//    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
//    val reducer  = IOCommon
//      .getProperty("hibench.default.shuffle.parallelism")
//      .map(_.toInt)
//      .getOrElse(parallel / 2)

    // tell the SQL engine how many shuffle partitions to use
//    spark.conf.set("spark.sql.shuffle.partitions", reducer)

    val io = new IOCommon(sc)

    // 1) read as DataFrame
    val df = io.loadDF(args(0))
    // 2) globally sort on the "value" column
    val sorted = df.orderBy(col("value"))
    // 3) write out as Parquet
    io.saveDF(args(1), sorted)

    sc.stop()
  }
}