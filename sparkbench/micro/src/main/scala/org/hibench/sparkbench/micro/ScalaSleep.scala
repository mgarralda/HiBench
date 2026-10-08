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

package org.hibench.sparkbench.micro

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.sum

/** One sleep per task; the task count and duration are the benchmark's load. */
object ScalaSleep {
  def generate(spark: SparkSession, seconds: Long, tasks: Int): DataFrame = {
    require(seconds >= 0 && seconds <= Long.MaxValue / 1000, "Invalid sleep duration")
    require(tasks > 0, "Task count must be positive")
    import spark.implicits._
    spark.range(0L, tasks.toLong, 1L, tasks).as[Long].mapPartitions { rows =>
      rows.map { _ => Thread.sleep(seconds * 1000L); 1L }
    }.toDF("completed")
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: ScalaSleep <seconds>")
    val spark = SparkSession.builder.appName("ScalaSleep").getOrCreate()
    try {
      val tasks = spark.conf.get("spark.default.parallelism", "2").toInt
      val completed = generate(spark, args(0).toLong, tasks).agg(sum("completed")).head().getLong(0)
      require(completed == tasks, "Not all sleep tasks completed")
      println(s"HIBENCH_SLEEP_TASKS=$completed;SECONDS_PER_TASK=${args(0)}")
    } finally spark.stop()
  }
}
