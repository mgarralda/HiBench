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

import org.apache.spark.sql.SparkSession

/** Preserves legacy N records per generation partition, each containing 200 bytes. */
object ScalaInMemRepartition {
  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Usage: ScalaInMemRepartition <recordsPerPartition> <output> <cacheInMemory> <disableOutput>")
    val spark = SparkSession.builder.appName("ScalaInMemRepartition").getOrCreate()
    try {
      val parts = spark.conf.get("spark.default.parallelism", "2").toInt
      val records = args(0).toLong
      require(parts > 0 && records > 0 && records <= Long.MaxValue / parts / 200,
        "Invalid in-memory record count")
      import spark.implicits._
      val data = spark.range(0L, records * parts, 1L, parts).as[Long].mapPartitions { rows =>
        val payload = (0 until 200).map(_.toByte).toArray
        rows.map(_ => payload.clone())
      }.toDF("value")
      MicroDataFrameIO.repartition(data, args(1), args(2).toBoolean, args(3).toBoolean)
    } finally spark.stop()
  }
}
