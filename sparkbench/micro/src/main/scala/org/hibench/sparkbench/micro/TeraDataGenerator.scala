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
import org.hibench.sparkbench.micro.terasort.TeraRecordGenerator

/** Exactly the original TeraGen records, stored in an explicit binary schema. */
object TeraDataGenerator {
  def generate(spark: SparkSession, records: Long, partitions: Int): DataFrame = {
    require(records > 0 && records <= Long.MaxValue / 100, "Invalid Tera record count")
    require(partitions > 0, "Generation partitions must be positive")
    import spark.implicits._
    spark.range(0L, records, 1L, partitions).as[Long].mapPartitions { rows =>
      val generator = new TeraRecordGenerator()
      rows.map { row =>
        val record = generator.next(row)
        (record.take(10), record.drop(10))
      }
    }.toDF("key", "value")
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: TeraDataGenerator <output> <records> <partitions>")
    val spark = SparkSession.builder.appName("TeraDataGenerator").getOrCreate()
    try generate(spark, args(1).toLong, args(2).toInt).write.mode("overwrite")
      .option("compression", "uncompressed").parquet(args(0))
    finally spark.stop()
  }
}
