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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BinaryType

object MicroDataFrameIO {
  def partitions(spark: SparkSession): Int = {
    val n = TextDataFrameIO.property("hibench.default.shuffle.parallelism",
      spark.conf.get("spark.sql.shuffle.partitions")).toInt
    require(n > 0, "Shuffle partitions must be positive")
    n
  }
  def readTera(spark: SparkSession, path: String): DataFrame = {
    val data = spark.read.parquet(path)
    require(data.schema.fieldNames.toSet == Set("key", "value") &&
      data.schema("key").dataType == BinaryType && data.schema("value").dataType == BinaryType,
      "Expected Tera Parquet schema: key binary(10 bytes), value binary(90 bytes); regenerate legacy raw input")
    data.select(
      when(col("key").isNotNull && length(col("key")) === 10, col("key"))
        .otherwise(raise_error(lit("Invalid Tera key: expected 10 bytes"))).as("key"),
      when(col("value").isNotNull && length(col("value")) === 90, col("value"))
        .otherwise(raise_error(lit("Invalid Tera value: expected 90 bytes"))).as("value"))
  }
  def consumeOrWrite(data: DataFrame, path: String, disableOutput: Boolean): Unit = {
    if (disableOutput) data.write.format("noop").mode("overwrite").save()
    else data.write.mode("overwrite").option("compression", "uncompressed").parquet(path)
  }
  def repartition(data: DataFrame, path: String, cached: Boolean, disableOutput: Boolean): Unit = {
    val input = if (cached) data.cache() else data
    try {
      if (cached) input.count()
      consumeOrWrite(input.repartition(partitions(data.sparkSession)), path, disableOutput)
    } finally { if (cached) input.unpersist() }
  }
}
