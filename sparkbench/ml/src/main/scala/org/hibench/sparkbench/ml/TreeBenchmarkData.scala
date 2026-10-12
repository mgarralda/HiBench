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

package org.hibench.sparkbench.ml

import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Continuous features and explicit class metadata, without an RDD import boundary. */
object TreeBenchmarkData {
  def split(spark: SparkSession, path: String, classes: Int,
            fraction: Double, seed: Long): (DataFrame, DataFrame, Long) = {
    require(classes >= 2 && fraction > 0 && fraction < 1, "Invalid classes or train fraction")
    val metadata = NominalAttribute.defaultAttr.withName("label").withNumValues(classes).toMetadata()
    val input = spark.read.parquet(path).select(col("label").as("label", metadata), col("features"))
    val parts = input.randomSplit(Array(fraction, 1.0 - fraction), seed)
    val summary = parts(0).agg(count(lit(1)).as("n"),
      sum(when(col("label").isNull || isnan(col("label")) || col("label") < 0 ||
        col("label") >= classes || col("label") =!= floor(col("label")), 1L).otherwise(0L)).as("invalid")).first()
    val n = summary.getLong(0)
    require(n > 0, "Tree training split is empty")
    require(summary.getLong(1) == 0, "Tree labels must be integer class IDs in the configured range")
    (parts(0), parts(1), n)
  }

  def evaluate(predictions: DataFrame): (Long, Double) = {
    val result = predictions.agg(count(lit(1)).as("n"),
      sum(when(col("label") =!= col("prediction"), 1L).otherwise(0L)).as("errors")).first()
    val n = result.getLong(0)
    require(n > 0, "Tree evaluation split is empty")
    val error = result.getLong(1).toDouble / n
    require(!error.isNaN && !error.isInfinity, "Non-finite tree test error")
    (n, error)
  }
}
