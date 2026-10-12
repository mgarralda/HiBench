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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Dense binary Gaussian classification, preserving the historical row-seeded profile. */
object BinaryGaussianDataGenerator {
  case class Example(id: Long, label: Double, features: Vector)
  case class Profile(seed: Long, examples: Long, dimensions: Int, partitions: Int,
                     classShift: Double, zeroLabels: Long, oneLabels: Long)

  def dataset(spark: SparkSession, count: Long, dimensions: Int, partitions: Int,
              seed: Long, shift: Double): DataFrame = {
    require(count > 0 && dimensions > 0 && partitions > 0, "Counts must be positive")
    require(seed >= 0 && BigInt(seed) + count - 1 <= Long.MaxValue, "Row seed exceeds Long range")
    require(shift >= 0 && !shift.isNaN && !shift.isInfinity, "Class shift must be finite and nonnegative")
    import spark.implicits._
    spark.range(0, count, 1, partitions).as[Long].map { id =>
      val rng = new java.util.Random(seed + id)
      val label = (id % 2).toDouble
      val features = Array.fill(dimensions)(rng.nextGaussian() + label * shift)
      Example(id, label, Vectors.dense(features))
    }.toDF()
  }

  def generate(args: Array[String], appName: String): Unit = {
    require(args.length == 6, "Usage: BinaryGaussianDataGenerator <OUTPUT> <EXAMPLES> <FEATURES> <PARTITIONS> <SEED> <CLASS_SHIFT>")
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    try {
      val count = args(1).toLong
      val dimensions = args(2).toInt
      val partitions = args(3).toInt
      val seed = args(4).toLong
      val shift = args(5).toDouble
      dataset(spark, count, dimensions, partitions, seed, shift)
        .write.mode("errorifexists").parquet(args(0))
      import spark.implicits._
      Seq(Profile(seed, count, dimensions, partitions, shift, count / 2 + count % 2, count / 2))
        .toDS().write.mode("errorifexists").parquet(args(0) + "/_generator_profile")
    } finally spark.stop()
  }
}
