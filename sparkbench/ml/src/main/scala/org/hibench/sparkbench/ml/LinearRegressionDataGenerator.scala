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

/** Preserves the original partition-seeded dense linear model, in Parquet. */
object LinearRegressionDataGenerator {
  case class Example(id: Long, label: Double, features: Vector)
  case class Profile(seed: Long, examples: Long, dimensions: Int, partitions: Int,
                     noiseStd: Double, weights: Vector)

  def weights(dimensions: Int, seed: Long): Array[Double] = {
    val rng = new java.util.Random(seed)
    Array.fill(dimensions)(rng.nextDouble() - 0.5)
  }

  def dataset(spark: SparkSession, count: Long, dimensions: Int, partitions: Int,
              seed: Long, eps: Double): DataFrame = {
    require(count > 0 && dimensions > 0 && partitions > 0, "Counts must be positive")
    require(eps >= 0 && !eps.isNaN && !eps.isInfinity, "Noise standard deviation must be finite and nonnegative")
    val coefficients = weights(dimensions, seed)
    import spark.implicits._
    spark.range(0, partitions.toLong, 1, partitions).as[Long].flatMap { partition =>
      // Same floor boundaries and RNG stream as parallelize(0 until n, p).
      val start = (BigInt(count) * partition / partitions).toLong
      val end = (BigInt(count) * (partition + 1) / partitions).toLong
      val rng = new java.util.Random(seed ^ partition)
      new Iterator[Example] {
        private var id = start
        def hasNext: Boolean = id < end
        def next(): Example = {
          if (!hasNext) throw new NoSuchElementException("Linear partition exhausted")
          val values = Array.fill(dimensions)((rng.nextDouble() - 0.5) * 2.0)
          // Preserve feature traversal and floating-point summation order.
          val label = coefficients.indices.iterator.map(i => coefficients(i) * values(i)).sum + eps * rng.nextGaussian()
          val row = Example(id, label, Vectors.dense(values))
          id += 1
          row
        }
      }
    }.toDF()
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 6, "Usage: LinearRegressionDataGenerator <OUTPUT> <EXAMPLES> <FEATURES> <PARTITIONS> <SEED> <NOISE_STD>")
    val spark = SparkSession.builder().appName("HiBench linear Parquet v1").getOrCreate()
    try {
      val count = args(1).toLong
      val dimensions = args(2).toInt
      val partitions = args(3).toInt
      val seed = args(4).toLong
      val eps = args(5).toDouble
      dataset(spark, count, dimensions, partitions, seed, eps)
        .write.mode("errorifexists").parquet(args(0))
      import spark.implicits._
      Seq(Profile(seed, count, dimensions, partitions, eps, Vectors.dense(weights(dimensions, seed))))
        .toDS().write.mode("errorifexists").parquet(args(0) + "/_generator_profile")
    } finally spark.stop()
  }
}
