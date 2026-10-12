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

/** Dense N(0,1) vectors. Fixed partition streams replace the historical clock seed. */
object SummarizerDataGenerator {
  case class Plan(partition: Int, start: Long, end: Long)
  case class Example(id: Long, features: Vector)
  case class Profile(seed: Long, examples: Long, dimensions: Int, partitions: Int,
                     distribution: String, algorithm: String)
  def dataset(spark: SparkSession, count: Long, dimensions: Int, partitions: Int, seed: Long): DataFrame = {
    require(count > 0 && dimensions > 0 && partitions > 0, "Counts must be positive")
    import spark.implicits._
    spark.range(0, partitions, 1, partitions).as[Long].flatMap { partition =>
      val start = (BigInt(count) * partition / partitions).toLong
      val end = (BigInt(count) * (partition + 1) / partitions).toLong
      val rng = new java.util.Random(seed ^ partition)
      new Iterator[Example] {
        private var id = start
        def hasNext: Boolean = id < end
        def next(): Example = {
          if (!hasNext) throw new NoSuchElementException("End of partition")
          val row = Example(id, Vectors.dense(Array.fill(dimensions)(rng.nextGaussian())))
          id += 1
          row
        }
      }
    }.toDF()
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 5, "Usage: SummarizerDataGenerator <OUTPUT> <EXAMPLES> <FEATURES> <PARTITIONS> <SEED>")
    val spark = SparkSession.builder().appName("SummarizerDataGenerator").getOrCreate()
    try {
      val n=args(1).toLong; val d=args(2).toInt; val p=args(3).toInt; val seed=args(4).toLong
      dataset(spark,n,d,p,seed).write.mode("errorifexists").parquet(args(0))
      import spark.implicits._
      Seq(Profile(seed,n,d,p,"normal(0,1)","java-gaussian-partition-v1"))
        .toDS().write.parquet(args(0)+"/_generator_profile")
    } finally spark.stop()
  }
}
