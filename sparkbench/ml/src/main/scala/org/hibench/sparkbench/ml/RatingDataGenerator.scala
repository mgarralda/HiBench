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

import org.hibench.sparkbench.common.IOCommon
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Parquet ratings v1. Preserves the original partition-seeded sampling profile. */
object RatingDataGenerator {
  case class RatingRow(user: Int, item: Int, rating: Float)

  def dataset(spark: SparkSession, users: Int, items: Int, count: Long,
              partitions: Int): DataFrame = dataset(spark, users, items, count, partitions, false)

  def dataset(spark: SparkSession, users: Int, items: Int, count: Long,
              partitions: Int, implicitPrefs: Boolean): DataFrame = {
    require(users > 0 && items > 0 && count > 0 && partitions > 0,
      "Users, items, ratings and partitions must be positive")
    require(count <= users.toLong * items.toLong, "Ratings must not exceed users * items")
    import spark.implicits._
    // One logical task per original partition; Dataset flatMap does not expose RDD APIs.
    spark.range(0, partitions.toLong, 1, partitions).as[Long].flatMap { partition =>
      val rng = new java.util.Random(partition)
      val start = BigInt(count) * partition / partitions
      val end = BigInt(count) * (partition + 1) / partitions
      new Iterator[RatingRow] {
        private var remaining = (end - start).toLong
        def hasNext: Boolean = remaining > 0
        def next(): RatingRow = {
          if (!hasNext) throw new NoSuchElementException("Rating partition exhausted")
          remaining -= 1
          val user = rng.nextInt(users)
          val item = rng.nextInt(items)
          val raw = (rng.nextInt(5) + 1).toFloat
          RatingRow(user, item, if (implicitPrefs) raw - 2.5f else raw)
        }
      }
    }.toDF()
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 5,
      "Usage: RatingDataGenerator <OUTPUT> <USERS> <ITEMS> <RATINGS> <IMPLICIT_PREFS>")
    val spark = SparkSession.builder().appName("HiBench ALS ratings Parquet v1").getOrCreate()
    try {
      val partitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
        .getOrElse(spark.conf.get("spark.sql.shuffle.partitions", "200")).toInt
      dataset(spark, args(1).toInt, args(2).toInt, args(3).toLong, partitions, args(4).toBoolean)
        .write.mode("errorifexists").parquet(args(0))
    } finally spark.stop()
  }
}
