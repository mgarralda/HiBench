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

package org.hibench.sparkbench.sql

import java.util.Random
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.hibench.sparkbench.sql.datagen._

case class SqlPage(pageId: Long, pageURL: String, links: Seq[Long])
case class RankInput(pageId: Long, pageURL: String, pageRank: Long, legacyPartition: Int)
case class Ranking(pageId: Long, pageURL: String, pageRank: Int, avgDuration: Int)
case class VisitInput(pageId: Long, pageURL: String, count: Long, legacyPartition: Int)
case class UserVisit(pageId: Long, sourceIP: String, destURL: String, visitDate: java.sql.Date,
  adRevenue: Double, userAgent: String, countryCode: String, languageCode: String,
  searchWord: String, duration: Int)

object SqlDataGenerator {
  def legacyPartition(id: org.apache.spark.sql.Column, reducers: Int): org.apache.spark.sql.Column =
    id.bitwiseXOR(shiftrightunsigned(id, 32)).cast("int").bitwiseAND(lit(Int.MaxValue)) % lit(reducers)

  def pages(spark: SparkSession, count: Long, maps: Int): DataFrame = {
    require(count >= 2 && count <= Long.MaxValue / 40 && maps > 0, "Invalid SQL page count/partitions")
    val zipf = new SqlZipfian(count, 0.5)
    zipf.setupZipf(count * 40, 0.1)
    val kernel = zipf.createSqlZipfCore()
    val slotSize = (count - 1) / maps + 1
    import spark.implicits._
    spark.range(1L, maps.toLong + 1L, 1L, maps).as[Long].flatMap { slot =>
      val generator = new SqlPageGenerator(slot.toInt, kernel)
      val start = slotSize * (slot - 1)
      val end = math.min(count, start + slotSize)
      new Iterator[SqlPage] {
        private var id = start
        def hasNext: Boolean = id < end
        def next(): SqlPage = {
          if (!hasNext) throw new NoSuchElementException("End of SQL page slot")
          val url = generator.nextUrl(); val links = generator.nextLinks().toSeq
          val record = SqlPage(id, url, links); id += 1; record
        }
      }
    }.toDF()
  }
  def rankings(spark: SparkSession, count: Long, maps: Int, reducers: Int): DataFrame = {
    require(reducers > 0, "Reducers must be positive")
    import spark.implicits._
    val data = pages(spark, count, maps)
    val references = data.select(explode(col("links")).as("pageId")).groupBy("pageId")
      .agg(countRows().as("pageRank"))
    data.select("pageId", "pageURL").join(references, Seq("pageId"), "inner")
      .withColumn("legacyPartition", legacyPartition(col("pageId"), reducers))
      .repartition(reducers, col("legacyPartition")).sortWithinPartitions("legacyPartition", "pageId")
      .as[RankInput].mapPartitions { rows =>
        var partition = -1; var random: Random = null
        rows.map { row =>
          if (row.legacyPartition != partition) { partition = row.legacyPartition; random = new Random(partition + 1) }
          require(row.pageRank <= Int.MaxValue, "Page rank exceeds the legacy INT schema")
          Ranking(row.pageId, row.pageURL, row.pageRank.toInt, random.nextInt(99) + 1)
        }
      }.toDF()
  }
  private def countRows(): org.apache.spark.sql.Column = org.apache.spark.sql.functions.count(lit(1))

  def uservisits(spark: SparkSession, ranking: DataFrame, pages: Long, visits: Long, maps: Int, reducers: Int): DataFrame = {
    require(visits > 0 && visits < Long.MaxValue - maps, "Invalid SQL visit count")
    import spark.implicits._
    val candidates = spark.range(1L, maps.toLong + 1L, 1L, maps).as[Long].flatMap { slot =>
      val random = new Random(slot)
      new Iterator[Long] {
        private var index = slot
        def hasNext: Boolean = index <= visits
        def next(): Long = {
          if (!hasNext) throw new NoSuchElementException("End of visit slot")
          index += maps
          math.floor(random.nextDouble() * pages).toLong
        }
      }
    }.toDF("pageId").groupBy("pageId").agg(countRows().as("count"))
    candidates.join(ranking.select("pageId", "pageURL"), Seq("pageId"), "inner")
      .withColumn("legacyPartition", legacyPartition(col("pageId"), reducers))
      .repartition(reducers, col("legacyPartition")).sortWithinPartitions("legacyPartition", "pageId")
      .as[VisitInput].mapPartitions { rows =>
        var partition = -1; var generator: SqlVisitGenerator = null
        rows.flatMap { row =>
          if (row.legacyPartition != partition) {
            partition = row.legacyPartition; generator = new SqlVisitGenerator(partition + 1, pages)
          }
          new Iterator[UserVisit] {
            private var remaining = row.count
            def hasNext: Boolean = remaining > 0
            def next(): UserVisit = {
              if (!hasNext) throw new NoSuchElementException("End of visit group")
              remaining -= 1
              val fields = generator.nextAccess(row.pageURL).split(",", -1)
              require(fields.length == 9, "Invalid original SQL visit vocabulary")
              UserVisit(row.pageId, fields(0), fields(1), java.sql.Date.valueOf(fields(2)), fields(3).toDouble,
                fields(4), fields(5), fields(6), fields(7), fields(8).toInt)
            }
          }
        }
      }.toDF()
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 5, "Usage: SqlDataGenerator <output> <pages> <visits> <generationPartitions> <reducers>")
    val spark = SparkSession.builder.appName("SqlDataGenerator").config("spark.sql.catalogImplementation", "in-memory").getOrCreate()
    try {
      val pageCount = args(1).toLong; val visitCount = args(2).toLong
      val maps = args(3).toInt; val reducers = args(4).toInt
      val root = args(0).stripSuffix("/")
      rankings(spark, pageCount, maps, reducers).write.mode("overwrite")
        .option("compression", "uncompressed").parquet(root + "/rankings")
      val ranks = spark.read.parquet(root + "/rankings")
      uservisits(spark, ranks, pageCount, visitCount, maps, reducers).write.mode("overwrite")
        .option("compression", "uncompressed").parquet(root + "/uservisits")
    } finally spark.stop()
  }
}
