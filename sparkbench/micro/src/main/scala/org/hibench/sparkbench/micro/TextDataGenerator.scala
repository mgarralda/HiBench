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

import java.util.Random
import org.apache.spark.sql.{DataFrame, SparkSession}

/** v2: original RandomTextWriter vocabulary, distributions and byte accounting. */
object TextDataGenerator {
  def dataset(spark: SparkSession, bytes: Long, partitions: Int, seed: Long): DataFrame = {
    require(bytes >= 2, "Text dataset size must be at least 2 bytes")
    require(partitions > 0 && bytes / partitions > 0, "Bytes per generation partition must be positive")
    val budget = bytes / partitions
    val maps = bytes / budget // Matches RandomTextWriter's floor(totalbytes / bytespermap).
    require(maps <= Int.MaxValue, "Too many generation partitions")
    import spark.implicits._
    spark.range(0, maps, 1, maps.toInt).as[Long].flatMap { mapId =>
      new Iterator[String] {
        private val random = new Random(seed + mapId)
        private var remaining = budget
        private def sentence(count: Int): String = {
          val text = new StringBuilder()
          for (_ <- 0 until count) text.append(OriginalTextVocabulary.words(random.nextInt(OriginalTextVocabulary.words.length))).append(' ')
          text.toString
        }
        def hasNext: Boolean = remaining > 0
        def next(): String = {
          if (!hasNext) throw new NoSuchElementException("Dataset partition exhausted")
          val keyCount = 5 + random.nextInt(5)
          val valueCount = 10 + random.nextInt(90)
          val key = sentence(keyCount)
          val value = sentence(valueCount)
          remaining -= key.length + value.length // Original excludes separator and LF.
          key + "\t" + value
        }
      }
    }.toDF("value")
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Usage: TextDataGenerator <OUTPUT> <LOGICAL_BYTES> <PARTITIONS> <SEED>")
    val spark = SparkSession.builder().appName("HiBench RandomTextWriter-compatible generator v2").getOrCreate()
    try dataset(spark, args(1).toLong, args(2).toInt, args(3).toLong)
      .write.mode("overwrite").option("compression", "none").option("lineSep", "\n").text(args(0))
    finally spark.stop()
  }
}
