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

import org.apache.spark.sql.SparkSession

/** Original normal(0,1) shape; deterministic Dataset partition streams replace clock-seeded RandomRDDs. */
object SVDDataGenerator {
  case class Profile(seed: Long, examples: Long, dimensions: Int, partitions: Int,
                     distribution: String, algorithm: String)
  def main(args: Array[String]): Unit = {
    require(args.length==5,"Usage: SVDDataGenerator OUTPUT EXAMPLES FEATURES PARTITIONS SEED")
    val spark=SparkSession.builder().appName("SVDDataGenerator").getOrCreate()
    try {
      val n=args(1).toLong;val d=args(2).toInt;val p=args(3).toInt;val seed=args(4).toLong
      SummarizerDataGenerator.dataset(spark,n,d,p,seed).write.mode("errorifexists").parquet(args(0))
      import spark.implicits._
      Seq(Profile(seed,n,d,p,"normal(0,1)","java-gaussian-partition-v1"))
        .toDS().write.parquet(args(0)+"/_generator_profile")
    } finally spark.stop()
  }
}
