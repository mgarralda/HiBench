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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: ScalaWordCount <TEXT_INPUT> <OUTPUT>")
    val spark = SparkSession.builder().appName("ScalaWordCount").getOrCreate()
    try {
      val result = TextDataFrameIO.read(spark, args(0))
        .select(explode(split(col("value"), "\\s+")).as("word"))
        .filter(length(col("word")) > 0).groupBy("word").count()
      TextDataFrameIO.write(result, args(1))
    } finally spark.stop()
  }
}
