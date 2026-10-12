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

import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


object SummarizerExample {

  case class Params( input: String = null )

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Summarizer") {
      head("Summarizer: an example app for computing Summarizer.")
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"SummarizerExample with $params")
      .getOrCreate()

    try {
      val training = spark.read.parquet(params.input).select("features")
      val summary = training.select(Summarizer.metrics("count", "mean", "variance", "max", "min",
        "numNonZeros", "normL1", "normL2").summary(col("features")).as("summary"))
        .select("summary.*").head()
      require(summary.getAs[Long]("count") > 0, "Summarizer input must be nonempty")
      println(s"Summarizer count=${summary.getAs[Long]("count")}")
      Seq("mean", "variance", "max", "min").foreach { metric =>
        println(s"$metric=${summary.getAs[org.apache.spark.ml.linalg.Vector](metric)}")
      }
    } finally spark.stop()
  }
}
