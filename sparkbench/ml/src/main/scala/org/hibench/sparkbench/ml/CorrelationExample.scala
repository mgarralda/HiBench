/*
Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.hibench.sparkbench.ml

import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser

object CorrelationExample {

  case class Params(
                     input: String = null,
                     corrType: String = "pearson")

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Correlation") {
      head("Correlation: an example app for computing correlations.")
      opt[String]("corrType")
        .text(s"String specifying the method to use for computing correlation. " +
          s"Supported: `pearson` (default), `spearman`, default: ${defaultParams.corrType}")
        .action((x, c) => c.copy(corrType = x))
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
      .appName(s"Correlations with $params")
      .getOrCreate()

    require(Set("pearson", "spearman").contains(params.corrType), "Unsupported correlation method")
    val training = spark.read.parquet(params.input).select("features").cache()
    try {
    val numTraining = training.count()

    require(numTraining > 1, "Correlation requires at least two rows")
    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println(s"  numTraining = $numTraining")
    println(s"  numFeatures = $numFeatures")

    println(s"Correlation ${params.corrType} between features")
    val df = training
    val Row(coeff1: Matrix) = Correlation.corr(df, "features", params.corrType).head()
    println(s"${params.corrType} correlation matrix:\n $coeff1.")

    } finally { training.unpersist(); spark.stop() }
  }
}
