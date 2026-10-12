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

import org.apache.spark.ml.feature.PCA
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object PCAExample {

  case class Params(
    k: Int = 2,
    maxResultSize: String = "1g",
    dataPath: String = null
  )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("PCA") {
      head("PCA: an example of principal component analysis.")
      opt[Int]("k")
        .text(s"k, default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[String]("maxResultSize")
        .text(s"maxResultSize, default: ${defaultParams.maxResultSize}")
        .action((x, c) => c.copy(maxResultSize = x))
      arg[String]("<dataPath>")
        .required()
        .text("data path of PCA")
        .action((x, c) => c.copy(dataPath = x))
    }
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder
      .appName("PCAExample")
      .config("spark.driver.maxResultSize", params.maxResultSize)
      .getOrCreate()

    try {
    val df = spark.read.parquet(params.dataPath).select("features")
    require(params.k > 0, "PCA k must be positive")
    val dimensions = df.head().getAs[org.apache.spark.ml.linalg.Vector](0).size
    require(params.k <= dimensions, "PCA k must not exceed the feature dimension")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(params.k)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")

    // Fit was the historical measured action; transform remains lazy for comparability.
    println(s"PCA dimensions=$dimensions k=${params.k} explainedVariance=${pca.explainedVariance}")
    } finally spark.stop()
  }
}
