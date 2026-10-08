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

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

object DenseKMeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import org.hibench.sparkbench.ml.DenseKMeans.InitializationMode._

  case class Params(
      input: String = null,
      k: Int = -1,
      numIterations: Int = 10,
      storageLevel: String= "MEMORY_ONLY",
      initializationMode: InitializationMode = Random)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default; ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
          s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      opt[String]("storageLevel")
        .text(s"storage level, default: ${defaultParams.storageLevel}")
        .action((x, c) => c.copy(storageLevel = x))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    require(params.k > 0 && params.numIterations > 0, "Positive k and iteration count required")
    val spark = SparkSession
      .builder
      .appName(s"DenseKMeans with $params")
      .getOrCreate()

    try {
      val cacheStart = System.currentTimeMillis()

      val storageLevel = StorageLevel.fromString(params.storageLevel)
      val examples = spark.read.parquet(params.input).select("features").persist(storageLevel)
      require(examples.schema("features").dataType == org.apache.spark.ml.linalg.SQLDataTypes.VectorType,
        "Expected Gaussian Parquet v1 with Spark ML vector features; regenerate legacy SequenceFile inputs.")

      val numExamples = examples.count()

      println(s"Loading data time (ms) = ${System.currentTimeMillis() - cacheStart}")
      println(s"numExamples = $numExamples.")
      require(numExamples >= params.k, "Dataset must contain at least k samples")
      val trainingStart = System.currentTimeMillis()

      val initMode = params.initializationMode match {
        case Random => "random"
        case Parallel => "k-means||"
      }

      val model = new KMeans()
        .setInitMode(initMode)
        .setK(params.k)
        .setMaxIter(params.numIterations)
        .setSeed(1L)
        .setTol(0)       //set convergence to 0, aiming to execute the number of iterations of the algorithm without being affected by the convergence parameters.
        .fit(examples)

      val cost = model.summary.trainingCost
      require(!cost.isNaN && !cost.isInfinity, "Non-finite KMeans training cost")

      println(s"Training time (ms) = ${System.currentTimeMillis() - trainingStart}")
      println(s"Total cost = $cost.")

      examples.unpersist()
    } finally spark.stop()
  }
}
