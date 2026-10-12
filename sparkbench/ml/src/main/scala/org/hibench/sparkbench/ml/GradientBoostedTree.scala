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

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object GradientBoostedTree {

  case class Params(
    numClasses: Int = 2,
    maxDepth: Int = 30,
    maxBins: Int = 32,
    numIterations: Int = 20,
    learningRate: Double = 0.1,
    dataPath: String = null,
    modelSeed: Long = 0L,
    splitSeed: Long = 11L,
    trainFraction: Double = 0.7
  )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("GBT"){
      head("GBT: an example of Gradient Boosted Tree for classification")
      opt[Int]("numClasses")
        .text(s"numClasses, default: ${defaultParams.numClasses}")
        .action((x,c) => c.copy(numClasses = x))
      opt[Int]("maxDepth")
        .text(s"maxDepth, default: ${defaultParams.maxDepth}")
        .action((x,c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"maxBins, default: ${defaultParams.maxBins}")
        .action((x,c) => c.copy(maxBins = x))
      opt[Int]("numIterations")
        .text(s"numIterations, default: ${defaultParams.numIterations}")
        .action((x,c) => c.copy(numIterations = x))
      opt[Double]("learningRate")
        .text(s"learningRate, default: ${defaultParams.learningRate}")
        .action((x,c) => c.copy(learningRate = x))
      opt[Long]("modelSeed").action((x, c) => c.copy(modelSeed = x))
      opt[Long]("splitSeed").action((x, c) => c.copy(splitSeed = x))
      opt[Double]("trainFraction").action((x, c) => c.copy(trainFraction = x))
      arg[String]("<dataPath>")
        .required()
        .text("data path for Gradient Boosted Tree")
        .action((x,c) => c.copy(dataPath = x))
    }
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    require(params.numClasses == 2 && params.numIterations > 0, "GBT classification requires exactly two classes and positive iterations")
    require(params.learningRate > 0 && params.learningRate <= 1, "GBT learning rate must be in (0, 1]")
    require(params.maxDepth >= 0 && params.maxDepth <= 30 && params.maxBins >= 2, "Invalid tree depth or bin count")
    val spark = SparkSession.builder().appName("HiBench GradientBoostedTree Spark ML v1").getOrCreate()
    try {
      val (training, test, trainingCount) = TreeBenchmarkData.split(
        spark, params.dataPath, params.numClasses, params.trainFraction, params.splitSeed)
      val estimator = new GBTClassifier()
        .setMaxIter(params.numIterations).setStepSize(params.learningRate)
        .setMaxDepth(params.maxDepth).setMaxBins(params.maxBins)
        .setLossType("logistic").setFeatureSubsetStrategy("all")
        .setSeed(params.modelSeed).setSubsamplingRate(1.0)
      val model = estimator.fit(training)
      val (testCount, error) = TreeBenchmarkData.evaluate(model.transform(test))
      println(s"Loaded data: numTraining = $trainingCount, numTest = $testCount, numFeatures = ${model.numFeatures}")
      println(s"numClasses = ${model.numClasses}; numTrees = ${model.trees.length}; totalNumNodes = ${model.totalNumNodes}")
      println(s"Model seed = ${params.modelSeed}; split seed = ${params.splitSeed}")
      println(s"Test Error = $error")
    } finally spark.stop()
  }
}
