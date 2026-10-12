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

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object RandomForestClassification {
  case class Params(
    inputPath: String = null,
    numTrees: Int = 3,
    numClasses: Int = 2,
    featureSubsetStrategy: String = "auto",
    impurity: String = "gini",
    maxDepth: Int = 4,
    maxBins: Int = 32,
    modelSeed: Long = 1L,
    splitSeed: Long = 11L,
    trainFraction: Double = 0.7)
	
  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("RF") {
      head("RF: an example app.")
      opt[Int]("numTrees")
        .text(s"numTrees, default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(numTrees = x))
      opt[Int]("numClasses")
        .text(s"numClasses, default: ${defaultParams.numClasses}")
        .action((x, c) => c.copy(numClasses = x))
      opt[Int]("maxDepth")
        .text(s"maxDepth, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"maxBins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[String]("featureSubsetStrategy")
        .text(s"featureSubsetStrategy, default: ${defaultParams.featureSubsetStrategy}")
        .action((x, c) => c.copy(featureSubsetStrategy = x))
      opt[String]("impurity")
        .text(s"impurity (smoothing constant), default: ${defaultParams.impurity}")
        .action((x, c) => c.copy(impurity = x))
      opt[Long]("modelSeed").action((x, c) => c.copy(modelSeed = x))
      opt[Long]("splitSeed").action((x, c) => c.copy(splitSeed = x))
      opt[Double]("trainFraction").action((x, c) => c.copy(trainFraction = x))
      arg[String]("<inputPath>")
        .required()
        .text("Input path of dataset")
        .action((x, c) => c.copy(inputPath = x))	
    }  
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }
  
  def run(params: Params): Unit = {
    require(params.numClasses >= 2 && params.numTrees > 0, "Invalid RF class/tree count")
    require(params.maxDepth >= 0 && params.maxDepth <= 30 && params.maxBins >= 2, "Invalid tree depth or bin count")
    val spark = SparkSession.builder().appName("HiBench RandomForestClassification Spark ML v1").getOrCreate()
    try {
      val (training, test, trainingCount) = TreeBenchmarkData.split(
        spark, params.inputPath, params.numClasses, params.trainFraction, params.splitSeed)
      val estimator = new RandomForestClassifier()
        .setNumTrees(params.numTrees).setFeatureSubsetStrategy(params.featureSubsetStrategy)
        .setImpurity(params.impurity).setMaxDepth(params.maxDepth).setMaxBins(params.maxBins)
        .setSeed(params.modelSeed).setSubsamplingRate(1.0).setBootstrap(params.numTrees > 1)
      val model = estimator.fit(training)
      val (testCount, error) = TreeBenchmarkData.evaluate(model.transform(test))
      println(s"Loaded data: numTraining = $trainingCount, numTest = $testCount, numFeatures = ${model.numFeatures}")
      println(s"numClasses = ${model.numClasses}; numTrees = ${model.trees.length}; totalNumNodes = ${model.totalNumNodes}")
      println(s"Model seed = ${params.modelSeed}; split seed = ${params.splitSeed}")
      println(s"Test Error = $error")
    } finally spark.stop()
  }
}
