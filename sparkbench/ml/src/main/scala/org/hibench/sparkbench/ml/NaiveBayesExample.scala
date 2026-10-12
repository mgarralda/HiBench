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

/**
 * Modified from Spark Naive Bayes example
 */
package org.hibench.sparkbench.ml

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object NaiveBayesExample {

  case class Params(
      input: String = null,
      lambda: Double = 1.0, trainFraction: Double = 0.8, splitSeed: Long = 1234L)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("NaiveBayesExample") {
      head("NaiveBayesExample: an example Naive Bayes app for parquet dataset.")
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Double]("trainFraction").action((x,c) => c.copy(trainFraction=x))
      opt[Long]("splitSeed").action((x,c) => c.copy(splitSeed=x))
      arg[String]("<input>")
        .text("input paths to labeled examples in parquet format")
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
    val spark = SparkSession
      .builder
      .appName(s"NaiveBayesExample with $params")
      .getOrCreate()

    try {
    require(params.lambda >= 0 && !params.lambda.isNaN && !params.lambda.isInfinity, "Invalid smoothing")
    require(params.trainFraction > 0 && params.trainFraction < 1, "Invalid training fraction")
    val df = spark.read.parquet(params.input).select("label","features")

    // Split the data into training and test sets (20% held out for testing)
    val Array(trainingData, testData) = df.randomSplit(Array(params.trainFraction, 1-params.trainFraction), seed = params.splitSeed)

    // Train a NaiveBayes model.
    val ntrain=trainingData.count();val ntest=testData.count()
    require(ntrain>0 && ntest>0,"Bayes split must be nonempty")
    val model = new NaiveBayes().setModelType("multinomial").setSmoothing(params.lambda).fit(trainingData)

    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show()

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    require(!accuracy.isNaN && !accuracy.isInfinity,"Accuracy must be finite")
    println(s"Bayes numTraining=$ntrain numTest=$ntest smoothing=${params.lambda} splitSeed=${params.splitSeed}")
    println(s"Test set accuracy = $accuracy")

    } finally spark.stop()
  }
}
