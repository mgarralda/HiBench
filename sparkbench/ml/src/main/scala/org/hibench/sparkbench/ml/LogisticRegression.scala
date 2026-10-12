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

import org.apache.spark.ml.classification.{LogisticRegression => SparkLogisticRegression}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Binary classification v2: modern estimator and explicit historical optimizer defaults. */
object LogisticRegression {
  def main(args: Array[String]): Unit = {
    require(args.length == 9, "Usage: LogisticRegression <INPUT> <MAX_ITER> <TOL> <REG_PARAM> <FIT_INTERCEPT> <STANDARDIZATION> <TRAIN_FRACTION> <SPLIT_SEED> <THRESHOLD>")
    val maxIter = args(1).toInt
    val tol = args(2).toDouble
    val regParam = args(3).toDouble
    val trainFraction = args(6).toDouble
    require(maxIter > 0 && tol > 0 && regParam >= 0, "Invalid optimizer settings")
    require(trainFraction > 0 && trainFraction < 1, "Train fraction must be between zero and one")
    val spark = SparkSession.builder().appName("HiBench Spark ML binary logistic v2").getOrCreate()
    var training: DataFrame = null
    var test: DataFrame = null
    try {
      val input = spark.read.parquet(args(0)).select("label", "features")
      val splits = input.randomSplit(Array(trainFraction, 1.0 - trainFraction), args(7).toLong)
      training = splits(0).cache()
      test = splits(1).cache()
      val counts = training.groupBy("label").count().collect() // only the bounded class-count summary
      require(counts.length == 2 && counts.forall(r => r.getDouble(0) == 0.0 || r.getDouble(0) == 1.0),
        "Training must contain both binary labels 0 and 1")
      val trainingCount = counts.map(_.getLong(1)).sum
      val testCount = test.count()
      require(testCount > 0, "Logistic evaluation split is empty")
      val estimator = new SparkLogisticRegression()
        .setFamily("binomial").setMaxIter(maxIter).setTol(tol)
        .setRegParam(regParam).setElasticNetParam(0.0)
        .setFitIntercept(args(4).toBoolean).setStandardization(args(5).toBoolean)
        .setThreshold(args(8).toDouble)
      val model = estimator.fit(training)
      val predictions = model.transform(test)
      val evaluation = predictions.agg(count(lit(1)).as("n"),
        sum(when(col("prediction") === col("label"), 1L).otherwise(0L)).as("correct")).first()
      val accuracy = evaluation.getLong(1).toDouble / evaluation.getLong(0)
      require(!accuracy.isNaN && !accuracy.isInfinity, "Non-finite held-out accuracy")
      println(s"Loaded data: numTraining = $trainingCount, numTest = $testCount, numFeatures = ${model.numFeatures}")
      println(s"numClasses = ${model.numClasses}; numIterations = ${model.summary.totalIterations}")
      println(s"Accuracy = $accuracy")
    } finally {
      if (training != null) training.unpersist()
      if (test != null) test.unpersist()
      spark.stop()
    }
  }
}
