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

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

/** Linear hinge-loss classification with the current DataFrame estimator. */
object LinearSVMExample {
  case class Params(numIterations: Int=100,regParam: Double=0.01,tolerance: Double=1e-6,
    dataPath: String=null,storageLevel: String="MEMORY_ONLY",trainFraction: Double=0.6,
    splitSeed: Long=11L,fitIntercept: Boolean=false,standardization: Boolean=false)
  def main(args: Array[String]): Unit = {
    val parser=new OptionParser[Params]("LinearSVM") {
      opt[Int]("numIterations").action((x,c)=>c.copy(numIterations=x))
      opt[Double]("regParam").action((x,c)=>c.copy(regParam=x))
      opt[Double]("tolerance").action((x,c)=>c.copy(tolerance=x))
      opt[String]("storageLevel").action((x,c)=>c.copy(storageLevel=x))
      opt[Double]("trainFraction").action((x,c)=>c.copy(trainFraction=x))
      opt[Long]("splitSeed").action((x,c)=>c.copy(splitSeed=x))
      opt[Boolean]("fitIntercept").action((x,c)=>c.copy(fitIntercept=x))
      opt[Boolean]("standardization").action((x,c)=>c.copy(standardization=x))
      arg[String]("input").required().action((x,c)=>c.copy(dataPath=x))
    }
    parser.parse(args,Params()) match {case Some(p)=>run(p);case _=>sys.exit(1)}
  }
  def run(p: Params): Unit = {
    val spark=SparkSession.builder().appName("LinearSVMExample").getOrCreate()
    var training: org.apache.spark.sql.DataFrame=null
    try {
      require(p.numIterations>0 && p.regParam>=0 && !p.regParam.isNaN && !p.regParam.isInfinity,"Invalid SVM optimizer settings")
      require(p.tolerance>0 && !p.tolerance.isNaN && !p.tolerance.isInfinity,"Invalid convergence tolerance")
      require(p.trainFraction>0 && p.trainFraction<1,"Invalid training fraction")
      val splits=spark.read.parquet(p.dataPath).select("label","features")
        .randomSplit(Array(p.trainFraction,1-p.trainFraction),p.splitSeed)
      training=splits(0).persist(StorageLevel.fromString(p.storageLevel))
      val test=splits(1)
      val ntrain=training.count();val ntest=test.count()
      require(ntrain>0 && ntest>0 && training.select("label").distinct().count()==2 && test.select("label").distinct().count()==2,
        "SVM training and test sets must contain both binary classes")
      val model=new LinearSVC().setMaxIter(p.numIterations).setRegParam(p.regParam)
        .setTol(p.tolerance).setFitIntercept(p.fitIntercept).setStandardization(p.standardization).fit(training)
      val auc=new BinaryClassificationEvaluator().setMetricName("areaUnderROC").setNumBins(0)
        .evaluate(model.transform(test))
      require(!auc.isNaN && !auc.isInfinity,"AUC must be finite")
      println(s"SVM algorithm=linear-svc-v1 numTraining=$ntrain numTest=$ntest splitSeed=${p.splitSeed}")
      println(s"Area under ROC = $auc")
    } finally {if(training!=null) training.unpersist();spark.stop()}
  }
}
