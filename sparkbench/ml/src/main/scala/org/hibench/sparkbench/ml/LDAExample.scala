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

import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.clustering.{LDA, DistributedLDAModel, LocalLDAModel}
import scopt.OptionParser
object LDAExample {
  case class Params(
      inputPath: String = null,
      outputPath: String = null,
      numTopics: Int = 10,
      maxIterations: Int = 10,
      optimizer: String = "online",
      maxResultSize: String = "1g", modelSeed: Long = 0L)
	  
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
	
    val parser = new OptionParser[Params]("LDA") {
	  head("LDA: an example app for LDA.")
	  opt[String]("optimizer")
        .text(s"optimizer, default: ${defaultParams.optimizer}")
        .action((x, c) => c.copy(optimizer = x))
      opt[String]("maxResultSize")
        .text("max resultSize, default: ${defaultParams.maxResultSize}")
        .action((x, c) => c.copy(maxResultSize = x))
      opt[Int]("numTopics")
        .text(s"number of Topics, default: ${defaultParams.numTopics}")
        .action((x, c) => c.copy(numTopics = x))
      opt[Int]("maxIterations")
        .text(s"number of max iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Long]("modelSeed").action((x,c)=>c.copy(modelSeed=x))
      arg[String]("<inputPath>")
        .required()
        .text("Input paths")
        .action((x, c) => c.copy(inputPath = x))
      arg[String]("<outputPath>")
        .required()
        .text("outputPath paths")
        .action((x, c) => c.copy(outputPath = x))		

    }
	parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }
  
  def run(params: Params): Unit = {
    val spark=SparkSession.builder().appName("LDAExample")
      .config("spark.driver.maxResultSize",params.maxResultSize).getOrCreate()
    try {
      require(params.numTopics>1 && params.maxIterations>0,"Invalid LDA topic/iteration count")
      require(Set("online","em").contains(params.optimizer),"Unsupported LDA optimizer")
      val corpus=spark.read.parquet(params.inputPath).select("id","features")
      val n=corpus.count();require(n>0,"LDA corpus is empty")
      val model=new LDA().setK(params.numTopics).setMaxIter(params.maxIterations)
        .setOptimizer(params.optimizer).setSeed(params.modelSeed).fit(corpus)
      model.write.save(params.outputPath)
      val saved=if(params.optimizer=="em") DistributedLDAModel.load(params.outputPath)
        else LocalLDAModel.load(params.outputPath)
      require(saved.getK==params.numTopics && saved.vocabSize==model.vocabSize,"LDA reload schema differs")
      println(s"LDA documents=$n vocabulary=${model.vocabSize} topics=${saved.getK} optimizer=${params.optimizer} seed=${params.modelSeed}")
    } finally spark.stop()
  }
}
