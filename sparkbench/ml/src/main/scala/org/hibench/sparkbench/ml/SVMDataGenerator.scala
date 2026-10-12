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

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SVMDataGenerator {
  case class Example(id: Long, label: Double, features: Vector)
  case class Profile(seed: Long, weightSeed: Long, noiseStd: Double, examples: Long,
                     dimensions: Int, partitions: Int, weights: Seq[Double])
  def dataset(spark: SparkSession, count: Long, dimensions: Int, partitions: Int,
              seed: Long, weightSeed: Long, noiseStd: Double): DataFrame = {
    require(count>0 && dimensions>0 && partitions>0,"Invalid SVM shape")
    require(seed>=0 && BigInt(seed)+count-1<=Long.MaxValue,"SVM row seed exceeds Long range")
    require(noiseStd>=0 && !noiseStd.isNaN && !noiseStd.isInfinity,"Invalid label noise")
    val global=new java.util.Random(weightSeed)
    val weights=Array.fill(dimensions)(global.nextGaussian())
    import spark.implicits._
    spark.range(0,count,1,partitions).as[Long].map { id =>
      val random=new java.util.Random(seed+id)
      val x=Array.fill(dimensions)(random.nextDouble()*2-1)
      val score=weights.indices.iterator.map(i => weights(i)*x(i)).sum+random.nextGaussian()*noiseStd
      Example(id,if(score<0) 0.0 else 1.0,Vectors.dense(x))
    }.toDF()
  }
  def main(args: Array[String]): Unit = {
    require(args.length==7,"Usage: SVMDataGenerator OUTPUT EXAMPLES FEATURES PARTITIONS SEED WEIGHT_SEED NOISE_STD")
    val spark=SparkSession.builder().appName("SVMDataGenerator").getOrCreate()
    try {
      val n=args(1).toLong;val d=args(2).toInt;val p=args(3).toInt
      val seed=args(4).toLong;val weightSeed=args(5).toLong;val noise=args(6).toDouble
      dataset(spark,n,d,p,seed,weightSeed,noise).write.mode("errorifexists").parquet(args(0))
      val rng=new java.util.Random(weightSeed)
      import spark.implicits._
      Seq(Profile(seed,weightSeed,noise,n,d,p,Array.fill(d)(rng.nextGaussian()).toSeq))
        .toDS().write.parquet(args(0)+"/_generator_profile")
    } finally spark.stop()
  }
}
