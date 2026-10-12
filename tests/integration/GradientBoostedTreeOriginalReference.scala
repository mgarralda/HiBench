// Frozen generation from Git 810a9b74723580b2c7f8ef5d86fa4bd2d503f3ef; historical test only.
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

import org.hibench.sparkbench.common.IOCommon

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate test data for Gradient Boosted Tree. This class chooses positive labels
 * with probability `probOne` and scales features for positive examples by `eps`.
 */
object GradientBoostedTreeOriginalReference {

  /**
   * Generate an RDD containing test data for Gradient Boosted Tree.
   *
   * @param sc SparkContext to use for creating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param eps Epsilon factor by which positive examples are scaled.
   * @param nparts Number of partitions of the generated RDD. Default value is 2.
   * @param probOne Probability that a label is 1 (and not 0). Default value is 0.5.
   */
  def generateGBTRDD(
    sc: SparkContext,
    nexamples: Int,
    nfeatures: Int,
    eps: Double,
    nparts: Int = 2,
    probOne: Double = 0.5): RDD[LabeledPoint] = {
    val data = sc.parallelize(0 until nexamples, nparts).map { idx =>
      val rnd = new Random(42 + idx)

      val y = if (idx % 2 == 0) 0.0 else 1.0
      val x = Array.fill[Double](nfeatures) {
        rnd.nextGaussian() + (y * eps)
      }
      LabeledPoint(y, Vectors.dense(x))
    }
    data
  }

  def reference(spark: org.apache.spark.sql.SparkSession, n: Int, d: Int,
                p: Int, shift: Double): org.apache.spark.sql.DataFrame = {
    import spark.implicits._
    generateGBTRDD(spark.sparkContext, n, d, shift, p)
      .map(r => org.apache.spark.ml.feature.LabeledPoint(r.label, r.features.asML)).toDF()
  }
}
