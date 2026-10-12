/*
Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/* Historical test adapter only; excluded from every production Maven module.
 * Uses Spark 3.5.9 legacy APIs with controlled inputs/settings for comparison.
 */
package org.hibench.sparkbench.ml

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest, GradientBoostedTrees}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy

object TreeAlgorithmOriginalReference {
  def predictions(spark: SparkSession, train: DataFrame, test: DataFrame,
                  forest: Boolean, trees: Int): DataFrame = {
    val data = train.rdd.map(r => LabeledPoint(r.getAs[Double]("label"),
      Vectors.fromML(r.getAs[Vector]("features"))))
    val predict: Vector => (Double, Double, Double) = if (forest) {
      val model = RandomForest.trainClassifier(data, 2, Map.empty[Int, Int],
        trees, "all", "gini", 3, 16, 1)
      v => {
        val old = Vectors.fromML(v)
        val positiveProbability = model.trees.map { tree =>
          var node = tree.topNode
          while (!node.isLeaf) {
            val split = node.split.get // controlled fixture: all continuous features
            node = if (old(split.feature) <= split.threshold) node.leftNode.get else node.rightNode.get
          }
          if (node.predict.predict == 1.0) node.predict.prob else 1.0 - node.predict.prob
        }.sum / trees
        (model.predict(old), positiveProbability, if (positiveProbability > 0.5) 1.0 else 0.0)
      }
    } else {
      val strategy = BoostingStrategy.defaultParams("Classification")
      strategy.numIterations = trees
      strategy.learningRate = 0.1
      strategy.treeStrategy.numClasses = 2
      strategy.treeStrategy.maxDepth = 3
      strategy.treeStrategy.maxBins = 16
      strategy.treeStrategy.categoricalFeaturesInfo = Map.empty[Int, Int]
      val model = new GradientBoostedTrees(strategy).run(data)
      v => {
        val p = model.predict(Vectors.fromML(v))
        (p, Double.NaN, p)
      }
    }
    import spark.implicits._
    test.rdd.map { r =>
      val (hard, probability, soft) = predict(r.getAs[Vector]("features"))
      (r.getAs[Long]("id"), hard, probability, soft)
    }.toDF("id", "legacyPrediction", "softProbability", "softPrediction")
  }
}
