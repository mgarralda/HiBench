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

import breeze.linalg.{DenseMatrix, eigSym}
import dev.ludovic.netlib.blas.BLAS
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/** Distributed packed Gram aggregation; only the reduced matrix reaches the driver.
  * Normal equations are unsuitable for resolving an extremely ill-conditioned tail.
  * The explicit roundoff floor prevents reporting numerical ghosts as singular values.
  */
object UncenteredSVD {
  case class Input(id: Long, features: Vector)
  case class Gram(rows: Long, upper: Array[Double])
  case class Result(rows: Long, singularValues: Array[Double], rightVectors: Array[Array[Double]])

  class GramSum(dimensions: Int) extends Aggregator[Input, Gram, Gram] {
    private val packed = (dimensions.toLong * (dimensions + 1L) / 2).toInt
    def zero: Gram = Gram(0L, new Array[Double](packed))
    def reduce(buffer: Gram, input: Input): Gram = {
      require(input.features.size == dimensions, "Inconsistent vector dimensions")
      val values = input.features.toArray
      require(values.forall(java.lang.Double.isFinite), "SVD requires finite features")
      BLAS.getInstance().dspr("U", dimensions, 1.0, values, 1, buffer.upper)
      Gram(Math.addExact(buffer.rows, 1L), buffer.upper)
    }
    def merge(a: Gram, b: Gram): Gram = {
      var i = 0
      while (i < packed) { a.upper(i) += b.upper(i); i += 1 }
      Gram(Math.addExact(a.rows, b.rows), a.upper)
    }
    def finish(buffer: Gram): Gram = buffer
    def bufferEncoder: Encoder[Gram] = Encoders.kryo[Gram]
    def outputEncoder: Encoder[Gram] = Encoders.kryo[Gram]
  }

  def decompose(data: DataFrame, dimensions: Int, requested: Int,
                rCond: Double, matrixBudgetBytes: Long): Result = {
    require(dimensions > 0 && requested > 0 && requested <= dimensions, "Invalid SVD dimensions/components")
    require(rCond > 0 && rCond < 1 && java.lang.Double.isFinite(rCond), "Invalid SVD reciprocal condition cutoff")
    val square = dimensions.toLong * dimensions
    require(square <= Int.MaxValue, "Dense driver matrix exceeds JVM array limit")
    // Estimate main packed/full/eigenvector/output arrays, not total JVM/native peak memory.
    val estimate = 8L * (3L * square + dimensions.toLong * requested + dimensions.toLong * (dimensions + 1L) / 2)
    require(matrixBudgetBytes > 0 && estimate <= matrixBudgetBytes,
      s"SVD matrix array estimate $estimate exceeds configured budget $matrixBudgetBytes")
    import data.sparkSession.implicits._
    val gram = data.select("id", "features").as[Input].select(new GramSum(dimensions).toColumn).head()
    require(gram.rows > 0, "SVD input is empty")
    require(gram.upper.forall(java.lang.Double.isFinite), "Gram aggregation overflow")
    val matrix = DenseMatrix.zeros[Double](dimensions, dimensions)
    var j = 0; var offset = 0
    while (j < dimensions) {
      var i = 0
      while (i <= j) { val value = gram.upper(offset); matrix(i,j)=value; matrix(j,i)=value; offset+=1; i+=1 }
      j+=1
    }
    val eigen = eigSym(matrix)
    val largest = math.max(0.0, eigen.eigenvalues(dimensions - 1))
    val cutoff = largest * math.max(rCond * rCond, dimensions * math.ulp(1.0))
    val retained = (0 until dimensions).reverse.filter(i => eigen.eigenvalues(i) > cutoff)
      .take(math.min(requested.toLong, gram.rows).toInt)
    val values = retained.map(i => math.sqrt(eigen.eigenvalues(i))).toArray
    val vectors = retained.map(i => Array.tabulate(dimensions)(row => eigen.eigenvectors(row,i))).toArray
    Result(gram.rows, values, vectors)
  }

  /** As in the original workload, computing U constructs a lazy distributed plan.
    * Callers doing numerical acceptance must explicitly materialize this plan.
    */
  def leftVectors(data: DataFrame, result: Result): DataFrame = {
    import data.sparkSession.implicits._
    data.select("id", "features").as[Input].map { row =>
      val x = row.features.toArray
      val u = result.rightVectors.indices.map { component =>
        var sum=0.0;var i=0
        while(i<x.length) {sum += x(i)*result.rightVectors(component)(i);i+=1}
        sum/result.singularValues(component)
      }
      (row.id, org.apache.spark.ml.linalg.Vectors.dense(u.toArray))
    }.toDF("id", "features")
  }
}
