/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hibench.sparkbench.ml

import java.util.Random
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Gaussian mixtures with the original cluster allocation and distribution ranges. */
object GaussianDataGenerator {
  case class Profile(clusterId: Int, startId: Long, samples: Long,
                     mean: Seq[Double], stddev: Seq[Double])
  case class Sample(id: Long, features: Vector)

  def profiles(spark: SparkSession, samples: Long, clusters: Int, dimensions: Int,
               seed: Long, meanMin: Double, meanMax: Double,
               stdMin: Double, stdMax: Double): DataFrame = {
    require(samples > 0 && clusters > 0 && dimensions > 0, "Positive samples, clusters and dimensions required")
    require(clusters.toLong * dimensions <= 1000000,
      "Mixture metadata is limited to one million cluster/feature pairs")
    require(Seq(meanMin, meanMax, stdMin, stdMax).forall(x => !x.isNaN && !x.isInfinity) &&
      meanMin < meanMax && stdMin > 0 && stdMin < stdMax, "Invalid Gaussian parameter ranges")
    val rng = new Random(seed)
    val base = samples / clusters
    val remainder = samples % clusters
    import spark.implicits._
    spark.createDataset((0 until clusters).map { cluster =>
      val values = (0 until dimensions).map { _ =>
        (meanMin + rng.nextDouble() * (meanMax - meanMin),
         stdMin + rng.nextDouble() * (stdMax - stdMin))
      }
      Profile(cluster, base * cluster + math.min(cluster.toLong, remainder),
        base + (if (cluster < remainder) 1 else 0), values.map(_._1), values.map(_._2))
    }).toDF()
  }

  def dataset(spark: SparkSession, profiles: DataFrame, samplesPerFile: Long,
              partitions: Int, seed: Long): DataFrame = {
    require(samplesPerFile > 0 && partitions > 0, "Positive chunk size and partitions required")
    import spark.implicits._
    // Each original per-cluster chunk owns an independent reproducible random stream.
    val chunks = profiles.as[Profile].flatMap { profile =>
      new Iterator[Profile] {
        private var offset = 0L
        def hasNext: Boolean = offset < profile.samples
        def next(): Profile = {
          if (!hasNext) throw new NoSuchElementException("Gaussian chunks exhausted")
          val count = math.min(samplesPerFile, profile.samples - offset)
          val chunk = profile.copy(startId = profile.startId + offset, samples = count)
          offset += count
          chunk
        }
      }
    }
    chunks.repartition(partitions).flatMap { profile =>
      new Iterator[Sample] {
        private var index = 0L
        private var rng: Random = null
        def hasNext: Boolean = index < profile.samples
        def next(): Sample = {
          if (!hasNext) throw new NoSuchElementException("Gaussian cluster exhausted")
          if (index % samplesPerFile == 0) {
            val chunkStart = profile.startId + index
            rng = new Random(seed + 0x9e3779b97f4a7c15L * (chunkStart + 1))
          }
          val values = profile.mean.indices.map { d =>
            profile.mean(d) + rng.nextGaussian() * profile.stddev(d)
          }.toArray
          val row = Sample(profile.startId + index, Vectors.dense(values))
          index += 1
          row
        }
      }
    }.toDF()
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 12,
      "Usage: GaussianDataGenerator <SAMPLES_PATH> <PROFILE_PATH> <COUNT> <CLUSTERS> <DIMENSIONS> <SAMPLES_PER_FILE> <PARTITIONS> <SEED> <MEAN_MIN> <MEAN_MAX> <STD_MIN> <STD_MAX>")
    val spark = SparkSession.builder().appName("HiBench Gaussian mixture Parquet v1").getOrCreate()
    try {
      val seed = args(7).toLong
      val profile = profiles(spark, args(2).toLong, args(3).toInt, args(4).toInt,
        seed, args(8).toDouble, args(9).toDouble, args(10).toDouble, args(11).toDouble)
      val data = dataset(spark, profile, args(5).toLong, args(6).toInt, seed)
      data.write.mode("errorifexists").option("maxRecordsPerFile", args(5)).parquet(args(0))
      profile.write.mode("errorifexists").parquet(args(1))
      println(s"Gaussian profile v1: count=${args(2)}, clusters=${args(3)}, dimensions=${args(4)}, seed=$seed")
    } finally spark.stop()
  }
}
