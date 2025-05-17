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

//package com.intel.hibench.sparkbench.micro

//import java.util.Random
////import com.intel.hibench.sparkbench.common.IOCommon
//import org.apache.hadoop.examples.terasort.{TeraInputFormat, TeraOutputFormat}
//import org.apache.hadoop.io.Text
//import org.apache.spark._
//import org.apache.spark.rdd.{RDD, ShuffledRDD}
//import org.apache.spark.storage.StorageLevel

//import scala.util.hashing

//object ScalaRepartition {
//
//  def main(args: Array[String]) {
//    if (args.length != 4) {
//      System.err.println(
//        s"Usage: $ScalaRepartition <INPUT_HDFS> <OUTPUT_HDFS> <CACHE_IN_MEMORY> <DISABLE_OUTPUT>"
//      )
//      System.exit(1)
//    }
//    val cache = toBoolean(args(2), ("CACHE_IN_MEMORY"))
//    val disableOutput = toBoolean(args(3), ("DISABLE_OUTPUT"))
//
//    val sparkConf = new SparkConf().setAppName("ScalaRepartition")
//    val sc = new SparkContext(sparkConf)
//
//
//    val data = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0)).map {
//      case (k,v) => k.copyBytes ++ v.copyBytes
//    }
//
//    if (cache) {
//      data.persist(StorageLevel.MEMORY_ONLY)
//      data.count()
//    }
//
//    // Original code
////     val mapParallelism = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
////     val reduceParallelism  = IOCommon.getProperty("hibench.default.shuffle.parallelism")
////     .getOrElse((mapParallelism / 2).toString).toInt
//
//    // attempt to read executor count + cores, else fallback to defaultParallelism
//    val conf = sc.getConf
//
//    val execInstances = conf.getInt("spark.executor.instances", sc.defaultParallelism)
//    val execCores     = conf.getInt("spark.executor.cores", 1)
//
//    val totalCores = execInstances * execCores
//
//    // always at least 1 partition
//    val reduceParallelism = math.max(1, totalCores / 2)
//
//    print(s"\n*** reduceParallelism (executor.instances * executor.cores)/2: $reduceParallelism ***\n")
//
//    val postShuffle = repartition(data, reduceParallelism)
//    if (disableOutput) {
//      postShuffle.foreach(_ => {})
//    } else {
//      postShuffle.map {
//        case (_, v) => (new Text(v.slice(0, 10)), new Text(v.slice(10, 100)))
//      }.saveAsNewAPIHadoopFile[TeraOutputFormat](args(1))
//    }
//
//    sc.stop()
//  }
//
//  // More hints on Exceptions
//  private def toBoolean(str: String, parameterName: String): Boolean = {
//    try {
//      str.toBoolean
//    } catch {
//      case e: IllegalArgumentException =>
//        throw new IllegalArgumentException(
//        s"Unrecognizable parameter ${parameterName}: ${str}, should be true or false")
//    }
//  }
//
//  // Save a CoalescedRDD than RDD.repartition API
//  private def repartition(previous: RDD[Array[Byte]], numReducers: Int): ShuffledRDD[Int, Array[Byte], Array[Byte]] = {
//    /** Distributes elements evenly across output partitions, starting from a random partition. */
//    val distributePartition = (index: Int, items: Iterator[Array[Byte]]) => {
//      var position = new Random(hashing.byteswap32(index)).nextInt(numReducers)
//      items.map { t =>
//        // Note that the hash code of the key will just be the key itself. The HashPartitioner
//        // will mod it with the number of total partitions.
//        position = position + 1
//        (position, t)
//      }
//    } : Iterator[(Int, Array[Byte])]
//
//    // include a shuffle step so that our upstream tasks are still distributed
//    new ShuffledRDD[Int, Array[Byte], Array[Byte]](previous.mapPartitionsWithIndex(distributePartition),
//      new HashPartitioner(numReducers))
//  }
//
//}

package com.intel.hibench.sparkbench.micro

import org.apache.hadoop.examples.terasort.{TeraInputFormat, TeraOutputFormat}
import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.hadoop.io.Text
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ScalaRepartition {
  def main(args: Array[String]): Unit = {
    require(args.length == 4,
      "Usage: ScalaRepartitionDF <INPUT_HDFS> <OUTPUT_HDFS> <CACHE_IN_MEMORY> <DISABLE_OUTPUT>")

    val Array(inPath, outPath, cacheFlag, disableFlag) = args
    val cache      = cacheFlag.toBoolean
    val disableOut = disableFlag.toBoolean

    // 1) Bootstrap SparkSession & SparkContext
    val spark = SparkSession.builder
      .appName("ScalaRepartition")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    // 2) Load your SequenceFile of raw bytes into a DataFrame(key: binary, value: binary)
    val io = new IOCommon(sc)
    // loadDFBytes is your helper that does sc.sequenceFile[BytesWritable,BytesWritable]
//    var df: DataFrame = io.loadDFBytes(inPath)

    var df = sc.newAPIHadoopFile[Text, Text, TeraInputFormat](args(0)).map {
      case (k,v) => k.copyBytes ++ v.copyBytes
    }.toDF()


    // 3) Optionally cache the whole DataFrame
    if (cache) {
      df = df.cache()
      df.count()  // force materialization
    }

    // 4) Compute reduceParallelism = max(1, (exec.instances * exec.cores) / 2)
    val conf        = sc.getConf
    val execInst    = conf.getInt("spark.executor.instances", sc.defaultParallelism)
    val execCores   = conf.getInt("spark.executor.cores", 1)
    val totalCores  = execInst * execCores
    val reducePar   = math.max(1, totalCores / 2)
    println(s"*** reduceParallelism = $reducePar ***")

    // 5) Repartition (this is the “shuffle”)
    val shuffled: DataFrame = df.repartition(reducePar)

    if (!disableOut) {
      // 6) Re‑split the concatenated bytes back into key/value
      //    first re‑concat key+value into a single binary column
      val packed = shuffled.withColumn("bytes", expr("concat(key, value)"))

      //    then slice out the first 10 bytes as key, next 90 as value
      val sliced = packed
        .withColumn("k", expr("slice(bytes, 1, 10)"))
        .withColumn("v", expr("slice(bytes, 11, 90)"))
        .select(
          col("k").cast("binary").as("key"),
          col("v").cast("binary").as("value")
        )

      // 7) Write out via Hadoop's SequenceFileOutputFormat
      sliced.write
        .format(classOf[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[Text,Text]].getName)
        .option("mapreduce.job.output.key.class",   classOf[Text].getName)
        .option("mapreduce.job.output.value.class", classOf[Text].getName)
        .save(outPath)
    }

    spark.stop()
  }
}
