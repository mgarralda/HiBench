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
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

object SVDExample {
  def main(args: Array[String]): Unit = {
    require(args.length==7,"Usage: SVDExample INPUT FEATURES COMPONENTS COMPUTE_U RCOND MATRIX_BUDGET_BYTES MAX_RESULT_SIZE")
    val lifecycleStarted = System.nanoTime()
    val spark=SparkSession.builder().appName("SVD packed-gram-v1")
      .config("spark.driver.maxResultSize",args(6)).getOrCreate()
    val applicationId = spark.conf.get("spark.app.id")
    val driverCores = spark.conf.getOption("spark.driver.cores")
    val driverMemory = spark.conf.getOption("spark.driver.memory")
    val phases = scala.collection.mutable.LinkedHashMap[String, Double](
      "spark_startup_s" -> ((System.nanoTime() - lifecycleStarted) / 1e9))
    val workloadStarted = System.nanoTime()
    var stopStarted = 0L
    try {
      val readStarted = System.nanoTime()
      val data=spark.read.parquet(args(0))
      phases("read_plan_s") = (System.nanoTime() - readStarted) / 1e9
      val result=UncenteredSVD.decompose(data,args(1).toInt,args(2).toInt,args(4).toDouble,args(5).toLong)
      phases ++= result.phaseSeconds
      val uStarted = System.nanoTime()
      if(args(3).toBoolean) {
        val u=UncenteredSVD.leftVectors(data,result)
        // Preserve the original lazy U contract; this benchmark does not write/materialize U.
        require(u.columns.contains("features"),"Invalid U plan")
      }
      phases("u_plan_s") = (System.nanoTime() - uStarted) / 1e9
      phases("workload_s") = (System.nanoTime() - workloadStarted) / 1e9
      println(s"SVD algorithm=packed-gram-v1 rows=${result.rows} requested=${args(2)} retained=${result.singularValues.length}")
      println("SVD singularValues="+result.singularValues.mkString("[",",","]"))
    } finally {
      stopStarted = System.nanoTime()
      spark.stop()
    }
    phases("spark_stop_s") = (System.nanoTime() - stopStarted) / 1e9
    phases("application_lifecycle_s") = (System.nanoTime() - lifecycleStarted) / 1e9
    val threads = Seq("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
      "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS").map(key => key -> sys.env.get(key)).toMap
    val runtime = ("eigensolver" -> "breeze.linalg.eigSym") ~
      ("blas_class" -> dev.ludovic.netlib.blas.BLAS.getInstance().getClass.getName) ~
      ("lapack_class" -> dev.ludovic.netlib.lapack.LAPACK.getInstance().getClass.getName) ~
      ("thread_environment" -> threads) ~ ("effective_native_threads" -> Option.empty[Int]) ~
      ("effective_driver_cores" -> Option.empty[Int]) ~
      ("driver_cores_configured" -> driverCores) ~
      ("driver_memory_configured" -> driverMemory) ~
      ("driver_logical_cpus" -> Runtime.getRuntime.availableProcessors()) ~
      ("java_version" -> System.getProperty("java.version"))
    println("HIBENCH_WORKLOAD_MEASUREMENT=" + compact(render(
      ("schema_version" -> 1) ~ ("protocol" -> "svd-phases-v1") ~
      ("application_id" -> applicationId) ~ ("phase_seconds" -> phases.toMap) ~
      ("numerical_runtime" -> runtime) ~ ("u_materialized" -> false))))
  }
}
