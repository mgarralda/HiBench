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

object SVDExample {
  def main(args: Array[String]): Unit = {
    require(args.length==7,"Usage: SVDExample INPUT FEATURES COMPONENTS COMPUTE_U RCOND MATRIX_BUDGET_BYTES MAX_RESULT_SIZE")
    val spark=SparkSession.builder().appName("SVD packed-gram-v1")
      .config("spark.driver.maxResultSize",args(6)).getOrCreate()
    try {
      val data=spark.read.parquet(args(0))
      val result=UncenteredSVD.decompose(data,args(1).toInt,args(2).toInt,args(4).toDouble,args(5).toLong)
      if(args(3).toBoolean) {
        val u=UncenteredSVD.leftVectors(data,result)
        // Preserve the original lazy U contract; this benchmark does not write/materialize U.
        require(u.columns.contains("features"),"Invalid U plan")
      }
      println(s"SVD algorithm=packed-gram-v1 rows=${result.rows} requested=${args(2)} retained=${result.singularValues.length}")
      println("SVD singularValues="+result.singularValues.mkString("[",",","]"))
    } finally spark.stop()
  }
}
