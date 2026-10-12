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

package org.hibench.sparkbench.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/** Three fixed Spark SQL queries over session-local views, without a Hive catalog. */
object ScalaSparkSQLBench {
  val visitColumns = Seq("sourceIP", "destURL", "visitDate", "adRevenue", "userAgent", "countryCode", "languageCode", "searchWord", "duration")
  private val visitTypes = Seq(StringType, StringType, DateType, DoubleType, StringType, StringType, StringType, StringType, IntegerType)
  private def requireSchema(data: DataFrame, columns: Seq[String], types: Seq[DataType]): Unit = {
    require(columns.zip(types).forall { case (name, expected) =>
      data.schema.fields.exists(field => field.name == name && field.dataType == expected)
    }, "Invalid SQL schema; regenerate or explicitly convert legacy SQL input")
  }
  def query(spark: SparkSession, workload: String, input: String): DataFrame = {
    val visits = spark.read.parquet(input.stripSuffix("/") + "/uservisits")
    requireSchema(visits, visitColumns, visitTypes)
    visits.selectExpr(visitColumns: _*).createOrReplaceTempView("hibench_uservisits")
    workload match {
      case "scan" => spark.sql("SELECT * FROM hibench_uservisits")
      case "aggregation" => spark.sql("SELECT sourceIP, SUM(adRevenue) AS sumAdRevenue FROM hibench_uservisits GROUP BY sourceIP")
      case "join" =>
        val rankings = spark.read.parquet(input.stripSuffix("/") + "/rankings")
        requireSchema(rankings, Seq("pageURL", "pageRank", "avgDuration"), Seq(StringType, IntegerType, IntegerType))
        rankings.selectExpr("pageURL", "pageRank", "avgDuration").createOrReplaceTempView("hibench_rankings")
        spark.sql("""SELECT sourceIP, AVG(pageRank) AS avgPageRank, SUM(adRevenue) AS totalRevenue
          FROM hibench_rankings R JOIN (
            SELECT sourceIP, destURL, adRevenue FROM hibench_uservisits
            WHERE datediff(visitDate, '1999-01-01') >= 0 AND datediff(visitDate, '2000-01-01') <= 0
          ) UV ON R.pageURL = UV.destURL
          GROUP BY sourceIP ORDER BY totalRevenue DESC""")
      case other => throw new IllegalArgumentException(s"Unknown SQL workload: $other")
    }
  }
  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: ScalaSparkSQLBench <scan|aggregation|join> <input> <output>")
    val spark = SparkSession.builder.appName("SQL " + args(0)).config("spark.sql.catalogImplementation", "in-memory").getOrCreate()
    try query(spark, args(0), args(1)).write.mode("overwrite").option("compression", "uncompressed").parquet(args(2))
    finally spark.stop()
  }
}
