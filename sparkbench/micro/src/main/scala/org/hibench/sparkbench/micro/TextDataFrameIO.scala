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


package org.hibench.sparkbench.micro

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Text input and DataFrame output, without a SparkContext or RDD boundary. */
object TextDataFrameIO {
  def property(key: String, default: String): String = {
    val props = new Properties()
    Option(System.getenv("SPARKBENCH_PROPERTIES_FILES")).getOrElse("").split(",").filter(_.nonEmpty).foreach { name =>
      val original = new File(name)
      val file = if (original.isFile) original else new File(SparkFiles.get(original.getName))
      val reader = new InputStreamReader(new FileInputStream(file), "UTF-8")
      try props.load(reader) finally reader.close()
    }
    props.getProperty(key, default).trim
  }
  def read(spark: SparkSession, path: String): DataFrame = {
    require(property("sparkbench.inputformat", "Text").equalsIgnoreCase("Text"),
      "WordCount/Sort accept text input only; convert legacy SequenceFiles explicitly")
    spark.read.text(path)
  }
  def write(data: DataFrame, path: String): Unit = {
    property("sparkbench.dataframe.outputformat", "Parquet").toLowerCase match {
      case "parquet" => data.write.mode("overwrite").option("compression", "snappy").parquet(path)
      case "null" => data.write.format("noop").mode("overwrite").save()
      case other => throw new IllegalArgumentException(s"Unsupported DataFrame output format: $other")
    }
  }
}
