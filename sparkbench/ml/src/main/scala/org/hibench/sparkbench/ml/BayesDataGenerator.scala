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

import java.util.Random
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.hibench.sparkbench.common.datagen.LegacyZipfian

object BayesDataGenerator {
  case class Dense(id: Long, origin_label: Double, features: Vector)
  case class Document(id: Long, origin_label: Double, tokens: Seq[String])
  case class Profile(mode: String, seed: Long, examples: Long, dimensions: Int,
                     classes: Int, partitions: Int, classShift: Double, dictionaryPositions: Int)
  val DictionaryPositions = 479623

  def dictionary(): Array[String] = {
    val stream = getClass.getResourceAsStream("/hibench-words")
    require(stream != null, "Historical dictionary resource is missing")
    val source = scala.io.Source.fromInputStream(stream, "UTF-8")
    try {
      val lines = source.getLines().take(DictionaryPositions).toArray
      // Historical RawData wrote readLine() + newline after EOF: literal "null" words.
      Array.tabulate(DictionaryPositions)(i => if (i < lines.length) lines(i) else "null")
    } finally source.close()
  }

  def dense(spark: SparkSession, examples: Long, dimensions: Int, classes: Int,
            partitions: Int, seed: Long, shift: Double): DataFrame = {
    require(examples > 0 && dimensions > 0 && classes > 1 && partitions > 0, "Invalid dense Bayes shape")
    require(seed >= 0 && BigInt(seed) + examples - 1 <= Long.MaxValue, "Dense row seed exceeds Long range")
    require(shift >= 0 && !shift.isNaN && !shift.isInfinity, "Invalid class shift")
    import spark.implicits._
    spark.range(0,examples,1,partitions).as[Long].map { id =>
      val random = new Random(seed + id)
      val label = random.nextInt(classes).toDouble
      Dense(id,label,Vectors.dense(Array.fill(dimensions)(math.abs(random.nextGaussian()+label*shift))))
    }.toDF()
  }

  def documents(spark: SparkSession, pages: Long, classes: Int, partitions: Int, seed: Long): DataFrame = {
    require(pages > 0 && pages <= Long.MaxValue / 760 && classes > 1 && partitions > 0, "Invalid document Bayes shape")
    require(seed >= 1 && seed + partitions - 1 <= Int.MaxValue, "Document slot seed exceeds Int range")
    val zipf = new LegacyZipfian(DictionaryPositions,0.9)
    zipf.setupZipf(pages*760,0.1)
    val kernel=zipf.createLegacyZipfCore()
    val slotSize=(pages-1)/partitions+1
    import spark.implicits._
    spark.range(0,partitions,1,partitions).as[Long].flatMap { partition =>
      val slot=(seed+partition).toInt
      val seeds=new Random(slot)
      seeds.nextLong(); seeds.nextLong() // URL and external-link streams, unused by Bayes.
      val words=kernel.copy(); words.setRandSeed(seeds.nextLong())
      val lengthRandom=new Random(seeds.nextLong())
      val labels=new Random(slot*1000+101) // Int arithmetic matches the original mapper.
      val dict=dictionary()
      val start=(BigInt(slotSize)*partition).min(BigInt(pages)).toLong
      val end=(BigInt(start)+slotSize).min(BigInt(pages)).toLong
      new Iterator[Document] {
        private var id=start
        def hasNext: Boolean=id<end
        def next(): Document = {
          if (!hasNext) throw new NoSuchElementException("End of document slot")
          val label=labels.nextInt(classes).toDouble
          var g=lengthRandom.nextGaussian()
          while(g < -10.0 || g > (Short.MaxValue-800)/80.0) g=lengthRandom.nextGaussian()
          val length=math.ceil(math.round(800+80*g)*500.0/800).toInt
          val tokens=if(length==0) Seq("") else Array.fill(length)(dict(words.next().toInt)).toSeq
          val result=Document(id,label,tokens); id+=1; result
        }
      }
    }.toDF()
  }

  def weighted(spark: SparkSession, documents: DataFrame): DataFrame = {
    import spark.implicits._
    val tokens=documents.select(col("id"),col("origin_label"),explode(col("tokens")).as("word"))
    val counts=tokens.groupBy("word").agg(count(lit(1)).as("frequency"))
    val total=counts.agg(sum(col("frequency"))).head().getLong(0)
    require(total>0,"Document corpus must contain tokens")
    val vocabulary=counts.withColumn("index",(row_number().over(Window.orderBy("word"))-1).cast("int"))
      .withColumn("weight",col("frequency").cast("double")/lit(total.toDouble)).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dimensions=vocabulary.count().toInt
      val terms=tokens.groupBy("id","origin_label","word").agg(count(lit(1)).as("termCount"))
        .join(vocabulary.select("word","index","weight"),Seq("word"))
        .select(col("id"),col("origin_label"),struct(col("index"), (col("termCount")*col("weight")).as("value")).as("term"))
      val vector=udf((terms: Seq[org.apache.spark.sql.Row]) => {
        val sorted=terms.sortBy(_.getInt(0))
        Vectors.sparse(dimensions,sorted.map(_.getInt(0)).toArray,sorted.map(_.getDouble(1)).toArray)
      })
      // Materialization in main occurs while the source corpus remains persisted.
      terms.groupBy("id","origin_label").agg(collect_list("term").as("terms"))
        .withColumn("features",vector(col("terms"))).select("id","origin_label","features")
    } finally vocabulary.unpersist()
  }

  def indexed(frame: DataFrame): DataFrame = new StringIndexer()
    .setInputCol("origin_label").setOutputCol("label").fit(frame).transform(frame)
    .select("id","label","features")

  def main(args: Array[String]): Unit = {
    require(args.length==9,"Usage: BayesDataGenerator OUTPUT DENSE EXAMPLES FEATURES PAGES CLASSES PARTITIONS SEED CLASS_SHIFT")
    val spark=SparkSession.builder().appName("BayesDataGenerator").getOrCreate()
    var corpus: DataFrame=null
    try {
      val useDense=args(1).toBoolean;val examples=args(2).toLong;val dimensions=args(3).toInt
      val pages=args(4).toLong;val classes=args(5).toInt;val partitions=args(6).toInt;val seed=args(7).toLong;val shift=args(8).toDouble
      val frame=if(useDense) dense(spark,examples,dimensions,classes,partitions,seed,shift) else {
        corpus=documents(spark,pages,classes,partitions,seed).persist(StorageLevel.MEMORY_AND_DISK)
        weighted(spark,corpus)
      }
      indexed(frame).write.mode("errorifexists").parquet(args(0))
      val actualDimensions=spark.read.parquet(args(0)).head().getAs[Vector]("features").size
      import spark.implicits._
      Seq(Profile(if(useDense) "dense" else "documents",seed,if(useDense) examples else pages,
        actualDimensions,classes,partitions,if(useDense) shift else 0.0,if(useDense) 0 else DictionaryPositions))
        .toDS().write.parquet(args(0)+"/_generator_profile")
    } finally {if(corpus!=null) corpus.unpersist();spark.stop()}
  }
}
