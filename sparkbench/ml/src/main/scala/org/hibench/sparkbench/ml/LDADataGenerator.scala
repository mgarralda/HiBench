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

import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.sql.{DataFrame,SparkSession}

object LDADataGenerator {
  case class Document(id: Long,features: Vector)
  case class Profile(seed: Long,documents: Long,vocabulary: Int,lengthMin: Int,lengthMax: Int,partitions: Int)
  def dataset(spark: SparkSession,documents: Long,vocabulary: Int,lengthMin: Int,lengthMax: Int,
              partitions: Int,seed: Long): DataFrame = {
    require(documents>0 && vocabulary>0 && partitions>0,"Invalid LDA shape")
    require(lengthMin>0 && lengthMax>=lengthMin && lengthMax.toLong-lengthMin+1<=Int.MaxValue,"Invalid document length range")
    import spark.implicits._
    spark.range(0,partitions,1,partitions).as[Long].flatMap { partition =>
      val rng=new java.util.Random(seed ^ partition)
      val start=(BigInt(documents)*partition/partitions).toLong
      val end=(BigInt(documents)*(partition+1)/partitions).toLong
      new Iterator[Document] {
        private var id=start
        def hasNext: Boolean=id<end
        def next(): Document = {
          if(!hasNext) throw new NoSuchElementException("End of LDA partition")
          val length=rng.nextInt(lengthMax-lengthMin+1)+lengthMin
          val entries=scala.collection.mutable.HashMap[Int,Int]()
          var i=0
          while(i<length) {val index=rng.nextInt(vocabulary);entries(index)=entries.getOrElse(index,0)+1;i+=1}
          val result=Document(id,Vectors.sparse(vocabulary,entries.toSeq.map {case(k,v)=>(k,v.toDouble)}))
          id+=1;result
        }
      }
    }.toDF()
  }
  def main(args: Array[String]): Unit = {
    require(args.length==7,"Usage: LDADataGenerator OUTPUT DOCUMENTS VOCABULARY MIN_LENGTH MAX_LENGTH PARTITIONS SEED")
    val spark=SparkSession.builder().appName("LDADataGenerator").getOrCreate()
    try {
      val n=args(1).toLong;val v=args(2).toInt;val lo=args(3).toInt;val hi=args(4).toInt;val p=args(5).toInt;val seed=args(6).toLong
      dataset(spark,n,v,lo,hi,p,seed).write.mode("errorifexists").parquet(args(0))
      import spark.implicits._
      Seq(Profile(seed,n,v,lo,hi,p)).toDS().write.parquet(args(0)+"/_generator_profile")
    } finally spark.stop()
  }
}
