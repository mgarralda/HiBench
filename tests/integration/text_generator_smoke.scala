import org.hibench.sparkbench.micro.{TextDataGenerator, OriginalTextVocabulary}
import java.util.Random
try {
  val original = Class.forName("org.apache.hadoop.examples.RandomTextWriter")
  val wordsField = original.getDeclaredField("words"); wordsField.setAccessible(true)
  val vocabulary = wordsField.get(null).asInstanceOf[Array[String]]
  assert(vocabulary.sameElements(OriginalTextVocabulary.words), "Vocabulary/order differs from original Hadoop JAR")
  val mapper = Class.forName("org.apache.hadoop.examples.RandomTextWriter$RandomTextMapper")
  val constructor = mapper.getDeclaredConstructor(); constructor.setAccessible(true)
  val randomField = mapper.getDeclaredField("random"); randomField.setAccessible(true)
  val sentence = mapper.getDeclaredMethod("generateSentence", classOf[Int]); sentence.setAccessible(true)
  def reference(bytes: Long, parts: Int, seed: Long): Array[String] = {
    val budget = bytes / parts
    (0L until bytes / budget).flatMap { part =>
      val instance = constructor.newInstance()
      val random = new Random(seed + part); randomField.set(instance, random)
      val rows = scala.collection.mutable.ArrayBuffer[String]()
      var remaining = budget
      while (remaining > 0) {
        val nk = 5 + random.nextInt(5); val nv = 10 + random.nextInt(90)
        val key = sentence.invoke(instance, Int.box(nk)).toString
        val value = sentence.invoke(instance, Int.box(nv)).toString
        remaining -= key.getBytes("UTF-8").length + value.getBytes("UTF-8").length
        rows += key + "\t" + value
      }
      rows
    }.toArray.sorted
  }
  for ((bytes, parts, seed) <- Seq((32000L,2,42L),(320000L,4,43L),(257L,2,44L),(2L,2,42L))) {
    val actual = TextDataGenerator.dataset(spark, bytes, parts, seed).collect().map(_.getString(0)).sorted
    val expected = reference(bytes, parts, seed)
    assert(actual.sameElements(expected), s"Different from original mapper: $bytes/$parts/$seed")
    assert(actual.forall { row =>
      val fields = row.split("\t", -1)
      val nk = fields(0).trim.split(" +").length; val nv = fields(1).trim.split(" +").length
      nk >= 5 && nk <= 9 && nv >= 10 && nv <= 99
    })
    println(s"HIBENCH_ORIGINAL_MATCH bytes=$bytes partitions=$parts seed=$seed records=${actual.length}")
  }
  TextDataGenerator.dataset(spark, 32000, 2, 42).write.format("noop").mode("overwrite").save()
  println("HIBENCH_TEXT_GENERATOR_SMOKE=original-compatible")
  System.exit(0)
} catch { case error: Throwable => error.printStackTrace(); System.exit(1) }
