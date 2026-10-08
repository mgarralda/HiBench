import org.hibench.sparkbench.micro._
import org.hibench.sparkbench.micro.terasort.TeraRecordGenerator
import org.apache.spark.sql.functions._

try {
  def hex(a: Array[Byte]): String = a.map(b => f"${b & 255}%02x").mkString
  // The historical Hadoop JAR is a test fixture, never a runtime dependency.
  val u = Class.forName("org.apache.hadoop.examples.terasort.Unsigned16")
  val r = Class.forName("org.apache.hadoop.examples.terasort.Random16")
  val g = Class.forName("org.apache.hadoop.examples.terasort.GenSort")
  val ctor = u.getDeclaredConstructor(java.lang.Long.TYPE); ctor.setAccessible(true)
  val skip = r.getDeclaredMethod("skipAhead", u); skip.setAccessible(true)
  val next = r.getDeclaredMethod("nextRand", u); next.setAccessible(true)
  val record = g.getDeclaredMethod("generateRecord", classOf[Array[Byte]], u, u); record.setAccessible(true)
  def original(row: Long): Array[Byte] = {
    val id = ctor.newInstance(Long.box(row)).asInstanceOf[AnyRef]; val rand = skip.invoke(null, id)
    next.invoke(null, rand)
    val bytes = new Array[Byte](100); record.invoke(null, bytes, rand, id); bytes
  }
  val generator = new TeraRecordGenerator()
  for (id <- Seq(0L, 1L, 2L, 99L, 31999L, 32000L, 3200000000L, 6000000000L, Long.MaxValue / 100))
    assert(generator.next(id).sameElements(original(id)), s"Original TeraGen mismatch at $id")
  for ((rows, parts) <- Seq((1L, 4), (257L, 3), (3200L, 2), (32000L, 4))) {
    val actual = TeraDataGenerator.generate(spark, rows, parts).collect()
      .map(row => hex(row.getAs[Array[Byte]]("key") ++ row.getAs[Array[Byte]]("value"))).sorted
    val expected = (0L until rows).map(id => hex(original(id))).sorted
    assert(actual.sameElements(expected), s"Tera records/count differ: $rows/$parts")
    println(s"HIBENCH_TERA_ORIGINAL_MATCH=$rows/$parts;LOGICAL_BYTES=${rows * 100}")
  }
  val root = "/tmp/hibench-micro-check-" + java.util.UUID.randomUUID().toString
  val input = root + "/input"
  TeraDataGenerator.generate(spark, 257, 3).write.option("compression", "uncompressed").parquet(input)
  spark.conf.set("spark.sql.shuffle.partitions", 3)
  spark.conf.set("spark.sql.adaptive.enabled", false)
  spark.conf.set("spark.default.parallelism", 2)
  val data = MicroDataFrameIO.readTera(spark, input)
  def records(path: String): Seq[String] = spark.read.parquet(path).collect().toSeq.map { row =>
    row.schema.fieldNames.sorted.map(name => hex(row.getAs[Array[Byte]](name))).mkString("/")
  }.sorted
  for (cached <- Seq(false, true)) {
    val output = root + "/repartition-" + cached
    MicroDataFrameIO.repartition(data, output, cached, false)
    assert(records(input) == records(output), s"Repartition changed records (cached=$cached)")
    MicroDataFrameIO.repartition(data, root + "/noop", cached, true)
  }
  val sorted = root + "/sorted"
  data.orderBy(col("key")).write.option("compression", "uncompressed").parquet(sorted)
  assert(records(input) == records(sorted), "Sort changed records")
  val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val files = fs.listStatus(new org.apache.hadoop.fs.Path(sorted)).map(_.getPath)
    .filter(_.getName.endsWith(".parquet")).sortBy(_.getName)
  val keys = files.flatMap { file =>
    val local = spark.read.parquet(file.toString).collect().map(row => hex(row.getAs[Array[Byte]]("key")))
    assert(local.sameElements(local.sorted), "Partition key order differs from unsigned binary order")
    local
  }
  assert(keys.sameElements(keys.sorted), "Global Tera key order differs")
  val start = System.nanoTime()
  assert(ScalaSleep.generate(spark, 1, 2).agg(sum("completed")).head().getLong(0) == 2)
  assert((System.nanoTime() - start) / 1000000L >= 1000L, "Sleep duration was optimized away")
  val bad = root + "/bad"
  import spark.implicits._
  Seq((Array[Byte](1), new Array[Byte](90))).toDF("key", "value").write.parquet(bad)
  var rejected = false
  try MicroDataFrameIO.consumeOrWrite(MicroDataFrameIO.readTera(spark, bad), root + "/bad-out", true)
  catch { case _: Exception => rejected = true }
  assert(rejected, "Invalid Tera key length was silently accepted")
  println("HIBENCH_MICRO_GENERATOR_SMOKE=passed")
} catch { case e: Throwable => e.printStackTrace(); System.exit(1) }
System.exit(0)
