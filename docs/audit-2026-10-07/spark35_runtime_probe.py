"""Focused audit probes; isolated temporary output, no existing benchmark data."""
import json
import tempfile
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("HiBench compatibility audit").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
results = {"spark": spark.version, "checks": []}

def check(name, operation, expected_error=None):
    try:
        value = operation()
        results["checks"].append({"name": name, "result": "ok", "value": str(value)})
    except Exception as exc:
        message = str(exc)
        results["checks"].append({"name": name, "result": "expected_error" if expected_error and expected_error in message else "error", "exception": type(exc).__name__, "message": message[:1400]})

tmp = tempfile.mkdtemp(prefix="hibench-audit-")
check("Hadoop InputFormat is not a SQL data source", lambda: spark.read.format("org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat").load(tmp), "not a valid Spark SQL Data Source")
df = spark.createDataFrame([(bytearray(b"0123456789" + b"x" * 90),)], ["value"])
check("Repartition key/value columns absent", lambda: df.withColumn("bytes", F.expr("concat(key, value)")), "UNRESOLVED_COLUMN")
check("slice on binary", lambda: df.selectExpr("slice(value, 1, 10)").collect(), "DATATYPE_MISMATCH")
check("Hadoop OutputFormat is not a SQL data source", lambda: df.write.format("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat").save(tmp + "/sequence"), "does not allow create table as select")
group = "lazy-repartition-probe"
spark.sparkContext.setJobGroup(group, "No output branch")
shuffled = spark.range(1000).repartition(2)
check("Repartition without action submits no job", lambda: list(spark.sparkContext.statusTracker().getJobIdsForGroup(group)))
check("WordCount DataFrame core", lambda: spark.createDataFrame([("a b a",)], ["value"]).select(F.explode(F.split("value", r"\s+")).alias("word")).groupBy("word").count().orderBy("word").collect())
check("Parquet roundtrip", lambda: (df.write.parquet(tmp + "/parquet"), spark.read.parquet(tmp + "/parquet").count())[1])
check("Actual Scala WordCount local output", lambda: [row.asDict() for row in spark.read.parquet("file:///tmp/hibench-audit-20261007/wordcount-output").orderBy("word").collect()])
check("Actual Scala Sort local output", lambda: [row.asDict() for row in spark.read.parquet("file:///tmp/hibench-audit-20261007/sort-output").orderBy("value").collect()])
check("Actual Scala WordCount YARN output", lambda: [row.asDict() for row in spark.read.parquet("hdfs://spark-cluster-master:9000/tmp/hibench-audit-20261007/wordcount-output").orderBy("word").collect()])
print("HIBENCH_AUDIT_JSON=" + json.dumps(results, sort_keys=True))
spark.stop()
