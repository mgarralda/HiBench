"""Check original/new tiny datasets against original volume and record contracts."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

parser = argparse.ArgumentParser()
parser.add_argument("original")
parser.add_argument("wordcount")
parser.add_argument("sort")
args = parser.parse_args()
vocabulary = json.loads((Path(__file__).parent / "randomtextwriter-reference.json").read_text())["vocabulary"]
allowed = set(vocabulary)
max_record = (9 + 99) * (max(map(len, vocabulary)) + 1)
spark = SparkSession.builder.appName("Original text profile comparison").getOrCreate()
try:
    summaries = {}
    for label in ("original", "wordcount", "sort"):
        path = getattr(args, label)
        rows = spark.read.text(path).withColumn("file", input_file_name()).collect()
        files = defaultdict(lambda: dict(logical_bytes=0, records=0))
        words = Counter()
        for row in rows:
            key, value = row.value.split("\t")
            assert key.endswith(" ") and value.endswith(" ")
            k, v = key.split(), value.split()
            assert 5 <= len(k) <= 9 and 10 <= len(v) <= 99
            assert set(k + v) <= allowed
            words.update(k + v)
            files[row.file]["records"] += 1
            files[row.file]["logical_bytes"] += len(key.encode()) + len(value.encode())
        assert len(files) == 2
        for file in files.values():
            assert 16000 <= file["logical_bytes"] < 16000 + max_record
        logical = sum(file["logical_bytes"] for file in files.values())
        physical = sum(len(row.value.encode()) + 1 for row in rows)
        assert physical == logical + 2 * len(rows)
        jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
        iterator = jpath.getFileSystem(spark._jsc.hadoopConfiguration()).listFiles(jpath, True)
        actual = 0
        while iterator.hasNext():
            file = iterator.next()
            if file.getPath().getName().startswith("part-"): actual += file.getLen()
        assert actual == physical
        summaries[label] = dict(logical_bytes=logical, physical_bytes=physical, records=len(rows),
                                words=sum(words.values()), vocabulary_seen=len(words), partitions=len(files))
    print("HIBENCH_ORIGINAL_PROFILE=" + json.dumps(summaries))
finally:
    spark.stop()
