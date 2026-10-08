"""Independent generator-v1 contract check on small HDFS/text datasets."""
import argparse
from collections import Counter
import hashlib
import json
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("bytes", type=int)
parser.add_argument("--partitions", type=int, default=2)
parser.add_argument("--seed", type=int, default=42)
args = parser.parse_args()
spark = SparkSession.builder.appName("Text generator contract check").getOrCreate()
try:
    from pathlib import Path
    reference = json.loads((Path(__file__).parent / "randomtextwriter-reference.json").read_text())
    words = reference["vocabulary"]
    class JavaRandom:
        def __init__(self, seed):
            self.state = (seed ^ 0x5DEECE66D) & ((1 << 48) - 1)
        def next_int(self, bound):
            while True:
                self.state = (self.state * 0x5DEECE66D + 11) & ((1 << 48) - 1)
                bits = self.state >> 17
                if bound & (bound - 1) == 0:
                    return (bound * bits) >> 31
                value = bits % bound
                if bits - value + bound - 1 < (1 << 31):
                    return value
    budget = args.bytes // args.partitions
    expected = []
    logical_bytes = 0
    for part in range(args.bytes // budget):
        random = JavaRandom(args.seed + part)
        remaining = budget
        while remaining > 0:
            nk = 5 + random.next_int(5)
            nv = 10 + random.next_int(90)
            key = "".join(words[random.next_int(len(words))] + " " for _ in range(nk))
            value = "".join(words[random.next_int(len(words))] + " " for _ in range(nv))
            length = len(key.encode()) + len(value.encode())
            remaining -= length
            logical_bytes += length
            expected.append(key + "\t" + value)
    actual = [r.value for r in spark.read.text(args.input).collect()]
    assert Counter(actual) == Counter(expected), "Generator content/seed differs from Python reference"
    expected_physical = logical_bytes + 2 * len(expected)
    assert sum(len(line.encode("ascii")) + 1 for line in actual) == expected_physical
    # Actual part-file lengths include newlines, unlike Spark's text rows.
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(args.input)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator = fs.listFiles(path, True)
    total = 0
    while iterator.hasNext():
        status = iterator.next()
        if status.getPath().getName().startswith("part-"):
            total += status.getLen()
    assert total == expected_physical, f"Actual text bytes {total} != {expected_physical}"
    print("HIBENCH_GENERATOR_CHECK=" + json.dumps(dict(requested_logical_bytes=args.bytes, logical_bytes=logical_bytes, physical_bytes=total, rows=len(expected), seed=args.seed)))
finally:
    spark.stop()
