"""Independent original Java-Random reference and Parquet/ML ALS validation."""
import collections
import json
import sys
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName("HiBench ALS profile verification").getOrCreate()
try:
    generator = spark._jvm.org.hibench.sparkbench.ml.RatingDataGenerator
    def data(users, items, count, partitions, implicit):
        return DataFrame(generator.dataset(spark._jsparkSession, users, items, count, partitions, implicit), spark)

    def reference(users, items, count, partitions, implicit):
        result = collections.Counter()
        for partition in range(partitions):
            rng = spark._jvm.java.util.Random(partition)
            for _ in range(count * (partition + 1) // partitions - count * partition // partitions):
                user, item = rng.nextInt(users), rng.nextInt(items)
                rating = float(rng.nextInt(5) + 1) - (2.5 if implicit else 0)
                result[user, item, rating] += 1
        return result

    checks = []
    for implicit in (False, True):
        for count, partitions in ((200, 2), (203, 7), (3, 8)):
            actual = data(100, 100, count, partitions, implicit)
            assert actual.schema.simpleString() == "struct<user:int,item:int,rating:float>"
            assert collections.Counter(tuple(r) for r in actual.collect()) == reference(100, 100, count, partitions, implicit)
            checks.append(dict(ratings=count, partitions=partitions, implicit=implicit, exact_original_match=True))

    with open(sys.argv[1], encoding="utf-8") as f:
        scales = json.load(f)
    for scale, values in scales.items():
        users, items, count = (int(values[k]) for k in ("users", "products", "ratings"))
        df = data(users, items, count, 2, True)
        summary = df.selectExpr("count(*) n", "min(user) umin", "max(user) umax", "min(item) imin", "max(item) imax", "min(rating) rmin", "max(rating) rmax").first()
        assert summary.n == count and 0 <= summary.umin <= summary.umax < users and 0 <= summary.imin <= summary.imax < items
        assert summary.rmin == -1.5 and summary.rmax == 2.5
        counts = {r.rating:r['count'] for r in df.groupBy('rating').count().collect()}
        assert set(counts) == {-1.5,-0.5,0.5,1.5,2.5}
        if count >= 20000:
            assert all(abs(n / count - 0.2) < 0.015 for n in counts.values())
        checks.append(dict(scale=scale, ratings=count, profile_verified=True, rating_counts=counts))

    root = sys.argv[2]
    for implicit in (False, True):
        path = root + ('/implicit' if implicit else '/explicit')
        df = data(100, 100, 200, 2, implicit)
        df.write.mode('overwrite').parquet(path)
        assert collections.Counter(tuple(r) for r in spark.read.parquet(path).collect()) == reference(100, 100, 200, 2, implicit)
    print("HIBENCH_ALS_VERIFIED=" + json.dumps(checks))
finally:
    spark.stop()
