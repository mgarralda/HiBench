"""Compare current SQL inputs and query outputs with the historical tiny fixture."""
import collections
import datetime
import json
import math
import sys
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.appName("HiBench SQL semantic verification").getOrCreate()
try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        original = json.load(stream)
    root = sys.argv[2].rstrip("/")
    ranks = spark.read.parquet(root + "/rankings").collect()
    visits = spark.read.parquet(root + "/uservisits").collect()
    expected_ranks = collections.Counter((key, fields[0], int(fields[1]), int(fields[2]))
        for key, value in original["rankings"] for fields in [value.split(",")])
    actual_ranks = collections.Counter((r.pageId, r.pageURL, r.pageRank, r.avgDuration) for r in ranks)
    assert actual_ranks == expected_ranks, ("rankings", list((actual_ranks-expected_ranks).items())[:3])
    columns = ["sourceIP", "destURL", "visitDate", "adRevenue", "userAgent", "countryCode", "languageCode", "searchWord", "duration"]
    expected_visits = collections.Counter((key, f[0], f[1], datetime.date.fromisoformat(f[2]), float(f[3]), *f[4:8], int(f[8]))
        for key, value in original["uservisits"] for f in [value.split(",")])
    actual_visits = collections.Counter((v.pageId, *(v[c] for c in columns)) for v in visits)
    assert actual_visits == expected_visits, ("uservisits", list((actual_visits-expected_visits).items())[:3])
    benchmark = spark._jvm.org.hibench.sparkbench.sql.ScalaSparkSQLBench
    scan = DataFrame(benchmark.query(spark._jsparkSession, "scan", root), spark).collect()
    assert collections.Counter(tuple(r) for r in scan) == collections.Counter(tuple(v[c] for c in columns) for v in visits)
    expected_aggregation = collections.defaultdict(float)
    expected_join = collections.defaultdict(lambda:[0,0,0.0])
    by_url = {r.pageURL:r.pageRank for r in ranks}
    for v in visits:
        expected_aggregation[v.sourceIP] += v.adRevenue
        if datetime.date(1999,1,1) <= v.visitDate <= datetime.date(2000,1,1) and v.destURL in by_url:
            group = expected_join[v.sourceIP]
            group[0] += by_url[v.destURL]; group[1] += 1; group[2] += v.adRevenue
    aggregation = DataFrame(benchmark.query(spark._jsparkSession, "aggregation", root), spark).collect()
    assert len(aggregation) == len(expected_aggregation)
    assert all(math.isclose(r.sumAdRevenue,expected_aggregation[r.sourceIP], rel_tol=1e-12,abs_tol=1e-12) for r in aggregation)
    join = DataFrame(benchmark.query(spark._jsparkSession, "join", root), spark).collect()
    assert len(join) == len(expected_join)
    for r in join:
        total,count,revenue = expected_join[r.sourceIP]
        assert math.isclose(r.avgPageRank,total/count,rel_tol=1e-12)
        assert math.isclose(r.totalRevenue,revenue,rel_tol=1e-12,abs_tol=1e-12)
    assert [r.totalRevenue for r in join] == sorted([r.totalRevenue for r in join], reverse=True)
    assert spark.conf.get("spark.sql.catalogImplementation") == "in-memory"
    print("HIBENCH_SQL_QUERIES_VERIFIED=" + json.dumps({"scan":len(scan),"aggregation":len(aggregation),"join":len(join),"catalog":"in-memory"}))
    print("HIBENCH_SQL_INPUT_VERIFIED=" + json.dumps({"rankings":len(ranks),"uservisits":len(visits),"comparison":"all fields, all records including original keys"}))
finally:
    spark.stop()
