"""Verify materialized SQL controller inputs and outputs independently of benchmark code."""
import collections
import datetime
import json
import math
import sys
from pyspark.sql import SparkSession, DataFrame
spark = SparkSession.builder.appName("SQL controller output verification").getOrCreate()
try:
    with open(sys.argv[1],encoding="utf-8") as f: original=json.load(f)
    with open(sys.argv[2],encoding="utf-8") as f: plan=json.load(f)
    ranks=collections.Counter((k,v.split(",")[0],int(v.split(",")[1]),int(v.split(",")[2])) for k,v in original["rankings"])
    visits=collections.Counter((k,f[0],f[1],datetime.date.fromisoformat(f[2]),float(f[3]),*f[4:8],int(f[8])) for k,v in original["uservisits"] for f in [v.split(",")])
    columns=["sourceIP","destURL","visitDate","adRevenue","userAgent","countryCode","languageCode","searchWord","duration"]
    scan=collections.Counter(row[1:] for row in visits.elements())
    aggregation=collections.defaultdict(float); join=collections.defaultdict(lambda:[0,0,0.0])
    rank_urls={row[1]:row[2] for row in ranks}
    for row in visits.elements():
        aggregation[row[1]]+=row[4]
        if datetime.date(1999,1,1)<=row[3]<=datetime.date(2000,1,1) and row[2] in rank_urls:
            group=join[row[1]];group[0]+=rank_urls[row[2]];group[1]+=1;group[2]+=row[4]
    for job in plan:
        actual=spark.read.parquet(job["input"]+"/rankings").collect()
        assert collections.Counter((r.pageId,r.pageURL,r.pageRank,r.avgDuration) for r in actual)==ranks
        actual=spark.read.parquet(job["input"]+"/uservisits").collect()
        assert collections.Counter((r.pageId,*(r[c] for c in columns)) for r in actual)==visits
        result=spark.read.parquet(job["output"]).collect()
        if job["workload"]=="scan": assert collections.Counter(tuple(r[c] for c in columns) for r in result)==scan
        elif job["workload"]=="aggregation":
            assert len(result)==len(aggregation)
            assert all(math.isclose(r.sumAdRevenue,aggregation[r.sourceIP],rel_tol=1e-12,abs_tol=1e-12) for r in result)
        else:
            assert len(result)==len(join)
            assert all(math.isclose(r.avgPageRank,join[r.sourceIP][0]/join[r.sourceIP][1],rel_tol=1e-12) and math.isclose(r.totalRevenue,join[r.sourceIP][2],rel_tol=1e-12,abs_tol=1e-12) for r in result)
        print("HIBENCH_SQL_CONTROLLER_VERIFIED="+json.dumps({"workload":job["workload"],"rankings":sum(ranks.values()),"uservisits":sum(visits.values()),"output_rows":len(result)}))
    edge="hdfs://spark-cluster-master:9000/HiBench-Validation/sql-query-boundaries-20261008"
    rows=[("A","x","1999-01-01",1.0),("A","y","2000-01-01",2.0),("A","x","1998-12-31",4.0),("B","x","2000-01-02",8.0),("B","missing","1999-06-01",16.0),("C","x","1999-06-01",5.0)]
    schema="sourceIP string,destURL string,visitDate date,adRevenue double,userAgent string,countryCode string,languageCode string,searchWord string,duration int"
    spark.createDataFrame([(ip,url,datetime.date.fromisoformat(date),revenue,"agent","USA","USA-EN","word",1) for ip,url,date,revenue in rows],schema).write.mode("overwrite").parquet(edge+"/uservisits")
    spark.createDataFrame([("x",10,1),("y",30,1)],"pageURL string,pageRank int,avgDuration int").write.mode("overwrite").parquet(edge+"/rankings")
    bench=spark._jvm.org.hibench.sparkbench.sql.ScalaSparkSQLBench
    actual=DataFrame(bench.query(spark._jsparkSession,"aggregation",edge),spark).collect()
    assert {r.sourceIP:r.sumAdRevenue for r in actual}=={"A":7.0,"B":24.0,"C":5.0}
    actual=DataFrame(bench.query(spark._jsparkSession,"join",edge),spark).collect()
    assert [(r.sourceIP,r.avgPageRank,r.totalRevenue) for r in actual]==[("C",10.0,5.0),("A",20.0,3.0)]
    print("HIBENCH_SQL_BOUNDARIES_VERIFIED=duplicate groups; inclusive dates; unmatched URLs; averages; descending revenues")
finally:
    spark.stop()
