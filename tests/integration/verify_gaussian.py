"""Validate clustering volume, original statistical profile and Parquet boundaries."""
import collections
import json
import math
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.linalg import VectorUDT

spark = SparkSession.builder.appName('Gaussian mixture profile verification').getOrCreate()
try:
    generator = spark._jvm.org.hibench.sparkbench.ml.GaussianDataGenerator
    seed = 1234
    def profile(n, k, d):
        return DataFrame(generator.profiles(spark._jsparkSession, n, k, d, seed, 0., 1000., .01, 100.), spark)
    def data(p, chunk, partitions):
        return DataFrame(generator.dataset(spark._jsparkSession, p._jdf, chunk, partitions, seed), spark)

    checks = []
    with open(sys.argv[1], encoding='utf-8') as f:
        reference = json.load(f)
    for workload in ('kmeans', 'gmm'):
        for scale, fields in reference[workload].items():
            n, k, d = (int(fields[key]) for key in ('num_of_samples', 'num_of_clusters', 'dimensions'))
            rows = profile(n, k, d).orderBy('clusterId').collect()
            assert sum(p.samples for p in rows) == n
            rng = spark._jvm.java.util.Random(seed)
            for cluster, p in enumerate(rows):
                assert p.samples == n // k + (cluster < n % k)
                assert p.startId == n // k * cluster + min(cluster, n % k)
                assert len(p.mean) == len(p.stddev) == d
                for mean, std in zip(p.mean, p.stddev):
                    assert mean == rng.nextDouble() * 1000
                    assert std == .01 + rng.nextDouble() * 99.99
            checks.append(dict(workload=workload, scale=scale, planned_samples=n, dimensions=d, clusters=k, allocation_verified=True))

    for n, k, chunk in ((101,5,7),(3,5,2),(100,5,1000)):
        p = profile(n,k,3)
        first = {r.id:tuple(r.features) for r in data(p,chunk,1).collect()}
        second = {r.id:tuple(r.features) for r in data(p,chunk,3).collect()}
        assert first == second and set(first) == set(range(n))
        checks.append(dict(samples=n, clusters=k, chunk=chunk, deterministic_across_partitions=True))

    # Match the original runner's conditional Gaussian profiles; RNG streams may differ.
    n,k,d = 100001,5,3
    rows = [(cluster, n//k*cluster+min(cluster,n%k), n//k+(cluster<n%k),
             [float(cluster*100+j*10) for j in range(d)], [float(1+cluster+j) for j in range(d)])
            for cluster in range(k)]
    p = spark.createDataFrame(rows, 'clusterId int,startId long,samples long,mean array<double>,stddev array<double>')
    generated = data(p,7000,3).collect()
    assert len(generated) == n and len({r.id for r in generated}) == n
    groups = collections.defaultdict(list)
    cross = collections.defaultdict(float)
    for r in generated:
        # The first cluster receives the remainder; use exact interval boundaries.
        cluster = next(i for i,row in enumerate(rows) if row[1] <= r.id < row[1]+row[2])
        normalized = []
        for feature,x in enumerate(r.features):
            value = (x-rows[cluster][3][feature])/rows[cluster][4][feature]
            groups[cluster,feature].append(value)
            normalized.append(value)
        for a in range(d):
            for b in range(a+1,d):
                cross[cluster,a,b] += normalized[a]*normalized[b]
    z = [x for values in groups.values() for x in values]
    mean = sum(z)/len(z)
    variance = sum(x*x for x in z)/len(z)-mean*mean
    tail = sum(abs(x)>2 for x in z)/len(z)
    with open(sys.argv[2], encoding='utf-8') as f:
        old = json.load(f)
    assert abs(mean-old['standardized_mean']) < .015
    assert abs(variance-old['standardized_variance']) < .025
    assert abs(tail-old['two_sigma_tail_fraction']) < .003
    for values in groups.values():
        m = sum(values)/len(values)
        v = sum(x*x for x in values)/len(values)-m*m
        assert abs(m)<.04 and abs(v-1)<.055
    covariance = []
    for (cluster,a,b), total in cross.items():
        first, second = groups[cluster,a], groups[cluster,b]
        value = total/len(first) - (sum(first)/len(first))*(sum(second)/len(second))
        assert abs(value)<.04
        covariance.append(abs(value))
    checks.append(dict(samples=n, standardized_mean=mean, standardized_variance=variance,
                       two_sigma_tail_fraction=tail, max_abs_conditional_covariance=max(covariance),
                       original_profile_equivalent=True))
    root = sys.argv[3]
    tiny = profile(30000,5,3)
    df = data(tiny,6000,2)
    df.write.mode('overwrite').option('maxRecordsPerFile',6000).parquet(root+'/samples')
    tiny.write.mode('overwrite').parquet(root+'/cluster')
    loaded = spark.read.parquet(root+'/samples')
    assert loaded.count()==30000 and loaded.select('id').distinct().count()==30000
    assert isinstance(loaded.schema['features'].dataType, VectorUDT), loaded.schema
    assert {r.id:tuple(r.features) for r in loaded.collect()} == {r.id:tuple(r.features) for r in df.collect()}
    assert all(r['count']<=6000 for r in loaded.selectExpr('input_file_name() file').groupBy('file').count().collect())
    checks.append(dict(samples=30000, dimensions=3, parquet_roundtrip=True, file_row_limit=6000))
    print('HIBENCH_GAUSSIAN_VERIFIED='+json.dumps(checks))
finally:
    spark.stop()
