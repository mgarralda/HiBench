"""Independent original RNG/formula reference; bounded small-data checks only."""
import json
import math
import sys
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('HiBench linear generator verification').getOrCreate()
try:
    generator = spark._jvm.org.hibench.sparkbench.ml.LinearRegressionDataGenerator
    def dataset(n, d, p, seed=42, eps=1.):
        return DataFrame(generator.dataset(spark._jsparkSession, n, d, p, seed, eps), spark)

    def reference(n, d, p, seed, eps):
        rng = spark._jvm.java.util.Random(seed)
        weights = [rng.nextDouble() - .5 for _ in range(d)]
        result = {}
        for part in range(p):
            rng = spark._jvm.java.util.Random(seed ^ part)
            for i in range(n * part // p, n * (part + 1) // p):
                x = [(rng.nextDouble() - .5) * math.sqrt(12 * (1 / 3)) for _ in range(d)]
                y = sum(w * v for w, v in zip(weights, x)) + eps * rng.nextGaussian()
                result[i] = (y, x)
        return result, weights

    checks = []
    for n, d, p, seed, eps in ((101, 9, 7, 42, 1.), (3, 5, 8, 42, 1.),
                               (100, 8, 2, 17, 0.), (100, 8, 2, 0, 2.)):
        expected, _ = reference(n, d, p, seed, eps)
        actual = dataset(n, d, p, seed, eps).collect()
        assert len(actual) == n and len({r.id for r in actual}) == n
        for row in actual:
            y, x = expected[row.id]
            assert list(row.features) == x
            assert math.isclose(row.label, y, rel_tol=0, abs_tol=1e-13)
        original = DataFrame(
            spark._jvm.org.hibench.sparkbench.ml.LinearRegressionOriginalReference.reference(
                spark._jsparkSession, n, d, p, seed, eps), spark).collect()
        original_values = sorted((r.label, tuple(r.features)) for r in original)
        new_values = sorted((r.label, tuple(r.features)) for r in actual)
        assert new_values == original_values, 'Frozen original implementation differs'
        checks.append(dict(rows=n, dimensions=d, partitions=p, seed=seed, noise_std=eps,
                           original_formula_verified=True, frozen_original_exact_match=True))

    # Conditional label noise, not only marginal feature/label distributions.
    n, d, p = 20001, 8, 3
    rng = spark._jvm.java.util.Random(42)
    weights = [rng.nextDouble() - .5 for _ in range(d)]
    rows = dataset(n, d, p).collect()  # bounded verification fixture, never a production path
    residuals = [r.label - sum(w * x for w, x in zip(weights, r.features)) for r in rows]
    mean = sum(residuals) / n
    variance = sum((r - mean)**2 for r in residuals) / n
    assert abs(mean) < .04 and abs(variance - 1) < .05
    for j in range(d):
        values = [float(r.features[j]) for r in rows]
        assert all(-1 <= x < 1 for x in values)
        mu = sum(values) / n
        var = sum((x - mu)**2 for x in values) / n
        covariance = sum((x - mu) * (e - mean) for x, e in zip(values, residuals)) / n
        assert abs(mu) < .025 and abs(var - 1/3) < .02 and abs(covariance) < .025
    checks.append(dict(rows=n, dimensions=d, noise_mean=mean, noise_variance=variance,
                       conditional_profile_verified=True))
    path = sys.argv[1]
    dataset(101, 9, 7).write.mode('overwrite').parquet(path)
    expected, _ = reference(101, 9, 7, 42, 1.)
    loaded = spark.read.parquet(path).collect()
    assert len(loaded) == 101
    for row in loaded:
        assert list(row.features) == expected[row.id][1]
        assert math.isclose(row.label, expected[row.id][0], rel_tol=0, abs_tol=1e-13)
    print('HIBENCH_LINEAR_VERIFIED=' + json.dumps(checks))
finally:
    spark.stop()
