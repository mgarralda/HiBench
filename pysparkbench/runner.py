# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""spark-submit entry point; algorithms live in their individual modules."""
import importlib
import json
import platform
import sys
import time
from importlib.metadata import version, PackageNotFoundError
from pyspark.sql import SparkSession
from pysparkbench.registry import MODULES


def execute(spark, module_name, args):
    module = importlib.import_module(module_name)
    return module.run(spark, module.parse(args))


def main(args):
    name, *workload_args = args
    allowed = {'pysparkbench.' + module for module in MODULES.values()}
    allowed.update('pysparkbench.sql.' + workload for workload in ('scan', 'aggregation', 'join'))
    if name not in allowed:
        raise ValueError('Unknown PySpark workload: ' + name)
    module = importlib.import_module(name)
    params = module.parse(workload_args)
    packages = {}
    for package in ('numpy', 'pandas', 'pyarrow'):
        try:
            packages[package] = version(package)
        except PackageNotFoundError:
            packages[package] = None
    print('HIBENCH_PYSPARK_RUNTIME=' + json.dumps({'python': platform.python_version(), 'packages': packages}, sort_keys=True))
    builder = SparkSession.builder.appName(name)
    if hasattr(params, 'maxResultSize'):
        builder = builder.config('spark.driver.maxResultSize', params.maxResultSize)
    if name.startswith('pysparkbench.sql.'):
        builder = builder.config('spark.sql.catalogImplementation', 'in-memory')
    lifecycle_started = time.perf_counter()
    spark = builder.getOrCreate()
    startup_s = time.perf_counter() - lifecycle_started
    checks = None
    try:
        checks = module.run(spark, params)
        if checks is not None:
            print('HIBENCH_WORKLOAD_CHECK=' + json.dumps({'module': name, **checks}, sort_keys=True))
    finally:
        stop_started = time.perf_counter()
        spark.stop()
    if checks and 'measurement' in checks:
        measurement = checks['measurement']
        measurement['phase_seconds'].update(spark_startup_s=startup_s,
            spark_stop_s=time.perf_counter() - stop_started,
            application_lifecycle_s=time.perf_counter() - lifecycle_started)
        print('HIBENCH_WORKLOAD_MEASUREMENT=' + json.dumps(measurement, sort_keys=True), flush=True)


if __name__ == '__main__':
    main(sys.argv[1:])
