# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""Verify SQL Scan's SequenceFile copy independently, on tiny inputs only."""
import sys,csv,json
from collections import Counter
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("HiBench SQL correctness").getOrCreate()
jvm=spark.sparkContext._jvm
conf=spark.sparkContext._jsc.hadoopConfiguration()
def records(path):
    location=jvm.org.apache.hadoop.fs.Path(path)
    fs=location.getFileSystem(conf)
    result=Counter()
    for status in fs.listStatus(location):
        if not status.isFile() or status.getPath().getName().startswith(("_",".")):continue
        reader=jvm.org.apache.hadoop.io.SequenceFile.Reader(fs,status.getPath(),conf)
        try:
            assert reader.getValueClass().getName()=="org.apache.hadoop.io.Text"
            key=jvm.org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getKeyClass(),conf)
            value=jvm.org.apache.hadoop.util.ReflectionUtils.newInstance(reader.getValueClass(),conf)
            while reader.next(key,value):
                row=next(csv.reader([value.toString()]))
                assert len(row)==9, "Unexpected uservisits schema"
                row[3]=float(row[3]); row[8]=int(row[8])
                result[tuple(row)]+=1
        finally:reader.close()
    return result
try:
    expected=records(sys.argv[1]); actual=records(sys.argv[2])
    assert expected==actual, "SQL Scan changed, lost or duplicated visits"
    assert expected, "Empty test dataset"
    print("HIBENCH_CORRECTNESS="+json.dumps(dict(sql_scan=True,rows=sum(actual.values()))))
finally:spark.stop()
