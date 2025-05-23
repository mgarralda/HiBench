#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -u

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
export HADOOP_HOME=/home/sparker/hadoop-3.3.2
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH

SKIP_PREPARE_IF_EXISTS=false

# Parse arguments
for arg in "$@"; do
    if [[ "$arg" == "--skip-if-exists" ]]; then
        SKIP_PREPARE_IF_EXISTS=true
    fi
done


current_dir=`dirname "$0"`
root_dir=`cd "${current_dir}/.."; pwd`

echo ${current_dir}
echo ${root_dir}

. ${root_dir}/bin/functions/color.sh

prepare_data=true
workloads_run=true
datasize="$(grep '^hibench.scale.profile' "$root_dir/conf/hibench.conf" | awk '{print $2}' | tr -d '\r\n')"

for benchmark in `cat $root_dir/conf/benchmarks.lst`; do
	if [[ $benchmark == \#* ]]; then
		continue
    fi

	# Must be at top of the file
	if [[ $benchmark == \skip.prepare.data ]]; then
		prepare_data=false
		echo -e "${UYellow}${BYellow}*** Skipping data preparation for workloads ***${BYellow}${Color_Off}"
		continue
	fi

    # Must be at top of the file
    if [[ $benchmark == \skip.workloads.run ]]; then
      workloads_run=false
      echo -e "${UYellow}${BYellow}*** Skipping Spark running workloads ***${BYellow}${Color_Off}"
      continue
    fi

    benchmark="${benchmark/.//}"
    WORKLOAD=$root_dir/bin/workloads/${benchmark}

	if $prepare_data; then
		echo -e "${UYellow}${BYellow}Prepare ${Yellow}${UYellow}${benchmark} ${BYellow}...${Color_Off}"
		echo -e "${BCyan}Exec script: ${Cyan}${WORKLOAD}/prepare/prepare.sh${Color_Off}"

    name=${benchmark##*/}  # Extract workload name (e.g., "als" or "wordcount")
    echo "Workload: $name"

    if [ ${#name} -le 3 ]; then
        name_capitalized=$(echo "$name" | tr '[:lower:]' '[:upper:]')
    else
        name_capitalized="${name^}"
    fi

    hdfs_path="/HiBench/${name_capitalized}/Input/${datasize}"
    echo "Checking HDFS path: $hdfs_path"

		if hdfs dfs -test -d $hdfs_path; then

			if $SKIP_PREPARE_IF_EXISTS; then
				echo "HDFS input directory exists: $hdfs_path"
				echo "Skipping preparation as --skip-if-exists was passed."
			else
				echo "Re-preparing input data as --skip-if-exists not passed."
				"${WORKLOAD}/prepare/prepare.sh"
				result=$?
				if [ $result -ne 0 ]
				then
					echo "ERROR: ${benchmark} prepare failed!"
						exit $result
				fi
			fi
		else
			echo "HDFS input directory does not exist!"
			"${WORKLOAD}/prepare/prepare.sh"

			result=$?
			if [ $result -ne 0 ]
			then
				echo "ERROR: ${benchmark} prepare failed!"
					exit $result
			fi
		fi
	fi

  if $workloads_run; then
    for framework in `cat $root_dir/conf/frameworks.lst`; do
		if [[ $framework == \#* ]]; then
			continue
		fi

		if [ $benchmark == "micro/dfsioe" ] && [ $framework == "spark" ]; then
			continue
		fi
		if [ $benchmark == "micro/repartition" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "websearch/nutchindexing" ] && [ $framework == "spark" ]; then
			continue
		fi
		if [ $benchmark == "graph/nweight" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "graph/pagerank" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/lr" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/als" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/svm" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/pca" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/gbt" ] && [ $framework == "hadoop" ]; then
			 continue
		fi
		if [ $benchmark == "ml/rf" ] && [ $framework == "hadoop" ]; then
			  continue
		fi
		if [ $benchmark == "ml/svd" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/linear" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/lda" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/gmm" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/correlation" ] && [ $framework == "hadoop" ]; then
			continue
		fi
		if [ $benchmark == "ml/summarizer" ] && [ $framework == "hadoop" ]; then
			 continue
		fi

		echo -e "${UYellow}${BYellow}Run ${Yellow}${UYellow}${benchmark}/${framework}${Color_Off}"
		echo -e "${BCyan}Exec script: ${Cyan}$WORKLOAD/${framework}/run.sh${Color_Off}"
		$WORKLOAD/${framework}/run.sh
		echo -e "$WORKLOAD/${framework}/run.sh"

		result=$?
		if [ $result -ne 0 ]
		then
			echo -e "${On_IRed}ERROR: ${benchmark}/${framework} failed to run successfully.${Color_Off}"
				exit $result
		fi
    done
  fi

done

echo "Run all done!"
