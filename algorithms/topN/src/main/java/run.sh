#!/usr/bin/env bash
export ALGORITHMS_HOME=/root/algorithms/
export APP_JAR=${ALGORITHMS_HOME}/jar/topN-1.0-SNAPSHOT.jar
INPUT=hdfs://master:9000/secondary_sort/input
OUTPUT=hdfs://master:9000/secondary_sort/output
hadoop fs -rm ${INPUT}/sample_input.txt
hadoop fs -copyFromLocal ${ALGORITHMS_HOME}/input/sample_input.txt ${INPUT}/sample_input.txt
hadoop fs -rm -r ${OUTPUT}
PROG=job.SecondarySortDriver
hadoop jar ${APP_JAR} ${PROG} ${INPUT} ${OUTPUT}
