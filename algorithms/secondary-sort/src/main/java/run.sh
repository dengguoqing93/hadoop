#!/usr/bin/env bash
export ALGORITHMS_HOME=/root/algorithms/jar
export APP_JAR=${ALGORITHMS_HOME}/secondary-sort-1.0-SNAPSHOT.jar
INPUT=hdfs://master:9000/secondary_sort/input
OUTPUT=hdfs://master:9000/secondary_sort/output
hadoop fs -rm -r ${OUTPUT}
PROG=job.SecondarySortDriver
hadoop jar ${APP_JAR} ${PROG} ${INPUT} ${OUTPUT}

