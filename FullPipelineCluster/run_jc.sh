#!/bin/bash

# this code is used to build the Jaccard Coefficient

MODE=client
MASTER=yarn
NUM_EXECS=16
NUM_CORES=1
MEM_EXECS=3G
NUM_TESTS=1
JAR_FILE=target/scala-2.11/fullpipelinecluster_2.11-0.1.jar
INPUT_DATA=hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11*

for i in `seq 1 $NUM_TESTS`
do
    # build Jaccard Coefficient table in Hive
	spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class JaccardCoefficient $JAR_FILE $i >> "results/output_JaccardCoefficientHive"
	sleep 10
done;
