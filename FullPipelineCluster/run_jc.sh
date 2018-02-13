#!/bin/bash

# this code is used to build the Jaccard Coefficient

MODE=client
MASTER=yarn
NUM_EXECS=5
NUM_CORES=6
MEM_EXECS=8G
NUM_TESTS=1
JAR_FILE=target/scala-2.11/fullpipelinecluster_2.11-0.1.jar
DRIVER_MEM=4G
DRIVER_CORES=6


for i in `seq 1 $NUM_TESTS`
do
    # build Jaccard Coefficient table in Hive
	spark-submit --master $MASTER --deploy-mode $MODE --driver-memory $DRIVER_MEM --driver-cores $DRIVER_CORES --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class JaccardCoefficient $JAR_FILE $i >> "results/output_JaccardCoefficientHive"
	sleep 10
done;
