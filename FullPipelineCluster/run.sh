#!/bin/bash

# this code is used populating the database

MODE=client
MASTER=yarn
NUM_EXECS=16
NUM_CORES=1
MEM_EXECS=3G
NUM_TESTS=10
JAR_FILE=target/scala-2.11/fullpipelinecluster_2.11-0.1.jar
INPUT_DATA=hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11*

for i in `seq 1 $NUM_TESTS`
do
    # build the edges table in Hive
	spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class CreateEdgesHive $JAR_FILE $INPUT_DATA $i >> "results/output_CreateEdgesHive"
	sleep 10

	# build the linkfiltering table in Hive
	spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class LinkFilteringHive $JAR_FILE $i >> "results/output_LinkFilteringHive"
	sleep 10

    # build edgesalpha table that stores both edges and linkfiletering in one table in Hive
	spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class CreateEdgesAlphaHive $JAR_FILE $i >> "results/output_CreateEdgesAlphaHive"
	sleep 10
done;
