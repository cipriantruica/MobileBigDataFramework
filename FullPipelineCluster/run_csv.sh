#!/bin/bash

# this code is used for computing louvain modularity

MODE=client
MASTER=yarn
NUM_EXECS=16
NUM_CORES=1
MEM_EXECS=3G
JAR_FILE=target/scala-2.11/fullpipelinecluster_2.11-0.1.jar
ALPHA0=1
ALPHA1=0.05
ALPHA2=0.01
ALPHA3=0.001
DATE='2013-11-08'
ECF=1000000000000 #EDGE COST FACTOR - used for normalizing the weight of a edge - this is the maximum needed for date 2013-11-08

#alpha=0.001
spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class CreateCSVLM $JAR_FILE $DATE $ALPHA3 $ECF 1 >> "results_lhm_csv/output_LMHCSV_"$DATE"_alpha_"$ALPHA3"_ECF_"$ECF
sleep 10

#alpha=0.01
spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class CreateCSVLM $JAR_FILE $DATE $ALPHA2 $ECF 1 >> "results_lhm_csv/output_LMHCSV_"$DATE"_alpha_"$ALPHA2"_ECF_"$ECF
sleep 10

#alpha=0.05
spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class CreateCSVLM $JAR_FILE $DATE $ALPHA1 $ECF 1 >> "results_lhm_csv/output_LMHCSV_"$DATE"_alpha_"$ALPHA1"_ECF_"$ECF
sleep 10

#alpha=1 - no filtering
spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class CreateCSVLM $JAR_FILE $DATE $ALPHA0 $ECF 1 >> "results_lhm_csv/output_LMHCSV_"$DATE"_alpha_"$ALPHA0"_ECF_"$ECF
sleep 10


