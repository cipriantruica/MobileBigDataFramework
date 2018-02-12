#!/bin/bash

MODE=client
MASTER=local[16]
NUM_EXECS=16
NUM_CORES=1
MEM_EXECS=3G
NUM_TESTS=10
JAR_FILE=target/scala-2.11/fullpipelinecluster_2.11-0.1.jar
ALPHA=0.05
ECF=1000000 #EDGE COST FACTOR


for NO_TBL in `seq 1 2`
do
  for j in `seq 1 30`
  do
      if [ "$j" -lt "10" ]
      then
        date='2013-11-0'$j
      else
        date='2013-11-'$j
      fi;


      for i in `seq 1 10`
      do
        spark-submit --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class Driver $JAR_FILE $date $ALPHA $ECF $NO_TBL $i >> "results/output_LMH_"$NO_TBL"TBL_"$date
        sleep 10
    done;
  done;
done;
