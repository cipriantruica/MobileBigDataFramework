#!/bin/bash

for j in `seq 1 1`
do
    if [ "$j" -lt "10" ]
    then
      date='2013-11-0'$j
    else
      date='2013-11-'$j
    fi;


    for i in `seq 1 1`
    do
      #spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class Driver target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $date 0.05 1000000 1 $i >> "results/output_lmh_1TBL_"$date
      #sleep 10

      spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class Driver target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $date 0.05 1000000 2 $i >> "results/output_lmh_2TBL_"$date
      #sleep 10

  done;
done;
