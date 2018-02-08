#!/bin/bash


for i in `seq 11 20`
do
	# spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class CreateEdgesHive target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $i >> "results/output_CreateEdgesHive"
	# sleep 10
	
	spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class LinkFilteringHive target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $i >> "results/output_LinkFilteringHive"
	sleep 10

	# spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class CreateEdgesAlphaHive target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $i >> "results/output_CreateEdgesAlphaHive"
	# sleep 10
done;
