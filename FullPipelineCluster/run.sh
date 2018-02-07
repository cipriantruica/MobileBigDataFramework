#!/bin/bash


for i in `seq 1 1`
do
	# spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class CreateEdgesHive target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $i >> "results/output_CreateEdgesHive"
	# spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class LinkFilteringHive target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $i >> "results/output_LinkFilteringHive"
	spark-submit --master yarn --deploy-mode client --num-executors 16 --executor-cores 1 --executor-memory 3G --class CreateEdgesAlphaHive target/scala-2.11/fullpipelinecluster_2.11-0.1.jar $i >> "results/output_CreateEdgesHive"
done;
