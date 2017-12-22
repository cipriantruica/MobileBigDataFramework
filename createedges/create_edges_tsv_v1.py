from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType
import time
import sys
from datetime import datetime as dt

if __name__ == "__main__":
    # input directory with the tsv
    inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11*" 
    # output file for edges
    outputFile = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/edges_parquet_py" 
    # the file with the mearsuments
    printFile = "./results/runtime_Create_Edges_SparkSQL_TSV_v1_py.txt"

    fileSchema = StructType([
        StructField("Timestamp", LongType(), True),
        StructField("SquareID1", IntegerType(), True),
        StructField("SquareID2", IntegerType(), True),
        StructField("DIS", DoubleType(), True)])

    startTimeTotal = time.time()

    sc = SparkContext(appName="Create Edges")
    spark = SparkSession(sparkContext=sc)

    df = spark.read.csv(inputDirectory, header=False, sep='\t', schema=fileSchema)
    df.createOrReplaceTempView("mi2mi_table")
    sqlEdges = spark.sql("select Date, SID1, SID2, sum(DIS) EdgeCost from (select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 union all select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2) group by Date, SID1, SID2 order by Date, SID1, SID2")

    sqlEdges.write.format("parquet").save(outputFile)

    endTimeTotal = time.time()
    totalTime = endTimeTotal - startTimeTotal
    with open(printFile, 'a') as timeFile:
        timeFile.write("Total time (seconds): "+ str(endTimeTotal - startTimeTotal) + "\n")