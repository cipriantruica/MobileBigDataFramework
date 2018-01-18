from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType
import time


if __name__ == "__main__":
    inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-0*"

    # the file with the measurements
    printFile = "./time.txt"

    fileSchema = StructType([
        StructField("Timestamp", LongType(), True),
        StructField("SquareID1", IntegerType(), True),
        StructField("SquareID2", IntegerType(), True),
        StructField("DIS", DoubleType(), True)])

    startTimeTotal = time.time()

    sc = SparkContext(appName="Create Edges Hive")
    hc = HiveContext(sparkContext=sc)

    df = hc.read.csv(inputDirectory, header=False, sep='\t', schema=fileSchema)
    df.createOrReplaceTempView("mi2mi_table")

    sqlEdges = hc.sql("select Date, SID1, SID2, sum(DIS) EdgeCost from (select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 union all select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2) group by Date, SID1, SID2 order by Date, SID1, SID2")

    sqlEdges.write.format("orc").saveAsTable("mi2mi.edges")


    endTimeTotal = time.time()
    totalTime = endTimeTotal - startTimeTotal
    with open(printFile, 'a') as timeFile:
        timeFile.write("Total time (seconds): " + str(totalTime) + "\n")
