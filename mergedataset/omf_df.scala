import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.{SparkConf, SparkContext}

/*
    This is the code I wrote that uses a Dataframe
*/

object OneMergedFileDF {
  def main(args: Array[String]): Unit = {
        // input directory with the tsv or a list of files or only some files
        val inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-0*" 
        // val inputDirectory = args(0)

        // output file - the merged one
        val outputFile = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/MI2MI_November.txt" 
        // val outputFile = args(1)

        // the file with the mearsuments
        val printFile = "/home/ciprian/mergetime_df.txt" 
        // val printFile = args(2)

        // the tsc schema
        val fileSchema = StructType(Array(
            StructField("Timestamp", LongType, true),
            StructField("SquareID1", IntegerType, true),
            StructField("SquareID2", IntegerType, true),
            StructField("DIS", DoubleType, true)))

        // Epoch to timestamp
        val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

        // PrintWriter
        import java.io._
        val pw = new PrintWriter(new File(printFile))

        pw.println("Hello, merging started!")
        pw.println("Start time: " + Calendar.getInstance().getTime())

        val t0 = System.nanoTime()

        // Spark session
        val spark = SparkSession.builder.appName("Merging").getOrCreate;

        

        val df = spark.read.format("csv")
            .option("header", "false")
            .option("delimiter", "\t")
            // .option("inferSchema", "true") // if you want to auto infer the tsv schema
            .schema(fileSchema)
            .load(inputDirectory)
        

        val rddFile = df.rdd.map(line => (
                sdf.format(line(0)),
                line(1),
                line(2),
                line(3)))
            .saveAsTextFile(outputFile)

        val t1 = System.nanoTime()
        pw.println("End time: " + Calendar.getInstance().getTime())
        pw.println("Elaspsed time (ms): " + ((t1 - t0)/1e6))
        pw.println("*************************************************")

        pw.close()
    }
}

