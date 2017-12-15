import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._
import org.apache.spark.{SparkConf, SparkContext}


/*
    Read parquet
*/
object ReadParquet {
  def main(args: Array[String]): Unit = {
        // input directory with the (compressed) parquet files
        val inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/parquet" 
        // val inputDirectory = args(0)

        val t0 = System.nanoTime()

        // Spark session
        val spark = SparkSession.builder.appName("Merging").getOrCreate;

        // this returns a data frame
        val df = spark.read.parquet(inputDirectory)
        df.printSchema()
        // just a simple SQL count
        df.createOrReplaceTempView("mi2mi_table")
        val sqlDF = spark.sql("select count(*) total_rows from mi2mi_table")
        
        sqlDF.show()

        df.take(5).foreach(println)
        val t1 = System.nanoTime()

        println("*************************************************")
        println("Elaspsed time (ms): " + ((t1 - t0)/1e6))
        println("*************************************************")
    }
}
