import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io._
import org.apache.spark.{SparkConf, SparkContext}

/*
    It reads all the data from the parquet files.
    It uses a Dataframe with select.
    It saves the output as parquet
*/

object CreateEdgesSparkSQLParquet {
  def main(args: Array[String]): Unit = {
        // input directory with the (compressed) parquet files
        val inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/parquet" 
        // val inputDirectory = args(0)

        // output file for edges
        val outputFile = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/edges" 
        // val outputFile = args(1)

        // the file with the mearsuments
        val printFile = "./results/runtime_edges_Create_Edges_SparkSQL_Parquet.txt" 
        // val printFile = args(2)

        // PrintWriter
        import java.io._
        val pw = new PrintWriter(new File(printFile))

        pw.println("Create_Edges_SparkSQL_Parquet!")
        pw.println("Start time: " + Calendar.getInstance().getTime())

        val t0 = System.nanoTime()

        // Spark session
        val spark = SparkSession.builder.appName("Create_Edges_SparkSQL_Parquet").getOrCreate;

        // this returns a data frame from the parquet files
        val df = spark.read.parquet(inputDirectory)

        // create a view to query
        df.createOrReplaceTempView("mi2mi_table")
        
        // create the edges and save them to parquet files
        val sqlEdges = spark.sql("select Date, SID1, SID2, sum(DIS) EdgeCost from (select cast(Timestamp as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 union all select cast(Timestamp as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2) group by Date, SID1, SID2 order by Date, SID1, SID2")
            .write
            .option("codec", "snappy")
            .parquet(outputFile)

        
        val t1 = System.nanoTime()

        pw.println("End time: " + Calendar.getInstance().getTime())
        pw.println("Elaspsed time (ms): " + ((t1 - t0)/1e6))
        pw.println("*************************************************")

        pw.close()
    }
}
