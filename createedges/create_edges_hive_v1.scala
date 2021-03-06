import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.{SparkConf, SparkContext}

/*
    It reads all the data from the tcv and computes the edges.
    It uses a Dataframe with a single select.
    It saves the output as parquet
*/

object CreateEdgesHive_v1 {
  def main(args: Array[String]): Unit = {
        // input directory with the tsv
        val inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11*" 
        // val inputDirectory = args(0)

        // the file with the mearsuments
        val printFile = "./results/runtime_Create_Edges_Hive_v1.txt"
        // val printFile = args(2)

        // the tsv schema
        val fileSchema = StructType(Array(
            StructField("Timestamp", LongType, true),
            StructField("SquareID1", IntegerType, true),
            StructField("SquareID2", IntegerType, true),
            StructField("DIS", DoubleType, true)))

        // PrintWriter
        import java.io._
        val pw = new PrintWriter(new File(printFile))

        pw.println("Create_Edges_Hive_v1!")
        pw.println("Start time: " + Calendar.getInstance().getTime())

        val t0 = System.nanoTime()

        // Spark session
        // Create spark configuration
        val sparkConf = new SparkConf().setAppName("Create_Edges_Hive_v1")

        // Create spark context
        val sc = new SparkContext(sparkConf)
        // Create Hive context
        val hc = new HiveContext(sc)

        // read the data from the tsv files
        val df = hc.read.format("csv")
            .option("header", "false")
            .option("delimiter", "\t")
            .schema(fileSchema)
            .load(inputDirectory)

        // create a view to query
        df.createOrReplaceTempView("mi2mi_table")

        // create the edges and save them to parquet files
        val sqlEdges = hc.sql("select Date MilanoDate, SID1, SID2, sum(DIS) EdgeCost from (select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 union all select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2) group by Date, SID1, SID2 order by Date, SID1, SID2")
            .write.format("orc").saveAsTable("mi2mi.edges")

        val t1 = System.nanoTime()
        pw.println("End time: " + Calendar.getInstance().getTime())
        pw.println("Elaspsed time (ms): " + ((t1 - t0)/1e6))
        pw.println("*************************************************")

        pw.close()
    }
}
