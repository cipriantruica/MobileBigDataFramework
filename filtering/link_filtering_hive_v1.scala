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

object LinkFilteringHive_v1 {
  def main(args: Array[String]): Unit = {
        // the file with the mearsuments
        val printFile = "./results/runtime_Link_Filtering_Hive_v1.txt"

        // Spark session
        // Create spark configuration
        val sparkConf = new SparkConf().setAppName("Link Filtering Hive v1")

        // Create spark context
        val sc = new SparkContext(sparkConf)
        // Create Hive context
        val hc = new HiveContext(sc)

        import java.io._
        val pw = new PrintWriter(new File(printFile))

        pw.println("Link Filtering Hive v1")
        pw.println("Start time: " + Calendar.getInstance().getTime())

        val t0 = System.nanoTime()
        // read the data from the Hive
        val tbl = hc.table("mi2mi.edges")
        tbl.registerTempTable("edges")

        // create a view to query
        // df.createOrReplaceTempView("mi2mi_table")

        // create the edges and save them to parquet files
        // val ties = hc.sql("select sid1, sid2, 1 - (select count(distinct sid1) - 2 from edges where MilanoDate=e.MilanoDate)*(EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) + pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate))) /((select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate) * (EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) - 1)) alpha from edges e where MilanoDate='2013-11-01'")
        val ties = hc.sql("select MilanoDate, sid1, sid2, 1 - (select count(distinct sid1) - 2 from edges where MilanoDate=e.MilanoDate)*(EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) + pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate))) /((select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate) * (EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1) - 1)) alpha from edges e")
        ties.write.format("orc").saveAsTable("mi2mi.LinkFiltering")
        ties.filter(ties("alpha") <= 0.05).show()

        val t1 = System.nanoTime()

        pw.println("End time: " + Calendar.getInstance().getTime())
        pw.println("Elaspsed time (ms): " + ((t1 - t0)/1e6))
        pw.println("*************************************************")

        pw.close()
    }
}
