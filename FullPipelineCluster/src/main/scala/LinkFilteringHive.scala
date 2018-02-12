import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
    It reads all the data from the tcv and computes the edges.
    It uses a Dataframe with a single select.
    It saves the output as parquet
*/

object LinkFilteringHive {
  def main(args: Array[String]): Unit = {
    // the file with the mearsuments
    val noTest = args(0)
    val printFile = "./results/runtime_Link_Filtering_Hive_test_" + noTest + ".txt"

    // Spark session
    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Link Filtering Hive Test no. " + noTest)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    hc.sql("drop table if exists mi2mi.LinkFiltering")

    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Link Filtering Hive")
    pw.println("Start time: " + Calendar.getInstance().getTime())

    val t0 = System.nanoTime()
    // read the data from the Hive (mi2mi - is the database name, edges is the table name)
    val tbl = hc.table("mi2mi.edges")
    // register the table so it can be used in SQL
    tbl.createOrReplaceTempView("edges")

    // create the alpha from equation (1) for each day 
    // equation (1) has the following solution after computations : alpha_ij = (1-p_ij)^(k - 1) where: 
    // k is the number of nodes
    // p_ij = w_ij/(sum_t (w_it) ) 
    // where: 
    //   w_ij is the Edge cost between node i and j
    //   sum_t is the sum of all the edge cost for node i
    // the following SQL query is used compute alpha for the entire data set and the results are stored in Hive
    // (currently in Hive are stored only the data for November)
    // see the ties_query.sql for the query explanations
    val ties = hc.sql("select MilanoDate, sid1, sid2, pow(1 - EdgeCost/(select sum(EdgeCost) node_strength from edges where sid1 != sid2 and MilanoDate=e.MilanoDate and sid1 = e.sid1 group by sid1), (select count(distinct sid1) - 1 from edges where MilanoDate=e.MilanoDate)) alpha from edges e where sid1 != sid2")
    ties.write.format("orc").saveAsTable("mi2mi.LinkFiltering")

    // tried a filter for alpha_threshold == 0.5
    // ties.filter(ties("alpha") <= 0.05).show()

    val t1 = System.nanoTime()

    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Link Filtering Hive Test no. " + noTest)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()
  }
}
