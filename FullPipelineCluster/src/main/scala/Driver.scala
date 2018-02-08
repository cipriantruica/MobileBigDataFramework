import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object Driver {

  def main(args: Array[String]): Unit = {

    // the day for wich we compute louvain modularity
    var date = args(0)
    // the alpha threshold filter value
    var alphaThreshold = args(1)
    // a constant for changing the edge cost factor
    var edgeCostFactor = args(2)
    var noTables = args(3).toInt // use 1 for  EdgesAlpha table or 2 for Edges + LinkFiltering tables
    val config = LouvainConfig(
      "mi2mi",
      "edges",
      "linkfiltering",
      "edgesalpha",
      "louvaincommunity",
      noTables,
      date,
      alphaThreshold,
      edgeCostFactor,
      2000,
      1)
    // the file with the mearsuments
    val printFile = "./results/runtime_LMH_" + date + "_noTbls_" + noTables + "_test_" + args(4) + ".txt"
    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Louvain with Hive Test no. " + args(4) + " for date: " + config.dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    // create the table if it doesn't exists
    hc.sql("DROP TABLE IF EXISTS " + config.hiveSchema + "." + config.hiveOutputTable)
    hc.sql("CREATE TABLE IF NOT EXISTS " + config.hiveSchema + "." + config.hiveOutputTable + "(MilanoDate date, SID1 int, community int, level int, alphaThreshold int, edgeCostFactor int)")

    // PrintWriter
    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Create Louvain Modularity Hive")
    pw.println("Start time: " + Calendar.getInstance().getTime())

    val t0 = System.nanoTime()

    // verify if the louvain modularity was already computed
    val louvainTbl = hc.table(config.hiveSchema + "." + config.hiveOutputTable)
    // register the table so it can be used in SQL
    louvainTbl.createOrReplaceTempView(config.hiveOutputTable)
    val exists = hc.sql("select count(MilanoDate) from " + config.hiveOutputTable + " where MilanoDate = '" + config.dateInput + "' and edgeCostFactor = " + config.edgeCostFactor + " and alphaThreshold = " + config.alphaThreshold + " * 1000")

    if (exists.first().getLong(0) == 0) {
      val louvain = new Louvain()
      louvain.run(sc, hc, config)
    }
    else {
      println("already computer for MilanoDate = " + config.dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
      pw.println("already computer for MilanoDate = " + config.dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
    }

    val t1 = System.nanoTime()
    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Louvain with Hive Test no. " + args(4) + " for date: " + config.dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()

  }
}
