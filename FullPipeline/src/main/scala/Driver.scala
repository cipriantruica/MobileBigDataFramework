
import java.util.Calendar

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext


object Driver {

  def main(args: Array[String]): Unit ={
    // the file with the mearsuments
    val printFile = "./results/runtime_Louvain_Modularity_Hive.txt"
    // the day for wich we compute louvain modularity
    var date = "2013-11-01" // args(0)
    // the alpha threshold filter value
    var alphaThreshold = "0.005"
    // a constant for changing the edge cost factor
    var edgeCostFactor = "1000000"
    val config = LouvainConfig(
      "mi2mi",
      "edges",
      "LinkFiltering",
      "LouvainCommunity", // this table must be created manualy in Hive
      date,
      alphaThreshold,
      edgeCostFactor,
      2000,
      1)



    // Create spark configuration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Louvain w/ Hive v1 date:" + date)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // create the table if not exists
    hc.sql("CREATE TABLE IF NOT EXISTS " + config.hiveOutputTable + "(MilanoDate date, SID1 int, community int, level int, alphaThreshold int, edgeCostFactor int)")

    // PrintWriter
    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Create Edges Hive")
    pw.println("Start time: " + Calendar.getInstance().getTime())
    val t0 = System.nanoTime()

    // verify if the louvain modularity was already computed
    val louvainTbl = hc.table(config.hiveOutputTable)
    // register the table so it can be used in SQL
    louvainTbl.createOrReplaceTempView(config.hiveOutputTable)

    val exists = hc.sql("select count(MilanoDate) from " + config.hiveOutputTable + " where MilanoDate = '" + config.dateInput + "' and edgeCostFactor = " + config.edgeCostFactor + " and alphaThreshold = " + config.alphaThreshold + " * 1000" )
    // val exists = hc.sql("select count(MilanoDate), alphaThreshold from " + config.hiveOutputTable + " where MilanoDate = '" + config.dateInput + "' and edgeCostFactor = " + config.edgeCostFactor + " group by alphaThreshold")
    // exists.show()
    // println("exists: " + exists.first().getLong(0))
    if(exists.first().getLong(0) == 0){
      val louvain = new Louvain()
      louvain.run(sc, hc, config)  
    }
    else{
      println("Louvain Modularity was already computer for MilanoDate = " + config.dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
      pw.println("Louvain Modularity was already computer for MilanoDate = " + config.dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
    }

    val t1 = System.nanoTime()
    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0)/1e6))
    pw.println("*************************************************")

    pw.close()
    

  }
}