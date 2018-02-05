import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext


object Driver {

  def main(args: Array[String]): Unit ={
    // the day for wich we compute louvain modularity
    var date = args(0)
    // the alpha threshold filter value
    var alphaThreshold = args(1)
    // a constant for changing the edge cost factor
    var edgeCostFactor = args(2)
    val config = LouvainConfig(
      "mi2mi",
      "edges",
      "LinkFiltering",
      "LouvainCommunity", // this table must be created manualy in Hive
      date,
      "output/louvain_filter/",
      2000,
      1,
      alphaThreshold,
      edgeCostFactor)

    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Louvain w/ Hive v1 date:" + date)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    // deleteOutputDir(config)
    // verify if the louvain modularity was already computed
    val louvainTbl = hc.table(config.hiveSchema + "." + config.hiveOutputTable)
    // register the table so it can be used in SQL
    louvainTbl.createOrReplaceTempView(config.hiveOutputTable)
    val exists = louvainTbl.sql("select count(*) from LouvainCommunity where MilanoDate = '" + config.dateInput + "' and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor).collectAsList().get(0).get(0).toInteger
    println(exists)
    // val louvain = new Louvain()
    // louvain.run(sc, hc, config)

  }
}
