import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext


object Driver {

  def main(args: Array[String]): Unit ={
    // the day for wich we compute louvain modularity
    var date = args(0)
    val config = LouvainConfig(
      "mi2mi",
      "edges",
      "louvaincommunity",
      date,
      "output/louvain_filter/",
      2000,
      1)

    // def deleteOutputDir(config: LouvainConfig): Unit = {
    //   val hadoopConf = new org.apache.hadoop.conf.Configuration()

    //   val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)

    //   try {
    //     hdfs.delete(new org.apache.hadoop.fs.Path(config.outputDir), true)
    //   }
    //   catch {
    //     case _ : Throwable => { }
    //   }
    // }

    // val conf = new SparkConf().setAppName("ApproxTriangles").setMaster("local[2]")
    // conf.set("spark.default.parallelism", (8).toString)
    // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.set("spark.logConf", "true")
    // //sparkConf.getAll.foreach(println(_))
    // val sc = new SparkContext(conf)
    // Logger.getRootLogger.setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Louvain w/ Hive v1 date:" + date)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    // deleteOutputDir(config)

    val louvain = new Louvain()
    louvain.run(sc, hc, config)

  }
}
