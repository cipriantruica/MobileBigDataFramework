import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Copyright (C) 2018 Ciprian-Octavian Truică <ciprian.truica@cs.pub.ro>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

object Driver {

  def main(args: Array[String]): Unit = {
    // the day for wich we compute louvain modularity
    var dateInput = args(0)
    // the alpha threshold filter value
    var alphaThreshold = args(1)
    // a constant for changing the edge cost factor
    var edgeCostFactor = args(2)
    var noTables = args(3).toInt // use 1 for  EdgesAlpha table or 2 for Edges + LinkFiltering tables
    val noTest = args(4) // the test number, just for testing
    val config = LouvainConfig(
      "mi2mi",
      "edges",
      "linkfiltering",
      "edgesalpha",
      "louvaincommunity",
      noTables,
      alphaThreshold,
      edgeCostFactor,
      2000,
      1)
    // the file with the mearsuments
    val printFile = "./results/runtime_LMH_" + dateInput + "_noTbls_" + noTables + "_test_" + noTest + "_alphaThreshold_" + config.alphaThreshold + "_edgeCostFactor_" + config.edgeCostFactor + ".txt"
    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Louvain with Hive Test no. " + noTest + " for " + noTables + " tables with date: " + dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    // drop table for testing
    hc.sql("DROP TABLE IF EXISTS " + config.hiveSchema + "." + config.hiveOutputTable)
    // create the table if it doesn't exists
    hc.sql("CREATE TABLE IF NOT EXISTS " + config.hiveSchema + "." + config.hiveOutputTable + "(MilanoDate date, SID1 int, community int, level int, alphaThreshold int, edgeCostFactor bigint)")

    // PrintWriter
    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Louvain with Hive Test no. " + noTest + " for " + noTables + " tables with date: " + dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
    pw.println("Start time: " + Calendar.getInstance().getTime())

    val t0 = System.nanoTime()

    // verify if the louvain modularity was already computed
    val louvainTbl = hc.table(config.hiveSchema + "." + config.hiveOutputTable)
    // register the table so it can be used in SQL
    louvainTbl.createOrReplaceTempView(config.hiveOutputTable)
    val exists = hc.sql("select count(MilanoDate) from " + config.hiveOutputTable + " where MilanoDate = '" + dateInput + "' and edgeCostFactor = " + config.edgeCostFactor + " and alphaThreshold = " + config.alphaThreshold + " * 1000")

    if (exists.first().getLong(0) == 0) {

      val louvain = new Louvain()
      louvain.run(sc, hc, config, dateInput)
      // TO DO - make louvain for all the dates using map
      // See how to modify the conde so that we don't send SparkContext and HiveContext to each worker!!!
      // be carefull which table you use
      // val edgesTbl = hc.table(config.hiveSchema + "." + config.hiveInputTable)
      // edgesTbl.createOrReplaceTempView(config.hiveInputTable)
      // hc.sql("select distinct MilanoDate from " + config.hiveInputTable).rdd.map(row => louvain.run(sc, hc, config, row(0).toString))

    }
    else {
      println("already computer for MilanoDate = " + dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
      pw.println("already computer for MilanoDate = " + dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
    }

    val t1 = System.nanoTime()
    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Louvain with Hive Test no. " + noTest + " for " + noTables + " tables with date: " + dateInput + " and alphaThreshold = " + config.alphaThreshold + " and edgeCostFactor =" + config.edgeCostFactor)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()

  }
}
