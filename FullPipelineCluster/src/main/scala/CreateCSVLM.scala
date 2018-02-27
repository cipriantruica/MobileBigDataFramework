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

/*
    Create A CSV from the Hive Database with Louvain Communities
*/

object CreateCSVLM {
  def main(args: Array[String]): Unit = {

    // the day for which we compute louvain modularity
    val dateInput = args(0)
    // the alpha threshold filter value
    val alphaThreshold = args(1)
    // a constant for changing the edge cost factor
    val edgeCostFactor = args(2)
    val noTest = args(3) // the test number, just for testing
    val printFile = "./results_lhm_csv/runtime_LMH_" + dateInput + "_test_" + noTest + "_alphaThreshold_" + alphaThreshold + "_edgeCostFactor_" + edgeCostFactor + ".txt"
    val csvPath = "./results_lhm_csv/runtime_LMH_" + dateInput + "_alphaThreshold_" + alphaThreshold + "_edgeCostFactor_" + edgeCostFactor + ".csv"

    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Create CSV Louvain Test no. " + noTest + " for date: " + dateInput + " and alphaThreshold = " + alphaThreshold + " and edgeCostFactor =" + edgeCostFactor)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)

    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Create CSV Louvain Communities")
    pw.println("Start time: " + Calendar.getInstance().getTime())

    val t0 = System.nanoTime()

    val lc = hc.table("mi2mi.louvaincommunity")
    lc.createOrReplaceTempView("louvaincommunity")

    val condition = "(select max(level) - 1 from louvaincommunity where milanodate=l.milanodate and alphathreshold=l.alphathreshold and edgecostfactor=l.edgecostfactor)"
    val query = "select l.sid1, community from louvaincommunity l where milanodate='" + dateInput + "' and alphathreshold= " + alphaThreshold + "*1000 and edgecostfactor=" + edgeCostFactor + " and l.level = " + condition
    val csvOutput = hc.sql(query)
    csvOutput.show()
    csvOutput.write.csv(csvPath)

    val t1 = System.nanoTime()
    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Create Edges Hive Test no. " + noTest)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()
  }
}
