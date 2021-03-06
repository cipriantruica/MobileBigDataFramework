import java.util.Calendar

import org.apache.spark.rdd.RDD
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
 Build Jaccard Coefficient for the entire dataset using DataFrames
 !!! THIS IS NOT TESTED !!!
*/

object JaccardCoefficient {

  def computeJC(
                 hc: HiveContext,
                 dateInput: String): Boolean = {
    val query_union = "select MilanoDate, SID1, SID2 common_node, EdgeCost from edges where MilanoDate = '" + dateInput + "' union all select MilanoDate, SID2, SID1, EdgeCost from edges where MilanoDate = '" + dateInput + "'"
    hc.sql(query_union).createOrReplaceTempView("edgesUnion")

    val query_min = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node and g1.MilanoDate = '" + dateInput + "') b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_max = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node)  and g1.MilanoDate = '" + dateInput + "') b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_jc = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min + ") d1 inner join (" + query_max + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate and d1.MilanoDate = '" + dateInput + "'"

    hc.sql(query_jc).write.format("orc").mode("append").insertInto("mi2mi.jaccardcoefficient")

    return true
  }

  def main(args: Array[String]): Unit = {
    val noTest = args(0)
    val printFile = "./results/runtime_Jaccard_Coefficient_Hive_test_" + noTest + ".txt"

    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Jaccard Coefficient Hive Test" + noTest)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // drop table if it exists
    hc.sql("drop table if exists mi2mi.jaccardcoefficient")
    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Jaccard Coefficient Hive")
    pw.println("Start time: " + Calendar.getInstance().getTime())

    val t0 = System.nanoTime()

    val edgesTbl = hc.table("mi2mi.edges")
    edgesTbl.createOrReplaceTempView("edges")

    /*
    // Version v1 - unoptimized SQL query

    val query_min1 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from edges g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) and (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2"
    val query_max1 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from edges g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) or (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2"
    val query_jc1 = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min1 + ") d1 inner join (" + query_max1 + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    // the sum of min for (node1, node2)
    hc.sql(query_min1).show()
    // the sum of max for (node1, node2)
    hc.sql(query_max1).show()
    // compute jaccard coefficient for (node1, node2)
    hc.sql(query_jc1).write.format("orc").saveAsTable("mi2mi.jaccardcoefficient")
    */


    // Version v2 - optimized SQL query

    val query_min2 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node ) b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_max2 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node) ) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_jc2 = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min2 + ") d1 inner join (" + query_max2 + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    // the sum of min for (node1, node2)
    hc.sql(query_min2).show()
    // the sum of max for (node1, node2)
    hc.sql(query_max2).show()
    // compute jaccard coefficient for (node1, node2)
    hc.sql(query_jc2).write.format("orc").saveAsTable("mi2mi.jaccardcoefficient")


    /*
    // Version v3 - the same as version 2 but with an additional query for Union

    val query_union = "select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges"
    hc.sql(query_union).createOrReplaceTempView("edgesUnion")
    val query_min3 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node ) b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_max3 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node) ) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_jc3 = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min3 + ") d1 inner join (" + query_max3 + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    // the sum of min for (node1, node2)
    hc.sql(query_min3).show()
    // the sum of max for (node1, node2)
    hc.sql(query_max3).show()
    // compute jaccard coefficient for (node1, node2)
    hc.sql(query_jc3).write.format("orc").saveAsTable("mi2mi.jaccardcoefficient")
    */
    // val query_date = "select distinct MilanoDate from edges"
    // val x = hc.sql(query_date).rdd.map(row => computeJC(hc, row(0).toString)).count()
    // println(x)

    /*
    hc.sql("CREATE TABLE IF NOT EXISTS mi2mi.jaccardcoefficient(MilanoDate date, SID1 int, SID2 int, jaccardcoefficient double)")
    val dateInput = "2013-11-01"
    val query_union = "select MilanoDate, SID1, SID2 common_node, EdgeCost from edges where MilanoDate = '" + dateInput + "' union all select MilanoDate, SID2, SID1, EdgeCost from edges where MilanoDate = '" + dateInput + "'"
    hc.sql(query_union).createOrReplaceTempView("edgesUnion")

    val query_min = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node and g1.MilanoDate = '" + dateInput + "') b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_max = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node)  and g1.MilanoDate = '" + dateInput + "') b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_jc = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min + ") d1 inner join (" + query_max + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate and d1.MilanoDate = '" + dateInput + "'"

    hc.sql(query_jc).write.format("orc").mode("append").insertInto("mi2mi.jaccardcoefficient")
    */
    val t1 = System.nanoTime()

    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Jaccard Coefficient Hive Test no. " + noTest)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()
  }
}

