import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
 Build Jaccard Coefficient for the entire dataset using DataFrames
 !!! THIS IS NOT TESTED !!!
*/

object JaccardCoefficient {
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

    val query_union = "select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges"
    val unionTbls = hc.sql(query_union).createOrReplaceTempView("egdesUnion")

    val query_min = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from edges g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) and (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2"
    val query_max = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from edges g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) or (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2"
    val query_min2 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from ( select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node ) b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_max2 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from ( select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node) ) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"

    val query_min3 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from ( select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node ) b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"
    val query_max3 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from ( select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join edgesUnion a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join edgesUnion a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join edgesUnion a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node) ) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2"

    val query_jc = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min + ") d1 inner join (" + query_max + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    val query_jc2 = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min2 + ") d1 inner join (" + query_max2 + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    val query_jc3 = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min3 + ") d1 inner join (" + query_max3 + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"


    // the sum of min for (node1, node2)
    // hc.sql(query_min).show()
    // the sum of max for (node1, node2)
    // hc.sql(query_max).show()
    // compute jaccard coefficient for (node1, node2)

    hc.sql(query_jc3).write.format("orc").saveAsTable("mi2mi.jaccardcoefficient")

    val t1 = System.nanoTime()

    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Jaccard Coefficient Hive Test no. " + noTest)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()
  }
}

