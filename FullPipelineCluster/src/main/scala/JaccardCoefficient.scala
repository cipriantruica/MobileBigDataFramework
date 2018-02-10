import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/*
  Build Jaccard Coefficient for the entire dataset using DataFrames
  !!! THIS IS NOT TESTED !!!
*/

object JaccardCoefficient {
  def main(args: Array[String]): Unit = {

    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Jaccard Coefficient")

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // drop table if it exists
    hc.sql("drop table if exists jaccardcoefficient")

    val graphTbl = hc.table("mi2mi.edges")
    graphTbl.createOrReplaceTempView("graph")

    val query_min = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from graph g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from graph union all select MilanoDate, SID2, SID1, EdgeCost from graph) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from graph union all select MilanoDate, SID2, SID1 from graph) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) and (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from graph union all select MilanoDate, SID2, SID1 from graph) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2"
    val query_max = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from graph g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from graph union all select MilanoDate, SID2, SID1, EdgeCost from graph) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from graph union all select MilanoDate, SID2, SID1 from graph) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) or  (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from graph union all select MilanoDate, SID2, SID1 from graph) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2"
    val query_jc = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min + ") d1 inner join (" + query_max + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    

    // the sum of min for (node1, node2)
    // hc.sql(query_min).show()
    // the sum of max for (node1, node2)
    // hc.sql(query_max).show()
    // compute jaccard coefficient for (node1, node2)

    hc.sql(query_jc).write.format("orc").saveAsTable("mi2mi.jaccardcoefficient")
  }
}

