import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/*
 This is a small test for computing Jaccard Coefficient for a graph
*/

object JC {
  def main(args: Array[String]): Unit = {
    // input directory with the tsv
    val inputDirectory = "graph.csv"
    // val inputDirectory = args(0)


    // the tsv schema
    val fileSchema = StructType(Array(
      StructField("Timestamp", DateType, true),
      StructField("SquareID1", IntegerType, true),
      StructField("SquareID2", IntegerType, true),
      StructField("DIS", DoubleType, true)))

    // Create spark configuration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Jaccard Coefficient")

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // drop table if it exists
    hc.sql("drop table if exists edges")


    // read the data from the tsv files
    val df = hc.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(fileSchema)
      .load(inputDirectory)
//    df.show()
    // create a view to query
    df.createOrReplaceTempView("edges_tbl")

    val sqlEdges = hc.sql("select Timestamp MilanoDate, SquareID1 SID1, SquareID2 SID2, DIS EdgeCost from edges_tbl")
      .write.format("orc").saveAsTable("edges")

    val edgesTbl = hc.table("edges")
    edgesTbl.createOrReplaceTempView("edges")

    // create the edges and save them to parquet files
//    hc.sql("select MilanoDate, SID1, SID2, EdgeCost from edges").show()

    val query_min = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from edges g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) and (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2 order by 1,2,3"
    val query_max = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from (select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a.common_node, a.EdgeCost from edges g1 inner join (select t.MilanoDate, t.SID2 common_node, t.SID1, t.EdgeCost from (select MilanoDate, SID1, SID2, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) t ) a on a.SID1 in (g1.SID1, g1.SID2) and a.MilanoDate = g1.MilanoDate where (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID1 and t1.MilanoDate = g1.MilanoDate) or  (a.common_node, a.MilanoDate) in (select t1.SID2, t1.MilanoDate from (select MilanoDate, SID1, SID2 from edges union all select MilanoDate, SID2, SID1 from edges) t1 where t1.SID1 = g1.SID2 and t1.MilanoDate = g1.MilanoDate)) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node) c group by c.MilanoDate, c.SID1, c.SID2 order by 1,2,3"
    val query_min2 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.mins) sum_mins from ( select b.MilanoDate, b.SID1, b.SID2, b.common_node, min(b.EdgeCost) mins from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a3.common_node = a1.common_node and a2.common_node = a1.common_node ) b  group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2 order by 1, 2, 3"
    val query_max2 = "select c.MilanoDate, c.SID1, c.SID2, sum(c.maxs) sum_maxs from ( select b.MilanoDate, b.SID1, b.SID2, b.common_node, max(b.EdgeCost) maxs from (select g1.MilanoDate, g1.SID1, g1.SID2, a1.common_node, a1.EdgeCost from edges g1 inner join (select MilanoDate, SID1, SID2 common_node, EdgeCost from edges union all select MilanoDate, SID2, SID1, EdgeCost from edges) a1 on  a1.SID1 in (g1.SID1, g1.SID2) and a1.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a2 on a2.SID1 = g1.SID1 and a2.MilanoDate = g1.MilanoDate inner join (select MilanoDate, SID1, SID2 common_node from edges union all select MilanoDate, SID2, SID1 from edges) a3 on a3.SID1 = g1.SID2 and a3.MilanoDate = g1.MilanoDate where a1.common_node in (a2.common_node, a3.common_node) ) b group by b.MilanoDate, b.SID1, b.SID2, b.common_node ) c  group by c.MilanoDate, c.SID1, c.SID2 order by 1, 2, 3"
    val query_jc = "select d1.MilanoDate, d1.SID1, d2.SID2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from (" + query_min + ") d1 inner join (" + query_max + ") d2 on d1.SID1 = d2.SID1 and d1.SID2 = d2.SID2 and d1.MilanoDate = d2.MilanoDate"
    // the sum of min for (node1, node2)
    println("MIN orig")
    hc.sql(query_min).show()
    println("MIN v2")
    hc.sql(query_min2).show()
    // the sum of max for (node1, node2)
    println("MAX orig")
    hc.sql(query_max).show()
    println("MAX v2")
    hc.sql(query_max2).show()
//     compute jaccard coefficient for (node1, node2)
//    hc.sql(query_jc).show()
  }
}
