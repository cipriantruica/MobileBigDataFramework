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
      StructField("Node1", IntegerType, true),
      StructField("Node2", IntegerType, true),
      StructField("Weight", DoubleType, true)))

    // Create spark configuration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Jaccard Coefficient")

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // drop table if it exists
    hc.sql("drop table if exists graph")


    // read the data from the tsv files
    val df = hc.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(fileSchema)
      .load(inputDirectory)
    df.show()
    // create a view to query
    df.createOrReplaceTempView("graph_tbl")

    val sqlEdges = hc.sql("select Node1, Node2, Weight from graph_tbl")
      .write.format("orc").saveAsTable("graph")

    val graphTbl = hc.table("graph")
    graphTbl.createOrReplaceTempView("graph")

    // create the edges and save them to parquet files
    hc.sql("select Node1, Node2, Weight from graph").show()

    val query_min = "select c.node1, c.node2, sum(c.mins) sum_mins from (select b.node1, b.node2, b.common_node, min(b.weight) mins from (select g1.node1, g1.node2, a.common_node, a.weight from graph g1 inner join (select t.node2 common_node, t.node1, t.weight from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t) a on a.node1 in (g1.node1, g1.node2) where a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1) and a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)) b group by b.node1, b.node2, b.common_node) c group by c.node1, c.node2"
    val query_max = "select c.node1, c.node2, sum(c.maxs) sum_maxs from (select b.node1, b.node2, b.common_node, max(b.weight) maxs from (select g1.node1, g1.node2, a.common_node, a.weight from graph g1 inner join (select t.node2 common_node, t.node1, t.weight from (select node1, node2, weight from graph union all select node2, node1, weight from graph) t) a on a.node1 in (g1.node1, g1.node2) where a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node1) or  a.common_node in (select t1.node2 from (select node1, node2 from graph union all select node2, node1 from graph) t1 where t1.node1 = g1.node2)) b group by b.node1, b.node2, b.common_node) c group by c.node1, c.node2"
    val query_jc  = "select d1.node1, d2.node2, d1.sum_mins/d2.sum_maxs jaccard_coefficient from(" + query_min + ") d1 inner join(" + query_max + ") d2 on d1.node1 = d2.node1 and d1.node2=d2.node2"
    // the sum of min for (node1, node2)
    hc.sql(query_min).show()
    // the sum of max for (node1, node2)
    hc.sql(query_max).show()
    // compute jaccard coefficient for (node1, node2)
    hc.sql(query_jc).show()
  }
}
