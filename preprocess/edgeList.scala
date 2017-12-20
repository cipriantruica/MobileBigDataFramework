
import org.apache.spark.rdd.RDD

object edgeList{
  def apply(rdd2: RDD[(String, String, String, String, String)], day:Int) = {
    val outedgelist = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/edgeslist/myedgelist_" + day.toString()
    println(outedgelist)
    rdd2.map(t => (t._1, t._2, t._3.toInt, t._4.toInt, t._5.toFloat))
      .map(t => {
        if (t._3 > t._4) (t._1, t._2, t._4, t._3, t._5) else (t._1, t._2, t._3, t._4, t._5)
      })
      .map(t => ((t._3, t._4), t._5)) /* making key,value pairs where the key is pair of nodes,
					 and the value is streght (weight) of the link between nodes */
      .reduceByKey(_ + _)  //summing up the streght values for unique key
      .map(e => (e._1._1, e._1._2, e._2)) //making the egde in format: node1, node2, weight
      .coalesce(1) //I wanted to have only one file per day as output
      .saveAsTextFile(outedgelist)

  }
}










