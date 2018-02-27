import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
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
    It reads all the data from the tcv and computes the edges.
    It uses a Dataframe with a single select.
    It saves the output in Hive
*/

object CreateEdgesHive {
  def main(args: Array[String]): Unit = {
    // input directory with the tsv
    //val inputDirectory = "hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11*"
    val inputDirectory = args(0)

    // the file with the mearsuments
    val noTest = args(1)
    val printFile = "./results/runtime_Create_Edges_Hive_test_" + noTest + ".txt"
    // val printFile = args(2)

    // the tsv schema
    val fileSchema = StructType(Array(
      StructField("Timestamp", LongType, true),
      StructField("SquareID1", IntegerType, true),
      StructField("SquareID2", IntegerType, true),
      StructField("DIS", DoubleType, true)))

    // Spark session
    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Create Edges Hive Test no. " + noTest)

    // Create spark context
    val sc = new SparkContext(sparkConf)
    // Create Hive context
    val hc = new HiveContext(sc)
    // drop table if it exists
    hc.sql("drop table if exists mi2mi.edges")

    // PrintWriter
    import java.io._
    val pw = new PrintWriter(new File(printFile))

    pw.println("Create Edges Hive")
    pw.println("Start time: " + Calendar.getInstance().getTime())

    val t0 = System.nanoTime()

    // read the data from the tsv files
    val df = hc.read.format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(fileSchema)
      .load(inputDirectory)

    // create a view to query
    df.createOrReplaceTempView("mi2mi_table")

    // create the edges and save them to parquet files
    val sqlEdges = hc.sql("select Date MilanoDate, SID1, SID2, sum(DIS) EdgeCost from (select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID1 SID1, SquareID2 SID2, DIS from mi2mi_table where SquareID1 <= SquareID2 union all select cast(from_unixtime(Timestamp/1000) as Date) Date, SquareID2, SquareID1, DIS from mi2mi_table where SquareID1 > SquareID2) group by Date, SID1, SID2")
      .write.format("orc").saveAsTable("mi2mi.edges")

    val t1 = System.nanoTime()
    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    println("Create Edges Hive Test no. " + noTest)
    println("Elapsed time (ms): " + ((t1 - t0) / 1e6))
    pw.println("*************************************************")

    pw.close()
  }
}
