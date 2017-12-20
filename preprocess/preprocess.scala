/**
  * Created by olivera on 6/27/17.
  */


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Preprocessing {

  def main(args: Array[String]): Unit = {

    // Create spark configuration
    val sparkConf = new SparkConf().setAppName("Preprocessing")

    // Create spark context
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    // PrintWriter
    import java.io._
    val pwFile = "/home/ciprian/mobiledata/" + args(0);
    val pw = new PrintWriter(new File(pwFile))
    pw.println("Hello, preprocessing is starting!")


    val filename = "/home/ciprian/mobiledata/" + args(1); /* in this file I kept local file paths */
    var day = 0
    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    for (line <- Source.fromFile(filename).getLines()) {
      day += 1
      
      //println("this is day " + day.toString + "   ", line)
      pw.println("this is day " + day.toString)

      //println("Start time: " + Calendar.getInstance().getTime())
      pw.println("Start time: " + Calendar.getInstance().getTime())
      val t0 = System.nanoTime()

      val inputFile = line
      val txtrdd = sc.textFile(inputFile, 10)

      /* Separate date and time,
	 first we need to format the timestamp to be readable in usual time/date format */
      val rdd2 = txtrdd
        .map(line => line.split("[ \t]+").toList)
        .map(list => (
          df.format(list(0).toLong).split(" ")(0),
          df.format(list(0).toLong).split(" ")(1),
          list(1),
          list(2),
          list(3))
        )
      
      edgeList(rdd2, day) // Make an edge list file
      val t1 = System.nanoTime()

      //println("End time: " + Calendar.getInstance().getTime())
      //println("*************************************************")

      pw.println("End time: " + Calendar.getInstance().getTime())
      pw.println("Elaspsed time (ns): " + (t1 - t0))
      pw.println("*************************************************")
      


    }

  pw.close()

  }
}

/*
the original data is tab separated line by line txt file with structure:
	timestamp	sqi1	sqid2	strenght
where 
timestamp - contains information about date and time of the record,
sqid1 - identifier of the grid cell where outgoing traffic is recorded,
sqid2 - identifier of the grid cell where incoming traffic is recorded,
strenght - this is the "weight" of the link between sqid1 and sqid2
*/

/* Since our edge list in the end doesn't contain the information about the time and date
   we could skip this step with timestamp transformation, but I needed it for making JSON
   files out of it, which in the end I didn't use because they were to large. So, just for
   making the edge lists you can skip the timestamp trensformation step. */

/* Also, I kept the info about processing time in the separate "consolePrint.txt" file, 
   console printline is not necessary, but I like to have that as well */










