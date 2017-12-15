import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

object one_merged_file {
  def main(args: Array[String]): Unit = {

    // Create spark configuration
    val sparkConf = new SparkConf()
      // .setMaster("local[*]")
      .setAppName("Merging")

    // Create spark context
    val sc = new SparkContext(sparkConf)

    // PrintWriter
    import java.io._

    val pw = new PrintWriter(new File("/home/olivera/Merging_time.txt"))
    pw.println("Hello, merging started!")


    println("Start time: " + Calendar.getInstance().getTime())
    pw.println("Start time: " + Calendar.getInstance().getTime())
    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    val myfile = "hdfs://hadoop-master:8020/user/ciprian/output/MI2MI/MI2MI_November.txt"

    val r1 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-01.txt")
    val r2 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-02.txt")
    val r3 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-03.txt")
    val r4 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-04.txt")
    val r5 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-05.txt")
    val r6 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-06.txt")
    val r7 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-07.txt")
    val r8 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-08.txt")
    val r9 = sc.textFile("hdfs://hadoop-master:8020/user/ciprian/input/MI2MI/MItoMI-2013-11-09.txt")
    // val r10 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-10.txt")
    // val r11 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-11.txt")
    // val r12 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-12.txt")
    // val r13 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-13.txt")
    // val r14 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-14.txt")
    // val r15 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-15.txt")
    // val r16 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-16.txt")
    // val r17 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-17.txt")
    // val r18 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-18.txt")
    // val r19 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-19.txt")
    // val r20 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-20.txt")
    // val r21 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-21.txt")
    // val r22 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-22.txt")
    // val r23 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-23.txt")
    // val r24 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-24.txt")
    // val r25 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-25.txt")
    // val r26 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-26.txt")
    // val r27 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-27.txt")
    // val r28 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-28.txt")
    // val r29 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-29.txt")
    // val r30 = sc.textFile("/home/olivera/Milano_november/MItoMI-2013-11-30.txt")

    val rdds = Seq(r1, r2, r3, r4, r5, r6, r7, r8, r9) //, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25, r26, r27, r28, r29, r30)
    val bigrdd = sc.union(rdds)
      .map(line => line.split("[ \t]+").toVector)
      .map(vec => (
        df.format(vec(0).toLong).split(" ")(0),
        df.format(vec(0).toLong).split(" ")(1),
        vec(1),
        vec(2),
        vec(3)))
      .coalesce(1)
      .saveAsTextFile(myfile)


    println("End time: " + Calendar.getInstance().getTime())
    pw.println("End time: " + Calendar.getInstance().getTime())
    pw.println("*************************************************")

    pw.close()
  }
}



