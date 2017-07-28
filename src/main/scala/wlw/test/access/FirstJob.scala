package wlw.test.access

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-7-16.
  */
object FirstJob {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FirstJob").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val access = sc.textFile("hdfs:/hadoop/zcw/access.log")
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replace("\"","")
      val traffic = splits(9)


      DateUtils.parse(time) + "\t" + url + "\t" + "" + traffic + "\t" + ip
    }).saveAsTextFile("hdfs:/hadoop/zcw/out1")//.take(5).foreach(println)

    sc.stop()
  }
}
