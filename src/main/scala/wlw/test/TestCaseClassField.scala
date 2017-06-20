package wlw.test

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by slview on 17-6-17.
  */


object TestCaseClassField {



  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("UserInfoGenerate").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val data = sc.textFile("hdfs://cdh-nn1:8020/tmp/test.txt").map(_.split("\\|")).map(l =>  new AirTraffic(
        Try(l(0).trim.toString).toOption, Try(l(1).trim.toString).toOption,
        Try(l(2).trim.toString).toOption, Try(l(3).trim.toString).toOption,
        Try(l(4).trim.toString).toOption, Try(l(5).trim.toString).toOption,
        Try(l(6).trim.toString).toOption, Try(l(7).trim.toString).toOption,
        l(8).trim.toString, Try(l(9).trim.toString).toOption,
        l(10).trim.toString, Try(l(11).trim.toString).toOption,
        Try(l(12).trim.toString).toOption, Try(l(13).trim.toString).toOption,
        Try(l(14).trim.toString).toOption, Try(l(15).trim.toString).toOption,
        l(16).trim.toString, l(17).trim.toString,
        Try(l(18).trim.toString).toOption,
        Try(l(19).trim.toString).toOption, Try(l(20).trim.toString).toOption,
        Try(l(21).trim.toString).toOption, l(22).trim.toString)).toDF()

    // register table with SQLContext
    data.registerTempTable("AirTraffic")

    val rdd = hiveContext.sql("SELECT * FROM AirTraffic").collect()
    rdd.foreach(println)
  }
}