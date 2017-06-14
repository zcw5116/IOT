package iot.streaming.auth

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-14.
  */
object AuthLogAnalysis {

  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getNextTimeStr(start_time:String, stepSeconds:Long)={
    var df:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHHmmss")
    var begin:Date=df.parse(start_time)
    var endstr:Long = begin.getTime() + stepSeconds*1000
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var nextTimeStr:String = sdf.format(new Date((endstr)))
    nextTimeStr
  }


  def main(args: Array[String]): Unit = {
   // val sparkConf = new SparkConf().setAppName("IOTCDRSparkSQL").setMaster("local[4]")
    //val sc = new SparkContext(sparkConf)
    //val hiveContext = new HiveContext(sc)
    val starttimeid = "20170523091501"
    val endtimeid = "20170523092000"
    println(getNextTimeStr(starttimeid, 300))

  }
}
