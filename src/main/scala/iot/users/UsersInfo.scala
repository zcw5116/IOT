package iot.users

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-12.
  */
object UsersInfo {

  case class companyinfo(companyid: String, companyname: String)

  def getNowDayid(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var dayid = dateFormat.format(now)
    dayid
  }

  def main(args: Array[String]): Unit = {

    var dayid = getNowDayid()

    if(args.length == 1){
      var dayid = args(0)
    }else{
      System.err.println("Usage:<dayid>, or NowDayid default ")
    }

    val sparkConf = new SparkConf().setAppName("UserInfoGenerate")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val tmpExternalTable = "tmp_external_users"
    val companytable = "companyinfo"

    hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val companyRdd = sc.textFile("/hadoop/IOT/ANALY_PLATFORM/BasicData/CompanyInfo/*").map(_.split("\\|")).map(c=>companyinfo(c(0),c(1)))
    companyRdd.toDF().registerTempTable(companytable)

    hiveContext.sql("create temporary external table " + tmpExternalTable + " ( mdn string, imsicdma string, " +
      "imsilte string, imei string, vpdncompanycode string, nettype string, vpdndomain string, isvpdn string, " +
      "subscribetimepcrf string, atrbprovince string, userprovince string)  row format delimited fields terminated by '|' LOCATION 'hdfs:/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/'")



    hiveContext.sql("ALTER TABLE iot_user_basic_info DROP IF EXISTS PARTITION (dayid="+ dayid +")")
    hiveContext.sql("insert into iot_user_basic_info partition(dayid=" + dayid + ") select mdn, imsicdma, imsilte, imei, vpdncompanycode, nettype, vpdndomain, isvpdn, subscribetimepcrf, atrbprovince, userprovince," +
      dayid + " as dayid from " + tmpExternalTable)

  }

}
