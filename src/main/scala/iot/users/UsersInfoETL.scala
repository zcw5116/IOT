package iot.users

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.HiveProperties

import scala.util.Try

/**
  * Created by slview on 17-6-12.
  */
object UsersInfoETL {


  implicit class StringConverter(val s: String) extends AnyVal {
    def tryGetInt = Try(s.trim.toInt).toOption

    def tryGetString = {
      val res = s.trim
     if (res.isEmpty) None else Try(res.toString).toOption
    }
    def tryGetBoolean = Try(s.trim.toBoolean).toOption
  }

  case class companyinfo(companyid: String, companyname: String)

  def getNowDayid(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var dayid = dateFormat.format(now)
    dayid
  }

  def main(args: Array[String]): Unit = {

    // var dayid = getNowDayid()
    if (args.length < 1) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }
    val dayid = args(0)

    val hiveDatabase = HiveProperties.HIVE_DATABASE

    val sparkConf = new SparkConf().setAppName("UserInfoGenerate")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val tmpTable = "tmpuserinfo"
    val userDF = sc.textFile("hdfs:/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/").map(_.split("\\|",18)).filter(_.length == 18)
    .map(u => new UsersInfo(u(0).tryGetString,u(1).tryGetString,u(2).tryGetString,u(3).tryGetString,
      u(4).tryGetString,u(5).tryGetString,u(6).tryGetString,u(7).tryGetString,u(8).tryGetString,
      u(9).tryGetString,u(10).tryGetString,u(11).tryGetString,u(12).tryGetString,u(13).tryGetString,
      u(14).tryGetString,u(15).tryGetString,u(16).tryGetString,u(17).tryGetString )).toDF().repartition(4)
    userDF.registerTempTable(tmpTable)

    // hiveContext.sql("select * from "+tmpTable+" limit 11").collect().foreach(println)

    sqlContext.sql("use " + hiveDatabase)
    // hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlContext.sql("ALTER TABLE iot_user_basic_info DROP IF EXISTS PARTITION (dayid="+ dayid +")")
    sqlContext.sql("insert into iot_user_basic_info partition(dayid=" + dayid + ")  " +
      " select mdn,imsicdma,imsilte,iccid,imei,company,vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa," +
      " subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince " +
      " from " + tmpTable + "  where mdn is not null")

    /*hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val companyRdd = sc.textFile("/hadoop/IOT/ANALY_PLATFORM/BasicData/CompanyInfo/*").map(_.split("\\|")).map(c=>companyinfo(c(0),c(1)))
    companyRdd.toDF().registerTempTable(companytable)

    hiveContext.sql("create temporary external table " + tmpExternalTable + " ( mdn string, imsicdma string, " +
      "imsilte string, imei string, vpdncompanycode string, nettype string, vpdndomain string, isvpdn string, " +
      "subscribetimepcrf string, atrbprovince string, userprovince string)  row format delimited fields terminated by '|' LOCATION 'hdfs:/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/'")



    hiveContext.sql("ALTER TABLE iot_user_basic_info DROP IF EXISTS PARTITION (dayid="+ dayid +")")
    hiveContext.sql("insert into iot_user_basic_info partition(dayid=" + dayid + ") select mdn, imsicdma, imsilte, imei, vpdncompanycode, nettype, vpdndomain, isvpdn, subscribetimepcrf, atrbprovince, userprovince," +
      dayid + " as dayid from " + tmpExternalTable)*/
      */

  }

}
