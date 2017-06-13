package iot.users

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import qoe.cdnnodeinfo

/**
  * Created by slview on 17-6-12.
  */
object UsersInfo {

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

    val sparkConf = new SparkConf().setAppName("IOTCDRLog").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val abc = hiveContext.sql("create temporary external table tmp_external_users ( mdn string, imsicdma string, imsilte string, imei string, vpdncompanycode string, nettype string, vpdndomain string, isvpdn string, subscribetimepcrf string, atrbprovince string, userprovince string)row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'  with serdeproperties( \"input.regex\" = \"([0-9]*)\\\\|%\\\\|([0-9]*)\\\\|%\\\\|([0-9]*)\\\\|" +
      "%\\\\|(.*)\\\\|%\\\\|(.*)\\\\|%\\\\|(.*)\\\\|%\\\\|(.*)\\\\|%\\\\|(.*)\\\\|%\\\\|(.*)\\\\|%\\\\|(.*)\\\\|%\\\\|(.*)\") " +
      "location '/hadoop/wlw/users/userinfo/'")

    hiveContext.sql("ALTER TABLE iot_user_basic_info DROP IF EXISTS PARTITION (dayid="+ dayid +")")
    hiveContext.sql("insert into iot_user_basic_info partition(dayid) select mdn, imsicdma, imsilte, imei, vpdncompanycode, nettype, vpdndomain, isvpdn, subscribetimepcrf, atrbprovince, userprovince," +
      dayid + " as dayid from tmp_external_users")



  }

}
