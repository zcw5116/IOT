package iot.auth

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-8.
  */
object AuthLog {

  def g3auth(sc:SparkContext, hiveContext: HiveContext, starttimeid:String, endtimeid:String) = {

  }

  //case class Auth3gaaa (auth_result:Int, auth_time:String, device:String, imsicdma:String, imsilte:String, mdn:String, nai_sercode:String, nasport:Int, nasportid:String, nasporttype:Int, pcfip:String, srcip:String)
  //case class Auth4gaaa (auth_result:Int, auth_time:String, device:String, imsicdma:String, imsilte:String, mdn:String, nai_sercode:String, nasport:Int, nasportid:String, nasporttype:Int, pcfip:String)
  //case class Authvpdn3gaaa(auth_result: Int, auth_time: String, device: String, entname: String, imsicdma: String, imsilte: String, lnsip: String, mdn: String, nai_sercode: String, pdsnip: String)
  //case class Authvpdn4gaaa(auth_result: Int, auth_time: String, device: String, entname: String, imsicdma: String, imsilte: String, lnsip: String, mdn: String, nai_sercode: String, pdsnip: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IOTAuthLog").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val auth3gdf = hiveContext.read.json("/hadoop/wlw/stream/auth/3gaaa/*")
    val auth4gdf = hiveContext.read.json("/hadoop/wlw/stream/auth/4gaaa/*")
    val authvpdn3gdf = hiveContext.read.json("/hadoop/wlw/stream/auth/vpdn3gaaa/*")
    val authvpdn4gdf = hiveContext.read.json("/hadoop/wlw/stream/auth/vpdn4gaaa/*")

    hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    auth3gdf.registerTempTable("auth3gaaa")
    //hiveContext.sql("insert into iot_userauth_3gaaa partition(dayid) select auth_result, auth_time, device, imsicdma, imsilte, mdn, nai_sercode, nasport, nasportid, nasporttype, pcfip, srcip, substr(regexp_replace(auth_time,'-',''),1,8) as dayid from auth3gaaa")


    auth3gdf.registerTempTable("auth4gaaa")
    hiveContext.sql("insert into iot_userauth_4gaaa partition(dayid) select auth_result, auth_time, device, imsicdma, imsilte, mdn, nai_sercode, nasport, nasportid, nasporttype, pcfip, substr(regexp_replace(auth_time,'-',''),1,8) as dayid from auth4gaaa")

    authvpdn3gdf.registerTempTable("authvpdn3gaaa")
    authvpdn3gdf.printSchema()
    hiveContext.sql("insert into iot_userauth_vpdn3gaaa partition(dayid) select auth_result, auth_time, device, entname, imsicdma, imsilte, lnsip, mdn, nai_sercode, pdsnip, substr(regexp_replace(auth_time,'-',''),1,8) as dayid from authvpdn3gaaa")

    authvpdn4gdf.registerTempTable("authvpdn4gaaa")
    authvpdn4gdf.printSchema()
    hiveContext.sql("insert into iot_userauth_vpdn4gaaa partition(dayid) select auth_result, auth_time, device, entname, imsicdma, imsilte, lnsip, mdn, nai_sercode, pdsnip, substr(regexp_replace(auth_time,'-',''),1,8) as dayid from authvpdn4gaaa")
    hiveContext.sql("from iot_userauth_vpdn4gaaa select count(*)").collect().foreach(println)
  }
}
