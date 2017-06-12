package iot.users

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import qoe.cdnnodeinfo

/**
  * Created by slview on 17-6-12.
  */
object UsersInfo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IOTCDRLog").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("create temporary external table tmp_external_users(text string) location '/hadoop/wlw/users/userinfo/'")
  }

}
