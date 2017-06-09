package iot.cdr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-9.
  */
object CDRLog {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IOTCDRLog").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val cdr3gdf = hiveContext.read.json("/hadoop/wlw/stream/cdr/3gaaa/*")
    cdr3gdf.printSchema()
    hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    cdr3gdf.registerTempTable("iot_cdr_3gaaa_ticket_tmp")
    hiveContext.sql("insert into iot_cdr_3gaaa_ticket partition(dayid) select MDN, T0, T1, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, T36, T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, T57, T58, T59, T6, T60, T61, T62, T63, T64, T65, T66, T7, T8, T800, T801, T802, T803, T804, T805, T806, T807, T808, T809, T9,  substr(regexp_replace(T37,'-',''),1,8) as dayid from iot_cdr_3gaaa_ticket_tmp")
    //hiveContext.sql("insert into iot_cdr_3gaaa_ticket select MDN, T0, T1, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, T36, T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, T57, T58, T59, T6, T60, T61, T62, T63, T64, T65, T66, T7, T8, T800, T801, T802, T803, T804, T805, T806, T807, T808, T809, T9,  substr(regexp_replace(T37,'-',''),1,8) as dayid from iot_cdr_3gaaa_ticket_tmp")



  }


}
