package iot.streaming.cdr

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{ConfigProperties, SQLContextSingleton}

/**
  * Created by slview on 17-6-13.
  */
object CDRStreamingTools {

  def toHiveTable(rdd:RDD[String], cdrtype:String): Unit ={
    if(!rdd.isEmpty()){
      var tableName=""
      var insertSql=""
      if(cdrtype.equals("3g")){
        tableName = "iot_cdr_3gaaa_ticket"
        insertSql = "insert into " + tableName + " partition(dayid) select MDN, T0, T1, T10, T11, T12, T13, T14, " +
          "T15, T16, T17, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, T36, " +
          "T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, T57, T58, " +
          "T59, T6, T60, T61, T62, T63, T64, T65, T66, T7, T8, T800, T801, T802, T803, T804, T805, T806, T807, T808, " +
          "T809, T9,  substr(regexp_replace(T37,'-',''),1,8) as dayid from " + tableName + "_tmp"
      }
      else if(cdrtype.equals("haccg")){
        tableName = "iot_cdr_haccg_ticket"
        insertSql = "insert into " + tableName + " partition(dayid) select MDN, T0, T1, T10, T11, T12, T13, T14, " +
          "T15, T16, T17, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, T36, " +
          "T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, T57, T58, " +
          "T59, T6, T60, T61, T62, T63, T64, T65, T7, T8, T800, T801, T802, T803, T804, T805, T806, T807, T808, " +
          "T809, T9,  substr(regexp_replace(T37,'-',''),1,8) as dayid from " + tableName + "_tmp"
      } else if(cdrtype.equals("pgw")){
        tableName = "iot_cdr_pgw_ticket"
        insertSql = "insert into " + tableName + " partition(dayid) select IMSI, MDN, T0, T1, T10, T11, T12, T13, " +
          "T14, T15, T16, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, " +
          "T36, T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, " +
          "T57, T6, T7, T8, T800, T801, T802, T804, T805, T806, T807, T809, T9,  " +
          "substr(regexp_replace(T46,'-',''),1,8) as dayid from " + tableName + "_tmp"
      }

      insert(rdd, tableName, insertSql)

    }
  }

  def insert(rdd:RDD[String], tableName:String, insertSQL:String) = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val hivedatabase = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use  " + hivedatabase)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val tmptable = tableName + "_tmp"
    val df = sqlContext.read.json(rdd)
    df.registerTempTable(tmptable)
    df.printSchema()
    sqlContext.sql(insertSQL)
  }

}
