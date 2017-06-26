package iot.cdr

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-26.
  */
object CDRDataToHiveByDay {
  //通过封装后的方法读取GBK文件, 并且每一行数据以字符串格式返回(RDD[String])
  def transfer(sc:SparkContext, path:String):RDD[String]={
    sc.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }

  def insertTable(sc:SparkContext, hiveContext: SQLContext, filepath:String, dftable:String, sqlString:String) = {
    //通过封装后的方法读取GBK文件, 并且每一行数据以字符串格式返回(RDD[String])
    val rddcdr = transfer(sc,filepath)
    val dfcdr = hiveContext.read.json(rddcdr)
    dfcdr.printSchema()
    // 将json文件注册为DataFrame
    dfcdr.registerTempTable(dftable)

    // 将解析的Json数据插入Hive表
    hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveContext.sql(sqlString)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IOTCDRLog").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    if (args.length < 3) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }

    val dayid = args(0)
    val cdrtype = args(1)
    val path = args(2)



    if(cdrtype == "haccg"){
      val filepath = path + "/*" + dayid + "*.TXT"
      val haccgDFtable = "iot_cdr_haccg_ticket_tmp"
      val haccgInsertSql = "insert into iot_cdr_haccg_ticket_day partition(dayid) select MDN, T0, T1, T10, T11, T12, T13, T14, " +
        "T15, T16, T17, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, T36, " +
        "T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, T57, T58, " +
        "T59, T6, T60, T61, T62, T63, T64, T65, T7, T8, T800, T801, T802, T803, T804, T805, T806, T807, T808, " +
        "T809, T9,  substr(regexp_replace(T37,'-',''),1,8) as dayid from " + haccgDFtable
      insertTable(sc, sqlContext, filepath, haccgDFtable, haccgInsertSql)
    }
    else if(cdrtype == "pgw"){
      val filepath = path + "/*" + dayid + "*.dat"
      val pgwDFtable = "iot_cdr_pgw_ticket_tmp"
      val pgwInsertSql = "insert into iot_cdr_pgw_ticket_day partition(dayid) select IMSI, MDN, T0, T1, T10, T11, T12, T13, " +
        "T14, T15, T16, T18, T19, T2, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T3, T30, T31, T32, T33, T34, T35, " +
        "T36, T37, T38, T39, T4, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T5, T50, T51, T52, T53, T54, T55, T56, " +
        "T57, T6, T7, T8, T800, T801, T802, T804, T805, T806, T807, T809, T9,  " +
        "substr(regexp_replace(T46,'-',''),1,8) as dayid from " + pgwDFtable

      insertTable(sc, sqlContext, filepath, pgwDFtable, pgwInsertSql)
    }

  }
}
