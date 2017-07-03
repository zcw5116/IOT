package com.zyuc.streaming.iot

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{ConfigProperties, SQLContextSingleton}

/**
  * Created by slview on 17-6-13.
  */
object MMEStreaming {

  def toHiveTable(rdd:RDD[String], cdrtype:String): Unit ={
    if(!rdd.isEmpty()){
      var tableName=""
      var insertSql=""
      if(cdrtype.equals("hw_mm")){
        tableName = "iot_mme_mm_hw"
        insertSql = "insert into " + tableName + " partition(dayid,hourid) select T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, " +
          "T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22, T23, T24, T25, T26, T27, T28, T29, T30, T31, " +
          "T32, T33, T34, T35, T36, T37, T38, T39, T40, T41, T42, T43, T44, T45, T46, T47, T48, T49, T50, T51," +
          "T99,  substr(regexp_replace(T0,'-',''),1,8) as dayid, substr(T0,12,2) as hourid from " + tableName + "_tmp"
      }
      else if(cdrtype.equals("hw_sm")){
        tableName = "iot_mme_sm_hw"
        insertSql = "insert into " + tableName + " partition(dayid,hourid) select T0, T1, T11, T12, T13, T14, T2, T24," +
          " T25, T26, T27, T3, T36, T37, T38, T4, T48, T49, T5, T50, T51, T6, T7, T8, T9, " +
          "T99,  substr(regexp_replace(T0,'-',''),1,8) as dayid, substr(T0,12,2) as hourid from " + tableName + "_tmp"
      } else if(cdrtype.equals("zt_mm")){
        tableName = "iot_mme_mm_zt"
        insertSql = "insert into " + tableName + " partition(dayid,hourid) select T0, T10, T12, T13, T14, T17, T18, " +
          "T19, T21, T22, T23, T24, T28, T43, T48, T5, T6, T7, T8, T99,  " +
          "substr(regexp_replace(T0,'-',''),1,8) as dayid, substr(T0,12,2) as hourid from " + tableName + "_tmp"
      } else if(cdrtype.equals("zt_sm")){
        tableName = "iot_mme_sm_zt"
        insertSql = "insert into " + tableName + " partition(dayid,hourid) select T0,  T10,  T12,  T13,  T14,  " +
          "T17,  T18,  T19,  T21,  T22,  T23,  T24,  T25,  T28,  T43,  T5,  T6,  T7,  T8,  T99,  " +
          "substr(regexp_replace(T0,'-',''),1,8) as dayid, substr(T0,12,2) as hourid from " + tableName + "_tmp"
      }

      insert(rdd, tableName, insertSql)

    }
  }

  def insert(rdd:RDD[String], tableName:String, insertSQL:String) = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val hivedatabase = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use  " + hivedatabase)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val tmptable = tableName + "_tmp"
    val df = sqlContext.read.json(rdd).coalesce(1)
    df.registerTempTable(tmptable)
    df.printSchema()
    sqlContext.sql(insertSQL)
  }

  def main(args: Array[String]): Unit = {
    // 参数接收话单类型/监控到目录
    if (args.length < 2) {
      System.err.println("Usage: <cdr-type>  <log-dir>")
      System.exit(1)
    }

    // mme类型
    val mmetype = args(0)
    // 监控目录
    val inputDirectory = args(1)

    // 监控目录中文件名包含指定字符串的文件
    val filenameContainStr =
      mmetype match {
        case "hw_sm" => "HuaweiUDN"
        case "hw_mm" => "HuaweiUDN-MM"
        case "zt_mm" => "sgsnmme_mm"
        case "zt_sm" => "sgsnmme_sm"
      }

    val sparkConf = new SparkConf()//.setAppName("sss").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // 监控目录过滤以.uploading结尾的文件， 文件名包含filenameContainStr
    def uploadingFilter(path: Path): Boolean = !path.getName().endsWith("._COPYING_") && !path.getName().endsWith(".uploading")  && path.getName().contains(filenameContainStr)

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory,uploadingFilter(_), true).map(x=>x._2.toString)
    //println(lines.foreachRDD( x => x.foreach(println)))
    lines.foreachRDD(toHiveTable(_, mmetype))
    // lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
