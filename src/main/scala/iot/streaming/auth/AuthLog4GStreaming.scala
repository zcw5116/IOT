package iot.streaming.auth

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.SQLContextSingleton

/**
  * Created by slview on 17-6-13.
  */
object AuthLog4GStreaming {
  def toHiveTable(rdd:RDD[String], cdrtype:String): Unit ={
    if(!rdd.isEmpty()){
      var tableName=""
      var insertSql=""
      if(cdrtype.equals("auth3gaaa")){
        tableName = "iot_userauth_3gaaa"
        insertSql = "insert into " + tableName + " partition(dayid) select auth_result, auth_time, device, imsicdma, " +
          "imsilte, mdn, nai_sercode, nasport, nasportid, nasporttype, pcfip, srcip, " +
          "substr(regexp_replace(auth_time,'-',''),1,8) as dayid from " + tableName + "_tmp"
      }
      else if(cdrtype.equals("auth4gaaa")){
        tableName = "iot_userauth_4gaaa"
        insertSql = "insert into " + tableName + " partition(dayid) select auth_result, auth_time, device, imsicdma, " +
          "imsilte, mdn, nai_sercode, nasport, nasportid, nasporttype, pcfip, " +
          "substr(regexp_replace(auth_time,'-',''),1,8) as dayid from " + tableName + "_tmp"
      } else if(cdrtype.equals("vpdn3gaaa")){
        tableName = "iot_userauth_vpdn"
        insertSql = "insert into " + tableName + " partition(dayid) select auth_result, auth_time, device, entname, " +
          "imsicdma, imsilte, lnsip, mdn, nai_sercode, pdsnip, " +
          "substr(regexp_replace(auth_time,'-',''),1,8) as dayid from " + tableName + "_tmp"
      }else if(cdrtype.equals("vpdn")){
        tableName = "iot_userauth_vpdn"
        insertSql = "insert into " + tableName + " partition(dayid) select auth_result, auth_time, device, entname," +
          " imsicdma, imsilte, lnsip, mdn, nai_sercode, pdsnip, substr(regexp_replace(auth_time,'-',''),1,8) as dayid " +
          "from " + tableName + "_tmp"
      }

      insert(rdd, tableName, insertSql)

    }
  }

  def insert(rdd:RDD[String], tableName:String, insertSQL:String) = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    sqlContext.sql("use iot")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val tmptable = tableName + "_tmp"
    val df = sqlContext.read.json(rdd)
    df.registerTempTable(tmptable)
    df.printSchema()

    sqlContext.sql(insertSQL)
  }

  def main(args: Array[String]): Unit = {
    // 参数接收监控到目录
    if (args.length < 1) {
      System.err.println("Usage: <cdr-type>  <log-dir>")
      System.exit(1)
    }

    // 话单类型
    val cdrtype = "auth4gaaa"
    // 监控目录
    val inputDirectory = args(0)

    val sparkConf = new SparkConf().setAppName("AuthLog4GStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // 监控目录过滤以.uploading结尾的文件
    def uploadingFilter(path: Path): Boolean = !path.getName().endsWith("._COPYING_") && !path.getName().endsWith(".uploading")

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory,uploadingFilter(_), false).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    lines.foreachRDD(toHiveTable(_, cdrtype))
    // lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
