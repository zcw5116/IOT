package iot.auth

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.ConfigProperties

import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
/**
  * Created by slview on 17-6-27.
  */
object AuthlogDayETL {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }

    val dayid = args(0)
    val authType = args(1)
    val authlogDir = args(2)

    val sparkConf = new SparkConf()//.setAppName("IOTAuthLog").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val filepath = authlogDir + "/" + "*" + dayid + "*.log"
    val conf = sc.hadoopConfiguration
    conf.set("mapreduce.input.fileinputformat.split.maxsize","256MB")
    // val frdd = sc.newAPIHadoopFile(filepath, "org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
    val frdd = sc.newAPIHadoopFile(filepath, classOf[CombineTextInputFormat],classOf[LongWritable], classOf[Text], conf).map(x=>x._2.toString)
    val authlogDF = sqlContext.read.json(frdd)
    val tmpTable = "authDFtempTable"
    authlogDF.registerTempTable(tmpTable)

    if(authType == "3g"){
      sqlContext.sql("insert into iot_userauth_3gaaa_day partition(dayid,hourid) select auth_result, auth_time, device, imsicdma, " +
        "imsilte, mdn, nai_sercode, nasport, nasportid, nasporttype, pcfip, srcip, " +
        "substr(regexp_replace(auth_time,'-',''),1,8) as dayid, substr(auth_time,12,2) as hourid from " + tmpTable)

    }else if(authType == "4g"){
      sqlContext.sql("insert into iot_userauth_4gaaa_day partition(dayid,hourid) select auth_result, auth_time, device, imsicdma,  " +
        "imsilte, mdn, nai_sercode, nasport, nasportid, nasporttype, pcfip, " +
        "substr(regexp_replace(auth_time,'-',''),1,8) as dayid, substr(auth_time,12,2) as hourid " +
        "from " + tmpTable)
    }else if(authType == "vpdn"){
      sqlContext.sql("insert into  iot_userauth_vpdn_day partition(dayid,hourid) select auth_result, auth_time, device, entname," +
        " imsicdma, imsilte, lnsip, mdn, nai_sercode, pdsnip, substr(regexp_replace(auth_time,'-',''),1,8) as dayidc " +
        "from " + tmpTable)
    }
  }
}
