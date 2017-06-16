package iot.operalog

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-16.
  */
object OperalogETL {
  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      System.err.println("Usage:<dayid>")
      System.exit(1)
    }
    val dayid = args(0)

    val sparkConf = new SparkConf().setAppName("OperaETL").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("use iot")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val PcrfOperalog = sqlContext.read.json("/hadoop/IOT/ANALY_PLATFORM/OperaLog/PCRF/*dat*"+dayid+"*")
    val tmppcrf = "pcroperalog"
    val prcftable = "iot_user_opera_pcrf_log"
    PcrfOperalog.registerTempTable(tmppcrf)
    PcrfOperalog.printSchema()
    sqlContext.sql("insert into " + prcftable + " partition(dayid) " +
      "select detailinfo,errorinfo,imsicdma,imsilte,mdn,netype,node,operclass,opertime,opertype,oper_result, " +
      "substr(regexp_replace(opertime,'-',''),1,8) as dayid " +
      " from " + tmppcrf + " ")

    val HssOperalog = sqlContext.read.json("/hadoop/IOT/ANALY_PLATFORM/OperaLog/HSS/*"+dayid+"*")
    val tmphss = "hssoperalog"
    val hsstable = "iot_user_opera_hss_log"
    HssOperalog.registerTempTable(tmphss)
    HssOperalog.printSchema()
    sqlContext.sql("insert into " + hsstable + " partition(dayid) " +
      "select detailinfo,errorinfo,imsicdma,imsilte,mdn,netype,node,operclass,opertime,opertype,oper_result, " +
      "substr(regexp_replace(opertime,'-',''),1,8) as dayid " +
      " from " + tmphss + " ")

    val hlrdayid = dayid.toString.substring(0,4)+"-" + dayid.toString.substring(4,6) + "-" + dayid.toString.substring(6,8)
    val HlrOperalog = sqlContext.read.json("/hadoop/IOT/ANALY_PLATFORM/OperaLog/HLR/*"+hlrdayid+"*")
    val tmphlr = "hlroperalog"
    val hlrtable = "iot_user_opera_hlr_log"
    HlrOperalog.registerTempTable(tmphlr)
    HlrOperalog.printSchema()
    sqlContext.sql("insert into " + hlrtable + " partition(dayid) " +
      "select detailinfo,errorinfo,imsicdma,imsilte,mdn,netype,node,operclass,opertime,opertype,oper_result, " +
      "substr(regexp_replace(opertime,'-',''),1,8) as dayid " +
      " from " + tmphlr + " ")


  }
}
