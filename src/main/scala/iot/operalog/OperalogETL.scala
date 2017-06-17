package iot.operalog

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.CharacterEncodeConversion

/**
  * Created by slview on 17-6-16.
  * 读取开销户日志， 并将日志存储到hive表中
  */
object OperalogETL {

  // 获取后面一天的日期
  // getPreviousDay("20170616") return:20170617
  def getNextDay(currentdayid: String) = {
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var next: Date = df.parse(currentdayid)
    var nexttime: Long = next.getTime() + 24 * 3600 * 1000
    var nextdayid: String = df.format(new Date((nexttime)))
    nextdayid
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Usage:<dayid>")
      System.exit(1)
    }
    val dayid = args(0)

    val sparkConf = new SparkConf().setAppName("OperaETL")
    //.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("use iot")
    //sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    try {
      // val PcrfOperalog = sqlContext.read.json("/hadoop/IOT/ANALY_PLATFORM/OperaLog/PCRF/*dat*" + dayid + "*")
      // 调用CharacterEncodeConversion.transfer转换中文编码， 返回rdd传入json方法
      val PcrfOperalog = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, "/hadoop/IOT/ANALY_PLATFORM/OperaLog/PCRF/*dat*" + dayid + "*", "GBK")).repartition(1)

      val tmppcrf = "pcroperalog"
      val prcftable = "iot_user_opera_pcrf_log"
      PcrfOperalog.registerTempTable(tmppcrf)
      PcrfOperalog.printSchema()
      sqlContext.sql("ALTER TABLE " + prcftable + " DROP IF EXISTS PARTITION(dayid='" + dayid + "')")
      sqlContext.sql("insert into " + prcftable + " partition(dayid='"+dayid+"') " +
        "select detailinfo,errorinfo,imsicdma,imsilte,mdn,netype,node,operclass,opertime,opertype,oper_result  " +
        " from " + tmppcrf + "  where substr(regexp_replace(opertime,'-',''),1,8)='"+dayid+"'")
    } catch {
      case ex: Exception => {
        println("pcroperalog failed. " + ex.getMessage)
      }
    }

    try {
      // 获取后面一天的日期, hss的文件名是次日到日期， 文件内容是文件名日期的前一天数据。
      val hssdayid = getNextDay(dayid)
      //val HssOperalog = sqlContext.read.json("/hadoop/IOT/ANALY_PLATFORM/OperaLog/HSS/*" + hssdayid + "*")
      // 调用CharacterEncodeConversion.transfer转换中文编码， 返回rdd传入json方法
      val HssOperalog = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, "/hadoop/IOT/ANALY_PLATFORM/OperaLog/HSS/*" + hssdayid + "*", "GBK")).coalesce(1)
      val tmphss = "hssoperalog"
      val hsstable = "iot_user_opera_hss_log"
      HssOperalog.registerTempTable(tmphss)
      HssOperalog.printSchema()
      sqlContext.sql("ALTER TABLE " + hsstable + " DROP IF EXISTS PARTITION(dayid='" + dayid + "')")
      sqlContext.sql("insert into " + hsstable + " partition(dayid='"+dayid+"') " +
        "select detailinfo,errorinfo,imsicdma,imsilte,mdn,netype,node,operclass,opertime,opertype,oper_result " +
        " from " + tmphss + "  where substr(regexp_replace(opertime,'-',''),1,8)='"+dayid+"'")
    } catch {
      case ex: Exception => {
        println("hssoperalog failed. " + ex.getMessage)
      }
    }

    try {
      // 将日期格式转换， 20170615 转换成 2017-06-15
      val hlrdayid = dayid.toString.substring(0, 4) + "-" + dayid.toString.substring(4, 6) + "-" + dayid.toString.substring(6, 8)
      // 调用CharacterEncodeConversion.transfer转换中文编码， 返回rdd传入json方法
      val HlrOperalog = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, "/hadoop/IOT/ANALY_PLATFORM/OperaLog/HLR/*" + hlrdayid + "*", "GBK")).repartition(1)
      val tmphlr = "hlroperalog"
      val hlrtable = "iot_user_opera_hlr_log"
      HlrOperalog.registerTempTable(tmphlr)
      HlrOperalog.printSchema()
      sqlContext.sql("ALTER TABLE " + hlrtable + " DROP IF EXISTS PARTITION(dayid='" + dayid + "')")
      sqlContext.sql("insert into " + hlrtable + " partition(dayid='"+dayid+"') " +
        "select detailinfo,errorinfo,imsicdma,imsilte,mdn,netype,node,operclass,opertime,opertype,oper_result " +
        " from " + tmphlr + "  where substr(regexp_replace(opertime,'-',''),1,8)='" + dayid + "'")
    } catch {
      case ex: Exception => {
        println("hlroperalog failed. " + ex.getMessage)
      }
    }
  }
}
