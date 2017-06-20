package com.zyuc.spark

import java.io.File
import java.io.PrintWriter
import java.text.DecimalFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by wangpf on 2017/4/27.
  */

case class topnCase(pppoeuser: String, ip: String, upflux: Long, downflux: Long)

case class onetonCase(pppoeuser: String, maxTerminal: Long)

case class pgtableCase(statisDay: String, WeekOfYear: String, zonename: String, adcodename: String,
                       pppoeuser: String, protocoldown: String, protocolup: String, address: String,
                       loid: String, oltname: String, ipaddr: String, oltdport: String, ifnormal: String)

case class localIlleCase(ipaddr: String, crange: String, whois: String, probename: String,
                         probeisp: String, starttime: String, endtime: String, pppoeuser: String,
                         hxusername: String, hxaddress: String, hxreplyremark: String, hxarea: String)

class DealSourceFile {
  /*获取TOPN数据并清洗注册为表topninfos*/
  def GetTopNData(sc: SparkContext, sqlContext: SQLContext, file: String) = {
    import sqlContext.implicits._

    //val lines = transfer(sc,file)
    val lines = sc.textFile(file)
    val infos = lines.map { x => x.split("&\\*&") }
      .filter(_.length == 5)
      .map { z => topnCase(z(0).replace("ad", ""), z(2), z(3).toLong, z(4).toLong) }
      .toDF()

    infos.registerTempTable("topndetail")
    sqlContext
      .sql("select pppoeuser,ip,sum(upflux) upflux,sum(downflux) downflux from topndetail group by pppoeuser,ip")
      .registerTempTable("topn")
  }

  /*获取一拖N数据并注册为表onetoninfos*/
  def GetOneToNData(sc: SparkContext, sqlContext: SQLContext, file: String) = {
    import sqlContext.implicits._

    //val lines = transfer(sc,file)
    val lines = sc.textFile(file)
    val infos = lines.map { x => x.split("&\\*&") }
      .filter(_.length == 3)
      .map { z => onetonCase(z(0).replace("ad", ""), z(2).toLong) }
      .toDF()

    infos.registerTempTable("oneton")
  }

  /*获取PG数据库导出信息*/
  def GetPgTableData(sc: SparkContext, sqlContext: SQLContext, file: String) = {
    import sqlContext.implicits._

    //val lines = transfer(sc,file)
    val lines = sc.textFile(file)
    val infos = lines.map { x => x.split("&\\*&") }
      .filter(_.length == 13)
      .map { z =>
        pgtableCase(z(0), z(1), z(2), z(3),
          z(4), z(5), z(6), z(7),
          z(8), z(9), z(10), z(11), z(12))
      }
      .toDF()

    infos.registerTempTable("pginfos")
  }

  /*获取本地违规数据并注册为表localIlle*/
  def GetLocalIlleData(sc: SparkContext, sqlContext: SQLContext, file: String) = {
    import sqlContext.implicits._

    //val lines = transfer(sc,file)
    val lines = sc.textFile(file)
    val infos = lines.map { x => x.split("&\\*&") }
      .filter(_.length == 12)
      .map { z =>
        localIlleCase(z(0), z(1), z(2), z(3),
          z(4), z(5), z(6), z(7),
          z(8), z(9), z(10), z(11))
      }
      .toDF()

    infos.registerTempTable("localIlle")
  }

  def transfer(sc: SparkContext, path: String): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}

object GetBlackBandFile extends App {
  val rs = new DealSourceFile()

  val conf = new SparkConf()
    .setAppName("RadiusTopNStatis")
    .setMaster("yarn-client")
    .set("spark.driver.maxResultSize", "6g")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val Array(topnFile, onetonFile, pgFile, localIlleFile, writeFile) = args
  /*根据文件获取数据并注册为表*/
  rs.GetTopNData(sc, sqlContext, topnFile)
  rs.GetOneToNData(sc, sqlContext, onetonFile)
  rs.GetPgTableData(sc, sqlContext, pgFile)
  rs.GetLocalIlleData(sc, sqlContext, localIlleFile)
  /*确定入库范围*/
  sqlContext.sql("select distinct pppoeuser from " +
    "(select pppoeuser from oneton " +
    "union all " +
    "select pppoeuser from topn) a")
    .registerTempTable("allpppoeuser")
  /*关联数据*/
  val result = sqlContext.sql(
    "select t2.statisDay," +
      "       t2.WeekOfYear," +
      "       t2.zonename," +
      "       t2.adcodename," +
      "       t2.pppoeuser," +
      "       t2.protocoldown," +
      "       t2.protocolup," +
      "       t2.address," +
      "       t3.maxTerminal," +
      "       t4.ip," +
      "       t4.upflux," +
      "       t4.downflux," +
      "       t2.loid," +
      "       t2.oltname," +
      "       t2.ipaddr," +
      "       t2.oltdport," +
      "       t2.ifnormal," +
      "       t5.ipaddr," +
      "       t5.crange," +
      "       t5.whois," +
      "       t5.probename," +
      "       t5.probeisp," +
      "       t5.starttime," +
      "       t5.endtime," +
      "       t5.hxusername," +
      "       t5.hxaddress," +
      "       t5.hxreplyremark," +
      "       t5.hxarea" +
      "  from allpppoeuser t1" +
      "  inner join pginfos t2" +
      "    on t1.pppoeuser = t2.pppoeuser" +
      "  left outer join oneton t3" +
      "    on t1.pppoeuser = t3.pppoeuser" +
      "  left outer join topn t4" +
      "    on t1.pppoeuser = t4.pppoeuser" +
      "  left outer join localIlle t5" +
      "    on t1.pppoeuser = t5.pppoeuser" +
      "  order by t4.downflux desc")

  val arr = result.collect
  val writer = new PrintWriter(new File(writeFile))

  val rankMap = scala.collection.mutable.Map[String, Int]()
  val df = new DecimalFormat("0.00")

  for (x <- arr) {
    /*处理数据*/
    val d_weekid = x(0)
    val d_week = x(1)
    val d_area = if (x(2) == null) "上海" else x(2)
    val d_ad = x(3)
    val d_pppoeuser = x(4)
    val d_protocoldown = x(5)
    val d_protocolup = x(6)
    val d_adress = x(7)
    val l_maxterminal = if (x(8) == null) 0 else x(8)
    val d_ifmultiu = if (x(8) == null) "否" else "是"
    val d_ifactiveu = ""
    val d_iftopnflux = if (x(9) == null) "否" else "是"
    val d_basip = if (x(9) == null) "" else x(9)
    val B_upflux = if (x(10) == null) -1 else x(10)
    val B_downflux = if (x(11) == null) -1 else x(11)
    val d_onlinetime = ""
    val d_ifblacku = ""
    val d_remark = ""
    val d_irregtype = ""
    val d_loid = x(12)
    /*获取排名*/
    var l_rank = 0
    if (x(2) != null && !x(2).equals("")) {
      if (rankMap.contains(x(2).toString))
        rankMap(x(2).toString) += 1
      else
        rankMap(x(2).toString) = 1
      l_rank = rankMap(x(2).toString)
    }
    var B_downratio = ""
    try {
      B_downratio = df.format(x(11).toString.toLong / (x(5).toString.replace("M", "").toLong * 7 * 24 * 3600 * 1024 * 1024 / 8.00) * 100)
      B_downratio = if (B_downratio.toDouble > 100.00) "100%" else B_downratio + "%"
    }
    catch {
      case e => ""
    }
    val d_type = if (x(5).equals(x(6))) "非家庭网关用户" else ""
    val d_oltname = x(13)
    val d_ponip = x(14)
    val d_pon = x(15)
    val d_ifirreg_pon = ""
    val d_irregtype_pon = ""
    val d_remark_pon = ""
    val d_ifnormal = if (x(16).equals("1")) "是" else ""

    val d_ipaddrbd = x(17)
    val d_crange = x(18)
    val d_whois = x(19)
    val d_probename = x(20)
    val d_probeisp = x(21)
    val d_starttime = x(22)
    val d_endtime = x(23)
    val d_hxusername = x(24)
    val d_hxaddress = x(25)
    val d_hxreplyremark = x(26)
    val d_hxarea = if (x(27) == null) d_area else x(27)

    writer.write(d_weekid + "&*&" + d_week + "&*&" + d_area + "&*&" + d_ad + "&*&" +
      d_pppoeuser + "&*&" + d_protocoldown + "&*&" + d_protocolup + "&*&" + d_adress + "&*&" +
      l_maxterminal + "&*&" + d_ifmultiu + "&*&" + d_ifactiveu + "&*&" + d_iftopnflux + "&*&" +
      d_basip + "&*&" + B_upflux + "&*&" + B_downflux + "&*&" + d_onlinetime + "&*&" +
      d_ifblacku + "&*&" + d_remark + "&*&" + d_irregtype + "&*&" + d_loid + "&*&" +
      l_rank + "&*&" + B_downratio + "&*&" + d_type + "&*&" + d_oltname + "&*&" +
      d_ponip + "&*&" + d_pon + "&*&" + d_ifirreg_pon + "&*&" + d_irregtype_pon + "&*&" +
      d_remark_pon + "&*&" + d_ifnormal + "&*&" + d_ipaddrbd + "&*&" +
      d_crange + "&*&" + d_whois + "&*&" + d_probename + "&*&" + d_probeisp + "&*&" +
      d_starttime + "&*&" + d_endtime + "&*&" + d_hxusername + "&*&" + d_hxaddress + "&*&" +
      d_hxreplyremark + "&*&" + d_hxarea + "\n")
  }
  writer.close()

  sc.stop()
}

