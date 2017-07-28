package iot.users

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.HiveProperties

import scala.util.Try

/**
  * Created by slview on 17-6-12.
  */
object UsersInfoIncETL {


  implicit class StringConverter(val s: String) extends AnyVal {
    def tryGetInt = Try(s.trim.toInt).toOption

    def tryGetString = {
      val res = s.trim
      if (res.isEmpty) None else Try(res.toString).toOption
    }

    def tryGetBoolean = Try(s.trim.toBoolean).toOption
  }

  case class companyinfo(companyid: String, companyname: String)

  def getNowDayid(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    var curdayid = dateFormat.format(now)
    curdayid
  }

  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getNextTimeStr(start_time: String, stepSeconds: Long) = {
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var begin: Date = df.parse(start_time)
    var endstr: Long = begin.getTime() + stepSeconds * 1000
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var nextTimeStr: String = sdf.format(new Date((endstr)))
    nextTimeStr
  }


  case class UserInfo(mdn:String, imsicdma:String, imsilte:String, iccid:String, imei:String, company:String, vpdncompanycode:String, nettype:String, vpdndomain:String, isvpdn:String, subscribetimeaaa:String, subscribetimehlr:String, subscribetimehss:String, subscribetimepcrf:String, firstactivetime:String, userstatus:String, atrbprovince:String, userprovince:String)

  def main(args: Array[String]): Unit = {

    // var dayid = getNowDayid()
    if (args.length < 2) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }
    val dayid = args(0)
    val dirpath = args(1)

    // lastday
    val lastday = getNextTimeStr(dayid, -24 * 60 * 60)

    val curdayid = getNowDayid()


    val hiveDatabase = HiveProperties.HIVE_DATABASE

    val sparkConf = new SparkConf()//.setAppName("UserInfoGenerate")
    //.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val tmpTable = "tmpuserinfo"
    val tmpIncrTable = "iot_tmp_incr_users"
    val tmpPartTable = "iot_tmp_mid_users"
    val userPartTable = "iot_user_basic_info_part"
    val userPartTmp = "iot_user_basic_info_tmp"


    val userTable = "iot_user_basic_info"
    val userRenameTo = "iot_user_basic_info_rename"

    sqlContext.sql("use " + hiveDatabase)

    try {

      /*val userDF = sc.textFile(dirpath + "/incr*" + dayid + "*[0-9]").map(_.split("\\|", 18)).filter(_.length == 18)
        .map(u => new UsersInfo(u(0).tryGetString, u(1).tryGetString, u(2).tryGetString, u(3).tryGetString,
          u(4).tryGetString, u(5).tryGetString, u(6).tryGetString, u(7).tryGetString, u(8).tryGetString,
          u(9).tryGetString, u(10).tryGetString, u(11).tryGetString, u(12).tryGetString, u(13).tryGetString,
          u(14).tryGetString, u(15).tryGetString, u(16).tryGetString, u(17).tryGetString)).toDF().repartition(4)*/


      val userDF = sc.textFile(dirpath + "/incr*" + dayid + "*[0-9]").map(_.split("\\|", 18)).filter(_.length == 18).map(u => UserInfo(u(0), u(1), u(2), u(3),u(4), u(5), u(6), u(7), u(8),u(9), u(10), u(11), u(12), u(13),u(14), u(15), u(16), u(17))).toDF().repartition(4)

      userDF.registerTempTable(tmpTable)

      val droptmpincrsql = "drop table if exists " + tmpIncrTable
      val createtmpincrsql = "create table " + tmpIncrTable + " like " + userTable
      val droptmpsql = "drop table if exists " + tmpPartTable
      val droprenamesql = "drop table if exists " + userRenameTo
      val createtmpsql = "create table " + tmpPartTable + " like " + userTable

      sqlContext.sql(droptmpincrsql)
      sqlContext.sql(createtmpincrsql)

      sqlContext.sql(droptmpsql)
      sqlContext.sql(droprenamesql)
      sqlContext.sql(createtmpsql)

      sqlContext.sql("insert into " + tmpIncrTable + " " +
        " select distinct mdn,imsicdma,imsilte,iccid,imei,company,companycode as vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa," +
        " subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince," + curdayid + " as crt_time" +
        " from " + tmpTable + " t lateral view explode(split(t.vpdncompanycode,',')) c as companycode where mdn is not null")


      val  tmppartuser = "tmppartuser"
      sqlContext.sql("select u.mdn, u.imsicdma, u.imsilte, u.iccid, u.imei, u.company, u.vpdncompanycode, u.nettype, u.vpdndomain, " +
        "    u.isvpdn, u.subscribetimeaaa, u.subscribetimehlr, u.subscribetimehss, u.subscribetimepcrf, u.firstactivetime, " +
        "    u.userstatus, u.atrbprovince, u.userprovince, u.crt_time " +
        "    from  iot_user_basic_info_part u where u.dayid='"+lastday+"'").coalesce(2).registerTempTable(tmppartuser)




    /*  val resultSql = " insert into "+tmpPartTable+"  select u.mdn, t.imsicdma, t.imsilte, t.iccid, t.imei, t.company, t.vpdncompanycode, t.nettype, t.vpdndomain, " +
        "t.isvpdn, t.subscribetimeaaa, t.subscribetimehlr, t.subscribetimehss, t.subscribetimepcrf, t.firstactivetime, t.userstatus, " +
        "t.atrbprovince, t.userprovince, t.crt_time  " +
        "from " + userPartTmp + " u, " + tmpIncrTable + " t where u.mdn=t.mdn " +
        " union all  " +
        "select  o.mdn, o.imsicdma, o.imsilte, o.iccid, o.imei, o.company, o.vpdncompanycode, o.nettype, o.vpdndomain, o.isvpdn, " +
        "o.subscribetimeaaa, o.subscribetimehlr, o.subscribetimehss, o.subscribetimepcrf, o.firstactivetime, o.userstatus, " +
        "o.atrbprovince, o.userprovince, o.crt_time  " +
        "from  ( " +
        "    select u.mdn, u.imsicdma, u.imsilte, u.iccid, u.imei, u.company, u.vpdncompanycode, u.nettype, u.vpdndomain, " +
        "    u.isvpdn, u.subscribetimeaaa, u.subscribetimehlr, u.subscribetimehss, u.subscribetimepcrf, u.firstactivetime, " +
        "    u.userstatus, u.atrbprovince, u.userprovince, u.crt_time, t.mdn as newmdn " +
        "    from " + userPartTmp + " u  join " + tmpIncrTable + " t on(u.mdn=t.mdn) " +
        "     ) o where o.newmdn is null "
      "  union all  " +
        "select o.mdn, o.imsicdma, o.imsilte, o.iccid, o.imei, o.company, o.vpdncompanycode, o.nettype, o.vpdndomain, o.isvpdn, " +
        "o.subscribetimeaaa, o.subscribetimehlr, o.subscribetimehss, o.subscribetimepcrf, o.firstactivetime, o.userstatus, " +
        "o.atrbprovince, o.userprovince, o.crt_time  " +
        "from  ( " +
        "    select t.mdn, t.imsicdma, t.imsilte, t.iccid, t.imei, t.company, t.vpdncompanycode, t.nettype, t.vpdndomain, " +
        "    t.isvpdn, t.subscribetimeaaa, t.subscribetimehlr, t.subscribetimehss, t.subscribetimepcrf, t.firstactivetime, " +
        "    t.userstatus, t.atrbprovince, t.userprovince, t.crt_time, u.mdn as newmdn " +
        "    from " + userPartTmp + " u right join " + tmpIncrTable + " t on(u.mdn=t.mdn) "  +
        "     ) o where o.newmdn is null "*/

     val resultTmpTable = "resultTmpTable"
      sqlContext.sql("drop table if exists " + resultTmpTable)
      val resultSql = "create table "+resultTmpTable+"  as select nvl(t.mdn, u.mdn) as mdn, if(t.mdn is null, u.imsicdma,t.imsicdma) as imsicdma,if(t.mdn is null, u.imsilte,t.imsilte) as imsilte," +
        "if(t.mdn is null, u.iccid,t.iccid) as iccid, if(t.mdn is null, u.imei,t.imei) as imei,if(t.mdn is null, u.company,t.company) as company,if(t.mdn is null, u.vpdncompanycode,t.vpdncompanycode) as vpdncompanycode," +
        "if(t.mdn is null, u.nettype,t.nettype) as nettype, if(t.mdn is null, u.vpdndomain,t.vpdndomain) as vpdndomain,if(t.mdn is null, u.isvpdn,t.isvpdn) as isvpdn," +
        "if(t.mdn is null, u.subscribetimeaaa,t.subscribetimeaaa) as subscribetimeaaa,if(t.mdn is null, u.subscribetimehlr,t.subscribetimehlr) as subscribetimehlr," +
        "if(t.mdn is null, u.subscribetimehss,t.subscribetimehss) as subscribetimehss,if(t.mdn is null, u.subscribetimepcrf,t.subscribetimepcrf) as subscribetimepcrf," +
        "if(t.mdn is null, u.firstactivetime,t.firstactivetime) as firstactivetime,if(t.mdn is null, u.userstatus,t.userstatus) as userstatus," +
        "if(t.mdn is null, u.atrbprovince,t.atrbprovince) as atrbprovince,if(t.mdn is null, u.userprovince,t.userprovince) as userprovince,if(t.mdn is null, u.crt_time,t.crt_time) as crt_time " +
        "from "+tmppartuser+" u full outer join  "+tmpIncrTable+" t on(u.mdn=t.mdn)"


      println(resultSql)
      sqlContext.sql(resultSql)


      sqlContext.sql("ALTER TABLE " + userPartTable + " DROP IF EXISTS PARTITION (dayid=" + dayid + ")")
      sqlContext.sql("insert into " + userPartTable + " partition(dayid=" + dayid + ")  " +
        " select mdn,imsicdma,imsilte,iccid,imei,company,vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa,subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince, crt_time" +
        "  from " + resultTmpTable)

      val userRenameToTmp = "alter table " + userTable + " RENAME TO " + userRenameTo
      val tmpRenameToUser = "alter table " + resultTmpTable + " RENAME TO " + userTable
      sqlContext.sql(userRenameToTmp)
      sqlContext.sql(tmpRenameToUser)

    } catch {
      case e: InvalidInputException => {
        println(e.getMessage)
        System.exit(1)
      }
      case e: Exception => {
        println(e.getMessage)
        System.exit(1)
      }
    }

    // hiveContext.sql("select * from "+tmpTable+" limit 11").collect().foreach(println)


    /*hiveContext.sql("use iot")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val companyRdd = sc.textFile("/hadoop/IOT/ANALY_PLATFORM/BasicData/CompanyInfo/*").map(_.split("\\|")).map(c=>companyinfo(c(0),c(1)))
    companyRdd.toDF().registerTempTable(companytable)

    hiveContext.sql("create temporary external table " + tmpExternalTable + " ( mdn string, imsicdma string, " +
      "imsilte string, imei string, vpdncompanycode string, nettype string, vpdndomain string, isvpdn string, " +
      "subscribetimepcrf string, atrbprovince string, userprovince string)  row format delimited fields terminated by '|' LOCATION 'hdfs:/hadoop/IOT/ANALY_PLATFORM/BasicData/UserInfo/'")



    hiveContext.sql("ALTER TABLE iot_user_basic_info DROP IF EXISTS PARTITION (dayid="+ dayid +")")
    hiveContext.sql("insert into iot_user_basic_info partition(dayid=" + dayid + ") select mdn, imsicdma, imsilte, imei, vpdncompanycode, nettype, vpdndomain, isvpdn, subscribetimepcrf, atrbprovince, userprovince," +
      dayid + " as dayid from " + tmpExternalTable)*/
      */

  }

}
