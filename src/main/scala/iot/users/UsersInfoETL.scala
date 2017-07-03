package iot.users

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.{ConfigProperties, HiveProperties}

import scala.util.Try

/**
  * Created by slview on 17-6-12.
  */
object UsersInfoETL {

  implicit class StringConverter(val s: String) extends AnyVal {
    def tryGetInt = Try(s.trim.toInt).toOption

    def tryGetString = {
      val res = s.trim
      if (res.isEmpty) None else Try(res.toString).toOption
    }
    def tryGetBoolean = Try(s.trim.toBoolean).toOption
  }

  case class UserInfo(mdn:String, imsicdma:String, imsilte:String, iccid:String, imei:String, company:String, vpdncompanycode:String, nettype:String, vpdndomain:String, isvpdn:String, subscribetimeaaa:String, subscribetimehlr:String, subscribetimehss:String, subscribetimepcrf:String, firstactivetime:String, userstatus:String, atrbprovince:String, userprovince:String)

  def getNowDayid(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    var curdayid = dateFormat.format(now)
    curdayid
  }

  def main(args: Array[String]): Unit = {

    // var dayid = getNowDayid()
    if (args.length < 2) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }
    val dayid = args(0)
    val dirpath = args(1)

    val curdayid = getNowDayid()
    val hiveDatabase = ConfigProperties.IOT_HIVE_DATABASE

    val sparkConf = new SparkConf().setAppName("UserInfoGenerate")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val tmpTable = "tmpuserinfo"
 /*   val userDF = sc.textFile(dirpath+"/all*"+dayid+"*[0-9]").map(_.split("\\|",18))
      .map(u => new UsersInfo(u(0).tryGetString,u(1).tryGetString,u(2).tryGetString,u(3).tryGetString,
        u(4).tryGetString,u(5).tryGetString,u(6).tryGetString,u(7).tryGetString,u(8).tryGetString,
        u(9).tryGetString,u(10).tryGetString,u(11).tryGetString,u(12).tryGetString,u(13).tryGetString,
        u(14).tryGetString,u(15).tryGetString,u(16).tryGetString,u(17).tryGetString )).toDF().repartition(4)*/

    val userDF = sc.textFile(dirpath+"/all*"+dayid+"*[0-9]").map(_.split("\\|",18))
      .map(u => UserInfo(u(0),u(1),u(2),u(3),u(4),u(5),u(6),u(7),u(8),u(9),u(10),u(11),u(12),u(13),u(14),u(15),u(16),u(17) )).toDF()


    userDF.registerTempTable(tmpTable)

    // hiveContext.sql("select * from "+tmpTable+" limit 11").collect().foreach(println)

    sqlContext.sql("use " + hiveDatabase)

    val tmpPartTable = "iot_tmp_part_users"
    val userPartTable = "iot_user_basic_info_part"
    val userTable = "iot_user_basic_info"
    val userRenameTo = "iot_user_basic_info_rename"
    val droptmpsql = "drop table if exists " + tmpPartTable
    val droprenamesql = "drop table if exists " + userRenameTo
    val createtmpsql = "create table " + tmpPartTable + " like " + userTable
    sqlContext.sql(droptmpsql)
    sqlContext.sql(droprenamesql)
    sqlContext.sql(createtmpsql)

    sqlContext.sql("insert into "+tmpPartTable + " " +
      " select distinct mdn,imsicdma,imsilte,iccid,imei,company,companycode as vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa," +
      " subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince,"+curdayid+" as crt_time" +
      " from " + tmpTable + " t lateral view explode(split(t.vpdncompanycode,',')) c as companycode where mdn is not null")


    sqlContext.sql("ALTER TABLE "+userPartTable+" DROP IF EXISTS PARTITION (dayid="+ dayid +")")
    sqlContext.sql("insert into "+userPartTable +" partition(dayid=" + dayid + ")  " +
      " select mdn,imsicdma,imsilte,iccid,imei,company,vpdncompanycode,nettype,vpdndomain,isvpdn,subscribetimeaaa,subscribetimehlr,subscribetimehss,subscribetimepcrf,firstactivetime,userstatus,atrbprovince,userprovince, crt_time" +
      "  from " + tmpPartTable)

    // 将正式表userTable（iot_user_basic_info）重命名为userRenameTo， 将tmpPartTable（iot_tmp_part_users）重命名成iot_user_basic_info
    // ALTER TABLE table_name RENAME TO new_table_name;
    val  userRenameToTmp = "alter table "+userTable+ " RENAME TO "+ userRenameTo
    val tmpRenameToUser = "alter table "+tmpPartTable+ " RENAME TO "+ userTable
    sqlContext.sql(userRenameToTmp)
    sqlContext.sql(tmpRenameToUser)

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
