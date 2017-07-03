package iot.users

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.ConfigProperties
import utils.DateUtil.timeCalcWithFormatConvert

/**
  * Created by slview on 17-6-28.
  */
object UserOnLine {

  def main(args: Array[String]): Unit = {
    if(args.length<1){
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }
    val dayid = args(0)
    val partdayid = dayid
    val parthourid = "00"
    val curtimestr = timeCalcWithFormatConvert(dayid+"000000","yyyyMMddHHmmss",0,"yyyy-MM-dd HH:mm:ss")

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    // sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    sqlContext.sql("use  iot")

    val userTable = "iot_user_basic_info"
    val pgwTable = "iot_cdr_pgw_ticket"
    //val pgwsql = "SELECT u.mdn, u.vpdncompanycode FROM iot_user_basic_info u LEFT SEMI JOIN iot_cdr_pgw_ticket t " +
    //  " ON  (a.mdn = b.mdn AND and t.dayid='20170605' and t.hourid='00'  and t.l_timeoffirstusage < 'curtime')"

    // val pgwsql = "insert into iot_user_online_day select '"+dayid+"' as dayid, o.vpdncompanycode, count(*) as onlinecnt from ( SELECT u.mdn, u.vpdncompanycode FROM iot_user_basic_info u LEFT SEMI JOIN iot_cdr_pgw_ticket t ON  (u.mdn = t.mdn and t.dayid='"+dayid+"' and t.l_timeoffirstusage < '"+curtimestr+"' )) o group by o.vpdncompanycode"

    val mydayid = "20170627"
    val pgwsql = "select '"+mydayid+"' as dayid, o.vpdncompanycode, count(*) as onlinecnt from ( SELECT u.mdn, u.vpdncompanycode FROM iot_user_basic_info u LEFT SEMI JOIN iot_cdr_pgw_ticket t ON  (u.mdn = t.mdn and t.dayid='"+dayid+"' and t.l_timeoffirstusage < '"+curtimestr+"' )) o group by o.vpdncompanycode"

   // haccg用户在线计算逻辑
//    create table t1 as
//      select t.mdn, t.account_session_id from iot_cdr_haccg_ticket t where t.acct_status_type<>'2' and t.dayid='20170701' and t.hourid=23;
//    create table t2 as
//      select t.mdn, t.account_session_id from iot_cdr_haccg_ticket t where t.acct_status_type='2' and t.dayid='20170702' and t.hourid=00;
//
//    create table t3 as
//      select t1.mdn from t1, t2 where t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id;
//
//    create table t4 as
//      select u.vpdncompanycode,count(*) from t3 t, iot_user_basic_info u where t.mdn=u.mdn group by u.vpdncompanycode;


    sqlContext.sql(pgwsql).coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save("/hadoop/IOT/ANALY_PLATFORM/UserOnline/" + mydayid)

  }

}
