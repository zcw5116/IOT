package wlw.cdr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-6.
  */
object CDRTicket {

  /*
  *   统计3G 流量
  *   按照时间、区域、企业的维度，统计该企业所有用户（卡）使用的流量、使用的用户数、平均流量、TOPN用户使用流量。
  *   TOPN用户使用 流量未统计
  * */
  def g3Flow(sc:SparkContext, hiveContext: HiveContext, starttimeid:String, endtimeid:String) = {
    hiveContext.sql("use default")
    hiveContext.sql("create table if not exists g3flowrealtime (starttimeid string, endtimeid string, companyid string, companyname string, type string, usercnt int, sumupflow bigint, sumdownflow bigint, avgupflow bigint, avgdownflow bigint) stored as orc")
    hiveContext.sql("create temporary table if not exists g3flowrealtimetmp (mdn string, companyid string, companyname string, type string, upflow bigint, downflow bigint)  stored as orc ")
    hiveContext.sql("insert into  g3flowrealtimetmp select  u.mdn, u.companyid,c.companyname, '3g' type, sum(originating) upflow, sum(termination) as downflow " +
      "from part_cdr_3gaaa_ticketbak t, iot_user_info u, iot_company_info c " +
      "where t.mdn = u.mdn and u.companyid = c.companyid  and  t.timeid between " + starttimeid + " and  " + endtimeid +
      " group by u.mdn, u.companyid,c.companyname")
    // 统计在线用户数
    hiveContext.sql("insert into g3flowrealtime select \"" + starttimeid + "\" as starttimeid, \""+ endtimeid +"\" as endtimeid, t.companyid, t.companyname, t.type, count(*) as usercnt, " +
      "sum(upflow) as sumupflow, sum(downflow) as sumdownflow, avg(upflow) as avgupflow, avg(downflow) as avgdownflow" +
      " from g3flowrealtimetmp t group by t.companyid, t.companyname, t.type")
  }


  /*
  *   统计4G 流量
  *   按照时间、区域、企业的维度，统计该企业所有用户（卡）使用的流量、使用的用户数、平均流量、TOPN用户使用流量。
  *   TOPN用户使用 流量未统计
  * */
  def g4Flow(sc:SparkContext, hiveContext: HiveContext, starttimeid:String, endtimeid:String) = {
    hiveContext.sql("use default")
    hiveContext.sql("create table if not exists g4flowrealtime (starttimeid string, endtimeid string, companyid string, companyname string, type string, usercnt int, sumupflow bigint, sumdownflow bigint, avgupflow bigint, avgdownflow bigint) stored as orc")
    hiveContext.sql("create temporary table if not exists g4flowrealtimetmp (mdn string, companyid string, companyname string, type string, upflow bigint, downflow bigint)  stored as orc ")
    // 统计每个公司的流量
    hiveContext.sql("insert into  g4flowrealtimetmp select  u.mdn,u.companyid,c.companyname, '4g' type," +
      "sum(l_datavolumeFBCDownlink) as upflow, sum(l_datavolumefbcdownlink) as downflow " +
      "from part_cdr_4g_ticket t, iot_user_info u, iot_company_info c " +
      "where t.mdn = u.mdn and u.companyid = c.companyid  and  regexp_replace(t.stoptime,'[-: ]','') between " + starttimeid + " and  " + endtimeid +
      " group by u.mdn, u.companyid,c.companyname")
    // 统计在线用户数
    hiveContext.sql("insert into g4flowrealtime select \"" + starttimeid + "\" as starttimeid, \""+ endtimeid +"\" as endtimeid, t.companyid, t.companyname, t.type, count(*) as usercnt, " +
      "sum(upflow) as sumupflow, sum(downflow) as sumdownflow, avg(upflow) as avgupflow, avg(downflow) as avgdownflow" +
      " from g4flowrealtimetmp t group by t.companyid, t.companyname, t.type")
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IOTCDRSparkSQL").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val starttimeid = "20170523091501"
    val endtimeid = "20170523092000"

    import hiveContext.implicits._
    //g4Flow(sc, hiveContext, "20170523091501", "20170523092000")
    g3Flow(sc, hiveContext, "20170523020501", "20170523021000")
    sc.stop()
  }

}
