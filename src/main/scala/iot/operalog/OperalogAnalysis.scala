package iot.operalog

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-17.
  */
object OperalogAnalysis {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }
    val dayid = args(0)
    val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("use iot")
    val prcftable = "iot_user_opera_pcrf_log"
    val hsstable = "iot_user_opera_hss_log"
    val hlrtable = "iot_user_opera_hlr_log"
    val operaresultTable = "iot_user_opera_result"
    val monthid = dayid.substring(0,6)
    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql("CACHE LAZY TABLE " + cachedUserinfoTable + "  as select u.mdn, u.vpdncompanycode " +
      "from iot_user_basic_info u  where u.dayid=" + dayid)

    sqlContext.sql("insert into " + operaresultTable + "  partition(monthid=" + monthid + ")  " +
      " select 'PCRF' as operatype, " + dayid + " as dayid, u.vpdncompanycode,l.opertype, count(*) as operacnt " +
      " from " + prcftable + " l, " + cachedUserinfoTable + " u " +
      " where l.opertype in('开户','销户')  and l.oper_result='成功'  and  l.mdn = u.mdn " +
      "  and u.dayid=" + dayid + " group by  u.vpdncompanycode,l.opertype " +
      "union all" +
      " select 'HSS' as operatype, " + dayid + " as dayid,  u.vpdncompanycode,l.opertype, count(*) as operacnt " +
      " from " + hsstable + " l, " + cachedUserinfoTable + " u " +
      " where l.opertype in('开户','销户')  and l.oper_result='成功'  and  l.mdn = u.mdn " +
      "  and dayid=" + dayid + "group by  u.vpdncompanycode,l.opertype " +
      "union all" +
      " select 'HLR' as operatype, " + dayid + " as dayid, u.vpdncompanycode,l.opertype, count(*) as operacnt " +
      " from " + hlrtable + " l, " + cachedUserinfoTable + " u " +
      " where l.opertype in('开户','销户')  and l.oper_result='成功'  and  l.mdn = u.mdn " +
      "  and dayid=" + dayid + " group by  u.vpdncompanycode,l.opertype "
    ).rdd
    sc.stop()
  }

}
