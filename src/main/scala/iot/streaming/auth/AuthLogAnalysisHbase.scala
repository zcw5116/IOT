package iot.streaming.auth

import java.text.SimpleDateFormat
import java.util.Date

import iot.streaming.auth.AuthService._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.DateUtil.getNextTime
import utils.HbaseUtil.createHTable
import utils.{ConfigProperties, DateUtil}

/**
  * Created by zhoucw on 17-6-14.
  */
object AuthLogAnalysisHbase {

  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getNextTimeStr(start_time: String, stepSeconds: Long) = {
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var begin: Date = df.parse(start_time)
    var endstr: Long = begin.getTime() + stepSeconds * 1000
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var nextTimeStr: String = sdf.format(new Date((endstr)))
    nextTimeStr
  }

  // 获取当前时间

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: <yyyyMMddHH>")
      System.exit(1)
    }

    val startminu = args(0)
    val starttimeid = startminu + "00"

    val partitiondayid = starttimeid.substring(0, 8)

    //val starttimeid = "20170523091500"
    val parthourid = starttimeid.substring(8, 10)

    // 将时间格式20170523091500转换为2017-05-23 09:15:00
    val starttimestr = getNextTimeStr(starttimeid, 0)

    // 根据开始时间获取300秒后的时间字符串
    val endtimestr = getNextTimeStr(starttimeid, 300)
    val endtimeid = endtimestr.replaceAll("[-: ]", "")

    val curtimeid = DateUtil.getNowTime()

    val sparkConf = new SparkConf()//.setAppName("AuthLogAnalysisHbase")
    //.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql("CACHE TABLE " + cachedUserinfoTable + "  as select u.mdn,u.imsicdma, " +
      "case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode " +
      "from iot_user_basic_info u ").coalesce(1)


    // val authtmpsql = "create table if not exists " + tmp_table + " as " +
    val authtmpsql = "select '3g' type, u.vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt, " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess, " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails  " +
      "from iot_userauth_3gaaa a, "+cachedUserinfoTable+" u  " +
      "where a.imsicdma = u.imsicdma  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "  and a.hourid="+ parthourid + "  " +
      "group by u.vpdncompanycode,u.mdn, a.auth_result  " +
      "union all  " +
      "select '4g' type, u.vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt,  " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess,  " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails  " +
      "from iot_userauth_4gaaa a, "+cachedUserinfoTable+" u   " +
      "where a.mdn = u.mdn  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "  and a.hourid="+ parthourid + "  " +
      "group by u.vpdncompanycode, u.mdn, a.auth_result  " +
      "union all  " +
      "select 'vpdn' type, u.vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt,  " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess,  " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails   " +
      "from iot_userauth_vpdn a, "+cachedUserinfoTable+" u   " +
      "where a.mdn = u.mdn  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "  and a.hourid="+ parthourid + "  " +
      "group by u.vpdncompanycode, u.mdn, a.auth_result "

    val authdftb = "authdftmp"
    sqlContext.sql(authtmpsql).repartition(1).registerTempTable(authdftb)

    val tmp_table = "auth_3gaaa_streaming_tmp"
    sqlContext.sql( "drop table if exists " + tmp_table)
    sqlContext.sql("create table if not exists " + tmp_table + " as " +
    "select type, vpdncompanycode, mdn, auth_result, authcnt, authsucess, authfails from " + authdftb)

    sqlContext.cacheTable(tmp_table)

    // 根据临时表二次聚合生成结果, 认证类型/企业/认证用户数/认证成功数/认证失败数/认证卡数/认证失败卡数
    //val auth3gaaaSql = "insert into auth_streaming_result partition(dayid)  " +
    // " + starttimeid + " as starttime, " + endtimeid + " as endtime,
    val authSql = "select a.type, a.vpdncompanycode, " +
      " sum(a.authcnt) as authcnt,sum(a.successcnt) as successcnt," +
      " sum(a.failedcnt) as failedcnt,count(*) as authmdnct," +
      " sum(case when a.failedcnt=0 then 0 else 1 end) as mdnfaieldcnt, " + partitiondayid + " as dayid  " +
      "from (" +
      " select t.type, t.vpdncompanycode, t.mdn,sum(t.authcnt) authcnt, " +
      "  sum(case when t.auth_result=0 then t.authcnt else 0 end ) as successcnt, " +
      "  sum(case when t.auth_result=0 then 0 else t.authcnt end ) as failedcnt  " +
      " from  " + tmp_table + " t " +
      " group by t.type, t.vpdncompanycode, t.mdn)  a " +
      "group by a.type, a.vpdncompanycode"

    println(authSql)

    val authdf = sqlContext.sql(authSql).coalesce(1)

    val htable = "iot_userauth_day_" + partitiondayid
    // 如果h表不存在， 就创建
    val connection= ConnectionFactory.createConnection(conf)
    val families = new Array[String](2)
    families(0) = "authresult"
    families(1) = "authfailed"
    // 创建表, 如果表存在， 自动忽略
    createHTable(connection,htable,families)


    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)
    val startminuteid = starttimeid.substring(8,12)
    val endminuteid = endtimeid.substring(8,12)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val hbaserdd = authdf.rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2), x.getLong(3),
      x.getLong(4), x.getLong(5), x.getLong(6)))
    val authcurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */

      val currentPut = new Put(Bytes.toBytes(arr._2 + "-" + startminuteid.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("c_"+arr._1+"_auth_cnt" ), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("c_"+arr._1+"_success_cnt"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("c_"+arr._1+"_failed_cnt"), Bytes.toBytes(arr._5.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("c_"+arr._1+"_authmdn_cnt"), Bytes.toBytes(arr._6.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("c_"+arr._1+"_mdnfaield_cnt"), Bytes.toBytes(arr._7.toString))

      //currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_"+arr._1+"_auth_cnt"), Bytes.toBytes(((arr._3-4)*(arr._3-4)/(arr._3+1)).toString))
      //currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_"+arr._1+"_success_cnt"), Bytes.toBytes(((arr._4-4)*(arr._4+4)/(arr._4+1)).toString))
     // currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_"+arr._1+"_failed_cnt"), Bytes.toBytes(((arr._5+2)*(arr._5-4)/(arr._5+1)).toString))
      //currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_"+arr._1+"_authmdn_cnt"), Bytes.toBytes(((arr._6+1)*(arr._6-2)/(arr._6+1)).toString))
      //currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_"+arr._1+"_mdnfaield_cnt"), Bytes.toBytes(((arr._7+4)*(arr._7-6)/(arr._7+1)).toString))

      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("datatime"), Bytes.toBytes(curtimeid.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }

    authcurrentrdd.saveAsHadoopDataset(authJobConf)



    val preJobConf = new JobConf(conf, this.getClass)
    preJobConf.setOutputFormat(classOf[TableOutputFormat])
    preJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)
    val authnextrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val nextPut = new Put(Bytes.toBytes(arr._2 + "-" + endminuteid.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("p_"+arr._1+"_auth_cnt"), Bytes.toBytes(arr._3.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("p_"+arr._1+"_success_cnt"), Bytes.toBytes(arr._4.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("p_"+arr._1+"_failed_cnt"), Bytes.toBytes(arr._5.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("p_"+arr._1+"_authmdn_cnt"), Bytes.toBytes(arr._6.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("p_"+arr._1+"_mdnfaield_cnt"), Bytes.toBytes(arr._7.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, nextPut)
    }
    }
    authnextrdd.saveAsHadoopDataset(preJobConf)


    // val authFailedSql = "insert into auth_streaming_result_failed partition(dayid)  " +
/*    val authFailedSql = "select " + starttimeid + " as starttime, " + endtimeid + " as endtime, b.type, b.vpdncompanycode, b.auth_result, " +
      "       b.authcnt, b.authrank, " + partitiondayid + " as dayid " +
      "from ( " +
      "      select a.type, a.vpdncompanycode, a.auth_result, authcnt," +
      "             row_number() over(partition by a.type, a.vpdncompanycode order by a.authcnt desc) as authrank " +
      "      from (" +
      "            select t.type, t.vpdncompanycode, t.auth_result, sum(t.authcnt) as authcnt " +
      "            from  " + tmp_table + " t " +
      "            where t.auth_result=0  " +
      "            group by t.type, t.vpdncompanycode, t.auth_result" +
      "           ) a" +
      ")  b where b.authrank<2"*/

 /*   val authFailedSql = " select a.type, a.vpdncompanycode, a.auth_result, authcnt," +
      " row_number() over(partition by a.type, a.vpdncompanycode order by a.authcnt desc) as authrank " +
      " from (" +
      "         select t.type, t.vpdncompanycode, t.auth_result, sum(t.authcnt) as authcnt " +
      "         from  " + tmp_table + " t " +
      "         where t.auth_result<>0  " +
      "         group by t.type, t.vpdncompanycode, t.auth_result" +
      " ) a"*/


    val authFailedSql = "select t.type, t.vpdncompanycode, t.auth_result, sum(t.authcnt) as authcnt " +
               "  from  " + tmp_table + " t " +
               "  where t.auth_result<>0  " +
               "  group by t.type, t.vpdncompanycode, t.auth_result"



    // type, vpdncompanycode, auth_result, authcnt, authrank
    val failedrdd = sqlContext.sql(authFailedSql).coalesce(1).rdd.map(x => (x.getString(0),  x.getString(1), x.getInt(2), x.getLong(3)))

    val curfailedRdd = failedrdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._2.toString + "-" + startminuteid.toString))
      currentPut.addColumn(Bytes.toBytes("authfailed"), Bytes.toBytes("c_" + arr._1 + "_" + arr._3.toString + "_cnt" ), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    curfailedRdd.saveAsHadoopDataset(authJobConf)


    // 将数据写入预警表
    import sqlContext.implicits._
    val hbaseRdd = registerRDD(sc,htable).toDF()
    val alarmHtable = "analyze_rst_tab"
    val hDFtable = "htable"
    hbaseRdd.registerTempTable(hDFtable)

    val alarmSql = "select  case when length(companycode)=0 then 'N999999999' else companycode end as companycode, nvl(c_3g_auth_cnt,0), nvl(c_3g_success_cnt,0), nvl(b_3g_auth_cnt,0), " +
      "nvl(b_3g_success_cnt,0),nvl(p_3g_auth_cnt,0), nvl(p_3g_success_cnt,0), nvl(c_4g_auth_cnt,0), nvl(c_4g_success_cnt,0), " +
      "nvl(b_4g_auth_cnt,0), nvl(b_4g_success_cnt,0),nvl(p_4g_auth_cnt,0), nvl(p_4g_success_cnt,0),nvl(c_vpdn_auth_cnt,0), " +
      "nvl(c_vpdn_success_cnt,0), nvl(b_vpdn_auth_cnt,0), nvl(b_vpdn_success_cnt,0), nvl(p_vpdn_auth_cnt,0), nvl(p_vpdn_success_cnt,0)  " +
      " from " + hDFtable + "  where time='" + startminuteid + "' "

    println(alarmSql)

    val alarmJobConf = new JobConf(conf, this.getClass)
    alarmJobConf.setOutputFormat(classOf[TableOutputFormat])
    alarmJobConf.set(TableOutputFormat.OUTPUT_TABLE, alarmHtable)

    val alarmdf = sqlContext.sql(alarmSql).coalesce(1)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val alarmrdd = alarmdf.rdd.map(x => (x.getString(0),
      x.getString(1)+"_"+x.getString(2), x.getString(3) +"_"+ x.getString(4), x.getString(5)+"_"+x.getString(6),
      x.getString(8)+"_"+x.getString(7), x.getString(10)+"_"+x.getString(9), x.getString(12)+"_"+x.getString(11),
      x.getString(14)+"_"+x.getString(13), x.getString(16)+"_"+x.getString(15), x.getString(18)+"_"+x.getString(17),
      divOpera(x.getString(2),x.getString(1)),divOpera(x.getString(4),x.getString(3)),divOpera(x.getString(6),x.getString(5)),
      divOpera(x.getString(8),x.getString(7)),divOpera(x.getString(10),x.getString(9)),divOpera(x.getString(12),x.getString(11)),
      divOpera(x.getString(14),x.getString(13)),divOpera(x.getString(16),x.getString(15)),divOpera(x.getString(18),x.getString(17))))


    val alarmhbaserdd = alarmrdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_3g_time"), Bytes.toBytes(startminu.toString))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_3g_request_cnt"), Bytes.toBytes(arr._2))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_3g_success_cnt"), Bytes.toBytes(arr._2))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_3g_ratio"), Bytes.toBytes(arr._11))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_3g_request_cnt"), Bytes.toBytes(arr._3))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_3g_success_cnt"), Bytes.toBytes(arr._3))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_3g_ratio"), Bytes.toBytes(arr._12))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_3g_request_cnt"), Bytes.toBytes(arr._4))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_3g_success_cnt"), Bytes.toBytes(arr._4))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_3g_ratio"), Bytes.toBytes(arr._13))


      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_4g_time"), Bytes.toBytes(startminu.toString))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_4g_request_cnt"), Bytes.toBytes(arr._5))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_4g_success_cnt"), Bytes.toBytes(arr._5))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_4g_ratio"), Bytes.toBytes(arr._14))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_4g_request_cnt"), Bytes.toBytes(arr._6))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_4g_success_cnt"), Bytes.toBytes(arr._6))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_4g_ratio"), Bytes.toBytes(arr._15))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_4g_request_cnt"), Bytes.toBytes(arr._7))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_4g_success_cnt"), Bytes.toBytes(arr._7))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_4g_ratio"), Bytes.toBytes(arr._16))



      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_vpdn_time"), Bytes.toBytes(startminu.toString))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_vpdn_request_cnt"), Bytes.toBytes(arr._8))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_vpdn_success_cnt"), Bytes.toBytes(arr._8))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_c_vpdn_ratio"), Bytes.toBytes(arr._17))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_vpdn_request_cnt"), Bytes.toBytes(arr._9))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_vpdn_success_cnt"), Bytes.toBytes(arr._9))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_b_vpdn_ratio"), Bytes.toBytes(arr._18))

      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_vpdn_request_cnt"), Bytes.toBytes(arr._10))
      //currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_vpdn_success_cnt"), Bytes.toBytes(arr._10))
      currentPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("auth_p_vpdn_ratio"), Bytes.toBytes(arr._19))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }

    alarmhbaserdd.saveAsHadoopDataset(alarmJobConf)

   sc.stop()
  }
}