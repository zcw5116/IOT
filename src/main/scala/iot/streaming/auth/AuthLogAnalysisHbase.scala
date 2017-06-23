package iot.streaming.auth

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.ConfigProperties

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


  def main(args: Array[String]): Unit = {

    val starttimeid = args(0)
    val partitiondayid = starttimeid.substring(0, 8)
    println(partitiondayid)
    //val starttimeid = "20170523091500"

    // 将时间格式20170523091500转换为2017-05-23 09:15:00
    val starttimestr = getNextTimeStr(starttimeid, 0)

    // 根据开始时间获取300秒后的时间字符串
    val endtimestr = getNextTimeStr(starttimeid, 300)
    val endtimeid = endtimestr.replaceAll("[-: ]", "")
    println(endtimestr)


    val sparkConf = new SparkConf().setAppName("AuthLogAnalysisHbase")
    //.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val tmp_table = "auth_3gaaa_streaming_tmp"
    val droptmpsql = "drop table if exists " + tmp_table
    sqlContext.sql(droptmpsql)


    val authtmpsql = "create table if not exists " + tmp_table + " as " +
      "select '3g' type, nvl(u.vpdncompanycode,'N999999999') as vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt, " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess, " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails  " +
      "from iot_userauth_3gaaa a, iot_user_basic_info u  " +
      "where a.imsicdma = u.imsicdma  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "   " +
      "group by u.vpdncompanycode,u.mdn, a.auth_result  " +
      "union all  " +
      "select '4g' type, nvl(u.vpdncompanycode,'N999999999') as vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt,  " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess,  " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails  " +
      "from iot_userauth_4gaaa a, iot_user_basic_info u   " +
      "where a.mdn = u.mdn  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "   " +
      "group by u.vpdncompanycode, u.mdn, a.auth_result  " +
      "union all  " +
      "select 'vpdn' type, nvl(u.vpdncompanycode,'N999999999') as vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt,  " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess,  " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails   " +
      "from iot_userauth_vpdn a, iot_user_basic_info u   " +
      "where a.mdn = u.mdn  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "  " +
      "group by u.vpdncompanycode, u.mdn, a.auth_result "

    println(authtmpsql)

    sqlContext.sql(authtmpsql)
    sqlContext.cacheTable(tmp_table)

    // 根据临时表二次聚合生成结果, 认证类型/企业/认证用户数/认证成功数/认证失败数/认证卡数/认证失败卡数
    //val auth3gaaaSql = "insert into auth_streaming_result partition(dayid)  " +
    // " + starttimeid + " as starttime, " + endtimeid + " as endtime,
    val authSql = "select a.type, a.vpdncompanycode, " +
      " sum(a.authcnt) as authcnt,sum(a.successcnt) as successcnt," +
      " sum(a.failedcnt) as failedcnt,count(*) as authmdnct," +
      " sum(case when a.failedcnt=0 then 0 else 1 end) as authfaieldcnt, " + partitiondayid + " as dayid  " +
      "from (" +
      " select t.type, t.vpdncompanycode, t.mdn,sum(t.authcnt) authcnt, " +
      "  sum(case when t.auth_result=0 then t.authcnt else 0 end ) as successcnt, " +
      "  sum(case when t.auth_result=0 then 0 else t.authcnt end ) as failedcnt  " +
      " from  " + tmp_table + " t " +
      " group by t.type, t.vpdncompanycode, t.mdn)  a " +
      "group by a.type, a.vpdncompanycode"

    println(authSql)
    val authdf = sqlContext.sql(authSql)

    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, "iot_userauth_day_20170617")

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val hbaserdd = authdf.rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2), x.getLong(3),
      x.getLong(4), x.getLong(5), x.getLong(6)))
    val authcurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._2 + "-" + arr._1 + "-" + starttimeid.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("authcnt"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("successcnt"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("failedcnt"), Bytes.toBytes(arr._5.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("authmdnct"), Bytes.toBytes(arr._6.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("authfaieldcnt"), Bytes.toBytes(arr._7.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }

    authcurrentrdd.saveAsHadoopDataset(authJobConf)

    val authnextrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val nextPut = new Put(Bytes.toBytes(arr._2 + "-" + arr._1 + "-" + endtimeid.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("authcnt"), Bytes.toBytes(arr._3.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("successcnt"), Bytes.toBytes(arr._4.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("failedcnt"), Bytes.toBytes(arr._5.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("authmdnct"), Bytes.toBytes(arr._6.toString))
      nextPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("authfaieldcnt"), Bytes.toBytes(arr._7.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, nextPut)
    }
    }
    authnextrdd.saveAsHadoopDataset(authJobConf)


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

    val authFailedSql = " select a.type, a.vpdncompanycode, a.auth_result, authcnt," +
      " row_number() over(partition by a.type, a.vpdncompanycode order by a.authcnt desc) as authrank " +
      " from (" +
      "         select t.type, t.vpdncompanycode, t.auth_result, sum(t.authcnt) as authcnt " +
      "         from  " + tmp_table + " t " +
      "         where t.auth_result=0  " +
      "         group by t.type, t.vpdncompanycode, t.auth_result" +
      " ) a"



    println(authFailedSql)
    val authFailedJobConf = new JobConf(conf, this.getClass)
    authFailedJobConf.setOutputFormat(classOf[TableOutputFormat])
    authFailedJobConf.set(TableOutputFormat.OUTPUT_TABLE, "authfailed")

    // type, vpdncompanycode, auth_result, authcnt, authrank
    val failedrdd = sqlContext.sql(authFailedSql).rdd.map(x => (x.getString(0),  x.getString(1), x.getInt(2), x.getLong(3),
      x.getInt(4)))

    val rdd = failedrdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._2.toString + "-" + arr._1 + "-" + starttimeid.toString))
      currentPut.addColumn(Bytes.toBytes("authfailed"), Bytes.toBytes("failedtypecnt_" + arr._3.toString), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("authfailed"), Bytes.toBytes("failedtyperank_" + arr._3.toString), Bytes.toBytes(arr._5.toString))

      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    rdd.saveAsHadoopDataset(authJobConf)
   sc.stop()
  }
}