package iot.streaming.auth

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by slview on 17-6-14.
  */
object AuthLogAnalysisToHbaseNewAPI {

  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getNextTimeStr(start_time:String, stepSeconds:Long)={
    var df:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHHmmss")
    var begin:Date=df.parse(start_time)
    var endstr:Long = begin.getTime() + stepSeconds*1000
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var nextTimeStr:String = sdf.format(new Date((endstr)))
    nextTimeStr
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("IOTCDRSparkSQL")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use iot")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val starttimeid = args(0)
    val partitiondayid = starttimeid.substring(0, 8)
    println(partitiondayid)
    //val starttimeid = "20170523091500"

    // 将时间格式20170523091500转换为2017-05-23 09:15:00
    val starttimestr = getNextTimeStr(starttimeid,0)

    // 根据开始时间获取300秒后的时间字符串
    val endtimestr = getNextTimeStr(starttimeid, 300)
    val endtimeid = endtimestr.replaceAll("[-: ]","")
    println(endtimestr)


    val tmp_table="auth_3gaaa_streaming_tmp"

    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"kaizi")
    //设置job的输出格式
    val job = new Job(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])






/*
    // 创建临时表的SQL
    val droptmpsql = "drop table if exists " + tmp_table
    sqlContext.sql(droptmpsql)
    val authtmpsql = "create table if not exists " + tmp_table + " as " +
      "select '3g' type, u.vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt, " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess, " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails  " +
      "from iot_userauth_3gaaa a, iot_user_basic_info u  " +
      "where a.imsicdma = u.imsicdma  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "   " +
      "group by u.vpdncompanycode,u.mdn, a.auth_result  " +
      "union all  " +
      "select '4g' type, u.vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt,  " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess,  " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails  " +
      "from iot_userauth_4gaaa a, iot_user_basic_info u   " +
      "where a.mdn = u.mdn  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "   " +
      "group by u.vpdncompanycode, u.mdn, a.auth_result  " +
      "union all  " +
      "select 'vpdn' type, u.vpdncompanycode, u.mdn, a.auth_result, count(*) as authcnt,  " +
      "sum(case when a.auth_result=0 then 1 else 0 end) as authsucess,  " +
      "sum(case when a.auth_result=0 then 0 else 1 end) as authfails   " +
      "from iot_userauth_vpdn a, iot_user_basic_info u   " +
      "where a.mdn = u.mdn  and a.auth_time>='" + starttimestr + "'  " +
      "and a.auth_time<'" + endtimestr + "'  and a.dayid=" + partitiondayid + "  " +
      "group by u.vpdncompanycode, u.mdn, a.auth_result "

    sqlContext.sql(authtmpsql)
    sqlContext.cacheTable(tmp_table)*/

    // 根据临时表二次聚合生成结果, 认证类型/企业/认证用户数/认证成功数/认证失败数/认证卡数/认证失败卡数
    val auth3gaaaSql = "select " + starttimeid + " as starttime, " + endtimeid + " as endtime, a.type, a.vpdncompanycode, " +
      " sum(a.authcnt) as authcnt,sum(a.successcnt) as successcnt," +
      " sum(a.failedcnt) as failedcnt,count(*) as authmdnct," +
      " sum(case when a.failedcnt=0 then 0 else 1 end) as authfaieldcnt, " + partitiondayid + " as dayid  " +
      "from (" +
      " select t.type, t.vpdncompanycode, t.mdn,sum(t.authcnt) authcnt, " +
      "  sum(case when t.auth_result=0 then t.authcnt else 0 end ) as successcnt, " +
      "  sum(case when t.auth_result=0 then 0 else t.authcnt end ) as failedcnt  " +
      " from  "+tmp_table + " t " +
      " group by t.type, t.vpdncompanycode, t.mdn)  a " +
      "group by a.type, a.vpdncompanycode"

    println(auth3gaaaSql)
    val tableName = "kaizi"
    val authrdd =  sqlContext.sql(auth3gaaaSql).rdd.map(x => (x.getLong(0), x.getLong(1),x.getString(2),x.getString(3)))

    val rdd = authrdd.map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr._1.toString + arr._4 + "new"))
      put.addColumn(Bytes.toBytes("basicxiaohe"),Bytes.toBytes("name"),Bytes.toBytes(arr._2))
      put.addColumn(Bytes.toBytes("basicxiaohe"),Bytes.toBytes("age"),Bytes.toBytes(arr._3))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}


    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()

  }
}
