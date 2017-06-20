package iot.streaming.auth

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by slview on 17-6-14.
  */
object AuthLogAnalysisToHbaseCommon {

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
    val authrdd =  sqlContext.sql(auth3gaaaSql).rdd
      //.map(x => (x.getLong(0), x.getLong(1),x.getString(2),x.getString(3)))







    authrdd.foreachPartition{
      x => {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
        //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
        val conn = ConnectionFactory.createConnection(conf)
        //从Connection获得 Admin 对象(相当于以前的 HAdmin)
        val admin = conn.getAdmin

        //本例将操作的表名
        val userTable = TableName.valueOf("kaizi")
        //创建 user 表
        val tableDescr = new HTableDescriptor(userTable)
        tableDescr.setMemStoreFlushSize(3*1024*1024)
        //获取 user 表
        val table = conn.getTable(userTable)

        x.foreach{
          y =>{
            //准备插入一条 key 为 id001 的数据
            val p = new Put(Bytes.toBytes(y.getLong(0).toString + y.getLong(4) + "common") )
            //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
            p.addColumn(Bytes.toBytes("basicxiaohe"),Bytes.toBytes("name"),Bytes.toBytes(y.getString(2) + "a"))
            p.addColumn(Bytes.toBytes("basicxiaohe"),Bytes.toBytes("age"),Bytes.toBytes(y.getString(3) + "b"))
            //提交
            table.put(p)
          }
        }

      }

    }


    sc.stop()

  }
}
