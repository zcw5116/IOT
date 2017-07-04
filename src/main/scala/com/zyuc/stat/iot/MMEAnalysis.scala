package com.zyuc.stat.iot

import java.text.SimpleDateFormat
import java.util.Date

import iot.streaming.auth.AuthLogAnalysisHbase.getNextTimeStr
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.ConfigProperties
import utils.DateUtil._
import utils.HbaseUtil.createHTable

/**
  * Created by slview on 17-7-3.
  */
object MMEAnalysis {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <yyyyMMddHH>")
      System.exit(1)
    }

    val startminu = args(0)
    val starttimeid = startminu + "00"

    // 获取两个分区字段
    val partitiondayid = starttimeid.substring(0, 8)
    val parthourid = starttimeid.substring(8, 10)

    // 将时间格式20170523091500转换为2017-05-23 09:15:00
    val starttimestr = getNextTimeStr(starttimeid, 0)
    // sql条件到起始时间
    val startstr = starttimestr + ".000"
    // 根据开始时间获取300秒后的时间字符串
    val endtimestr = timeCalcWithFormatConvert(starttimeid,"yyyyMMddHHmmss",300,"yyyyMMddHHmmss")
    // sql条件的结束时间
    val endstr = endtimestr + ".000"
    // 结束时间 id
    val endtimeid = endtimestr.replaceAll("[-: ]", "")
    // 获取当前时间
    val curtimeid =getNowTime()

    val sparkConf = new SparkConf().setAppName("MMEAnalysis").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    // hbase配置
    val conf = HBaseConfiguration.create()
   // conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
   // conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", "cdh-nn1,cdh-dn1,cdh-yarn1")

    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)
    // 开启动态分区
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql("CACHE TABLE " + cachedUserinfoTable + "  as select u.mdn, " +
      "case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode " +
      "from iot_user_basic_info u ").coalesce(1)

    val mdnsql= "select m.msisdn as mdn, case when m.pcause='0x0000' then 'success' else m.pcause end as pcause, count(*) as req_cnt, sum(case when pcause='0x0000' then 1 else 0 end ) as reqsucess_cnt " +
      " from iot_mme_mm_hw m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"' " +
      " group by m.msisdn, m.pcause  " +
      "union all " +
      "select m.msisdn as mdn, case when m.pcause in('0x0000','0xFFFF') then 'success' else m.pcause end as pcause, count(*) as req_cnt, sum(case when pcause in('0x0000','0xFFFF') then 1 else 0 end ) as reqsucess_cnt  " +
      " from iot_mme_sm_hw m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"' " +
      " group by m.msisdn, m.pcause  " +
      "union all " +
      "select m.msisdn as mdn, case when m.pcause='4294967295' then 'success' else m.pcause end as pcause, count(*) as req_cnt, sum(case when pcause='4294967295' then 1 else 0 end ) as reqsucess_cnt " +
      " from iot_mme_mm_zt m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"' " +
      " group by m.msisdn, m.pcause  " +
      "union all " +
      "select m.msisdn as mdn, case when m.pcause='4294967295' then 'success' else m.pcause end as pcause, count(*) as req_cnt,sum(case when pcause='4294967295' then 1 else 0 end ) as reqsucess_cnt " +
      " from iot_mme_sm_zt m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"' " +
      " group by m.msisdn, m.pcause  "

    val mdnregtable = "mdnregtable"
    sqlContext.sql(mdnsql).repartition(1).registerTempTable(mdnregtable)

    val tmpmdntable = "tmpmdntable"
    sqlContext.sql( "drop table if exists " + tmpmdntable)
    sqlContext.sql("create table " + tmpmdntable + " as " +
      "select mdn, pcause, req_cnt, reqsucess_cnt from " + mdnregtable)

     // 统计企业到MME附着数据
    val mmecompanysql = "select u.vpdncompanycode,sum(m.req_cnt) as req_cnt, sum(reqsucess_cnt) as reqsucess_cnt, " +
      " count(*) as req_mdncnt, sum(case when pcause ='success' then 0 else 1 end ) as req_mdnfailedcnt " +
      " from "+tmpmdntable+" m, "+cachedUserinfoTable+" u " +
      " where m.mdn=u.mdn  " +
      " group by u.vpdncompanycode"

    println(mmecompanysql)

    val mmecompanydf = sqlContext.sql(mmecompanysql).coalesce(1)

    val htable = "iot_mme_day_" + partitiondayid
    // 如果h表不存在， 就创建
    val connection= ConnectionFactory.createConnection(conf)
    val families = new Array[String](2)
    families(0) = "mmeinfo"
    families(1) = "mmefailed"
    // 创建表, 如果表存在， 自动忽略
    createHTable(connection,htable,families)


    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)
    val startminuteid = starttimeid.substring(8,12)
    val endminuteid = endtimeid.substring(8,12)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val hbaserdd = mmecompanydf.rdd.map(x => (x.getString(0), x.getLong(1), x.getLong(2), x.getLong(3),
      x.getLong(4)))
    val mmecurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */

      val currentPut = new Put(Bytes.toBytes(arr._1 + "-" + startminuteid.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_cnt" ), Bytes.toBytes(arr._2.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_reqsucess_cnt"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_mdncnt"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("mmeinfo"), Bytes.toBytes("c_req_mdnfailedcnt"), Bytes.toBytes(arr._5.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    mmecurrentrdd.saveAsHadoopDataset(authJobConf)


  // 失败原因
  val failedSql = "select  u.vpdncompanycode, m.pcause, sum(m.req_cnt) as req_cnt " +
    " from "+tmpmdntable+" m, "+cachedUserinfoTable+" u " +
    " where m.mdn = u.mdn and m.pcause<>'success'  " +
    " group by u.vpdncompanycode, m.pcause"

    // type, vpdncompanycode, auth_result, authcnt, authrank
    val failedrdd = sqlContext.sql(failedSql).coalesce(1).rdd.map(x => (x.getString(0),  x.getString(1), x.getLong(2)))

    val curfailedRdd = failedrdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1.toString + "-" + startminuteid.toString))
      currentPut.addColumn(Bytes.toBytes("mmefailed"), Bytes.toBytes("c_" + arr._2 + arr._3.toString + "_cnt" ), Bytes.toBytes(arr._3.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    curfailedRdd.saveAsHadoopDataset(authJobConf)


    val failedmmesql= "select m.msisdn as mdn, ngummegi, count(*) as req_cnt, sum(case when pcause='0x0000' then 1 else 0 end ) as reqsucess_cnt " +
      " from iot_mme_mm_hw m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"'  and  m.pcause<>'0x0000'  " +
      " group by m.msisdn, m.pcause  " +
      "union all " +
      "select m.msisdn as mdn, case when m.pcause not in('0x0000','0xFFFF') then 'success' else m.pcause end as pcause, count(*) as req_cnt, sum(case when pcause in('0x0000','0xFFFF') then 1 else 0 end ) as reqsucess_cnt  " +
      " from iot_mme_sm_hw m " +
      " where m.dayid='" + partitiondayid + "'  and m.hourid='"+ parthourid +"'  " +
      " group by m.msisdn, m.pcause  " +
      "union all " +
      "select m.msisdn as mdn, case when m.pcause<>'4294967295' then 'success' else m.pcause end as pcause, count(*) as req_cnt, sum(case when pcause='4294967295' then 1 else 0 end ) as reqsucess_cnt " +
      " from iot_mme_mm_zt m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"'  and m.pcause<>'4294967295'  " +
      " group by m.msisdn, m.pcause  " +
      "union all " +
      "select m.msisdn as mdn, case when m.pcause<>'4294967295' then 'success' else m.pcause end as pcause, count(*) as req_cnt,sum(case when pcause='4294967295' then 1 else 0 end ) as reqsucess_cnt " +
      " from iot_mme_sm_zt m " +
      " where m.dayid='" + partitiondayid + "' and m.hourid='"+ parthourid +"'  and m.pcause<>'4294967295'  " +
      " group by m.msisdn, m.pcause  "




  }

}
