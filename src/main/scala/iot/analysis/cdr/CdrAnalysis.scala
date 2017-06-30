package iot.analysis.cdr

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
import utils.HbaseUtil.createHTable

/**
  * Created by slview on 17-6-23.
  */
object CdrAnalysis {

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

    if (args.length < 1) {
      System.err.println("Usage: <yyyyMMddHH>")
      System.exit(1)
    }

    val starttimeid = args(0) + "00"
    val partitiondayid = starttimeid.substring(0, 8)
    val parthourid = starttimeid.substring(8, 10)


    // 将时间格式20170523091500转换为2017-05-23 09:15:00
    val starttimestr = getNextTimeStr(starttimeid, 0)

    // 根据开始时间获取600秒后的时间字符串
    val endtimestr = getNextTimeStr(starttimeid, 300)

    val endtimeid = endtimestr.replaceAll("[-: ]", "")

    // 分钟级别 比如 8点20转换为： 0820
    val startminuteid = starttimeid.substring(8,12)
    val endminuteid = endtimeid.substring(8,12)

    println(endtimestr)

    val sparkConf = new SparkConf()//.setAppName("AuthLogAnalysisHbase").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)



    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)

    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql("select u.mdn, u.vpdncompanycode " +
      "from iot_user_basic_info u ").coalesce(1).registerTempTable(cachedUserinfoTable)


    val cdrsql = "select nvl(u.vpdncompanycode,'N999999999') as vpdncompanycode, 'haccg' type,  nvl(sum(originating),0) as upflow, nvl(sum(termination),0) as downflow  " +
      "from  iot_cdr_haccg_ticket t, "+cachedUserinfoTable+" u  " +
      "where t.mdn = u.mdn and t.event_time>='" + starttimestr + "' and t.event_time<'" + endtimestr + "'  " +
      "      and t.dayid='" + partitiondayid + "' and t.hourid='" + parthourid +"'  " +
      " group by u.vpdncompanycode  " +
      " union all  " +
      " select nvl(u.vpdncompanycode,'N999999999') as vpdncompanycode, 'pgw' type, nvl(sum(l_datavolumefbcuplink),0) as upflow, nvl(sum(l_datavolumefbcdownlink),0) as downflow  " +
      " from  iot_cdr_pgw_ticket t, "+cachedUserinfoTable+" u   " +
      " where t.mdn = u.mdn and t.l_timeoflastusage>='"+starttimestr+"' and t.l_timeoflastusage<'"+endtimestr+"'  " +
      " and t.dayid='"+partitiondayid+"'  and t.hourid='" + parthourid + "'  " +
      " group by u.vpdncompanycode"

    val cdrdf = sqlContext.sql(cdrsql).coalesce(1)

    val hbasetable = "iot_cdr_flow_stat_" + partitiondayid

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    val connection= ConnectionFactory.createConnection(conf)
    val families = new Array[String](1)
    families(0) = "flowinfo"
    // 创建表, 如果表存在， 自动忽略
    createHTable(connection,hbasetable,families)


    val cdrJobConf = new JobConf(conf, this.getClass)
    cdrJobConf.setOutputFormat(classOf[TableOutputFormat])
    cdrJobConf.set(TableOutputFormat.OUTPUT_TABLE, hbasetable)


    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val hbaserdd = cdrdf.rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2), x.getLong(3)))
    val cdrcurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1 + "-" + startminuteid.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("c_"+arr._2+"_upflow"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("c_"+arr._2+"_downflow"), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    cdrcurrentrdd.saveAsHadoopDataset(cdrJobConf)

    val cdrnextrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val nextPut = new Put(Bytes.toBytes(arr._1 + "-" + endminuteid.toString))
      nextPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("p_"+arr._2+"_upflow"), Bytes.toBytes(arr._3.toString))
      nextPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("p_"+arr._2+"_downflow"), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, nextPut)
    }
    }
    cdrnextrdd.saveAsHadoopDataset(cdrJobConf)

    sc.stop()
  }
}
