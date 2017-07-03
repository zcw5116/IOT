package iot.analysis.cdr

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.ConfigProperties
import utils.DateUtil.timeCalcWithFormatConvert
import utils.HbaseUtil._

/**
  * Created by slview on 17-6-28.
  * 计算认证日志基线
  */

object CDRBaseData {

  def registerRDD(sc:SparkContext, htable:String):RDD[HbaseCDRBaseLog] = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    //hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    hBaseConf.set("hbase.zookeeper.quorum",ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    //设置zookeeper连接端口，默认2181
    //hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)

    hBaseConf.set(TableInputFormat.INPUT_TABLE,htable)

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val cdrDD = hbaseRDD.map(r=>HbaseCDRBaseLog(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_haccg_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_haccg_downflow"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pgw_downflow")))

    ))
    cdrDD
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val endtime = args(0)
    val starttime = timeCalcWithFormatConvert(endtime,"yyyyMMddHHmm",-7*24*60*60,"yyyyMMddHHmm")  //args(0)
    val startminu =  starttime.substring(8,12)
    val endminu = endtime.substring(8,12)
    // 写入到目标表+8小时
    val targetdayid = timeCalcWithFormatConvert(endtime,"yyyyMMddHHmm",12*60*60,"yyyyMMdd")

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    import sqlContext.implicits._
    val hbaseRdd1 = registerRDD(sc,"iot_cdr_flow_stat_20170630").toDF()
    val hbaseRdd2 = registerRDD(sc,"iot_cdr_flow_stat_20170701").toDF()
    //val hbaseRdd3 = registerRDD(sc,"iot_userauth_day_20170630").toDF()
    val htable1 = "htable1"
    val htable2 = "htable2"
    //val htable3 = "htable3"

    hbaseRdd1.registerTempTable(htable1)
    hbaseRdd2.registerTempTable(htable2)
    //hbaseRdd3.registerTempTable(htable3)

    val cdrsql = "select company_time, " +
      "  nvl(c_haccg_upflow,0) as c_haccg_upflow, nvl(c_haccg_downflow,0) as c_haccg_downflow, " +
      "  nvl(c_pgw_upflow,0) as c_pgw_upflow, nvl(c_pgw_downflow,0) as c_pgw_downflow  " +
      " from " + htable1 + "  where time>'" + startminu + "'  " +
      " union all  " +
      "select company_time, " +
      "  nvl(c_haccg_upflow,0) as c_haccg_upflow, nvl(c_haccg_downflow,0) as c_haccg_downflow, " +
      "  nvl(c_pgw_upflow,0) as c_pgw_upflow, nvl(c_pgw_downflow,0) as c_pgw_downflow  " +
      " from " + htable2 + " where time<'" + endminu + "'"

    println(cdrsql)
    val tmpcdrbasic = "tmpcdrbasic"
    sqlContext.sql("drop table if exists " + tmpcdrbasic)
    val regtmptable = "cdrregtmp"
    sqlContext.sql(cdrsql).coalesce(5).registerTempTable(regtmptable)
    sqlContext.sql("create table " + tmpcdrbasic + " as select company_time, " +
      " c_haccg_upflow, c_haccg_downflow, c_pgw_upflow, c_pgw_downflow  " +
      " from  " + regtmptable )

    val avgsql = "select company_time, " +
      "  avg(c_haccg_upflow) as c_haccg_upflow, avg(c_haccg_downflow) as c_haccg_downflow, " +
      "  avg(c_pgw_upflow) as c_pgw_upflow, avg(c_pgw_downflow) as c_pgw_downflow " +
      "  from  " + tmpcdrbasic + " o group by company_time"

    val cdrdf = sqlContext.sql(avgsql)


    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val targetHtable = "iot_cdr_flow_stat_" + targetdayid
    val families = new Array[String](1)
    families(0) = "flowinfo"
    createIfNotExists(targetHtable,families)


    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, targetHtable)


    // company_time, c_haccg_upflow, c_haccg_downflow, c_pgw_upflow, c_pgw_downflow
    val hbaserdd = cdrdf.rdd.map(x => (x.getString(0), x.getDouble(1), x.getDouble(2), x.getDouble(3),
      x.getDouble(4)))
    val authcurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("b_haccg_upflow"), Bytes.toBytes(arr._2.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("b_haccg_downflow"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("b_pgw_upflow"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("flowinfo"), Bytes.toBytes("b_pgw_downflow"), Bytes.toBytes(arr._5.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }

    authcurrentrdd.saveAsHadoopDataset(authJobConf)
  }
}
