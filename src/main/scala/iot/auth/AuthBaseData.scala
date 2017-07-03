package iot.auth
import iot.streaming.auth.hbase_auth_base_log
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
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

object AuthBaseData {

  def registerRDD(sc:SparkContext, htable:String):RDD[hbase_auth_base_log] = {
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
    val authRDD = hbaseRDD.map(r=>hbase_auth_base_log(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_success_cnt"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_success_cnt"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_success_cnt")))

    ))
    authRDD
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
    val hbaseRdd1 = registerRDD(sc,"iot_userauth_day_20170628").toDF()
    val hbaseRdd2 = registerRDD(sc,"iot_userauth_day_20170629").toDF()
    val hbaseRdd3 = registerRDD(sc,"iot_userauth_day_20170630").toDF()
    val htable1 = "htable1"
    val htable2 = "htable2"
    val htable3 = "htable3"

    hbaseRdd1.registerTempTable(htable1)
    hbaseRdd2.registerTempTable(htable2)
    hbaseRdd3.registerTempTable(htable3)

    val authsql = "select company_time, " +
      "  nvl(c_3g_auth_cnt,0) as c_3g_auth_cnt, nvl(c_3g_success_cnt,0) as c_3g_success_cnt, " +
      "  nvl(c_4g_auth_cnt,0) as c_4g_auth_cnt, nvl(c_4g_success_cnt,0) as c_4g_success_cnt, " +
      "  nvl(c_vpdn_auth_cnt,0) as c_vpdn_auth_cnt, nvl(c_vpdn_success_cnt,0) as c_vpdn_success_cnt " +
      " from " + htable1 + "  where time>'" + startminu + "'  " +
      " union all  " +
      "select company_time, " +
      "  nvl(c_3g_auth_cnt,0) as c_3g_auth_cnt, nvl(c_3g_success_cnt,0) as c_3g_success_cnt, " +
      "  nvl(c_4g_auth_cnt,0) as c_4g_auth_cnt, nvl(c_4g_success_cnt,0) as c_4g_success_cnt, " +
      "  nvl(c_vpdn_auth_cnt,0) as c_vpdn_auth_cnt, nvl(c_vpdn_success_cnt,0) as c_vpdn_success_cnt " +
      " from " + htable2 +
      " union all  " +
      "select company_time, " +
      "  nvl(c_3g_auth_cnt,0) as c_3g_auth_cnt, nvl(c_3g_success_cnt,0) as c_3g_success_cnt, " +
      "  nvl(c_4g_auth_cnt,0) as c_4g_auth_cnt, nvl(c_4g_success_cnt,0) as c_4g_success_cnt, " +
      "  nvl(c_vpdn_auth_cnt,0) as c_vpdn_auth_cnt, nvl(c_vpdn_success_cnt,0) as c_vpdn_success_cnt " +
      " from " + htable3 + " where time<'" + endminu + "'"

    println(authsql)
    val tmpauthbasic = "tmpauthbasic"
    sqlContext.sql("drop table if exists " + tmpauthbasic)
    sqlContext.sql(authsql).coalesce(5).registerTempTable("authbasedatatmp")
    sqlContext.sql("create table " + tmpauthbasic + " as select company_time, " +
      "c_3g_auth_cnt, c_3g_success_cnt, c_4g_auth_cnt, c_4g_success_cnt, " +
      " c_vpdn_auth_cnt, c_vpdn_success_cnt  from  authbasedatatmp" )

    val avgsql = "select company_time, " +
      "  avg(c_3g_auth_cnt) as c_3g_auth_cnt, avg(c_3g_success_cnt) as c_3g_success_cnt, " +
      "  avg(c_4g_auth_cnt) as c_4g_auth_cnt, avg(c_4g_success_cnt) as c_4g_success_cnt, " +
      "  avg(c_vpdn_auth_cnt) as c_vpdn_auth_cnt, avg(c_vpdn_success_cnt) as c_vpdn_success_cnt  " +
      "  from  " + tmpauthbasic + " o group by company_time"

    val authdf = sqlContext.sql(avgsql)


    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val targetHtable = "iot_userauth_day_" + targetdayid
    val families = new Array[String](2)
    families(0) = "authresult"
    families(1) = "authfailed"
    createIfNotExists(targetHtable,families)


    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, targetHtable)


    // c_3g_auth_cnt, c_3g_success_cnt, c_4g_auth_cnt, c_4g_success_cnt, c_vpdn_auth_cnt, c_vpdn_success_cnt
    val hbaserdd = authdf.rdd.map(x => (x.getString(0), x.getDouble(1), x.getDouble(2), x.getDouble(3),
      x.getDouble(4), x.getDouble(5), x.getDouble(6)))
    val authcurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_auth_cnt"), Bytes.toBytes(arr._2.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_success_cnt"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_4g_auth_cnt"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_4g_success_cnt"), Bytes.toBytes(arr._5.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_vpdn_auth_cnt"), Bytes.toBytes(arr._6.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_vpdn_success_cnt"), Bytes.toBytes(arr._7.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }

    authcurrentrdd.saveAsHadoopDataset(authJobConf)




  }
}
