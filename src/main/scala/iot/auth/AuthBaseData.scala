package iot.auth
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
import utils.HbaseUtil
/**
  * Created by slview on 17-6-28.
  */
//case class time(companycode:String,  name:String, aa:String)String
case class hbase_auth_log(companycode:String, time:String, company_time:String, c_3g_auth_cnt:String,c_3g_success_cnt:String,c_3g_failed_cnt:String,c_3g_authmdn_cnt:String, c_3g_mdnfaield_cnt:String,
                          c_4g_auth_cnt:String,c_4g_success_cnt:String,c_4g_failed_cnt:String,c_4g_authmdn_cnt:String, c_4g_mdnfaield_cnt:String,
                          c_vpdn_auth_cnt:String,c_vpdn_success_cnt:String,c_vpdn_failed_cnt:String,c_vpdn_authmdn_cnt:String, c_vpdn_mdnfaield_cnt:String)

object AuthBaseData {

  def registerRDD(sc:SparkContext, htable:String):RDD[hbase_auth_log] = {
    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    //设置zookeeper连接端口，默认2181
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    hBaseConf.set(TableInputFormat.INPUT_TABLE,htable)

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val authRDD = hbaseRDD.map(r=>hbase_auth_log(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_failed_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_authmdn_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_mdnfaield_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_failed_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_authmdn_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_mdnfaield_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_failed_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_authmdn_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_mdnfaield_cnt")))
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
    val targetdayid = timeCalcWithFormatConvert(endtime,"yyyyMMddHHmm",8*60*60,"yyyyMMdd")


    import sqlContext.implicits._
    val hbaseRdd1 = registerRDD(sc,"iot_userauth_day_20170628").toDF()
    val hbaseRdd2 = registerRDD(sc,"iot_userauth_day_20170629").toDF()
    val htable1 = "htable1"
    val htable2 = "htable2"

    hbaseRdd1.registerTempTable(htable1)
    hbaseRdd2.registerTempTable(htable2)

    val authsql = "select company_time, c_3g_auth_cnt, c_3g_success_cnt, c_3g_failed_cnt, c_3g_authmdn_cnt, c_3g_mdnfaield_cnt," +
      "  c_4g_auth_cnt, c_4g_success_cnt, c_4g_failed_cnt, c_4g_authmdn_cnt, c_4g_mdnfaield_cnt," +
      " c_vpdn_auth_cnt, c_vpdn_success_cnt, c_vpdn_failed_cnt, c_vpdn_authmdn_cnt, c_vpdn_mdnfaield_cnt " +
      " from " + htable1 + "  where time>'" + startminu + "'  " +
      " union all  " +
      "select company_time, c_3g_auth_cnt, c_3g_success_cnt, c_3g_failed_cnt, c_3g_authmdn_cnt, c_3g_mdnfaield_cnt," +
      "  c_4g_auth_cnt, c_4g_success_cnt, c_4g_failed_cnt, c_4g_authmdn_cnt, c_4g_mdnfaield_cnt," +
      " c_vpdn_auth_cnt, c_vpdn_success_cnt, c_vpdn_failed_cnt, c_vpdn_authmdn_cnt, c_vpdn_mdnfaield_cnt " +
      " from " + htable2 + " where time<'" + endminu + "'"

    println(authsql)

    val avgsql = "select company_time, nvl(avg(c_3g_auth_cnt),0) as c_3g_auth_cnt, nvl(avg(c_3g_success_cnt),0) as c_3g_success_cnt, nvl(avg(c_3g_failed_cnt),0) as c_3g_failed_cnt, nvl(avg(c_3g_authmdn_cnt),0) as c_3g_authmdn_cnt, nvl(avg(c_3g_mdnfaield_cnt),0) as c_3g_mdnfaield_cnt," +
      "  nvl(avg(c_4g_auth_cnt),0) as c_4g_auth_cnt, nvl(avg(c_4g_success_cnt),0) as c_4g_success_cnt, nvl(avg(c_4g_failed_cnt),0) as c_4g_failed_cnt, nvl(avg(c_4g_authmdn_cnt),0) as c_4g_authmdn_cnt, nvl(avg(c_4g_mdnfaield_cnt),0) as c_4g_mdnfaield_cnt," +
      " nvl(avg(c_vpdn_auth_cnt),0) as c_vpdn_auth_cnt, nvl(avg(c_vpdn_success_cnt),0) as c_vpdn_success_cnt, nvl(avg(c_vpdn_failed_cnt),0) as c_vpdn_failed_cnt, nvl(avg(c_vpdn_authmdn_cnt),0) as c_vpdn_authmdn_cnt, nvl(avg(c_vpdn_mdnfaield_cnt),0) as  c_vpdn_mdnfaield_cnt " +
      " from  ( " + authsql + " ) o group by company_time"

    val authdf = sqlContext.sql(avgsql)


    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)

    val authJobConf = new JobConf(conf, this.getClass)
    authJobConf.setOutputFormat(classOf[TableOutputFormat])
    authJobConf.set(TableOutputFormat.OUTPUT_TABLE, "iot_userauth_day_" + targetdayid)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val hbaserdd = authdf.rdd.map(x => (x.getString(0), x.getDouble(1), x.getDouble(2), x.getDouble(3),
      x.getDouble(4), x.getDouble(5), x.getDouble(6), x.getDouble(7), x.getDouble(8), x.getDouble(9), x.getDouble(10),
      x.getDouble(11), x.getDouble(12), x.getDouble(13), x.getDouble(14), x.getDouble(15)))
    val authcurrentrdd = hbaserdd.map { arr => {
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */

      val currentPut = new Put(Bytes.toBytes(arr._1))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_auth_cnt"), Bytes.toBytes(arr._2.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_success_cnt"), Bytes.toBytes(arr._3.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_failed_cnt"), Bytes.toBytes(arr._4.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_authmdn_cnt"), Bytes.toBytes(arr._5.toString))
      currentPut.addColumn(Bytes.toBytes("authresult"), Bytes.toBytes("b_3g_mdnfaield_cnt"), Bytes.toBytes(arr._6.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }

    authcurrentrdd.saveAsHadoopDataset(authJobConf)
    // 测试
    val df2 = sqlContext.sql(avgsql).collect().foreach(println)

    //df2.foreach(println)
  }
}
