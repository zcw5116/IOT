package iot.auth
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by slview on 17-6-28.
  */
//case class time(companycode:String,  name:String, aa:String)
case class hbase_auth_log(companycode:String, time:String, c_3g_auth_cnt:Long,c_3g_success_cnt:Long,c_3g_failed_cnt:Long,c_3g_authmdn_cnt:Long, c_3g_mdnfaield_cnt:Long,
                          c_4g_auth_cnt:Long,c_4g_success_cnt:Long,c_4g_failed_cnt:Long,c_4g_authmdn_cnt:Long, c_4g_mdnfaield_cnt:Long,
                          c_vpdn_auth_cnt:Long,c_vpdn_success_cnt:Long,c_vpdn_failed_cnt:Long,c_vpdn_authmdn_cnt:Long, c_vpdn_mdnfaield_cnt:Long)

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
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_auth_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_success_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_failed_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_authmdn_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_mdnfaield_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_auth_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_success_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_failed_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_authmdn_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_mdnfaield_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_auth_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_success_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_failed_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_authmdn_cnt"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_mdnfaield_cnt")))
    ))
    authRDD
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hbaseRdd1 = registerRDD(sc,"iot_userauth_day_20170628").toDF()
    hbaseRdd1.registerTempTable("shop22")

    // 测试
    val df2 = sqlContext.sql("SELECT * FROM shop22 limit 10").collect().foreach(println)

    //df2.foreach(println)

  }
}
