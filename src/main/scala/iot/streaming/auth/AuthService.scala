package iot.streaming.auth

import java.text.DecimalFormat

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.ConfigProperties


/**
  * Created by slview on 17-6-30.
  */
object AuthService {
  def registerRDD(sc:SparkContext, htable:String):RDD[hbase_auth_log] = {
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
    val authRDD = hbaseRDD.map(r=>hbase_auth_log(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_3g_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("b_3g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("b_3g_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("p_3g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("p_3g_success_cnt"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_4g_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("b_4g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("b_4g_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("p_4g_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("p_4g_success_cnt"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("c_vpdn_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("b_vpdn_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("b_vpdn_success_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("p_vpdn_auth_cnt"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("authresult"),Bytes.toBytes("p_vpdn_success_cnt")))
    ))
    authRDD
  }

  def divOpera(numerator:String, denominator:String ):String = {
    try {
      val ratio = if (denominator.toInt <=0 ) 0 else  if(denominator.toInt > numerator.toInt)  numerator.toFloat / denominator.toInt else 1
      f"$ratio%1.4f"
    } catch {
      case e => {
        println(e.getMessage)
        "0"
      }
    }
  }

}
