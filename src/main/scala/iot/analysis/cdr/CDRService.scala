package iot.analysis.cdr

import iot.streaming.auth.hbase_auth_log
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.ConfigProperties

/**
  * Created by slview on 17-7-2.
  */
object CDRService {
  def registerCdrRDD(sc:SparkContext, htable:String):RDD[HbaseCdrLog] = {
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
    val cdrRDD = hbaseRDD.map(r=>HbaseCdrLog(
      (Bytes.toString(r._2.getRow)).split("-")(0), (Bytes.toString(r._2.getRow)).split("-")(1),
      Bytes.toString(r._2.getRow),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_haccg_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_haccg_downflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("c_pgw_downflow"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_haccg_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_haccg_downflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("p_pgw_downflow"))),

      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_haccg_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_haccg_downflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_pgw_upflow"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("flowinfo"),Bytes.toBytes("b_pgw_downflow")))
    ))
    cdrRDD
  }
}
