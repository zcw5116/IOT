package qoe

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import utils.{RedisClient, RedisProperties}

case class cdnnodeinfo(tag_node:String, node:String, node_addr:String, node_capacity:Long, node_flow:Long)
case class cdnserverinfo(tag_node:String, server_ip:String, server_tag:String, server_info:String, server_capacity:Long, server_flow:Long)
/**
  * Created by slview on 17-6-6.
  */
object CDNHour {

  def getNowDayid():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var dayid = dateFormat.format( now )
    return dayid
  }

  def main(args: Array[String]): Unit = {
    val dayid = getNowDayid()
    val sparkConf = new SparkConf().setAppName("IOTCDRSparkSQL").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val jedis = new Jedis(RedisProperties.REDIS_SERVER,RedisProperties.REDIS_PORT)
    val cdnfile = jedis.hget("qoe::cdn::location","cdn");
    val serverfile = jedis.hget("qoe::cdn::location","server")

    val cdnnoderdd = sc.textFile("hdfs://cdh-nn1:8020/hadoop/qoe/cdninfo/cdnnodeinfo.txt").map(_.split(",")).map(n => cdnnodeinfo(n(0), n(1),n(2), n(3).toLong,n(4).toLong))
    val cdnserverrdd = sc.textFile("hdfs://cdh-nn1:8020/hadoop/qoe/cdninfo/cdnserverinfo.txt").map(_.split(",")).map(s => cdnserverinfo(s(0), s(1),s(2), s(3), s(4).toLong,s(5).toLong))
    cdnnoderdd.toDF().registerTempTable("cdnnodeinfo")
    cdnserverrdd.toDF().registerTempTable("cdnserverinfo")
    hiveContext.sql("use default")
    hiveContext.sql("create table if not exists mycdnnode (tag_node string, time string, capacity bigint, " +
      "count_in_flow bigint, count_out_flow bigint, live_in_flow bigint, live_out_flow bigint, vod_in_flow bigint, " +
      "vod_out_flow bigint, tag_parent_live string, tag_parent_vod string, capacityratio float, outflowratio float, inflowratio float )")

    hiveContext.sql("insert into  mycdnnode select n.tag_node, n.time, n.capacity, n.count_in_flow, n.count_out_flow, " +
      "n.live_in_flow, n.live_out_flow, n.vod_in_flow, n.vod_out_flow, n.tag_parent_live, n.tag_parent_vod, " +
      "n.capacity/c1.node_capacity as capacityratio, n.count_out_flow/c1.node_flow as outflowratio, n.count_in_flow/c2.node_flow as inflowratio " +
      "from cdnnode n, cdnnodeinfo c1, cdnnodeinfo c2 where n.tag_node=c1.tag_node" +
      " and n.tag_parent_live=c2.tag_node")


    /*hiveContext.sql("create table if not exists mycdnserver (ip_addr string, tag string, time string, " +
      "capacity bigint, count_in_flow bigint, count_out_flow bigint, capacityratio float, bandratio float )")
    //hiveContext.sql("select server_tag,server_capacity, server_flow from cdnserverinfo").collect().foreach(println)
    hiveContext.sql("insert into  mycdnserver select s.ip_addr, s.tag, s.time, s.capacity, s.count_in_flow, s.count_out_flow, " +
      "s.capacity/i.server_capacity as capacityratio,s.count_out_flow/i.server_flow as bandratio " +
     "from cdnserver s, cdnserverinfo i where s.tag=i.server_tag")*/



    //hiveContext.sql("use default")
    //hiveContext.sql("select tag_node,time,capacity,count_in_flow,count_out_flow,live_in_flow,live_out_flow,vod_in_flow,vod_out_flow,tag_parent_live,tag_parent_vod " +
    //  ", vod_in_flow/n.server_flow, vod_out_flow/n.server_flow  from cdnnode partition(" + dayid + ") n, nodeinfo i where n.tag_node = i.tag_node")



  }

}
