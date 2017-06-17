package qoe

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import utils.{RedisClient, RedisProperties}


case class cdnnodeinfo(tag_node: String, node: String, node_addr: String, node_capacity: Long, node_flow: Long)

case class cdnserverinfo(tag_node: String, server_ip: String, server_tag: String, server_info: String, server_capacity: Long, server_flow: Long)

/**
  * Created by slview on 17-6-6.
  */
object CDNHour {

  val filterF = new Function[Path, Boolean] {
    def apply(x: Path): Boolean = {
      val flag = if(x.toString.split(".").last.toString.endsWith("uploading")) true else false
      println("uploading")
      return flag
    }
  }

  def getNowDayid(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var dayid = dateFormat.format(now)
    dayid
  }

  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      System.err.println("Usage:<hourid>")
      System.exit(1)
    }

    val hourid = args(0)
    val dayid = hourid.substring(1,8)

    val sparkConf = new SparkConf().setAppName("IOTCDRSparkSQL").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val jedis = new Jedis(RedisProperties.REDIS_SERVER, RedisProperties.REDIS_PORT)
    jedis.auth(RedisProperties.REDIS_PASSWORD)
    val cdnfile = jedis.hget("qoe::cdn::location", "cdn")
    val serverfile = jedis.hget("qoe::cdn::location", "server")
    println(cdnfile)
    println(serverfile)


    val cdnnoderdd = sc.textFile("hdfs://nameservice1" + cdnfile).map( _.split("\\|\\|")).map(n => cdnnodeinfo(n(0), n(1), n(2), n(3).toLong, n(4).toLong))
    val cdnserverrdd = sc.textFile("hdfs://nameservice1" + serverfile).map(_.split("\\|\\|")).map(s => cdnserverinfo(s(0), s(1), s(2), s(3), s(4).toLong, s(5).toLong))
    cdnnoderdd.toDF().registerTempTable("cdnnodeinfo")
    cdnserverrdd.toDF().registerTempTable("cdnserverinfo")
    hiveContext.sql("use default")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    hiveContext.sql("insert into cdnfluxh partition(monthid) " +
      "select tag_parent_live as livefathercdnnode, tag_parent_vod as vodfathercdnnode, n.tag_node as cdnnode, substr(n.time,1,10) as fluxtime, " +
      "avg(n.capacity) as capacity, avg(count_in_flow) as totalinavgvec, avg(count_out_flow) as totaloutavgvec,  " +
      "avg(live_in_flow) as liveinavgvec, avg(live_out_flow) as liveoutavgvec," +
      "avg(vod_in_flow) as vodinavgvec, avg(vod_out_flow)  as vodoutavgvec," +
      "avg(n.capacity/c1.node_capacity) as capacityratio, " +
      "avg(n.count_out_flow/c1.node_flow) as totaloutavgratio," +
      "avg(n.count_in_flow/c2.node_flow)  as totalinavgratio," +
      " '' as liveinavgratio, '' as liveoutavgratio, '' as vodoutavgratio, '' as vodinavgratio," +
      " substr(n.time,1,6) as dayid" +
      "  from cdnnode n, cdnnodeinfo c1, cdnnodeinfo c2 " +
      "where n.tag_node=c1.tag_node and n.tag_parent_live=c2.tag_node  and substr(n.time,1,10)=" + hourid +
      " group by tag_parent_live, tag_parent_vod, n.tag_node, substr(n.time,1,10), substr(n.time,1,6)")


    hiveContext.sql("insert into cdnserverfluxh partition(dayid) " +
      "select s.tag as servertag, substr(s.time,1,10) as fluxtime, " +
      "avg(s.capacity) as capacity , avg(s.capacity/i.server_capacity) as capacityratio," +
      "avg(s.count_in_flow) as inavgvec, '' as inavgratio, avg(s.count_out_flow) as outavgvec, " +
      "avg(s.count_out_flow/i.server_flow) as outavgratio," +
      "substr(s.time,1,8) as dayid " +
      "from cdnserver s, cdnserverinfo i " +
      "where s.tag=i.server_tag and substr(s.time,1,10)=" + hourid +
      " group by s.tag, substr(s.time,1,10), substr(s.time,1,8)")


  }

}
