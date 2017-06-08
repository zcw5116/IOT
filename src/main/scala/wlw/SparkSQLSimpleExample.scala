package wlw

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._

object SparkSQLSimpleExample {
  case class word(wid:Int, aid:Int, times:Int)

  def ls(fileSystem:FileSystem, path:String) = {
    println("list path:" + path)
    val fs = fileSystem.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)
    for(p <- listPath){
      println(p)
    }
    println("--------------------------------")
  }

  def insertHive() = {
    val sparkConf = new SparkConf().setAppName("IOTCDRSparkSQL")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("use default")
    //val words = sc.textFile("hdfs://EPC-LOG-NM-15:8020/tmp/a.test").map(_.split(",")).filter(_.length == 3).map(w => word(w(0).toInt, w(1).toInt, w(2).toInt)).toDF()
    val words = sc.textFile("hdfs://cdh-nn1:8020/tmp/a.test").map(_.split(",")).filter(_.length == 3).map(w => word(w(0).toInt, w(1).toInt, w(2).toInt)).toDF()
    words.registerTempTable("word")
    hiveContext.sql("create table if not exists mytest (wid int, aid int, tid int )")
    //hiveContext.sql("insert into  mytest select wid, aid, times from word")
    hiveContext.sql("select wid,aid,times from word").collect().foreach(println)
    val test = hiveContext.sql("from mytest select count(*)").collect()
    val abc = hiveContext.sql("select wid, aid, tid from  mytest ").collect()
    for(k <- abc){
      val a = k(0)
      val b = k(1)
      val c = k(2)
      println(a + "**" + b + "##" +c)
    }

    hiveContext.sql("create table if not exists g3flow (mdn string, companyid string, companyname string, type string, sumtermination bigint, sumoriginating bigint)")
    hiveContext.sql("insert into  g3flow select  u.mdn, u.companyid,c.companyname, '3g' type,sum(termination) as sumtermination, sum(originating) sumoriginating " +
      "from part_cdr_3gaaa_ticketbak t, iot_user_info u, iot_company_info c " +
      "where t.mdn = u.mdn and u.companyid = c.companyid  and  t.timeid between 20170523020501 and  20170523021000 " +
      "group by u.mdn, u.companyid,c.companyname")
    sc.stop()
  }

  def main(args: Array[String]) {
    insertHive()
    val conf = new Configuration()
    println(conf)
    val fileSystem = FileSystem.get(conf)
    ls(fileSystem,"/")

  }
}
