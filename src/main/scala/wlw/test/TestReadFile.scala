package wlw.test

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.ConfigProperties

/**
  * Created by zhoucw on 17-7-13.
  *
  */

case class PDSN(MDN:String, PCF:String, AcctSessionTime:String, Termination:String, Originating:String, CELLID:String, Option:String, AcctStatusType:String, EventTime:String, BSID:String)

object TestReadFile {

  val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sc)
  val hivedb = ConfigProperties.IOT_HIVE_DATABASE
  sqlContext.sql("use " + hivedb)

  val files = sqlContext.read.format("text").load("/tmp/pdsn/*.txt").coalesce(20)
  files.write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713")
  import  sqlContext.implicits._
  val orcfile = sqlContext.read.format("orc").load("/tmp/20170713").rdd.map(x => x.getString(0).split("\\|",10)).filter(_.length==10).map(x => PDSN(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9))).toDF()

  orcfile.printSchema()
  orcfile.registerTempTable("tmp")
  sqlContext.sql("select * from tmp limit 5").collect().foreach(println)

  val df =
  sqlContext.sql(
    s""" select  BSID, PCF, CELLID, count(*) as access_cnt, count(distinct MDN) as user_cnt,
       |sum(Originating + Termination ) as flow_sum, sum(AcctSessionTime) as duration  from tmp group by BSID, PCF, CELLID
     """.stripMargin)
  df.printSchema()
    df.coalesce(2).write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713out")


  sqlContext.sql(
    s""" select  BSID, PCF, CELLID, count(*) as access_cnt, count(distinct MDN) as user_cnt,
       |sum(Originating + Termination ) as flow_sum, sum(AcctSessionTime) as duration  from tmp group by BSID, PCF, CELLID
     """.stripMargin)

  /*
  create external table t_e(BSID string, PCF string, CELLID string,access_cnt bigint, user_cnt bigint , flow_sum double, duration double) stored as orc location '/tmp/20170713out';

  xport as CSV:
   insert overwrite local directory '/slview/test/zcw/tmp/pdsn' row format delimited fields terminated by ',' select * from t_e;

  */

  ///sqlContext.sql("select * from tmp1 limit 5").collect().foreach(println)


}
