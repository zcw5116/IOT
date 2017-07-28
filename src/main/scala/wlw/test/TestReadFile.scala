package wlw.test

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import utils.ConfigProperties

/**
  * Created by zhoucw on 17-7-13.
  *
  */

case class PDSN(MDN:String, PCF:String, AcctSessionTime:String, Termination:String, Originating:String, CELLID:String, Option:String,
                AcctStatusType:String, EventTime:String, BSID:String,AcctInputPackets:String,AcctOutputPackets:String,ActiveTime:String )
case class MDN(mdn:String)

object TestReadFile {

  val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sc)
  val hivedb = ConfigProperties.IOT_HIVE_DATABASE
  sqlContext.sql("use " + hivedb)

  val files = sqlContext.read.format("text").load("/tmp/pdsn/*.txt").coalesce(20)
  files.select("T4","T20","T53","T34","T35","T806","T27","T1","T37","T21","T54","T55","T38").write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713")

  val files2 = sqlContext.read.format("orc").load("/tmp/20170713").coalesce(20)

  files2.select("T4","T20","T53","T34","T35","T806","T27","T1","T37","T21","T54","T55","T38").withColumnRenamed("T4","MDN")
    .withColumnRenamed("T20","PCF").withColumnRenamed("T53","AcctSessionTime").withColumnRenamed("T34","Termination").withColumnRenamed("T35","Originating")
    .withColumnRenamed("T806","CELLID").withColumnRenamed("T27","Option").withColumnRenamed("T1","AcctStatusType").withColumnRenamed("T37","EventTime")
    .withColumnRenamed("T21","BSID").withColumnRenamed("T54","AcctInputPackets").withColumnRenamed("T55","AcctOutputPackets").withColumnRenamed("T38","ActiveTime")
  .registerTempTable("tmp")


  files.write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713")




  import  sqlContext.implicits._
  val orcfile = sqlContext.read.format("orc").load("/tmp/20170713").rdd.map(x => x.getString(0).split("\\|",12)).
    filter(_.length==12).map(x => PDSN(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12))).toDF()

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

  val df2 =
    sqlContext.sql(
      s""" select  BSID, PCF, CELLID, count(*) as access_cnt, count(distinct MDN) as user_cnt,
         |sum(Originating + Termination ) as flow_sum, sum(AcctSessionTime) as accesstime_sum,  sum(AcctInputPackets + AcctOutputPackets ) as pact_sum,
         |sum(ActiveTime) as activetime_sum
         | from tmp where AcctStatusType='2' and Option='33'
         | group by BSID, PCF, CELLID
     """.stripMargin)
  df.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713out")


  val file3 = sqlContext.read.format("text").load("/hadoop/IOT/ANALY_PLATFORM/interphone/mdn/mdn.txt").
    rdd.map(x=>x.getString(0)).map(_.split("\\|")).map(x => MDN("86" + x(1).trim)).toDF()
  file3.registerTempTable("mdn")

  val df3 =
    sqlContext.sql(
      s""" select  BSID, PCF, CELLID, count(*) as access_cnt, count(distinct t.MDN) as user_cnt,
         |sum(Originating + Termination ) as flow_sum, sum(AcctSessionTime) as accesstime_sum,  sum(AcctInputPackets + AcctOutputPackets ) as pact_sum,
         |sum(ActiveTime) as activetime_sum
         | from mdn m , tmp t  where t.mdn = m.mdn and AcctStatusType='2' and Option='33'
         | group by BSID, PCF, CELLID
     """.stripMargin)
  df3.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713out")



  file3.write.mode(SaveMode.Overwrite).format("orc").save(s"/tmp/20170713")
  /*
  create external table t_e(BSID string, PCF string, CELLID string,access_cnt bigint, user_cnt bigint , flow_sum double, duration double) stored as orc location '/tmp/20170713out';

  xport as CSV:
   insert overwrite local directory '/slview/test/zcw/tmp/pdsn' row format delimited fields terminated by ',' select * from t_e;

drop table t_e;
create external table t_e(BSID string, PCF string, CELLID string,access_cnt bigint, user_cnt bigint , flow_sum double, acctime_sum double, pact_sum double, activetime_sum double) stored as orc location '/tmp/20170713out';

  xport as CSV:
insert overwrite local directory '/slview/test/zcw/tmp/pdsn1' row format delimited fields terminated by ',' select substr(BSID,1, 12) as bsid,pcf,cellid,access_cnt,user_cnt,flow_sum,acctime_sum,pact_sum, activetime_sum from t_e;

  */

  ///sqlContext.sql("select * from tmp1 limit 5").collect().foreach(println)




}
