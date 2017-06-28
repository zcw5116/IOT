package utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-28.
  */
object CombineFiles {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AuthLogAnalysisHbase")
    //.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val orcfile = "hdfs:///user/hive/warehouse/iot.db/iot_cdr_pgw_ticket/dayid=20170624/*"
    val df = sqlContext.read.format("orc").load(orcfile)
    df.coalesce(10).write.format("orc").save("/tmp/zzz")


    val rows = sc.hadoopConfiguration
  }
}
