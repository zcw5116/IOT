package wlw

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by slview on 17-6-6.
  */
object testHiveDynamicPartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("testHiveDynamicPartition").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    hiveContext.sql("use default")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveContext.sql("insert into part_test partition(dayid) select 2, 'name', '20170605' from part_cdr_haccg_ticket limit 1")
    hiveContext.sql("from part_test  select count(*)").collect().foreach(println)
    sc.stop()
  }

}
