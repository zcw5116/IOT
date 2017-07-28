package wlw.test.access

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-7-17.
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStatCleanJob").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val accessRDD = sc.textFile("/hadoop/zcw/out1/part*")
    // accessRDD.take(20).foreach(println)

    val accessDF = sqlContext.createDataFrame(accessRDD.map(x => AccessConverUtil.parseLog(x)), AccessConverUtil.struct).filter(" cmsType in ('video','article') ")

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("cmsType","day").save("/hadoop/zcw/clean")

    sc.stop()
  }
}
