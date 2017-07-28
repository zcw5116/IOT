package wlw.test.access

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-7-17.
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TopNStatJob").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val accessDF = sqlContext.read.format("parquet").load("/hadoop/zcw/clean/")
    accessDF.printSchema()
    accessDF.show(false)
    videoAccessTopNStat(sqlContext, accessDF)
  }

  def videoAccessTopNStat(sqlContext:SQLContext, accessDF:DataFrame) :Unit = {
    import sqlContext.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === "20161110" && $"cmsType" === "video")
    .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    videoAccessTopNDF.show(false)
  }

}
