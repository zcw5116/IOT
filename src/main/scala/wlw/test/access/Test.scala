package wlw.test.access

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import wlw.test.access.util.FileUtils

import scala.collection.mutable

/**
  * Created by zhoucw on 17-7-18.
  * 新建目录：  hdfs dfs -mkdir /tmp/outputPath/temp/123456/d=170718/h=11/m5=05
  */
object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FirstJob").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

   // val appName = sc.getConf.get("spark.app.name")

   // val inputPath = sc.getConf.get("spark.app.inputPath")
    //val outputPath = sc.getConf.get("spark.app.outputPath")
    val outputPath = "hdfs://cdh-nn1:8020/tmp/outputPath/"
   // val fileCount = sc.getConf.get("spark.app.file.count").toInt
    //val checkPeriod = sc.getConf.get("spark.app.check.period").toInt
    //val checkTimes = sc.getConf.get("spark.app.check.times").toInt
    //val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // val loadTime = appName.substring(appName.lastIndexOf("_") + 1)
    val loadTime = "123456"
    val fastDateFormat = FastDateFormat.getInstance("yyMMddHHmm")

    val partitions = "d,h,m5"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template
    }

    val sqlContext = new HiveContext(sc)
    var begin = new Date().getTime


    println(outputPath + "temp/" + loadTime + getTemplate + "/*.orc")
    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      println(nowPath)
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
    }

    println("filePartitions:"+filePartitions)

    FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)

    filePartitions.foreach(partition => {
      var d = ""
      var h = ""
      var m5 = ""
      partition.split("/").map(x => {
        if (x.startsWith("d=")) {
          d = x.substring(2)
        }
        if (x.startsWith("h=")) {
          h = x.substring(2)
        }
        if (x.startsWith("m5=")) {
          m5 = x.substring(3)
        }
        null
      })

      if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
        val sql = s"alter table test_cdncol add IF NOT EXISTS partition(d='$d', h='$h',m5='$m5')"

        println(sql)
      }
    })

  }
}
