package wlw.test.access

/**
  * Created by zhoucw on 17-7-18.
  */
import java.util.Date

import wlw.test.access.util.{FileUtils, LogConvertUtils}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable


/**
  * 日志Spark处理
  */
object CdnCol extends Logging {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val appName = sc.getConf.get("spark.app.name")

    val inputPath = sc.getConf.get("spark.app.inputPath")
    val outputPath = sc.getConf.get("spark.app.outputPath")
    val fileCount = sc.getConf.get("spark.app.file.count").toInt
    val checkPeriod = sc.getConf.get("spark.app.check.period").toInt
    val checkTimes = sc.getConf.get("spark.app.check.times").toInt
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)

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

    try {
      val fileUploaded = FileUtils.checkFileUpload(fileSystem, inputPath, fileCount, checkPeriod, checkTimes, 0)
      logInfo("fileUploaded: " + fileUploaded) //测试完注释掉

      if (fileUploaded < fileCount) {
        logError("失败 文件上传不完全,文件路径[" + inputPath + "]期望文件数[" + fileCount + "]实际文件数[" + fileUploaded + "]")
        System.exit(1)
      }

      val coalesce = FileUtils.makeCoalesce(fileSystem, inputPath, coalesceSize)
      logInfo(s"$inputPath , $coalesceSize, $coalesce")

      var logDF = sqlContext.read.text(inputPath).coalesce(coalesce)

      logDF = sqlContext.createDataFrame(logDF.map(x =>
        LogConvertUtils.parseLog(x.getString(0))).filter(_.length != 1), LogConvertUtils.struct)



      logDF.write.mode(SaveMode.Overwrite).format("orc")
        .partitionBy(partitions.split(","):_*).save(outputPath + "temp/" + loadTime)

      logInfo("[" + appName + "] 转换用时 " + (new Date().getTime - begin) + " ms")

      begin = new Date().getTime

      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
      }

      FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)
      logInfo("[" + appName + "] 存储用时 " + (new Date().getTime - begin) + " ms")

      begin = new Date().getTime
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
          logInfo(s"partition $sql")
          sqlContext.sql(sql)
        }
      })

      logInfo("[" + appName + "] 刷新分区表用时 " + (new Date().getTime - begin) + " ms")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadTime)).map(fileSystem.delete(_, true))
        SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadTime)).map(fileSystem.delete(_, false))
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
    } finally {
      sc.stop()
    }
  }
}
