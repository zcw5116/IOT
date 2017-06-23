package iot.streaming.cdr

import iot.streaming.cdr.CDRStreamingTools.toHiveTable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by slview on 17-6-23.
  */
object CDRPgwStreaming {

  def main(args: Array[String]): Unit = {
    // 参数接收监控到目录
    if (args.length < 1) {
      System.err.println("Usage: <cdr-type>  <log-dir>")
      System.exit(1)
    }

    // 话单类型
    val cdrtype = "pgw"
    // 监控目录a
    val inputDirectory = args(0)

    val sparkConf = new SparkConf().setAppName("CDRPgwStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // 监控目录过滤以.uploading结尾的文件
    def uploadingFilter(path: Path): Boolean = !path.getName().endsWith("._COPYING_") && !path.getName().endsWith(".uploading")

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory,uploadingFilter(_), true).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    lines.foreachRDD(toHiveTable(_, cdrtype))
    // lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
