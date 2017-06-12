package iot.cdr

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by slview on 17-6-12.
  */
object TestSparkStreaming {

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("Usage: <log-dir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SpoolDirSpark").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val inputDirectory = args(0)

    val filterF = new Function[Path, Boolean] {
      def apply(x: Path): Boolean = {
        val flag = if(x.toString.split(".").last.toString.endsWith("uploading")) true else false
        if(flag){
          println("uploading")
        }
        else{
          println("nouploading")
        }
        return flag
      }
    }

    def defaultFilter(path: Path): Boolean = !path.getName().endsWith("uploading")

    def uploadingFilter(path: Path): Boolean = {
         if(!path.getName().endsWith("uploading")){
           println("not uploading")
           true
         }else
           {
             println("uploading")
             false
           }
    }


    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory,uploadingFilter(_), false).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    lines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
