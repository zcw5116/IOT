package wlw.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

/**
  * Created by slview on 17-6-20.
  */

object HbaseTest {
  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("Usage: SaveData <input file>")
      System.exit(1)
    }
    val sconf = new SparkConf().setAppName("SaveRDDToHBase")
    val sc = new SparkContext(sconf)
    val input = sc.objectFile[((String,Int),Seq[(String,Int)])](args(0))
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop04")
    val jobConf = new JobConf(hconf, this.getClass)
    //指定输出表
    val tableName = "wanglei:" + args(0).split('/')(1)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    //设置job的输出格式
    val job = new Job(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val data = input.map{case(k,v) => convert(k,v)}
    //保存到HBase表
    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }

  //RDD转换函数
  def convert(k:(String,Int), v: Iterable[(String, Int)]) = {
    val rowkey = (k._1).reverse + k._2
    val put = new Put(Bytes.toBytes(rowkey))
    val iter = v.iterator
    while(iter.hasNext) {
      val pair = iter.next()
      put.addColumn(Bytes.toBytes("labels"), Bytes.toBytes(pair._1), Bytes.toBytes(pair._2))
    }
    (new ImmutableBytesWritable, put)
  }
}
