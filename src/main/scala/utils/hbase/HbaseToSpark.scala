package utils.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by slview on 17-6-21.
  */
object HBaseSpark {
  def main(args:Array[String]): Unit ={

    // 本地模式运行,便于测试
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("HBaseTest")

    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.quorum", "cdh-nn1,cdh-dn1,cdh-yarn1")

    hBaseConf.set(TableInputFormat.INPUT_TABLE,"kaizi")

    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    // 将数据映射为表  也就是将 RDD转化为 dataframe schema
    val shop = hbaseRDD.map(r=>(
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("basicxiaohe"),Bytes.toBytes("name"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("basicxiaohe"),Bytes.toBytes("age")))
    )).toDF("rowkey","name","age")




    shop.registerTempTable("shop")

    // 测试
    val df2 = sqlContext.sql("SELECT rowkey,name, age FROM shop")

    df2.foreach(println)
  }

}
