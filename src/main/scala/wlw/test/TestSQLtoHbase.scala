package wlw.test


import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by slview on 17-6-19.
  */
object TestSQLtoHbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkOnHBase").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    // 创建Hbase连接， 有两种方式：1.将hbase的配置文件复制到resources目录下面， 无需使用conf.set设置属性。 2.通过conf.set设置hbase_site.xml里面的属性
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")

    // ======Save RDD to HBase========
    // step 1: JobConf setup
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "kaizi")

    // step 2: rdd mapping to table

    // 在 HBase 中表的 schema 一般是这样的
    // *row   cf:col_1    cf:col_2
    // 而在Spark中，我们操作的是RDD元组，比如(1,"lilei",14) , (2,"hanmei",18)
    // 我们需要将 *RDD[(uid:Int, name:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    // 我们定义了 convert 函数做这个转换工作
    def convert(triple: (String, String, Long)) = {
      val p = new Put(Bytes.toBytes(triple._1 + triple._2 ))
      p.addColumn(Bytes.toBytes("basicxiaohe"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      p.addColumn(Bytes.toBytes("basicxiaohe"), Bytes.toBytes("age"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, p)
    }

    // step 3: read RDD data from somewhere and convert
    val rawData = List((1, "lilei", 14), (2, "hanmei", 18), (3, "someone", 38))
    //val localData = sc.parallelize(rawData).map(convert)

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    sqlContext.sql("use iot")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val mydf = sqlContext.sql("select nettype,userprovince, count(*) as cnt from  iot_user_basic_info " +
      " group by nettype,userprovince ").rdd.map(x => (x.getString(0), x.getString(1), x.getLong(2)))
    val cl = mydf.map(convert)

    //val myrdd = mydf.map(x => (x(0).toString + x(1).toString + x(2).toString + "test1", x(1).toString, x(2).toString)).map(convert)

    // myrdd.collect().foreach(println)
   cl.saveAsHadoopDataset(jobConf)




    sc.stop()
  }


}
