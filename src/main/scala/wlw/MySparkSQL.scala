package wlw


import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by Administrator on 2015/7/7.
  */
object MySparkSQL {

  case class Trade(user_id: String, create_time: Long, update_time: Long, in_out_type: String, order_title: String, order_status: String, total_amount: Double)

  case class Balance(user_id: String, create_time: Long, update_time: Long, in_amount: Int, type1: String, balance: Double)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("XX").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val now = new java.util.Date()
    val today = now.getTime
    //注意 这边如果数字后面不加L会错误
    val oneYearAgo = today - (365L * 24L * 60L * 60L * 1000L)
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    import sqlContext.implicits._
    //下面的sdf.parse("...").getTime是将字符串形式的日期转化为Long型的，以便比较
    val trade = sc.textFile("hdfs://ns1/user/ubuntu/input/trade.txt").map(_.split(",")).filter(_.length == 7).map(t => Trade(t(0).trim, sdf.parse(t(1).trim).getTime, sdf.parse(t(2).trim).getTime, t(3).trim, t(4).trim, t(5).trim, t(6).trim.toDouble)).toDF()
    trade.registerTempTable("trade")

    val balance = sc.textFile("hdfs://ns1/user/ubuntu/input/balance.txt").map(_.split(",")).filter(_.length == 6).
      map(b => Balance(b(0).trim, sdf.parse(b(1).trim).getTime, sdf.parse(b(2).trim).getTime, b(3).trim.toInt, b(4).trim, b(5).trim.toDouble)).toDF()
    balance.registerTempTable("balance")

    val balanceArray = sqlContext.sql("select *  from balance where  create_time >= %s".format(oneYearAgo)).map(fields => (fields(0), fields(3).toString.toLong)).reduceByKey((_ + _)).filter(_._2 > 30000 * 12).map(x => x._1.toString).collect()
    val tradeArray = sqlContext.sql("select distinct user_id from trade where create_time <= %s".format(oneYearAgo)).map(x => x(0)).collect()

    tradeArray.filter(x => balanceArray.contains(x)).foreach(println)
  }
}