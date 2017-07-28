package wlw.test

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import iot.users.UsersInfoIncETL.getNextTimeStr
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import utils.DateUtil.timeCalcWithFormatConvert


/**
  * Created by slview on 17-5-27.
  */
object test {
  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var hehe = dateFormat.format( now )
    hehe
  }

  def getNowTime():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmms")
    var timeid = dateFormat.format( now )
    timeid
  }

  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getPreviousDay(currentdayid:String)={
    var df:SimpleDateFormat=new SimpleDateFormat("yyyyMMdd")
    var previous:Date=df.parse(currentdayid)
    var previoustime:Long = previous.getTime() - 24*3600*1000
    var previousdayid:String = df.format(new Date((previoustime)))
    previousdayid
  }


  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getNextTimeStr(start_time: String, stepSeconds: Long) = {
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var begin: Date = df.parse(start_time)
    var endstr: Long = begin.getTime() + stepSeconds * 1000
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var nextTimeStr: String = sdf.format(new Date((endstr)))
    nextTimeStr
  }

  def getDayidViaStep(step:Int):String = {
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE, step)
    var dayid=dateFormat.format(cal.getTime())
    dayid
  }


  //创建表
  def createHTable(connection: Connection,tablename: String, familyarr:Array[String]): Unit=
  {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)

      familyarr.foreach(f => tableDescriptor.addFamily(new HColumnDescriptor(f.getBytes())))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
  }

  def divOpera(numerator:String, denominator:String ):String = {
    try {
      val ratio = if (denominator.toInt > 0) numerator.toFloat / denominator.toInt else if (numerator.toInt > denominator.toInt) 1 else 0
      f"$ratio%1.4f"
    } catch {
      case e => {
        println(e.getMessage)
        "0"
      }
    }
  }



  def main(args: Array[String]): Unit = {
    /*val starttimeid = "20170523091500"
    val parthourid = starttimeid.substring(8,12)
    println(parthourid)

    val conf = HBaseConfiguration.create
    val tablename = "blog"
   // conf.set("hbase.zookeeper.quorum", "EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    //conf.set("hbase.zookeeper.property.clientPort", "2181")
    //val connection= ConnectionFactory.createConnection(conf)

    val familys = new Array[String](3)
    familys(0) = "test1"
    familys(1) = "test2"
    familys(2) = "test3"
   // createHTable(connection, "blog2",familys)

    val yesterday = getNextTimeStr("20170630", -24 * 60 * 60)
    println(yesterday)*/

   //println(divOpera("ab","10"))

    //println(timeCalcWithFormatConvert("20170701"+"000000","yyyyMMddHHmmss",0,"yyyy-MM-dd HH:mm:ss"))
    val url = "-"
    val domain = "http://www.imooc.com/"
    val cms = url.substring(url.indexOf(domain) + domain.length)
    val cmsTypeId = cms.split("/")



  }

}
