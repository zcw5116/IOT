package wlw.test

import java.text.SimpleDateFormat
import java.util.Date


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

  //根据起始时间和间隔， 计算出下个时间到字符串，精确到秒
  def getPreviousDay(currentdayid:String)={
    var df:SimpleDateFormat=new SimpleDateFormat("yyyyMMdd")
    var previous:Date=df.parse(currentdayid)
    var previoustime:Long = previous.getTime() - 24*3600*1000
    var previousdayid:String = df.format(new Date((previoustime)))
    previousdayid
  }


  def main(args: Array[String]): Unit = {
    println("Hello, Scala ")
    println(getNowDate())

   val dayid = "20170506"
    println(dayid.substring(0,6))

  }

}
