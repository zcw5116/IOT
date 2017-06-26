package wlw.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


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

  def main(args: Array[String]): Unit = {
    println(getNextTimeStr("20170617", -24*60*60))

  }

}
