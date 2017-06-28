package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by slview on 17-6-27.
  */
object DateUtil {
  def getNowTime():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmms")
    var timeid = dateFormat.format( now )
    timeid
  }

  def getNextday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, 1)
    var nextday = dateFormat.format(cal.getTime())
    nextday
  }

  def getNextTime(start_time: String, stepSeconds: Long, format:String) = {
    var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var begin: Date = df.parse(start_time)
    var endstr: Long = begin.getTime() + stepSeconds * 1000
    var sdf: SimpleDateFormat = new SimpleDateFormat(format)
    var nextTimeStr: String = sdf.format(new Date((endstr)))
    nextTimeStr
  }

  def timeFormatConvert(sourcetime:String, sourceformat:String, targetformat:String):String = {
    var sourceDF: SimpleDateFormat = new SimpleDateFormat(sourceformat)
    var sourceDate: Date = sourceDF.parse(sourcetime)
    var sourceTime: Long = sourceDate.getTime() + 0
    var targetDF: SimpleDateFormat = new SimpleDateFormat(targetformat)
    var targettime: String = targetDF.format(new Date((sourceTime)))
    targettime
  }


  def main(args: Array[String]): Unit = {
    println(timeFormatConvert("20170628230500","yyyyMMddHHmmss","yyyy-MM-dd HH:mm:ss"))
    println(getNextTime("20170628230500",1,"yyyy-MM-dd HH:mm:ss"))
  }
}
