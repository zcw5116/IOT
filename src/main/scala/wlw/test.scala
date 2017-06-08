package wlw

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

  def main(args: Array[String]): Unit = {
    println("Hello, Scala ")
    println(getNowDate())
  }

}
