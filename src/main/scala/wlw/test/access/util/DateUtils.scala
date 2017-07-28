package wlw.test.access.util

import java.util.{Date, Locale}



import org.apache.commons.lang3.time.FastDateFormat



/**

  * 时间日期解析类

  */

object DateUtils {



  //输入文件日期时间格式

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)



  //目标日期格式

  val TARGET_FORMAT = FastDateFormat.getInstance("yyMMddHHmm")



  /**

    * 获取日志时间，long类型

    *

    * 输入格式：[13/Sep/2016:16:54:29 +0800]

    */

  def getTime(time: String) = {

    try {

      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf('[') + 1, time.lastIndexOf(']'))).getTime

    } catch {

      case e: Exception =>

        println(">>>>>" + time + e.getMessage)

        0l

    }

  }



  /**

    * 获取时区

    */

  def getTimezone(time: String) = {

    try {

      time.substring(time.indexOf('[') + 1, time.lastIndexOf(']')).split(" ")(1)

    } catch {

      case e: Exception =>

        println(">>>>>" + time + e.getMessage)

        "-"

    }

  }



  /**

    * 获取时间：yyMMddHHmm

    *

    * @param time dd/MMM/yyyy:HH:mm:ss Z类型的字符串

    * @return 成功：yyMMddHHmm类型的字符串：1609131654； 失败：7001010000

    */

  def parseToMinute(time: String) = {

    TARGET_FORMAT.format(new Date(getTime(time)))

  }



  /**

    * 获取日期：yyMMdd

    *

    * @param minute yyMMddHHmm类型的字符串

    * @return yyMMdd

    */

  def getDay(minute: String) = {

    minute.substring(0, 6)

  }



  /**

    * 获取小时：HH

    *

    * @param minute yyMMddHHmm类型的字符串

    * @return HH

    */

  def getHour(minute: String) = {

    minute.substring(6, 8)

  }



  /**

    * 获取分钟

    *

    * @param minute yyMMddHHmm类型的字符串

    * @return

    */

  def getMin(minute: String) = {

    minute.substring(8, 10)

  }



  /**

    * 获取分钟，每5分钟一个间隔

    * 00~04为00

    * 05~09为05

    * 10~14为10

    * ...

    *

    * @param minute yyMMddHHmm类型的字符串

    * @return

    */

  def get5Min(minute: String) = {

    val m = minute.substring(8, 10).toInt

    if (m < 10)

      "0" + m / 5 * 5

    else

      "" + m / 5 * 5

  }



}
