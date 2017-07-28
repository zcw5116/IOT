package wlw.test.access.util

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * 日志转换处理类
  */
object LogConvertUtils extends Logging {

  //定义要生成的DataFrame的schema格式 五字段一块
  val struct = StructType(Array(
    StructField("cdn", StringType),
    StructField("region", StringType),
    StructField("node", StringType),
    StructField("type", StringType),
    StructField("level", StringType),

    StructField("time", LongType),
    StructField("useTime", IntegerType),
    StructField("remoteIp", StringType),
    StructField("remoteIpLine", StringType),
    StructField("remoteIpState", StringType),

    StructField("remoteIpProvince", StringType),
    StructField("remoteIpCity", StringType),
    StructField("remoteIpIsp", StringType),
    StructField("remotePort", IntegerType),
    StructField("xForwordedFor", StringType),

    StructField("localIp", StringType),
    StructField("localPort", IntegerType),
    StructField("requestSize", LongType),
    StructField("domain", StringType),
    StructField("method", StringType),

    StructField("urlPara", StringType),
    StructField("path", StringType),
    StructField("hlsid", StringType),
    StructField("version", StringType),
    StructField("range", StringType),
    StructField("contentRange", StringType),

    StructField("status", StringType),
    StructField("code", IntegerType),
    StructField("extend1", StringType),
    StructField("dataType", StringType),
    StructField("responseSize", LongType),

    StructField("requestBodySize", LongType),
    StructField("rebackIp", StringType),
    StructField("rebackIpFInfo", StringType),
    StructField("rebackPort", IntegerType),
    StructField("rebackCode", IntegerType),

    StructField("rebackTime", IntegerType),
    StructField("xInfoBucketOwner", StringType),
    StructField("xInfoBucketName", StringType),
    StructField("xInfoOp", StringType),
    StructField("connection", StringType),

    StructField("pno", StringType),
    StructField("server", StringType),
    StructField("referer", StringType),
    StructField("userAgent", StringType),
    StructField("cookie", StringType),

    StructField("xInfoRequester", StringType),
    StructField("xKssRequestId", StringType),
    StructField("xInfoKey", StringType),
    StructField("xInfoErrCode", IntegerType),
    StructField("upstreamResponseTime", IntegerType),

    StructField("xInfoObjSize", LongType),
    StructField("bodyBytesSent", LongType),
    StructField("remoteUser", StringType),
    StructField("minute", StringType),
    StructField("d", StringType),

    StructField("h", StringType),
    StructField("m5", StringType),
    StructField("extend2", StringType), //预留字符串维度
    StructField("extend3", StringType), //预留字符串维度
    StructField("extend4", StringType), //预留字符串维度

    StructField("extend5", StringType), //预留字符串维度
    StructField("extend6", StringType), //预留字符串维度
    StructField("extend7", StringType), //预留度量
    StructField("extend8", StringType), //预留度量
    StructField("extend9", StringType), //预留度量

    StructField("extend10", StringType), //预留度量
    StructField("timezone", StringType)
  ))


  def parseLog(log: String) = {
    try {
      val p = log.split("\t")

      val index = p(1).lastIndexOf("-")
      val region = p(1).substring(0, index)
      val node = p(1).substring(index + 1)

      val minute = DateUtils.parseToMinute(p(4))
      val time = DateUtils.getTime(p(4))
      val timezone = DateUtils.getTimezone(p(4))

      val remoteIpPort = ValueUtils.parseTuple(p(6), ":")
      val remoteIp = remoteIpPort._1
      val remoteIpLine = ""
      val ipInfos = ""
      val remoteIpState = ""
      val remoteIpProvince = ""
      val remoteIpCity = ""
      var remoteIpIsp = "-"
      remoteIpIsp = ""

      val remotePort = ValueUtils.getDefaultValue(remoteIpPort._2, "-", 0)

      val localIpPort = ValueUtils.parseTuple(p(8), ":")
      val localIp = localIpPort._1
      val localPort = ValueUtils.getDefaultValue(localIpPort._2, "-", 0)

      val requestSize = ValueUtils.getDefaultValue(p(9), "-", 0l)
      val responseSize = ValueUtils.getDefaultValue(p(19), "-", 0l)
      val requestBodySize = ValueUtils.getDefaultValue(p(20), "-", 0l)

      val cacheStatus = ValueUtils.parseTuple(p(16), "/")
      val status = cacheStatus._1
      val code = cacheStatus._2.toInt

      val rebackIpPort = ValueUtils.parseTuple(p(21), ":")
      val rebackIp = ""
      val ipInfosReback = ""
      val rebackIpFInfo = ""

      val rebackPort = ValueUtils.getDefaultValue(rebackIpPort._2, "-", 0)

      val rebackCode = ValueUtils.getDefaultValue(p(22), "-", 200)
      val rebackTime = ValueUtils.getDefaultValue(p(23), "-", 0)

      val xInfoErrCode = ValueUtils.getDefaultValue(p(36), "-", 200)
      val upstreamResponseTime = ValueUtils.getDefaultValue(p(37), "-", 0)
      val xInfoObjSize = ValueUtils.getDefaultValue(p(38), "-", 0l)
      val bodyBytesSent = ValueUtils.getDefaultValue(p(39), "-", 0l)


      /* p(12) 为url  http://ottvideoyd.hifuntv.com/e024f6ed4a6438ac3db0487d216c6e24/574ee943/internettv/c1/2016/dianshiju/qinaidefanyiguan/15163ad82ca3afefe712dfcd48a7768d7.hb
      *  path 为p(12)中第一个/后 到第一个?之前的内容 url中除去域名和参数的内容
      */
      var path = "-"
      val pathTemp = p(12).replaceFirst("//", "")
      var pathIndex = pathTemp.indexOf("/")
      var urlPara = ""
      if (pathIndex != -1) {
        path = pathTemp.substring(pathIndex)
        pathIndex = path.indexOf("?")
        if (pathIndex != -1) {
          urlPara = path.substring(pathIndex + 1)
          path = path.substring(0, pathIndex)
        }
      }

      var hlsid = "-"
      if (path.endsWith(".m3u8") || path.endsWith(".ts")) {
        hlsid = path.substring(0, path.lastIndexOf("/"))
      }

      Row(//五字段一行
        p(0), region, node, p(2), p(3),
        time, p(5).toInt, remoteIp, remoteIpLine, remoteIpState,
        remoteIpProvince, remoteIpCity, remoteIpIsp, remotePort, p(7),
        localIp, localPort, requestSize, p(10), p(11),
        urlPara, path, hlsid, p(13), p(14), p(15),
        status, code, p(17), p(18), responseSize,
        requestBodySize, rebackIp, rebackIpFInfo, rebackPort, rebackCode,
        rebackTime, p(24), p(25), p(26), p(27),
        p(28), p(29), p(30), p(31), p(32),
        p(33), p(34), p(35), xInfoErrCode, upstreamResponseTime,
        xInfoObjSize, bodyBytesSent, p(40), DateUtils.getMin(minute), DateUtils.getDay(minute),
        DateUtils.getHour(minute), DateUtils.get5Min(minute), p(41), p(42), p(43),
        p(44), p(45), p(46), p(47), p(48),
        p(49), timezone
      )
    } catch {
      case e: Exception =>
        logError("ParseError log[" + log + "] msg[" + e.getMessage + "]")
        Row(0)
    }
  }
}