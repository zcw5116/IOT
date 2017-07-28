package wlw.test.access.util

/**
  * 获取值辅助类
  */
object ValueUtils {

  /**
    * 将字符串按照指定的分隔符处理成一个tuple
    *
    * @param str   ip:port
    * @param split 分隔符
    * @return (ip, port)
    */
  def parseTuple(str: String, split: String) = {
    val tmp = str.split(split)
    if (tmp.length > 1)
      (tmp(0).trim, tmp(1).trim)
    else
      (tmp(0).trim, "-")
  }

  /**
    * 将原始默认值转成Int
    *
    * @param value              字段值
    * @param originDefaultValue 原始默认值，比如-
    * @param targetDefaultValue 处理后的默认值，比如0
    * @return
    */
  def getDefaultValue(value: String, originDefaultValue: String, targetDefaultValue: Int) = {
    if (value.equals(originDefaultValue)) targetDefaultValue else value.toDouble.toInt
  }

  /**
    * 将原始默认值转成Long
    *
    * @param value              字段值
    * @param originDefaultValue 原始默认值，比如-
    * @param targetDefaultValue 处理后的默认值，比如0
    * @return
    */
  def getDefaultValue(value: String, originDefaultValue: String, targetDefaultValue: Long) = {
    if (value.equals(originDefaultValue)) targetDefaultValue else value.toDouble.toLong
  }
}