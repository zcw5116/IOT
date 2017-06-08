package utils

import scala.xml.XML

/**
  * Created by slview on 17-6-7.
  */
object RedisProperties {

  val REDIS_SERVER:String = getRedis._1
  val REDIS_PORT:Int = getRedis._2

  def getRedis(): Tuple2[String,Int] ={
    var REDIS_SERVER = ""
    var REDIS_PORT=""
    val someXML = XML.load("/slview/nms/cfg/shconfig.xml")
    val headerField = someXML\"ParaNode"
    headerField.foreach{x =>
      val node = (x\\"ParaNode").foreach{ child =>
        val paraname = (child\\"ParaName").text
        if (paraname.equals("RedisIP")){
          REDIS_SERVER = (child\\"ParaValue").text
        }else if(paraname.equals("RedisPort")){
          REDIS_PORT = (child\\"ParaValue").text
        }
      }

    }
    val tuple:(String, Int) = Tuple2(REDIS_SERVER, REDIS_PORT.toInt)
    return tuple

  }



}
