package wlw.test.access

/**
  * Created by zhoucw on 17-7-17.
  */
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

/**
  * WordCount业务线Job
  */
object WordCountJob extends Logging{

  val input = "/hadoop/zcw/tmp/install.log"
  val output = "/hadoop/zcw/tmp/wcout/"

  def doJob(parentContext: SQLContext):String = {
    var response = "success"

    var sqlContext = parentContext.newSession()
    try {
      sqlContext.sparkContext.textFile(input)
        .flatMap(x=>x.split("\t")).map((_,1)).reduceByKey(_+_)
        .saveAsTextFile(output)
    } catch {
      case e:Exception => {
        logError("Execute Failure", e)
        response = "Execute Failure:" + e.getMessage
      }
    } finally {
      if (sqlContext != null) {
        sqlContext.clearCache()
        sqlContext = null
      }
    }
    response
  }
}