package wlw.test.access
import java.util.Properties
import java.io.FileInputStream
/**
  * Created by zhoucw on 17-7-17.
  */
object LoadProperties {
  def loadProperties():Unit = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("test.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    println(properties.getProperty("key1"))//读取键为ddd的数据的值
    println(properties.getProperty("value1","没有值"))//如果ddd不存在,则返回第二个参数
    properties.setProperty("ddd","123")//添加或修改属性值
  }

  def main(args: Array[String]): Unit = {
    loadProperties()

  }
}
