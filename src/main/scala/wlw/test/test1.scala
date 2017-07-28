package wlw.test
import java.io.{IOException, InputStream, OutputStream}
import java.net.HttpURLConnection
import java.nio.charset.Charset
import java.text.DecimalFormat
import java.util
import java.util.{List, Map, Set}

import com.sun.net.httpserver.{Headers, HttpExchange}
/**
  * Created by zhoucw on 17-7-17.
  */
object test1 {

  def convertByteArrayToString(encoding: Charset): Unit = {
    val byteArray = Array[Byte](87, 79, 87, 46, 46, 46)
    val value = new String(byteArray, encoding)
    System.out.println(value)
  }

  @throws[IOException]
   def handle(he: HttpExchange): Unit = {
    System.out.println("Serving the request")
    if (he.getRequestMethod.equalsIgnoreCase("POST")) try {
      val requestHeaders = he.getRequestHeaders
      val entries = requestHeaders.entrySet
      val contentLength = requestHeaders.getFirst("Content-length").toInt
      System.out.println("" + requestHeaders.getFirst("Content-length"))
      val inputStream = he.getRequestBody
      val data = new Array[Byte](contentLength)
      val length = inputStream.read(data)
      System.out.print("data:" + new String(data))
      val responseHeaders = he.getResponseHeaders
      he.sendResponseHeaders(HttpURLConnection.HTTP_OK, contentLength)
      val os = he.getResponseBody
      os.write(data)
      he.close()
    } catch {
      case e@(_: NumberFormatException | _: IOException) =>

    }
  }
}
