package wlw.test

import com.sun.net.httpserver.HttpServer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.Int
import java.net.InetSocketAddress
import java.net.URI


/**
  * Created by zhoucw on 17-7-18.
  */
object TestHDFS {
  /**
    * @param hdfs FileSystem 对象
    * @param path 文件路径
    */
  def iteratorShowFilesWithDir(hdfs: FileSystem, path: Path): Unit = {
    try {
      if (hdfs == null || path == null) return
      //获取文件列表
      val files = hdfs.listStatus(path)
      //展示文件信息
      var i = 0
      while ( {
        i < files.length
      }) {
        try
            if (files(i).isDirectory) {
              System.out.println(">>>" + files(i).getPath + ", dir owner:" + files(i).getOwner)
              //递归调用
              iteratorShowFiles(hdfs, files(i).getPath)
            }
            else if (files(i).isFile) System.out.println("   " + files(i).getPath + ", length:" + files(i).getLen + ", owner:" + files(i).getOwner)
        catch {
          case e: Exception =>
            e.printStackTrace()
        }

        {
          i += 1; i - 1
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def iteratorShowFiles(hdfs: FileSystem, path: Path): Unit = {
    try {
      if (hdfs == null || path == null) return
      val files = hdfs.listStatus(path)
      var filesize = 0l
      var i = 0
      while ( {
        i < files.length
      }) {
        try
            if (files(i).isFile) {
              filesize = filesize + files(i).getLen
              System.out.println("   " + files(i).getPath + ", length:" + files(i).getLen + ", owner:" + files(i).getOwner)
            }
        catch {
          case e: Exception =>
            e.printStackTrace()
        }

        {
          i += 1; i - 1
        }
      }
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024
      val numsize = Math.ceil(msize)
      System.out.println("numsize:" + numsize)
      System.out.println("filesize:" + filesize)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    var hdfs: FileSystem=null
    try {
      val config = new Configuration
      // 程序配置
      config.set("fs.default.name", "hdfs://cdh-nn1:8020")
      //config.set("hadoop.job.ugi", "feng,111111");
      //config.set("hadoop.tmp.dir", "/tmp/hadoop-fengClient");
      //config.set("dfs.replication", "1");
      //config.set("mapred.job.tracker", "master:9001");
      hdfs = FileSystem.get(new URI("hdfs://cdh-nn1:8020"), config)
      val path = new Path("/hadoop/zcw/tmp/wcout")
      val asize = hdfs.getContentSummary(path).getLength
      System.out.println("asize:" + asize)
      iteratorShowFiles(hdfs, path)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (hdfs != null) try
      hdfs.close()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
