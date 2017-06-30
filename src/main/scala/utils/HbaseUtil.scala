package utils

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.Connection

/**
  * Created by slview on 17-6-29.
  */
object HbaseUtil {
  //创建表
  def createHTable(connection: Connection,tablename: String, familyarr:Array[String]): Unit=
  {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)

      familyarr.foreach(f => tableDescriptor.addFamily(new HColumnDescriptor(f.getBytes())))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
  }

}
