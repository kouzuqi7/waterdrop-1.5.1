package io.github.interestinglab.waterdrop.utils

import java.sql._

import scala.collection.mutable.ArrayBuffer

class JDBCUtil{

  private var url = "jdbc:mysql://localhost:3306/test"
  private var user = "root"
  private var password = "root"
  private var driverClass="com.mysql.jdbc.Driver"
  private val threadLocal:ThreadLocal[Connection]=new ThreadLocal

  //获取连接并放入连接池
  private def getConnection() = {
    var connection = threadLocal.get()
    if (connection == null) {
      connection = DriverManager.getConnection(url, user, password)
      threadLocal.set(connection)
    }
    connection
  }

  //关闭连接并释放连接池
  private def closeConnection() {
    val connection = threadLocal.get()
    if (connection != null) {
      connection.close()
    }
    threadLocal.remove()
  }

  def executeBatchUpdate(sqls: ArrayBuffer[String]){
    var conn:Connection = null
    var stat:Statement = null
    try {
      conn = getConnection()
      stat = conn.createStatement()

      for(sql<-sqls)
      {
        stat.addBatch(sql)
      }
      stat.executeBatch()
    }catch{
      case e : Exception => {
        throw new Exception(e)
      }
    }finally{
      if(stat!=null) {
        stat.close()
      }
      if(conn!=null){
        closeConnection()
      }
    }
  }


  def this(url: String, user: String, password: String, driverClass: String){
    this()
    this.url=url
    this.user=user
    this.password=password
    this.driverClass=driverClass
    //注册驱动
    Class.forName(driverClass)
  }

}