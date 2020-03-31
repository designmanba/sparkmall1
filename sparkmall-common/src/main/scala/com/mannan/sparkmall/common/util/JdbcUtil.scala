package com.mannan.sparkmall.common.util

import java.sql.PreparedStatement
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory


/**
  * jdbc 工具
  */
object JdbcUtil {

   var dataSource = init()


  /**
    * 初始化方法
    */

  def init()={

    val prop = PropertiesUtil.load("config.properties")
    val properties = new Properties()
    properties.setProperty("driverClassName","com.mysql.jdbc.Driver")
    properties.setProperty("url",prop.getProperty("jdbc.url"))
    properties.setProperty("username",prop.getProperty("jdbc.user"))
    properties.setProperty("password",prop.getProperty("jdbc.password"))
    properties.setProperty("maxActive",prop.getProperty("jdbc.datasource.size"))

    //连接池
    DruidDataSourceFactory.createDataSource(properties)

  }


  def executeUpdate(sql:String,params:Array[Any])={

    var rtn =0
    var pstmt:PreparedStatement=null
    val connection = dataSource.getConnection()

    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))

        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e:Exception => e.printStackTrace()
    }

    rtn
  }


  def executeBatchUpdate(sql:String,paramsList:Iterable[Array[Any]])={
    var rtn:Array[Int] = null
    var pstmt:PreparedStatement = null
    val connection = dataSource.getConnection()

    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- 0 until params.length) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }

      }
      rtn = pstmt.executeBatch()
      connection.commit()
    } catch {
      case e:Exception => e.printStackTrace
    }
    rtn


  }


  def main(args: Array[String]): Unit = {
    JdbcUtil.executeUpdate("insert into category_top10 values(?,?,?,?,?)",Array("task1","10",20,30,40))
  }


}
