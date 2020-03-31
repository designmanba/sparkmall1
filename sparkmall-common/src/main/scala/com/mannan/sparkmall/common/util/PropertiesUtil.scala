package com.mannan.sparkmall.common.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val prop: Properties = PropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))

  }

  def load(propertName:String) ={
    val properties = new Properties()
    properties.load(new InputStreamReader
    (Thread.currentThread().getContextClassLoader.getResourceAsStream(propertName),"UTF-8"))
    properties
  }


}
