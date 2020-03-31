package com.mannan.sparkmall.offline.handler

import com.mannan.sparkmall.common.model.DataModel.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 调用累加器 根据累加器进行整理，排序，截取
  * 调用存储方法 保存到mysql
  */
object CategoryTop10Handler {
  def handle(userActionRDD:RDD[UserVisitAction],sparkSession:SparkSession,taskId:String): Unit ={



  }
}
