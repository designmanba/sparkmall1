package com.mannan.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 制作累加器 传入key  返回一个map
  */
class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  //定义一个内部存储的累加器 用来存储数据
  var categoryCountMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  //判断累加器是否为空
  override def isZero: Boolean = categoryCountMap.isEmpty

  //拷贝
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {

    val accumulator = new CategoryCountAccumulator
    accumulator.categoryCountMap ++= categoryCountMap
    accumulator
  }

  ///重置
  override def reset(): Unit = {
    categoryCountMap.clear()

  }

  //累加
  override def add(key: String): Unit = {

    // 原值+1
    categoryCountMap(key) = categoryCountMap.getOrElse(key, 0L) + 1L
  }


  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    // 两个map 之间合并
    val otherMap: mutable.HashMap[String, Long] = other.value
    categoryCountMap = categoryCountMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count
      otherMap

    }

  }

  override def value: mutable.HashMap[String, Long] = {
    this.categoryCountMap
  }

}