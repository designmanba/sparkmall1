package com.mannan.sparkmall.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.JSON
import com.mannan.sparkmall.common.model.DataModel.UserVisitAction
import com.mannan.sparkmall.common.util.{JdbcUtil, PropertiesUtil}
import com.mannan.sparkmall.offline.acc.CategoryCountAccumulator
import com.mannan.sparkmall.offline.bean.CategoryCount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

object OfflineApp {
  def main(args: Array[String]): Unit = {

    val taskId: String = UUID.randomUUID().toString

    //初始化
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val properties = PropertiesUtil.load("config.properties")

    //读取过滤条件
    val conditionPro: Properties = PropertiesUtil.load("conditions.properties")
    val conditionJson: String = conditionPro.getProperty("condition.params.json")

    //解析成json对象
    val conditionJSONobj = JSON.parseObject(conditionJson)
    val startDate = conditionJSONobj.getString("startDate")
    val endDate = conditionJSONobj.getString("endDate")
    val startAge = conditionJSONobj.getString("startAge")
    val endAge = conditionJSONobj.getString("endAge")
    //先择读取的数据库
    val database = properties.getProperty("hive.database")
    sparkSession.sql("use " + database)

    //拼接sql
    var sql = " select uv.* from user_visit_action uv join user_info ui on uv.user_id=ui.user_id where 1=1"

    //条件判断
    if (startDate != null && startDate.length > 0) {
      sql += " and uv.date >= '" + startDate + "'"
    }
    if (endDate != null && endDate.length > 0) {
      sql += " and uv.date <= '" + endDate + "'"
    }
    if (startAge != null && startAge.length > 0) {
      sql += " and ui.age >=" + startAge
    }
    if (endAge != null && endAge.length >= 0) {
      sql += " and ui.age <=" + endAge
    }

    //将查询结果DF 转化成DS 主要是将sql 查询结果保存为样例类对象
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd


    //利用累加器进行统计操作，得到一个map 结构的统计结果

    val accumulator = new CategoryCountAccumulator

    //注册累加器
    sparkSession.sparkContext.register(accumulator)

    /**
      * 累加器结构
      * click_category_id_click  100
      * order_category_id_order  20
      * pay_category_id_pay      30
      */
    rdd.foreach { userVisitAction =>
      if (userVisitAction.click_category_id != -1L) {
        accumulator.add(userVisitAction.click_category_id + "_click")
      } else if (userVisitAction.order_category_ids != null) {
        //有多个id 要进行切割
        val cidArray: Array[String] = userVisitAction.order_category_ids.split(",")
        for (cid <- cidArray) {
          accumulator.add(cid + "_order")
        }
      } else if (userVisitAction.pay_category_ids != null) {
        //有多个id 要进行切割
        val cidArray: Array[String] = userVisitAction.pay_category_ids.split(",")
        for (cid <- cidArray) {
          accumulator.add(cid + "_pay")
        }

      }

    }

    // 获取累加器的结果
    val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value

    //  println(categoryCountMap.mkString("\n"))

    //对hashmap 操作
    val countGroupByCidMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy { case (cid_action, count) => cid_action.split("_")(0) }

    val categoryCountItr: immutable.Iterable[CategoryCount] = countGroupByCidMap.map { case (cid, countMap) =>
      CategoryCount(taskId, cid, countMap.getOrElse(cid + "_click", 0L), countMap.getOrElse(cid + "_order", 0L), countMap.getOrElse(cid + "_pay", 0L))
    }

    //排序
    val sortedCategoryCountList: immutable.Seq[CategoryCount] = categoryCountItr.toList.sortWith { (categoryCount1, categoryCount2) =>
      if (categoryCount1.clickCount > categoryCount2.clickCount) {
        true
      } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
        if (categoryCount1.orderCount > categoryCount2.orderCount) {
          true
        } else {
          false
        }
      } else {
        false
      }

    }

    //截取前十
    val top10List: immutable.Seq[CategoryCount] = sortedCategoryCountList.take(10)

    // 调整成
    val top10Arraylist: immutable.Seq[Array[Any]] = top10List.map{ categoryCount => Array(categoryCount.taskId,categoryCount.cid,categoryCount.clickCount,categoryCount.orderCount,categoryCount.payCount)}

    // 保存到mysql中
    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",top10Arraylist)
  }
}