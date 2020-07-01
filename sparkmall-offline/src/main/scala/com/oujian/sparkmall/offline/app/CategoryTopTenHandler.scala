package com.oujian.sparkmall.offline.app

import com.oujian.sparkmall.offline.bean.{CategoryCountInfo, UserVisitAction}
import com.oujian.sparkmall.offline.util.JdbcUitl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryTopTenHandler {
  def handle(taskId:String,userActionRDD:RDD[UserVisitAction], sparkSession: SparkSession): Array[CategoryCountInfo] = {
    val accumulator = new CategoryCountAccumulator()
    //注册累加器
    sparkSession.sparkContext.register(accumulator)
    userActionRDD.foreach {
      userAction =>
        //点击事件
        if (userAction.click_category_id != -1L) {
          accumulator.add(userAction.click_category_id + "_click")
        } else if (userAction.order_category_ids != null) {
          val categoryIds: Array[String] = userAction.order_category_ids.split(",")
          categoryIds.foreach(categoryId => {
            accumulator.add(categoryId + "_order")
          })
        } else if (userAction.pay_category_ids != null) {
          val categoryIds: Array[String] = userAction.pay_category_ids.split(",")
          categoryIds.foreach(categoryId => {
            accumulator.add(categoryId + "_pay")
          })
        }
    }
        val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value

        val actionCountByActionCidMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy { case (cidAction, count) =>
          val cid = cidAction.split("_")(0)
          cid
        }
        val list: List[CategoryCountInfo] = actionCountByActionCidMap.map {
          case (cid, actionMap) =>
            CategoryCountInfo(taskId, cid, actionMap.getOrElse(cid + "_click", 0), actionMap.getOrElse(cid + "_order", 0),
              actionMap.getOrElse(cid + "_pay", 0))
        }.toList


        val sortCategoryData: Array[CategoryCountInfo] = list.toArray.sortWith((categoryCount1, categoryCount2) =>
          if (categoryCount1.clickCount > categoryCount2.clickCount) {
            true
          } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
            if (categoryCount1.orderCount > categoryCount2.orderCount) {
              true
            } else if (categoryCount1.orderCount == categoryCount2.orderCount) {
              if (categoryCount1.payCount > categoryCount2.payCount) {
                true
              } else {
                false
              }
            } else {
              false
            }
          } else {
            false
          }
        )
        val infoes: Array[CategoryCountInfo] = sortCategoryData.take(10)
        val data: List[Array[Any]] = infoes
          .map(info => {
            Array(info.taskInd, info.categoryId, info.clickCount, info.orderCount, info.payCount)
          }).toList


        JdbcUitl.executeBatchUpdate("insert into category_top10 value (?,?,?,?,?)", data)
    infoes

  }
}
