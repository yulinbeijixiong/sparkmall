package com.oujian.sparkmall.offline.bean

import com.alibaba.fastjson.JSON
import com.oujian.sparkmall.offline.util.PropertiesUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConvertRationHandler {
  def hand(sparkSession: SparkSession, userVisitActionRDD:RDD[UserVisitAction], taskId: String)= {
    val jsonCondition = PropertiesUtil.load("condition.properties").getProperty("condition.params.json")
    val conditionObject = JSON.parseObject(jsonCondition)
    val targetPageFlowStr: String = conditionObject.getString("targetPageFlow")
    val targetPage: Array[String] = targetPageFlowStr.split(",")
    val targetPageId: Array[String] = targetPage.slice(0, targetPage.length - 1)
    val targetNextPageId: Array[String] = targetPage.slice(1, targetPage.length - 1)
    val pageCountBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageId)
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction => pageCountBC.value.contains(userVisitAction.page_id.toString) }
    val pageAndCount: collection.Map[Long, Long] = filterRDD.map { userVisitAction => (userVisitAction.page_id, 1L) }.countByKey()

    //页面跳转分组
    val pageConvert: Array[String] = targetPageId.zip(targetNextPageId).map { case (page1, page2) => page1 + "_" + page2 }
    val pageConvertBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageConvert)
    val sessionGroup: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.map { action => (action.session_id, action) }.groupByKey()
    val page: RDD[String] = sessionGroup.flatMap { case (sessionId, actionIte) =>
      val actions: List[UserVisitAction] = actionIte.toList.sortWith { case (action1, action2) => action1.action_time > action2.action_time }
      val pageList: List[Long] = actions.map(_.page_id)
      val beforPage: List[Long] = pageList.slice(0, pageList.toArray.length - 1)
      val nextPage: List[Long] = pageList.slice(1, pageList.toArray.length - 1)
      val jumpPage: List[String] = beforPage.zip(nextPage).map { case (page1, page2) => page1 + "_" + page2 }
      val filePageConverList: List[String] = jumpPage.filter(pageId => pageConvertBC.value.contains(pageId))
      filePageConverList
    }
    val convertPageCount: collection.Map[String, Long] = page.map{(_,1L)}.countByKey()
    val result: collection.Map[String, Double] = convertPageCount.map { case (jumpage, count) =>
      val fromPage: String = jumpage.split("_")(0)
      val pageCount: Long = pageAndCount.getOrElse(fromPage.toLong, Long.MaxValue)
      val rate = count * 100.0 / pageCount
      (jumpage, rate)
    }
    result.foreach(r=>println(r))
  }

}
