package com.oujian.sparkmall.offline.app

import com.oujian.sparkmall.offline.bean.{CategoryTopN, TopSessionPerCidId, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TopSessionPerTopCategoryApp {
  def getTopSession(sparkSession:SparkSession, topCategoryList:List[CategoryTopN], taskId:String, userVisitActionRDD:RDD[UserVisitAction]): RDD[TopSessionPerCidId] ={
    //排名前十的品类id,广播变量可以减少磁盘空间

    val topN = sparkSession.sparkContext.broadcast(topCategoryList)
    //1、过滤筛选出包含的top10点击记录
    val topCidActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
      //将字符串转为数字
      val longs = topN.value.map { category => category.category_id.toLong }
      longs.contains(userVisitAction.click_category_id)
    }
    //2、categoryId+sessionId
    val topCidSessionClickRDD: RDD[(String, Long)] = topCidActionRDD.map { topCidAction =>
      (topCidAction.click_category_id + "_" + topCidAction.session_id, 1L)
    }
    //用reduce 进行聚合
    val topCidSessionCountRDD: RDD[(String, Long)] = topCidSessionClickRDD.reduceByKey(_ + _)
    //转化集合状态（cid,TopSession(sessionId,count)）
    val topSessionPerCid: RDD[(String, TopSessionPerCidId)] = topCidSessionCountRDD.map { case (topCidSessionId, count) =>
      val strings: Array[String] = topCidSessionId.split("_")
      val categoryId = strings(0)
      val sessionId = strings(1)
      (categoryId, TopSessionPerCidId(taskId, categoryId, sessionId, count))
    }
    //将集合压平，然后排序截取前10名
    val topSessionItrPerCidRDD: RDD[(String, Iterable[TopSessionPerCidId])] = topSessionPerCid.groupByKey()
    val top10SessionPerCid: RDD[TopSessionPerCidId] = topSessionItrPerCidRDD.flatMap { case (cid, topSessionItr) =>
      val top10SessionList: List[TopSessionPerCidId] = topSessionItr.toList.sortWith { case (session1, session2) =>
        session1.clickCount > session2.clickCount
      }.take(10)
      top10SessionList
    }
    top10SessionPerCid



  }

}
