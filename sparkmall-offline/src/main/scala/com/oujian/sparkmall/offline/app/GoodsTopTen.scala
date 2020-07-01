package com.oujian.sparkmall.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.oujian.sparkmall.offline.bean.{CategoryCountInfo, PageConvertRationHandler, UserVisitAction}
import com.oujian.sparkmall.offline.util.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GoodsTopTen {
  def main(args: Array[String]): Unit = {
    val taskId: String = UUID.randomUUID().toString
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("goodsTopTen")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val value: RDD[UserVisitAction] = readUserVisitActionToRDD(session)
    val infoes: Array[CategoryCountInfo] = CategoryTopTenHandler.handle(taskId ,value,session)
    PageConvertRationHandler.hand(session,value,taskId)
    //val categoryTop: List[CategoryTopN] = infoes.map(categoryInfo=>CategoryTopN (categoryInfo.categoryId)).toList
    //val topSessionPerCidIdRDD: RDD[TopSessionPerCidId] = TopSessionPerTopCategoryApp.getTopSession(session,categoryTop,taskId,value)
    //val unit: RDD[Array[Any]] = topSessionPerCidIdRDD.map{ topSessionPerCidId=>Array(taskId,topSessionPerCidId.categoryId,topSessionPerCidId.sessionId,topSessionPerCidId.clickCount)}

    //JdbcUitl.executeBatchUpdate("insert into category_session_click_top_10 value(?,?,?,?)",unit.collect().toList)
  }

  def readUserVisitActionToRDD(sparkSession: SparkSession): RDD[UserVisitAction] ={

    val properties: Properties = PropertiesUtil.load("config.properties")
    val hiveDatabase: String = properties.getProperty("hive.database")
    //处理过滤条件
    val conditionProperties: Properties = PropertiesUtil.load("condition.properties")
    val condition: String = conditionProperties.getProperty("condition.params.json")
    val jstr: JSONObject = JSON.parseObject(condition)
    val startDate = jstr.getString("startDate")
    val endDate = jstr.getString("endDate")

    val sql = new
        StringBuilder("select v.* from user_visit_action v join user_info u on u.user_id = v.user_id")
    sql.append(" where v.date >='"+startDate+"' and v.date<'"+endDate+"'")
    val conditionSql=new StringBuilder
    if(!jstr.getString("startAge").isEmpty){
      conditionSql.append(" and u.age >="+jstr.getString("startAge"))
    }
    if(!jstr.getString("endAge").isEmpty){
      conditionSql.append(" and u.age <"+jstr.getString("endAge"))
    }
    if(!jstr.getString("professionals").isEmpty){
      conditionSql.append(" and professional ="+jstr.getString("professional"))
    }
    if(!jstr.getString("city").isEmpty){
      conditionSql.append(" and city ="+jstr.getString("city"))
    }
    if(!jstr.getString("gender").isEmpty){
      conditionSql.append(" and gender >="+jstr.getString("gender"))
    }
    sql.append(conditionSql)
    println(sql)
    if(hiveDatabase !=""){
      sparkSession.sql("use " + hiveDatabase)
    }

    val dataFrame = sparkSession.sql(sql.toString())
    import sparkSession.implicits._
    dataFrame.as[UserVisitAction].rdd

  }






}
