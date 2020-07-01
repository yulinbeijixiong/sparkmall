package com.oujian.sparkmall.offline.app

import com.oujian.sparkmall.offline.udf.TopCityRemarkUDF
import com.oujian.sparkmall.offline.util.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Top3ProductCountPerAreaApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Offline").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val hiveDataBase: String = PropertiesUtil.load("config.properties").getProperty("hive.database")
    //注册自定义函数
    sparkSession.udf.register("city_remark",new TopCityRemarkUDF())
    sparkSession.sql("use "+hiveDataBase)
    //连表
    sparkSession.sql("select ci.area,uv.click_product_id,ci.city_id,ci.city_name from user_visit_action uv " +
      "join city_info ci on uv.city_id = ci.city_id where uv.click_product_id >0 ").createOrReplaceTempView("tmp_uv_ci")
    //统计点击次数
    sparkSession.sql("select area,click_product_id,count(click_product_id) click_count, city_remark(city_name) city_remark from tmp_uv_ci " +
      "group by area,click_product_id").createOrReplaceTempView("tmp_uv_ci_count")

    sparkSession.sql("select *, rank()over(partition by area order by click_count desc) click_rank from tmp_uv_ci_count")
      .createOrReplaceTempView("tmp_uv_ci_rank")
    sparkSession.sql("select area,product_name,click_count,city_remark from tmp_uv_ci_rank tr " +
      "join product_info pi on tr.click_product_id = pi.product_id where click_rank <= 3")
      .show(100,false)
  }
}
