package com.oujian

import java.sql
import java.sql.{DriverManager, PreparedStatement}

import com.mysql.jdbc.Connection
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

object DemoJdbc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("app")
    val sc = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val username ="root"
    val password ="000000"

    val url = "jdbc:mysql://hadoop101:3306/rdd"
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, username, password)
    },
      "select * from `user` where `id` >? and `id`>?;",
      0L,
      10L,
      1,
      r=>(r.getInt(1),r.getString(2))
    )
    println(rdd.count())
    rdd.collect().foreach(println)
    Class.forName(driver)
    val connection: sql.Connection = DriverManager.getConnection(url, username, password)
    val ps: PreparedStatement = connection.prepareStatement("insert into  user(name) values(?)")
    ps.setString(1,"sdads")
    ps.executeUpdate()

    sc.stop()
  }

}
