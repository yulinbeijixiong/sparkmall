package com.oujian

import org.apache.spark.SparkConf

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    new SparkConf().setMaster("local[*]")
  }
}
