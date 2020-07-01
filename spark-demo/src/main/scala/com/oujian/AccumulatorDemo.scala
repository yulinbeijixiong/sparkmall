package com.oujian

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc = new SparkContext(conf)
    val broadcast: Broadcast[Array[Int]] = sc.broadcast(Array(1,2,3))
    println(broadcast.value.toList)

    //获得累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val rdd: RDD[Int] = sc.makeRDD(List(12,3,34,5,6,7,5,6,6,6,5),4)
    rdd.foreach{r=>
      //每个数字求和
      sum.add(r.toLong)
    }
    println(sum.value)
  }
}
