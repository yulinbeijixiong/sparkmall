package com.oujian

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
  * 累加器第一个泛型为输入
  * 第二个为输出
  */
class CustomizeAccumulator extends AccumulatorV2[String,ArrayBuffer[String]]{
  var arrayBuffer = new ArrayBuffer[String]()
  //累加器是否处于初始阶段
  override def isZero: Boolean = arrayBuffer.isEmpty
  //拷贝
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = new CustomizeAccumulator()
  //重置累加器
  override def reset(): Unit = {
    new CustomizeAccumulator()
  }
  //进行累加操作
  override def add(v: String): Unit = {
    arrayBuffer.append(v)
  }
  //与其他的累加器进行合并
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    arrayBuffer.appendAll(other.value)
  }
  //获取累加器的值
  override def value: ArrayBuffer[String] = {
    arrayBuffer
  }
}
