package com.oujian.sparkmall.offline.app

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.HashMap

class CategoryCountAccumulator extends AccumulatorV2[String,HashMap[String,Long]]{
  private var categoryCountMap:HashMap[String,Long] = new HashMap[String, Long]
  override def isZero: Boolean = categoryCountMap.isEmpty

  override def copy(): AccumulatorV2[String, HashMap[String, Long]] = {
    val accumulator = new CategoryCountAccumulator
    accumulator.categoryCountMap++=this.categoryCountMap
    accumulator
  }

  override def reset(): Unit = {
    categoryCountMap =new HashMap[String,Long]
  }

  override def add(v: String): Unit = {
    categoryCountMap(v)=categoryCountMap.getOrElse(v,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, HashMap[String, Long]]): Unit = {
    var sessionMapOther: HashMap[String, Long] = other.value
    val mergedSessionMap = this.categoryCountMap.foldLeft(sessionMapOther) {
      case (sessionOther: HashMap[String, Long], (key, count)) =>
        sessionOther(key) = sessionOther.getOrElse(key, 0L) + count
        sessionOther
    }
    this.categoryCountMap= mergedSessionMap
  }

  override def value: HashMap[String, Long] = {
    categoryCountMap
  }
}
