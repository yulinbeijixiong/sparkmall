package com.oujian.sparkmall.offline.udf


import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, types}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

class TopCityRemarkUDF extends UserDefinedAggregateFunction {
  //输入格式
  override def inputSchema: StructType = new StructType(Array(StructField("cityName",StringType)))
  //储存格式 HashMap(String,long) 用于储存城市和点击量，用于地区储存点击量的总数
  override def bufferSchema: StructType = new StructType(Array(StructField("cityCount",types.MapType(StringType,LongType))
  ,StructField("total",LongType)))
  //输出格式
  override def dataType: DataType = StringType
  //是否动态变更值
  override def deterministic: Boolean = false
  //初始化容器
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=HashMap[String,Long]()
    buffer(1)=0L

  }
  //更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName = input.getString(0)
    val cityNameCount:Map[String,Long] = buffer.getAs[Map[String,Long]](0)
    val total: Long = buffer.getAs[Long](1)
    buffer(0) = cityNameCount + (cityName->(cityNameCount.getOrElse(cityName,0L)+1L))
    buffer(1) = total+1L

  }
  //合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cityNameCount1:Map[String,Long] = buffer1.getAs[Map[String,Long]](0)
    val total1: Long = buffer1.getAs[Long](1)

    val cityNameCount2:Map[String,Long] = buffer2.getAs[Map[String,Long]](0)
    val total2: Long = buffer2.getAs[Long](1)

    buffer1(0)=cityNameCount1.foldLeft(cityNameCount2){ case(map2,(cityName,count))=>
      map2+(cityName->(map2.getOrElse(cityName,0L)+count))
    }
    buffer1(1)=total1+total2
  }
  //展示
  override def evaluate(buffer: Row): Any = {
    val cityNameCount:Map[String,Long] = buffer.getAs[Map[String,Long]](0)
    val total: Long = buffer.getAs[Long](1)
    val cityRate: ListBuffer[CityRate] = new ListBuffer[CityRate]()
    for(city<-cityNameCount){
      val rate: Double = Math.round(city._2.toDouble*1000/total)/10.0
      cityRate.append(CityRate(city._1,rate))
    }
    val top2: ListBuffer[CityRate] = cityRate.sortWith { case (city1, city2) => city1.rate>city2.rate}.take(2)
    var otherRate =100.0
    if(top2.size>=2){
      for(city<-top2){
        otherRate -=city.rate
      }
      top2.append(CityRate("其他",Math.round(otherRate*10)/10.0))
    }
    top2.mkString(",")
  }
}
case class CityRate(cityName:String,rate:Double){
  override def toString: String = {
    cityName+":"+rate+"%"
  }


}
