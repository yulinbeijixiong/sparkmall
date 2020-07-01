package com.oujian

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HBaseDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("hbase").setMaster("local[*]")
    val context = new SparkContext(conf)
    //创建hbase配置类
    val hbaseConf: Configuration = HBaseConfiguration.create()
    //创建zookeep 连接
    hbaseConf.set("hbase.zookeeper.quorum","hadoop100:2181,hadoop101:2181,hadoop102:2181")
    //设置获取表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student")
    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = context.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      //序列化方式
      classOf[ImmutableBytesWritable],
      //返回结果
      classOf[Result])
    println(hbaseRdd.count())
    hbaseRdd.foreach{case(_,result)=>
    val key: String = Bytes.toString(result.getRow())
      val name=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))
      val sex=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("sex")))
      val age=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("age")))
        println(s"no=${key},name=${name},age=${age},sex=${sex}")
    }
    //创建配置
    val jobConf = new  JobConf(hbaseConf)
    //设置输出格式
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"spark_student")
    //对数据进行转化
    def convert(tuple:(String,String,String,String))={
      val put = new Put(Bytes.toBytes(tuple._1))
      put.addImmutable(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(tuple._2))
      put.addImmutable(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(tuple._3))
      put.addImmutable(Bytes.toBytes("info"),Bytes.toBytes("sex"),Bytes.toBytes(tuple._4))
      (new ImmutableBytesWritable,put)
    }
val rdd: RDD[(String, String, String, String)] = context.makeRDD(List(("1","zhangsan","18","male"),("2","lisi","18","male"),("3","wangwu","18","male")))
    val value: RDD[(ImmutableBytesWritable, Put)] = rdd.map(convert)
    //保存
    value.saveAsHadoopDataset(jobConf)

    context.stop()
  }
}
