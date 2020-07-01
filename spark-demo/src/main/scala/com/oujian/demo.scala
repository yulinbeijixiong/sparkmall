package com.oujian

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partition, SparkConf, SparkContext}

object demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val rdd: RDD[List[Int]] = sc.parallelize(Array(List(11, 2, 3), List(11, 2, 3), List(11, 2, 3)))
    //  val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4)
//    val rdd1: RDD[Int] = rdd.flatMap(x=>x)
//    rdd1.collect().foreach(m=>println(m))
//    val rdd = sc.makeRDD(Array(("a",1),("a",2),("b",1),("b",1)),2)
//    val unit: RDD[(String, Int)] = rdd.reduceByKey((x,y)=>x+y)
//    rdd.collect().foreach(println)
//    val rdd1: RDD[String] = sc.makeRDD(List("a","s","a","b","c","s","s"))
//    val rdd2: RDD[(String, Int)] = rdd1.map(w=>(w,1))
//    val rdd3: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
//    rdd3.collect.foreach(println)
//      val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
//    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey((_, 1), (arr: (Int, Int), v) => (arr._1 + v, arr._2 + 1), (arr: (Int, Int), arr1: (Int, Int))
//    => (arr._1 + arr._1, arr1._2 + arr1._2))
//    rdd1.map{case(key,r)=> (key,r._1/r._2)}
//    val unit: RDD[(String, Int)] = rdd.sortByKey(true)
//    unit.collect().foreach(println)
//    println(rdd.partitions.size)
//    val value: RDD[Int] = rdd.sortBy(x=>x*(-1),false,3)
//    //rdd.repartition(3)
//    //val value: RDD[Int] = rdd.coalesce(3,false)
//    println(value.partitions.size)
//    value.collect().foreach(array=>println(array))
//    val rdd = sc.makeRDD(List(("a",2),("b",1),("a",5)))
//    val rdd1 = sc.makeRDD(List((1,"c"),(2,"b"),(2,"c"),(1,3)))
////    val rdd3 =rdd.cogroup( rdd1)
////    rdd3.collect().foreach(println)
//     val tuple: (String, Int) = rdd.reduce((x,y)=>(x._1+y._1,x._2+y._2))
//    println(tuple)
    //(1,(2,"c")) (2,(1,"b"))
//    val rdd: RDD[Int] = sc.makeRDD(1 to 10 ,2)
//     rdd.saveAsTextFile("output")
//
    val rdd: RDD[String] = sc.makeRDD(Array("aa","bb","cc","aa","bb"),3)
    rdd.foreach(println)
    val search = new Search("aa")
    val rdd2: RDD[String] = search.getMatche2(rdd).persist(StorageLevel.MEMORY_ONLY)

    rdd2.collect().foreach(println)
  }
}
