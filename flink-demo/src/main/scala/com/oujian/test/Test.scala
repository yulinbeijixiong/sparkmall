package com.oujian.test

import scala.None
import scala.collection.{SortedMap, SortedSet, immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Test {
  def main(args: Array[String]): Unit = {
//   //print( listDemo(List(1,2,3,4)) )
//   var map =mutable.HashMap("A"->1,"B"->2)
//    val map1 = mutable.HashMap("G"->1)
//    map("A")=5
//    println(map)
//    map += ("A"->15)
//    println(map)
//    map -=("B")
//    println(map)
//    map ++= map1+("D"->2)
//    println(map)
//    for((k,v)<-map){
//      println(k,v)
//    }
//    val chars: mutable.Iterable[Char] = map.flatMap(_._1)
//    print(chars)
//    val names = List("Alice", "Bob", "Nick")
//    println(names.flatMap(_.toUpperCase()))
//      val list = List(1,2,3,4,5)
//    for(l <-list){
//
//    }
//    val list1 = List(1,2,3,4,5)
//      val tuples: List[(Int, Int)] = list zip list1
//      print(tuples)
//      val ints: List[Int] = list.scan(0)(_+_)
//    print(ints)

//      val i: Int = list.reduceLeft(_-_)
//      println(i)
//      val j: Int = list.reduceRight(_-_)
//      println(j)
//    println("---------------")
//    val z: Int = list./:(1){(x,y)=> println(x+y);x+y}
//
//    print(z)
//    val sentence = "一首现代诗《笑里藏刀》:哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈刀哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈"
//    val charToInt: Map[Char, Int] = (Map[Char,Int]()/:sentence)((m,c)=>m +(c->(m.getOrElse(c,0)+1)))
//    println(charToInt)
  }
  def numsForm(n:BigInt):Stream[BigInt]=n#:: numsForm(n+1)
  def mi(x:Int,n:Int): Int ={
    if(n==0) 1
    else if(n <0) 1/mi(x,n-1)
    else x*mi(x,n-1)
  }
  def make(n:Int):Array[Int]={
    val array = new Array[Int](n)
    val random =new scala.util.Random()
    val ints: Array[Int] = for(i<-array) yield random.nextInt(n)
    ints

  }
  def Replacement(array:Array[Int]):Array[Int] ={
    for(i<-0 until(array.length-1,2)){
      println(i)
      var temp=0
      temp= array(i)
      array(i)=array(i+1)
      array(i+1)=temp

    }
    array
  }
  def sigNumArr(array:Array[Int]):Array[Int]={
    var ints: List[Int] =List()

    var ints1: List[Int] = List()
    for(i<-array.toList reverse){
      if(i.signum>0){
       ints= i :: ints
      }else{
       ints1=i :: ints1
      }
    }
    val ints2: List[Int] =  ints ::: ints1
    ints2.toArray
  }
  def signNum1(array: Array[Int]): Array[Int] ={
    var ints = new ArrayBuffer[Int]()
    ints ++= (for (a<-array if a >0) yield a)
    ints ++=(for (a<-array if a <=0) yield a)
    ints.toArray
  }
  def timeDemo()={
    import java.util.TimeZone.getAvailableIDs
    val arr: Array[String] = java
      .util.TimeZone.getAvailableIDs()
    val strings: Array[String] =(for (i <- arr if i.startsWith("America/")) yield {
        i.drop("America/".length)
    })
    scala.util.Sorting.quickSort(strings)
    strings
  }
  def demo()={
    val map = Map("book"->10,"gun"->18,"ipad"->1000)
    val stringToDouble: Map[String, Double] = for((k,v)<-map) yield{
      (k,0.9*v)
    }
    stringToDouble
  }
  def wordCount(txt:String)={
    var stringToInt: immutable.SortedMap[String, Int] =scala.collection.immutable.SortedMap[String,Int]()
    var stringToInt1 = scala.collection.immutable.TreeMap[String,Int]()
    for(i <-txt.split(" ")){
      stringToInt1 += (i-> (stringToInt.getOrElse(i,0)+1))

    }
    stringToInt
  }
  def maxminTest(arr:Array[Int]) ={
    (arr.max,arr.min)
  }
  def indexes(s:String):mutable.HashMap[Char,SortedSet[Int]]={
    val charToInts = new mutable.HashMap[Char,SortedSet[Int]]()
    var i = 0
    s.foreach { c =>
      charToInts.get(c) match {
        case Some(result) => charToInts(c) = result + i
        case None => charToInts += (c->SortedSet{i})
          i +=1
      }

    }
    charToInts
  }
  def removeZero(list:List[Int])={
    val ints: List[Int] = list.filter(i=>i!=0)
    ints
  }
  def strMap(strArr:Array[String],map:Map[String,Int]):Array[Int]={
    strArr.flatMap(map.get(_))
  }
  def mkString(listString:List[String]):String={
    listString.reduceLeft(_+_)
  }
  def listDemo(lst:List[Int])={

  }
}
