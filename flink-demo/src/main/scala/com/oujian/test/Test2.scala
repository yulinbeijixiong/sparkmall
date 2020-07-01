package com.oujian.test

object Test2  {
  def main(args: Array[String]): Unit = {
    var result = -2
    val op =2
    op match {
      case '+' => result =1
      case '-' => result = -1
      case _ => result =0

    }
    print(result)
    println("----------")
//    for(ch <-"sdfsdfsdfs+-*!"){
//      ch match{
//        case 's' => println(1)
//        case 'd'=> println(3)
//        case '+' => println("+")
//        case _ => println("*********")
//      }
//    }
//    val str = "sdf-3242fsd"
//    //
//    for(i <-str.indices){
//      str(i) match{
//        case '+' => println("+")
//        case '-' => println("-")
//        case ch if ch.isDigit => println(Character.digit(ch,4))
//        case _ => println("ssssss")
//      }
//
//    }
//    for (lst <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {
//      val result = lst match {
////        case 0 :: Nil => "0"
//        case x :: y :: Nil => x + " " + y
////        case 0 :: tail => "0 ..."
//        case _ => "something else"
//      }
//      println(result)
//    }
    for(pair <-Array((0,1),(1,0),(1,1))){
      var result= pair match{
        case (0,_) => println("0000")
        case (_,0) => println("1111")
        case _ => println("222")
      }

    }
    var map =Map("a"->2)
    val a = "a"
    map.get(a) match{
      case None => println("meiyouzhe")
      case Some(b) => println(b)
      case _ =>
    }
    var n = 0
    val unit: Unit = while (n < 10){
      n += 1
    }
    println(n)
    println(unit)
  }
}
class Demo extends Test3 with Test4  {
  def test()={
   implicit def a(d:Double)=d.toInt
    val i:Int = 4.5
  }

}
case class Student(id:Int,name:String)