package com.oujian.mock.util

import scala.util.Random
import util.control.Breaks._
object RandomNum{

def main (args: Array[String] ): Unit = {
  println(multi(10,20,12,"*",false))
}
  def apply(fromNum:Int,toNum:Int):Int={
    fromNum+new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean):String={
    //当最大数和最小数间隔不足以达到要求数，且数据不能重复时,直接返回""
    if(toNum-fromNum+1<amount && !canRepeat){
      return ""
    }
    var hasProductNum:List[Int]= List()
    //如果产生重复数增加循环次数
    var repeatAddCycles:Int=0
    var i =1;
    while(amount+repeatAddCycles>=i){
      i +=1
      val randRandomNum:Int = apply(fromNum, toNum)
      breakable {
        if (!canRepeat) {
          if (hasProductNum.contains(randRandomNum)) {
            repeatAddCycles = repeatAddCycles + 1
            break

          }

        }
        hasProductNum=hasProductNum :+ randRandomNum
      }
    }
    hasProductNum.mkString(delimiter)

  }

}
