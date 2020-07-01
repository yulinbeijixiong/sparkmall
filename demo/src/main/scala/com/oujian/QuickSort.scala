package com.oujian

import scala.util.control._

object QuickSort {
  def main(args: Array[String]): Unit = {
    val array: Array[Int] = List(12,23,45,3,4,5,10,50,70).toArray
    bubbleSort(array)
    array.toIterator.foreach{i=>
      println(i)
    }
  }

  def quickSort(left: Int, right: Int, arr: Array[Int]): Array[Int] = {

    var l: Int = left
    var r: Int = right
    var pivot = arr((left + right) / 2)
    var temp = 0
    val breaks = new Breaks()
    breaks.breakable {
      while (l < r) {
        while (arr(l) < pivot) {
          l += 1
        }
        while (arr(r) > pivot) {
          r -= 1

        }
        if (l > r) {
          breaks.break()
        }
      }
    }
    temp = arr(l)
    arr(l) = arr(r)
    arr(r) = temp

    if (l == r) {
      l -= 1
      r += 1
    }
    if (left < r) {
      quickSort(left, r, arr)
    }
    if (right > l) {
      quickSort(left, r, arr)
    }
    arr
  }
  def bubbleSort(arr:Array[Int]): Unit ={
    val breaks = new Breaks()
    for(i <- 0 until arr.length-1){
      var flag = false
      for(j <- 0 until arr.length-i-1){
        var temp =0
        if(arr(j)>arr(j+1)){
          temp=arr(j)
          arr(j) = arr(j+1)
          arr(j+1) =temp
          flag =true
        }
      }
      breaks.breakable{
        if(!flag){
          breaks.break()
        }
      }


    }
  }


}
