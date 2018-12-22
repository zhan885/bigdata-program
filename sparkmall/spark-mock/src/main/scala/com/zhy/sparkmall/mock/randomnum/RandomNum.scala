package com.zhy.sparkmall.mock.randomnum

import scala.util.Random
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

/**
  * Created by Administrator on 2018/12/07.
  */
object RandomNum {
  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复     1,20,       1~5的随机数amount,",",false
    //case "order" =>orderCategoryIds=RandomNum.multi(1,cargoryNum,RandomNum(1,5),",",false)
    //orderProductIds=RandomNum.multi(1,cargoryNum,RandomNum(1,5),",",false)
    //10:0~9
    if (canRepeat) {
      val list = ListBuffer[Int]()
      for (i <- 1 to amount) {
        var num: Int = apply(fromNum, toNum)
        list += num
      }
      list.mkString(delimiter)
    } else {
      val hashSet = HashSet[Int]()
      while (hashSet.size < amount) {
        var num: Int = apply(fromNum, toNum)
        hashSet += num
      }
      hashSet.mkString(delimiter)
    }
  }

  def main(args: Array[String]): Unit = {
    println(multi(1, 5, 3, ",", false))
  }

}
