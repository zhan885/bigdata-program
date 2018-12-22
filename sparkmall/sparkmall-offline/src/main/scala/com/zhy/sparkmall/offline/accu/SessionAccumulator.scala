package com.zhy.sparkmall.offline.accu

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * Created by Administrator on 2018/12/07.
  */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var sessionMap = new mutable.HashMap[String, Long]()

  //判断是否是初始值
  override def isZero: Boolean = {
    sessionMap.isEmpty
  }

  //给每个excute端拷贝累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator: SessionAccumulator = new SessionAccumulator()
    accumulator.sessionMap = this.sessionMap
//    accumulator.sessionMap ++= sessionMap  //会将sessionMap的内容放入accumulator.sessionMap
    accumulator
  }

  //重置
  override def reset(): Unit = {
    sessionMap = new mutable.HashMap[String,Long]()
  }

  //累加
  override def add(key: String): Unit = {
    sessionMap(key) = sessionMap.getOrElse(key, 0L) + 1L
  }
  //合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap: mutable.HashMap[String, Long] = other.value
    sessionMap = sessionMap.foldLeft(otherMap) {
      case (otherMap, (key, count)) =>
        //从otherMap中遍历，寻找相同key的count值然后进行累加，如果没有就初始0加count
        otherMap(key) = otherMap.getOrElse(key, 0L) + count
        otherMap
    }

  }

  //
  override def value: mutable.HashMap[String, Long] = {
    sessionMap
  }

}
