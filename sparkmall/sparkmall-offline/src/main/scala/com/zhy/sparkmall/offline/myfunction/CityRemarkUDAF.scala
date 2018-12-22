package com.zhy.sparkmall.offline.myfunction


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable._

/**
  * Created by Administrator on 2018/12/11.
  */
class CityRemarkUDAF extends UserDefinedAggregateFunction {

  //定义输入结构
  override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))

  //缓存累积值 两个累积值：map[city_name,count]、total_count
  override def bufferSchema: StructType = StructType(Array(StructField("city_count", MapType(StringType, LongType)), StructField("total_count", LongType)))

  //输出类型 打印结果的类型
  override def dataType: DataType = StringType

  //校验一致性 相同的输入有相同的输出 => true
  override def deterministic: Boolean = true

  //初始化 buffer存放累积值用的
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    //不支持可变的
    buffer(0) = new HashMap[String, Long]()
    buffer(1) = 0L
  }

  //更新 每进入一条数据 进行累加
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //取出城市名 输入结构中包含城市名
    val cityName: String = input.getString(0)

    if (cityName == null || cityName.isEmpty) {
      return
    }

    // 得出第一个累积值：map[city_name,count]
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    // 得出第二个累计值：total_count
    val totalCount: Long = buffer.getLong(1)

    //用原有的map和新的键值对组合，产生新的map
    val cityCountNewMap: Map[String, Long] = cityCountMap + (cityName -> (cityCountMap.getOrElse(cityName, 0L) + 1L))

    //更新bffer两个累积值
    buffer(0) = cityCountNewMap
    buffer(1) = totalCount + 1L

  }

  //合并 把不同分区的结果进行合并 两个buffer合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    // 得出第一个累积值：map[city_name,count]
    val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    // 得出第二个累计值：total_count
    val totalCount1: Long = buffer1.getLong(1)

    // 得出第一个累积值：map[city_name,count]
    val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    // 得出第二个累计值：total_count
    val totalCount2: Long = buffer2.getLong(1)

    //合并map
    val cityCountNewMap2: Map[String, Long] = cityCountMap1.foldLeft(cityCountMap2) {
      case (cityCountMap2, (cityName, count)) =>
        cityCountMap2 + (cityName -> (cityCountMap2.getOrElse(cityName, 0L) + count))
    }

    buffer1(0) = cityCountNewMap2
    //合并总数
    buffer1(1) = totalCount1 + totalCount2

  }

  //评估 输出value 展示出结果
  override def evaluate(buffer: Row): Any = {
    // 得出第一个累积值：map[city_name,count]
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    // 得出第二个累计值：total_count
    val totalCount: Long = buffer.getLong(1)

    //取出所有城市的集合，按cityCount排序，截取前2，计算出其他城市
    val cityInfoList: List[CityCountInfo] = cityCountMap.map {
      case (cityName, count) =>
        CityCountInfo(cityName, count, Math.round(count/totalCount.toDouble*1000) / 10)
    }.toList

    val top2CityCountList: List[CityCountInfo] = cityInfoList.sortWith {
      (cityInfo1, cityInfo2) =>
        cityInfo1.cityCount > cityInfo2.cityCount
    }.take(2)

    //如果城市数大于2再算其他的城市
    if (cityInfoList.size > 2) {
      var otherRatio = 100D
      top2CityCountList.foreach {
        cityInfo =>
          otherRatio -= cityInfo.cityRatio
      }
//      println("为啥不进来呢？")
      val cityInfoWithOthersList: List[CityCountInfo] = top2CityCountList :+ CityCountInfo("其他", 0L, otherRatio)
      cityInfoWithOthersList.mkString(",")
    } else {
//      println("啦啦啦")
      top2CityCountList.mkString(",")
    }

  }

  case class CityCountInfo(cityName: String, cityCount: Long, cityRatio: Double) {
    override def toString: String = {
      cityName + ":" + cityRatio + "%"
    }
  }

}
