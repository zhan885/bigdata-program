package com.zhy.sparkmall.realtime.app

import com.zhy.sparkmall.common.util.RedisUtil
import com.zhy.sparkmall.realtime.bean.RealtimeAdslog
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Created by Administrator on 2018/12/13.
  */
object LastHourCountPerAds {

  def calcLastHourCountPerAds(filteredRealtimelog:DStream[RealtimeAdslog]) ={
    //需求十
    //需求：各广告最近1小时内各分钟的点击量
    //目标数据结构：
    // key：last_hour_ads_click
    // field： 广告id  value:{ 小时分钟1:点击次数, 小时分钟2:点击次数…..}
    //思路：
    //依据需求八数据：过滤后的数据RealtimeAdslog(date,area,city,userId,adsId)
    //RDD[Realtimelog]=> window=>map=>
    //RDD[(adsid_hour_min,1L)]
    // -> reducebykey =>RDD[adsid_hour_min,count]
    //=>RDD[(adsId,(hour_min,count)] .groupbykey
    //=>RDD[adsId,Iterable[(hour_min,count)]]
    //RDD[adsId,hourminCountJson]
    //map(adsid,json)

    //利用开窗函数取最近一小时的日志
    val lastHourLogDStream: DStream[RealtimeAdslog] = filteredRealtimelog.window(Minutes(60), Seconds(10))
      //60分钟窗口大小，滑动步长10秒更新

    //目标：RDD[(adsid_hour:min,1L)]=>reduceByKey
    //按照每广告+小时分钟 统计个数
    val lastHourAdsCountDStream: DStream[(String, Long)] = lastHourLogDStream.map {
      realtimelog =>
        val adsId: String = realtimelog.adsId
        val hourminuStr: String = realtimelog.getHourMinuString()
        val key: String = adsId + "_" + hourminuStr
        (key, 1L)
    }.reduceByKey(_ + _)


    //=>RDD[(adsId,(hour_min,count)] .groupbykey
    //=>RDD[adsId,Iterable[(hour_min,count)]]
    //按广告分组，把本小时内相同广告id集合到一起
    val hourMinuCountGroupByAdsDStream: DStream[(String, Iterable[(String, Long)])] = lastHourAdsCountDStream.map {
      case (adsId_hourMinu, count) =>
        val adsId = adsId_hourMinu.split("_")(0)
        val hourMinu = adsId_hourMinu.split("_")(1)
        (adsId, (hourMinu, count))
    }.groupByKey()


    //RDD[adsId,hourminCountJson]
    //map(adsid,json)
    val hourMinuJsonPerAdsDStream: DStream[(String, String)] = hourMinuCountGroupByAdsDStream.map {
      case (adsId, (hourMinuCountItr)) =>
        val hourMinuList: List[(String, Long)] = hourMinuCountItr.toList

        val hourMinuJsonStr: String = compact(render(hourMinuList))
        (adsId, hourMinuJsonStr)
    }

    //存redis  foreachRDD运行在driver端
    val jedisClient = RedisUtil.getJedisClient
    hourMinuJsonPerAdsDStream.foreachRDD{
      rdd=>
        val hourMinuJsonPerAads: Array[(String,String)] = rdd.collect()

        //隐式转换
        import collection.JavaConversions._
        jedisClient.hmset("last_hour_ads_click",hourMinuJsonPerAads.toMap)
    }













  }



}
