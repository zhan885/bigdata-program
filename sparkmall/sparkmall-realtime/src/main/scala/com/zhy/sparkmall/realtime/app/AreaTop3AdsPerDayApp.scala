package com.zhy.sparkmall.realtime.app

import com.zhy.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2018/12/13.
  */
object AreaTop3AdsPerDayApp {

  //需求七每天汇总的数值
  def calcTop3Ads(areaCityAdsTotalCountDstream: DStream[(String, Long)]) = {

    ////////需求九////////
    //需求：每天各个地区热门广告的top3
    //思路
    //依据需求八数据：每天每地区每广告点击量
    //目标数据结构：(key:每天，field:地区 value:{"广告1":点击数},{...},{...})
    //RDD[(day_area_city_adsId,count)]
    //RDD[(day_area_adsId,count)]未聚合，key有重复
    // => reduceByKey 将不同城市相同地区进行聚合RDD[(day_area_adsId,count)]
    // => map RDD[(day,(area,(adsId,count)))]
    // => groupByKey按天聚合 目标结构RDD[(day,iterable(area,(adsId,count)))] 其中的area有可能重复
    // => map
    // -> 相同地区必须聚合，对iterable(area,(adsId,count)) groupBy
    // => Map[area,Iterable[(area,(adsId,count))]]
    // 注：按area进行groupby相当于在原来的迭代数据外面套了一层area
    // => map 去掉迭代器中冗余area->Map[area,Iterable[(adsId,count)]]
    // Iterable[(adsId,count)] => toList =>sortWith =>take(3)
    // Iterable[(adsId,count)] =>JsonString
    // RDD[(day,Map[area,map[adsId,count]])]
    // RDD[(day,Map[area,jsonString])]


    // RDD[(day_area_city_adsId,count)]
    // RDD[(day_area_adsId,count)]未聚合，key有重复
    // => reduceByKey 将不同城市相同地区进行聚合RDD[(day_area_adsId,count)]
    val areaAdsTotalCountDStream: DStream[(String, Long)] = areaCityAdsTotalCountDstream.map {
      case (area_city_adsId_day, count) =>
        val keyArr: Array[String] = area_city_adsId_day.split(":")
        val area: String = keyArr(0)
        val city: String = keyArr(1)
        val adsId: String = keyArr(2)
        val day: String = keyArr(3)
        val newKey: String = area + ":" + adsId + ":" + day
        (newKey, count)
    }.reduceByKey(_ + _)

    //按相同day聚合
    val areaAdsGroupbyDay: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsTotalCountDStream.map {
      case (area_adsId_day, count) =>
        val keyArr: Array[String] = area_adsId_day.split(":")
        val area: String = keyArr(0)
        val adsId: String = keyArr(1)
        val day: String = keyArr(2)
        (day, (area, (adsId, count)))
    }.groupByKey()

    val areaTop3adsPerDayDStream: DStream[(String, Map[String, String])] = areaAdsGroupbyDay.map {

      case (daykey, areaIter) =>
        val adsCountGroupbyArea: Map[String, Iterable[(String, (String, Long))]] = areaIter.groupBy {
          case (area, (adsId, count)) =>
            // -> 对iterable(area,(adsId,count)) groupBy
            // =>iterable(area,(adsId,count))=> Map[area,Iterable[(area,(adsId,count))]]
            // 注：按area进行groupby相当于在原来的迭代数据外面套了一层area
            area
        }
        //Map[area,Iterable[(area,(adsId,count))]]=>Map[area,Iterable[(adsId,count)]]
        val areaAdsCountTop3Json: Map[String, String] = adsCountGroupbyArea.map {
          case (area, areaAdsIter) =>

            //去掉迭代中冗余的area
            val adsCountItr: Iterable[(String, Long)] = areaAdsIter.map {
              case (area, (adsId, count)) =>
                (adsId, count)
            }
            //转List，排序，取前三
            val top3AdsCountList: List[(String, Long)] = adsCountItr.toList.sortWith {
              (adsCount1, adsCount2) =>
                adsCount1._2 > adsCount2._2
            }.take(3)

            //转json  利用json4s
            val top3AdsCountJsonString: String = compact(render(top3AdsCountList))

            (area, top3AdsCountJsonString)
        }

        (daykey, areaAdsCountTop3Json)
    }

    //存redis
    areaTop3adsPerDayDStream.foreachRDD {
      //按分区缓存foreachPartition
      rdd =>
        rdd.foreachPartition {
          areaTop3adsPerDayItr =>
            val jedisClient: Jedis = RedisUtil.getJedisClient
            //遍历每一个（k,v），依次缓存入redis的hashMap
            for ((daykey, areaMap) <- areaTop3adsPerDayItr) {
              //导入隐式转换 scala的hashmap不是java的hashmap
              import collection.JavaConversions._
              jedisClient.hmset("top3_ads_per_day:" + daykey, areaMap)
            }
            jedisClient.close()
        }

    }


  }

}
