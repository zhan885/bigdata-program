package com.zhy.sparkmall.realtime.app

import java.util
import java.util.Date

import com.zhy.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import com.zhy.sparkmall.realtime.bean.RealtimeAdslog
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2018/12/12.
  */
object RealtimelogApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("realtime_ads").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)

    val adsStringDStream: DStream[String] = inputDStream.map {
      record =>
        record.value()
    }
    //测试 每5秒输出rdd
    //    adsStringDStream.foreachRDD {
    //      rdd =>
    //        println(rdd.collect().mkString(","))
    //    }

    ////////需求七////////
    //需求：实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。
    //思路：
    //1 检查访问流量，根据要求生成黑名单
    //1.1 保存每个用户每天点击每个广告的次数
    // 每次rdd => 取出结果 redis[k,v] key:用户+日期+广告 value:点击次数
    // rdd[String] => rdd[readttimelog]
    // => rdd[(userId_adsId_date,1L)]
    // =>reduceByKey => rdd[(userId_adsId_date,count)] => Map[key,value]
    // => 保存redis
    val realtimeLogDStream: DStream[RealtimeAdslog] = adsStringDStream.map {
      adsString => {
        val logArr: Array[String] = adsString.split(" ")
        val dateMillSec: String = logArr(0)
        val area: String = logArr(1)
        val city: String = logArr(2)
        val userId: String = logArr(3)
        val adsId: String = logArr(4)
        val date: Date = new Date(dateMillSec.toLong)
        RealtimeAdslog(date, area, city, userId, adsId)
      }
    }

    //    val jedisClient: Jedis = RedisUtil.getJedisClient //在driver执行，Jedis对象，放在外面可能会出现序列化异常
    //        /////方法一：过滤掉黑名单中的用户数据（每5秒每行记录都要执行一次）
    //        val filteredRealtimelog: DStream[RealtimeAdslog] = realtimeLogDStream.filter {
    //          realtimeLog =>
    //            val jedisClient: Jedis = RedisUtil.getJedisClient //在excute执行
    //            //留不是黑名单中的用户
    //            !jedisClient.sismember("user_blacklist", realtimeLog.userId) //excute 根据每一条数据来判断
    //        }

    /////方法二（推荐）：每5秒查询一次的数据可以放入transform中
    val jedisClient: Jedis = RedisUtil.getJedisClient
    //只执行一次
    val filteredRealtimelog: DStream[RealtimeAdslog] = realtimeLogDStream.transform {
      rdd =>
        val blackList: util.Set[String] = jedisClient.smembers("user_blacklist")
        //driver 每5秒查询一次redis
        //将blackList封装成广播变量
        val blackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackList)
        //driver 每5秒查询一次redis
        //将driver查询的结果 广播变量 转到rdd中
        val filteredRealtimeLogRDD: RDD[RealtimeAdslog] = rdd.filter {
          realtimeLog =>
            !blackListBC.value.contains(realtimeLog.userId)
        }
        filteredRealtimeLogRDD
    }

    //按天+用户+广告 进行聚合 计算点击量
    // => rdd[(userId_adsId_date,1L)]
    // =>reduceByKey => rdd[(userId_adsId_date,count)] => Map[key,value]
    val userAdsCountPerDayDStream: DStream[(String, Long)] = filteredRealtimelog.map {
      realtimeLog =>
        val key: String = realtimeLog.userId + ":" + realtimeLog.adsId + ":" + realtimeLog.getDateString()
        (key, 1L)
    }.reduceByKey(_ + _) //5秒


    //向redis中存放用户点击广告累积值
    //foreachRDD内的操作都是在driver端执行的
    userAdsCountPerDayDStream.foreachRDD {
      rdd =>

        ////在分片中建立连接的优化
        //        rdd.foreachPartition {
        //          itrabc =>
        //            val jedisClient: Jedis = RedisUtil.getJedisClient
        //            for (abc <- itrabc) {
        //              jedisClient //用当前链接处理abc，不用网络传输
        //            }
        //        }
        //将RDD转化为Array数组Array[(field, value)
        val userAdsCountPerDayArr: Array[(String, Long)] = rdd.collect()
        //定义Jedis变量
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //        rdd.map {
        //          log =>
        //            //业务上需要每条数据都要进行redis查询
        //            jedisClient
        //        }

        for ((key, count) <- userAdsCountPerDayArr) {
          // 点击次数达到阈值加入黑名单
          val countString: String = jedisClient.hget("user_ads_count_perday", key)
          //点击超过20就设置成黑名单
          if (countString != null && countString.toLong > 20) {
            // 黑名单 结构 String list(可重复) set(不可重复) hash(k,v) zset(排序)
            // 结构用set保存
            val userId: String = key.split(":")(0)
            jedisClient.sadd("user_blacklist", userId)

          }
          //收集
          //jedisClient.hset("user_ads_count_perday", key, count.toString)  //单次统计5秒的数据
          jedisClient.hincrBy("user_ads_count_perday", key, count) //将每次5秒统计的数据累加
        }

    }

    ////////需求八////////
    //需求：每天各地区各城市各广告的点击流量实时统计
    //思路
    //接着需求七中过滤掉黑名单用户的日志开始分析
    //1 把明细DStream[Realtimelog] => 变成[k,v]结构 => (地区+城市+广告+天,count)
    //2 转化成 DStream[(date:area:city:ads, 1L)]
    //3 => reduceByKey()  每5秒的所有key累计值
    val areaCityAdsCountDstream: DStream[(String, Long)] = filteredRealtimelog.map {
      realtimeAdslog =>
        val key = realtimeAdslog.area + ":" + realtimeAdslog.city + ":" + realtimeAdslog.adsId + ":" + realtimeAdslog.getDateString()
        (key, 1L)
    } reduceByKey (_ + _)

    //4 => updateStateByKey 来汇总到历史计数中
    sc.setCheckpointDir("./checkpoint") //缓存
    val areaCityAdsTotalCountDstream: DStream[(String, Long)] = areaCityAdsCountDstream.updateStateByKey {
      (adsCount: Seq[Long], totalCount: Option[Long]) =>
        val adsCountSum: Long = adsCount.sum
        val newTotalCount: Long = totalCount.getOrElse(0L) + adsCountSum
        Some(newTotalCount)
    }

    //5 输出到redis中
    areaCityAdsTotalCountDstream.foreachRDD {
      rdd =>
        val areaCityAdsTotalCountArr: Array[(String, Long)] = rdd.collect() //collect收集成Array

        //遍历每一个元素放入redis中
        for ((key, count) <- areaCityAdsTotalCountArr) {
          jedisClient.hset("area_city_ads_day_clickcount", key, count.toString)
        }

    }


    ////////需求九////////
    //需求：每天各个地区热门广告的top3
    AreaTop3AdsPerDayApp.calcTop3Ads(areaCityAdsTotalCountDstream)
    println("需求九完成！")

    ////////需求十////////
    //需求：各广告最近1小时内各分钟的点击量
    LastHourCountPerAds.calcLastHourCountPerAds(filteredRealtimelog)
    println("需求十完成！")



    ssc.start()
    ssc.awaitTermination()

  }
}
