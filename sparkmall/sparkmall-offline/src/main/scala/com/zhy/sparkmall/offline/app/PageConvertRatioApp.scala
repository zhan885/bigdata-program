package com.zhy.sparkmall.offline.app

import com.zhy.sparkmall.common.model.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhy.sparkmall.common.util.JdbcUtil
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion
import org.apache.spark.broadcast.Broadcast

import scala.collection.Map
import scala.collection.immutable.IndexedSeq

/**
  * Created by Administrator on 2018/12/11.
  */
object PageConvertRatioApp {


  def calcPageConvertRatio(sparkSession: SparkSession, conditionJsonString: String, taskId: String, userActionRDD: RDD[UserVisitAction]) = {

    //1 转化率公式 ：页面跳转的次数 / 前一个页面的访问次数
    //2 1,2,3,4,5,6,7 => 1-2,2-3,3-4,4-5,5-6,6-7次数 / 1,2,3,4,5,6,7次数
    //获取目标访问页面
    val pageVisitArr: Array[String] = JSON.parseObject(conditionJsonString).getString("targetPageFlow").split(",")

    //计算跳转页面
    //3 可以用zip 1-6 zip 2-7 来实现1-2,2-3,3-4,4-5,5-6,6-7
    val pageJumpTupleArr = pageVisitArr.slice(0, pageVisitArr.length - 1).zip(pageVisitArr.slice(1, pageVisitArr.length))
    val targetPageJumpArr: Array[String] = pageJumpTupleArr.map {
      case (page1, page2) =>
        page1 + "-" + page2
    }

    //广播变量（前一个访问页面）
    val targetVisitPageBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageVisitArr.slice(0, pageVisitArr.length - 1))
    //4 前一个页面的访问次数 => 1-6的访问次数 =>取pageId为1-6的访问次数
    // 过滤出pageId为1-6的访问记录 => map =>(pageId,1L) => countByKey => Map[pageId,count]
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter {
      userAction =>
        targetVisitPageBC.value.contains(userAction.page_id.toString)
    }
    // 访问页面数
    val pageVisitCountMap: Map[String, Long] = filteredUserActionRDD.map {
      userAction =>
        (userAction.page_id.toString, 1L)
    }.countByKey()

    //5 页面的跳转次数
    // 根据sessionId进行聚合 => RDD[sessionId,iterable[UserVisitAction]]
    // => 按照每个session的访问记录时间进行排序 sortWith =>List[UserVisitAction]
    // => List[pageId] => 1,2,3,4,5,6,7,8,12,16
    // zip: 1-12 zip 2-16  =>map List(1-2,2-3,3-4,...)
    // 过滤
    // =>map:(1-2,1L)
    // => reduceByKey (1-2,300L),(2-3,200L),...
    //Map[key,count]累计转换页面的次数
    val userActionsBySessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map {
      userAction =>
        (userAction.session_id, userAction)
    }.groupByKey()

    val pageJumpsRDD: RDD[String] = userActionsBySessionRDD.flatMap {
      case (sessionId, itrUserVisitAction) =>
        // => 按照每个session的访问记录时间进行排序 sortWith =>List[UserVisitAction]
        val userActionSortedList: List[UserVisitAction] = itrUserVisitAction.toList.sortWith {
          (action1, action2) =>
            action1.action_time < action2.action_time
        }
        // => List[pageId]
        val pageVisitList: List[String] = userActionSortedList.map {
          userActionList =>
            userActionList.page_id.toString
        }
        // =>map 1-2,2-3,3-4,...
        val pageJumpList: List[String] = pageVisitList.slice(0, pageVisitList.length - 1).zip(pageVisitList.slice(1, pageVisitList.length)).map {
          case (page1, page2) =>
            page1 + "-" + page2
        }

        pageJumpList //1-2,2-3,3-4,4-5,5-6...
    }

    // 广播变量（目标跳转页面）
    val targetPageJumpsBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageJumpArr)
    // 过滤
    val filteredPageJumRDD: RDD[String] = pageJumpsRDD.filter {
      pageJumps =>
        targetPageJumpsBC.value.contains(pageJumps)
    }
    // 跳转页面数
    val pageJumpCountMap: Map[String, Long] = filteredPageJumRDD.map {
      pageJump =>
        (pageJump, 1L)
    }.countByKey()

    println(pageJumpCountMap.mkString("\n"))
    println(pageVisitCountMap.mkString("\n"))
    /*
    2-3 -> 29
    4-5 -> 22
    5-6 -> 14
    3-4 -> 26
    6-7 -> 24
    1-2 -> 25
    4 -> 1145
    5 -> 1094
    6 -> 1189
    1 -> 1171
    2 -> 1118
    3 -> 1103
     */

    //6 两个map 分别进行除法得到转换率
    val pageConvertRatio: Iterable[Array[Any]] = pageJumpCountMap.map {
      case (pageJumps, count) =>
        val prePage = pageJumps.split("-")(0)
        val prePageCount = pageVisitCountMap.getOrElse(prePage, 1L)
        var ratio = Math.round(count * 1000 / prePageCount.toDouble) / 10.0
        Array(taskId, pageJumps, ratio)
    }

    /*
    Create Table

    CREATE TABLE `page_convert_rate` (
      `taskId` text,
      `pageJump` text,
      `rate` double DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
       */

    //7 存入MySQL库
    JdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?)",pageConvertRatio)

  }
}
