package com.zhy.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.Date

import com.zhy.sparkmall.common.model.UserVisitAction
import com.zhy.sparkmall.offline.bean.SessionInfo
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.HashSet

/**
  * Created by Administrator on 2018/12/09.
  */
object SessionExtractApp {


  //需求二：按比例抽取session
  /*
  批次号
  sessionId
  开始时间
  步长
  时长
  搜索过的关键词
  点击过的商品
  下过单的商品
  支付过的商品
   */
  val exNum = 1000  //要抽取的数

  def sessionExtract(sessionCount: Long, taskId: String, sessionActionRDD: RDD[(String, Iterable[UserVisitAction])]): RDD[SessionInfo] = {

    //    1  所有session集合，以sessionId为单位
    //    2  抽取
    //      某个小时要抽取得session个数=某个小时的session个数 /总session数量 *1000
    //
    //    RDD[sessionId,Iterable[UserAction]]
    //    按天+小时进行聚合 ，求出每个【天+小时】的session个数
    //
    // 1 RDD[sessionId,Iterable[UserAction]]
    //    =>map=>
    //求时长和开始时间

    // 创建一个标准日期格式
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sessionInfoRDD: RDD[SessionInfo] = sessionActionRDD.map {
      //map是转换，foreach是执行,执行的结果就是得到累加器
      case (sessionId, itrActions) =>
        var maxActionTime = -1L
        var minActionTime = Long.MaxValue

        //存放关键字的容器
        val keywordBuffer = new ListBuffer[String]()
        val clickBuffer = new ListBuffer[String]()
        val orderBuffer = new ListBuffer[String]()
        val payBuffer = new ListBuffer[String]()

        // for循环遍历元组中actions的每个元素，并求出该元素的时间最大和最小值
        for (action <- itrActions) {
          // 将字符串的时间进行日期格式化后得到毫秒时间数
          val actionTimeMillSec: Long = format.parse(action.action_time).getTime

          // 用变量maxActionTime依次与action中的每个时间数进行比较，求出最大时间数
          maxActionTime = Math.max(maxActionTime, actionTimeMillSec)
          // 用变量minActionTime依次与action中的每个时间数进行比较，求出最小时间数
          minActionTime = Math.min(minActionTime, actionTimeMillSec)

          //判断每个action的操作类型，多个action进行合并
          if (action.search_keyword != null) {
            keywordBuffer += action.search_keyword
          } else if (action.click_product_id != -1) {
            clickBuffer += action.click_product_id.toString
          } else if (action.order_product_ids != null) {
            orderBuffer += action.order_product_ids
          } else if (action.pay_product_ids != null) {
            payBuffer += action.pay_product_ids
          }

        }

        // 求出当前session的最大时长
        val visitLenth: Long = maxActionTime - minActionTime
        val stepLength: Int = itrActions.size
        //开始时间
        val startTime: String = format.format(new Date(minActionTime))

        // 2 RDD[ sessionInfo]
        SessionInfo(taskId, sessionId, startTime, stepLength.toLong, visitLenth,
          keywordBuffer.mkString(","), clickBuffer.mkString(","), orderBuffer.mkString(","), payBuffer.mkString(","))

    }

    //=>map=>
    //3 RDD[ day_hour,sessionInfo]
    val dayHourSessionsRDD: RDD[(String, SessionInfo)] = sessionInfoRDD.map {
      sessionInfo =>
      val dayHourKey: String = sessionInfo.startTime.split(":")(0)
      (dayHourKey, sessionInfo)
    }

    //=>groupbykey
    //4 RDD[day_hour, Iterable[sessionInfo]]  多个
    //  =》抽取
    val dayHourSessionGroupRDD: RDD[(String, Iterable[SessionInfo])] = dayHourSessionsRDD.groupByKey()

    //5 RDD[day_hour, Iterable[sessionInfo]]  少量
    //  =》RDD[sessionInfo] 1000个
    val sessionExRDD: RDD[SessionInfo] = dayHourSessionGroupRDD.flatMap {
      case (dayHourKey, itrSessions) =>
        // 1确定抽取的个数
        // 公式：当前小时的session数/总session数sessionCount * 一共要抽取的数exNum
        //itrSessions.size / sessionCount * exNum 不能这么写sessionCount和exNum在driver端不一定是在同一台机器内，所以要用广播变量
        val dayHourNum: Long = Math.round(itrSessions.size / sessionCount.toDouble * exNum)

        // 2按照要求的个数进行抽取
        val sessionSet: mutable.HashSet[SessionInfo] = randomExtract(itrSessions.toArray, dayHourNum)
        sessionSet

    }
    sessionExRDD

    //6 把RDD 保存到mysql中
    //Iterable[sessionInfo]   要抽取  个数= .size /sessioncount *1000
    //在另一个APP完成
  }
  //写一个抽取方法
  //实现从一个集合中 按照规定的数量随机抽取放到另一个集合中

  def randomExtract[T](arr:Array[T],num:Long): mutable.HashSet[T] ={

    //循环num次随机产生一个下标值（0~arr.length）
    val resultSet = new mutable.HashSet[T]()
    //避免重复数据，抽到满足为止
    while(resultSet.size<num){
      val index: Int = new Random().nextInt(arr.length)
      val value: T = arr(index)
      resultSet+=value
    }
    resultSet
  }

  def main(args: Array[String]): Unit = {
    println(randomExtract(Array(1, 2, 3, 4, 5), 3).mkString(","))
  }
}





















