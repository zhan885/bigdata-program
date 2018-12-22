package com.zhy.sparkmall.offline.app

import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhy.sparkmall.common.model.UserVisitAction
import com.zhy.sparkmall.common.util.{ConfigUtil, JdbcUtil}
import com.zhy.sparkmall.offline.accu.{CategoryActionCountAccumulator, SessionAccumulator}
import com.zhy.sparkmall.offline.bean.CategoryTopN
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/12/07.
  */
object SessionStatApp {
  /**
    * 步骤一： 从hive中获取访问日志,生成RDD
    * 从hive中获取访问日志，并根据条件过滤,生成RDD
    * *
    * 步骤二：根据sessionId把访问记录聚合成集合
    * 变成 一个 sessionid对应多个该sessionId的访问记录
    * *
    * 步骤三：计算单个session的统计信息
    * 单个session的统计信息的类
    * *
    * 步骤四：遍历所有session，分类累加，聚合信息
    * 制作累加器
    * *
    * 步骤五 ：根据每个session的类别分别计数到累加器中
    * *
    * 根据累加器结果计算占比
    * *
    * 占比结果类
    * *
    * 步骤六：保存数据库类
    */

  def readUserVisitActionToRDD(sparkSession: SparkSession, conditionJsonString: String): RDD[UserVisitAction] = {
    //查库 条件
    //进入对应的数据库
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    //从config.properties配置文件中获取数据库名并use进入该数据库
    val databaseName: String = config.getString("hive.database")
    sparkSession.sql("use " + databaseName)

    //从conditions.properties配置文件中取出过滤的条件
    //val conditionConfig: FileBasedConfiguration = ConfigUtil("condition.properties").config
    //从该配置文件得到condition.params.json条件属性
    //val conditionJsonString: String = conditionConfig.getString("condition.params.json")
    //将条件String转化成JsonObject类型（反序列化）
    val jsonObject: JSONObject = JSON.parseObject(conditionJsonString)

    //sql
    //添加where 1=1 防止后面添加的条件不满足where是空
    val sql = new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1")

    if (jsonObject.getString("startDate") != null) {
      sql.append(" and date >='" + jsonObject.getString("startDate") + "'")
    }
    if (jsonObject.getString("endDate") != null) {
      sql.append(" and date <='" + jsonObject.getString("endDate") + "'")
    }
    if (jsonObject.getString("startAge") != null) {
      sql.append(" and u.age >=" + jsonObject.getString("startAge"))
    }
    if (jsonObject.getString("endAge") != null) {
      sql.append(" and u.age <=" + jsonObject.getString("endAge"))
    }
    if (!(jsonObject.getString("professionals") isEmpty)) {
      sql.append(" and u.professional in (" + jsonObject.getString("professionals") + ")")
    }

    println(sql)
    //select v.* from user_visit_action v join user_info u on v.user_id=u.user_id where 1=1 and date >='2018-11-01' and date <='2018-12-28' and u.age >=20 and u.age <=50
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

    rdd
  }

  /**
    * 原数据
    * 用户访问动作表user_visit_action join user_info
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
    // SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 注册累加器
    val accumulator: SessionAccumulator = new SessionAccumulator()
    sparkSession.sparkContext.register(accumulator)

    //定义批次号
    val taskId: String = UUID.randomUUID().toString
    //本次过滤条件
    //从conditions.properties配置文件中取出过滤的条件
    val conditionConfig: FileBasedConfiguration = ConfigUtil("condition.properties").config
    //从该配置文件得到condition.params.json条件属性
    val conditionJsonString: String = conditionConfig.getString("condition.params.json")
    //将条件String转化成JsonObject类型（反序列化）
    val jsonObject: JSONObject = JSON.parseObject(conditionJsonString)

    // 筛选 要关联用户 sql jion user_info where condition =>DF=>RDD[UserVisitAction]
    val userActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession, conditionJsonString)

    // rdd=>RDD[UserVisitAction]=>groupByKey=>RDD[(sessionId,iterable[UserVisitAction])]
    val userSessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map(
      userAction =>
        (userAction.session_id, userAction)
    ).groupByKey()
    userSessionRDD.cache()

    // 求session总数量
    val userSessionCount: Long = userSessionRDD.count() //总数

    // 遍历每个session
    // 每个session时长=最大时间-最小时间
    userSessionRDD.foreach {
      //map是转换，foreach是执行,执行的结果就是得到累加器
      case (sessionId, actions) =>
        var maxActionTime = -1L
        var minActionTime = Long.MaxValue

        // for循环遍历元组中actions的每个元素，并求出该元素的时间最大和最小值
        for (action <- actions) {
          // 创建一个标准日期格式
          val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          // 将字符串的时间进行日期格式化后得到毫秒时间数
          val actionTimeMillSec: Long = format.parse(action.action_time).getTime

          // 用变量maxActionTime依次与action中的每个时间数进行比较，求出最大时间数
          maxActionTime = Math.max(maxActionTime, actionTimeMillSec)
          // 用变量minActionTime依次与action中的每个时间数进行比较，求出最小时间数
          minActionTime = Math.min(minActionTime, actionTimeMillSec)
        }

        // 求出当前session的最大时长
        val visitTime: Long = maxActionTime - minActionTime
        // 时长大于10秒和小于10秒的session分类计数
        // 遍历一下全部session，对每个session的类型进行判断，来进行分类的累加计数（累加器）
        if (visitTime > 10000) {
          //累加计数
          accumulator.add("session_visitLength_gt_10_count")

        } else {
          //累加计数
          accumulator.add("session_visitLength_le_10_count")

        }
        // 步长：某个session一共触发的action个数
        // 步长大于5和小于5的session分类计数
        if (actions.size > 5) {
          //累加计数
          accumulator.add("session_stepLength_gt_5_count")

        } else {
          //累加计数
          accumulator.add("session_stepLength_le_5_count")
        }

    }
    println(userSessionCount)
    //5596
    // 提取累加器中的值
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value

    println(sessionCountMap.mkString(","))
    //session_stepLength_le_5_count -> 2252,session_stepLength_gt_5_count -> 3344,session_visitLength_gt_10_count -> 1464,session_visitLength_le_10_count -> 4132

    // 把累计值计算为比例
    val session_visitLength_gt_10_ratio = Math.round(1000.0 * sessionCountMap("session_visitLength_gt_10_count") / userSessionCount) / 10.0
    val session_visitLength_le_10_ratio = Math.round(1000.0 * sessionCountMap("session_visitLength_le_10_count") / userSessionCount) / 10.0
    val session_stepLength_gt_5_ratio = Math.round(1000.0 * sessionCountMap("session_stepLength_gt_5_count") / userSessionCount) / 10.0
    val session_stepLength_le_5_ratio = Math.round(1000.0 * sessionCountMap("session_stepLength_le_5_count") / userSessionCount) / 10.0

    /**
      * 需求一：目标数据
      * 统计批次编号
      * 本次过滤条件
      * 总session个数
      * 小于等于10秒
      * 大于10秒
      * 步长小于等于5次
      * 大于5次
      */
    val resultArray = Array(taskId, conditionJsonString, userSessionCount, session_visitLength_gt_10_ratio, session_visitLength_le_10_ratio, session_stepLength_gt_5_ratio, session_stepLength_le_5_ratio)

    /*
    建表
    CREATE TABLE `session_stat_info` (
      `taskId` text,
      `conditions` text,
      `session_count` bigint(20) DEFAULT NULL,
      `session_visitLength_gt_10_ratio` double DEFAULT NULL,
      `session_visitLength_le_10_ratio` double DEFAULT NULL,
      `session_stepLength_gt_5_ratio` double DEFAULT NULL,
      `session_stepLength_le_5_ratio` double DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8

     */

    //保存到mysql中
    JdbcUtil.executeUpdate("insert into session_stat_info value(?,?,?,?,?,?,?)", resultArray)

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
    val sessionExRDD = SessionExtractApp.sessionExtract(userSessionCount, taskId, userSessionRDD)

    /*
    建表
    CREATE TABLE `random_session_info` (
      `taskId` text,
      `sessionId` text,
      `visitLength` bigint(20) DEFAULT NULL,
      `stepLength` bigint(20) DEFAULT NULL,
      `startTime` text,
      `searchKeywords` text,
      `clickProductIds` text,
      `orderProductIds` text,
      `payProductIds` text
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
     */
    import sparkSession.implicits._
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    sessionExRDD.toDF.write.format("jdbc")
      .option("url", config.getString("jdbc.url"))
      .option("user", config.getString("jdbc.user"))
      .option("password", config.getString("jdbc.password"))
      .option("dbtable", "random_session_info").mode(SaveMode.Append).save

    /*
    需求三：根据品类的点击、下单、支付次数依次进行排序，取前十名数据

    目标数据
    taskId
    category_id 品类id
    click_count 点击次数
    order_count 下单次数
    pay_count 付款次数
    */

    // 注册累加器
    val categoryActionCountAccu: CategoryActionCountAccumulator = new CategoryActionCountAccumulator()
    sparkSession.sparkContext.register(categoryActionCountAccu)

    //用户访问动作表=>品类_动作->次数
    userActionRDD.foreach {
      userAction =>
        if (userAction.click_category_id != -1L) {
          //给当前品类的点击次数加1
          categoryActionCountAccu.add(userAction.click_category_id + "_click")
        } else if (userAction.order_category_ids != null && userAction.order_category_ids.length != 0) {
          //由于订单涉及多个品类，要用,分割，循环进行累加
          val orderCidArr: Array[String] = userAction.order_category_ids.split(",")
          //循环订单中的category列表数组
          for (orderCid <- orderCidArr) {
            //针对每一个category类别进行累加计数
            categoryActionCountAccu.add(orderCid + "_order")
          }
        } else if (userAction.pay_category_ids != null && userAction.pay_category_ids.length != 0) {
          //由于支付涉及多个品类，要用,分割，循环累加
          val payCidArr: Array[String] = userAction.pay_category_ids.split(",")
          //循环每个品类，进行累加
          for (payCid <- payCidArr) {
            categoryActionCountAccu.add(payCid + "_pay")
          }
        }
    }
    //打印
    println(categoryActionCountAccu.value.mkString("\n"))
    /*
    9_click -> 1864
    6_pay -> 367
    16_pay -> 386
    2_click -> 1894
    16_order -> 582
    19_order -> 569
    10_click -> 1888
    8_order -> 571
    1_order -> 542
    ...
     */

    //groupby 定义用category来进行分组
    //按照品类将原来的map[String,Long]进行分组->map[String,map[String,Long]]
    val actionCountByCidMap: Map[String, mutable.HashMap[String, Long]] = categoryActionCountAccu.value.groupBy {
      case (cidAction, count) =>
        val cid: String = cidAction.split("_")(0)
        cid
    }

    //把结果转成对象CategoryTopN(taskId, category_id, click_count, order_count, pay_count)
    //再转化成list格式，方便后续对它进行排序
    val catagoryTopNList: List[CategoryTopN] = actionCountByCidMap.map {
      case (cid, actionMap) =>
        CategoryTopN(taskId, cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
    }.toList

    //sortWith(函数：排序规则)
    val categoryTop10: List[CategoryTopN] = catagoryTopNList.sortWith {
      (ctn1, ctn2) =>
        if (ctn1.click_count > ctn2.click_count) {
          true
        } else if (ctn1.click_count == ctn2.click_count) {
          if (ctn1.order_count > ctn2.order_count) {
            true
          } else if (ctn1.order_count == ctn2.order_count) {
            if (ctn1.pay_count > ctn2.pay_count) {
              true
            } else {
              false
            }
          } else {
            false
          }
        } else {
          false
        }
    }.take(10)
    //打印
    println(categoryTop10.mkString("\n"))
    /*
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,12,1976,581,366)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,1,1968,542,383)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,6,1957,598,367)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,19,1947,569,381)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,5,1901,540,331)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,2,1894,570,379)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,15,1893,553,411)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,14,1890,575,399)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,10,1888,637,402)
    CategoryTopN(e2103fb9-37fe-476f-a918-47fbdbaec8b2,4,1885,551,381)
     */

    //组合成jdbcUtil插入需要的结构=>List[CategoryTopN]=>ListBuffer[Array[Any]]
    val categoryList = new ListBuffer[Array[Any]]()

    for (categoryTopN <- categoryTop10) {
      val paramArray = Array(categoryTopN.taskId,categoryTopN.category_id,categoryTopN.click_count,categoryTopN.order_count,categoryTopN.pay_count)
      categoryList.append(paramArray)
    }
    /*
    建表
    CREATE TABLE `category_top10`(
    `taskId` TEXT,
    `category_id` TEXT,
    `click_count` BIGINT(20) DEFAULT NULL,
    `order_count` BIGINT(20) DEFAULT NULL,
    `pay_count` BIGINT(20) DEFAULT NULL
    ) ENGINE=INNODB DEFAULT CHARSET=utf8
     */

    //保存到数据库
    JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?)",categoryList)


    //需求四
    CategorySessionTopApp.statCategorySession(config,sparkSession,taskId,userActionRDD,categoryTop10)
    println("需求四完成！")


    //需求五
    PageConvertRatioApp.calcPageConvertRatio(sparkSession,conditionJsonString,taskId,userActionRDD)
    println("需求五完成！")


  }

}
