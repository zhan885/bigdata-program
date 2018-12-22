package com.zhy.sparkmall.offline.app

import com.zhy.sparkmall.common.model.UserVisitAction
import com.zhy.sparkmall.offline.bean.{CategorySessionTopN, CategoryTopN}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Administrator on 2018/12/10.
  */
object CategorySessionTopApp {

  def statCategorySession(config: FileBasedConfiguration, sparkSession: SparkSession, taskId: String, userActionRDD: RDD[UserVisitAction], categoryTop10List: List[CategoryTopN]): Unit = {

    val categoryTop10BC: Broadcast[List[CategoryTopN]] = sparkSession.sparkContext.broadcast(categoryTop10List)
    
    //1、过滤: 过滤出所有排名前十品类的action=>RDD[UserVisitAction]
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter {
      userAction =>
        var matchflag = false
        for (categoryTop10 <- categoryTop10BC.value) {
          if (categoryTop10.category_id == userAction.click_category_id.toString)
            matchflag = true
        }
        matchflag
    }

    //2 相同的cid+sessionId进行累加计数
    //       RDD[userAction.clickcid+userAction.sessionId,1L]
    //        reducebykey(_+_)  =>RDD[cid_sessionId,count]
    val cidSessionIdCountRDD: RDD[(String, Long)] = filteredUserActionRDD.map {
      userAction =>
        (userAction.click_category_id + "_" + userAction.session_id, 1L)
    }.reduceByKey(_ + _)

    //3、根据cid进行聚合
    //RDD[cid_sessionId,count]
    //=>RDD[cid,(sessionId,count)]
    //=> groupbykey => RDD[cid,Iterable[(sessionId,clickCount)]]
    //前十品类的对应的所有的sessionId,click_count
    //因为后续要对其进行排序，所以要把value变成可迭代类型
    val sessionCountByCidRDD: RDD[(String, Iterable[(String, Long)])] = cidSessionIdCountRDD.map {
      case (cid_sessionId, count) =>
        val arr: Array[String] = cid_sessionId.split("_")
        val cid: String = arr(0)
        val sessionId: String = arr(1)
        (cid, (sessionId, count))
    }.groupByKey()

    //4、聚合后进行排序、截取
    //=>RDD[cid,Iterable[(sessionId,clickCount)]]
    //=>把iterable 进行排序截取前十=>RDD[cid,Iterable[(sessionId,clickCount)]]
    val categorySessionTopRDD: RDD[CategorySessionTopN] = sessionCountByCidRDD.flatMap {
      //将itrSessionCount中的map转化为List,方便按照点击数量进行排序
      case (cid, itrSessionCount) =>
        val sessionCount10: List[(String, Long)] = itrSessionCount.toList.sortWith {
          (sessionCount1, sessionCount2) =>
            sessionCount1._2 > sessionCount2._2
        }.take(10)
        //将List中二元组转化为CategorySessionTopN对象
        val categorySessionTopList: List[CategorySessionTopN] = sessionCount10.map {
          case (sessionId, count) =>
            CategorySessionTopN(taskId, cid, sessionId, count)
        }
        categorySessionTopList //利用flatmap将对象集合打散变成一个个对象RDD[List[CategorySessionTopN]]->RDD[CategorySessionTopN]

    }
    println("categorySessionTopRDD输出："+categorySessionTopRDD.collect().mkString("\n"))

    /*
      建表
      CREATE TABLE`category_top10_session_count` (
        `taskId` TEXT,
        `category_id` TEXT,
        `sessionId` TEXT,
        `click_count` BIGINT(20) DEFAULT NULL
      ) ENGINE=INNODB DEFAULT CHARSET=utf8
       */

    //5、 结果转换成对象然后 存储到数据库中
    import sparkSession.implicits._
    categorySessionTopRDD.toDF().write.format("jdbc")
      .option("url",config.getString("jdbc.url"))
      .option("user",config.getString("jdbc.user"))
      .option("password",config.getString("jdbc.password"))
      .option("dbtable","category_top10_session_count")
      .mode(SaveMode.Append).save()

    //=>RDD[CategorySessionTop]=>存数据库
  }
}
