package com.zhy.sparkmall.offline.bean

/**
  * Created by Administrator on 2018/12/09.
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
case class SessionInfo(taskId: String, sessionId: String, startTime: String, stepLength: Long, visitLength: Long, searchKeywords: String, clickProductIds: String, orderProductIds: String, payProductIds: String) {

}
