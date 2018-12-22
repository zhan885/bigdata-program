package com.zhy.sparkmall.offline.bean

/**
  * Created by Administrator on 2018/12/10.
  * 需求四：目标数据结构
  * taskId
  * 品类id
  * sessionID
  * 点击次数
  */
case class CategorySessionTopN(taskId:String, category_id:String, sessionId:String, click_count:Long) {

  //注意：如果未来将数据插入到已经建好的数据库表里
  // 那么这里的属性的名字和数据库中的字段名称要一模一样
}
