package com.zhy.sparkmall.offline.bean

/**
  * Created by Administrator on 2018/12/10.
  * 目标数据
  * taskId
  * category_id 品类id
  * click_count 点击次数
  * order_count 下单次数
  * pay_count 付款次数
  */
case class CategoryTopN(taskId: String, category_id: String, click_count: Long, order_count: Long, pay_count: Long) {

}
