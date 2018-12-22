package com.zhy.sparkmall.realtime.bean


import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Administrator on 2018/12/12.
  */
case class RealtimeAdslog(logdate: Date, area: String, city: String, userId: String, adsId: String) {

  def getDateTimeString() = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(logdate)
  }

  def getDateString() = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    format.format(logdate)
  }

  def getHourMinuString() = {
    val format: SimpleDateFormat = new SimpleDateFormat("HH:mm")
    format.format(logdate)
  }

}
