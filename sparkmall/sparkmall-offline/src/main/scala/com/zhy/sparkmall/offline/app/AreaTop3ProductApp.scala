package com.zhy.sparkmall.offline.app

import com.zhy.sparkmall.common.util.ConfigUtil
import com.zhy.sparkmall.offline.myfunction.CityRemarkUDAF
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/12/11.
  */
object AreaTop3ProductApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("area_top3").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //注册
    sparkSession.udf.register("city_remark",new CityRemarkUDAF())

    //连接数据库
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")
    sparkSession.sql("use " + databaseName)

    // 地区表和访问记录表关联 => 带地区的访问记录
    val sql1 = "select c.area, p.product_id, p.product_name, city_name " +
      "from user_visit_action v " +
      "join city_info c " +
      "on v.city_id=c.city_id " +
      "join product_info p " +
      "on p.product_id=v.click_product_id "

    sparkSession.sql(sql1).createOrReplaceTempView("area_product_click_detail")

    // 以地区+商品Id 作为key，count作为点击次数
    val sql2 = "select area, product_id, product_name, count(*) clickcount, city_remark(city_name) cityremark " +
      "from area_product_click_detail " +
      "group by area, product_id, product_name"
    sparkSession.sql(sql2).createOrReplaceTempView("area_product_click_count")

    // 利用开窗函数进行分组排序 => 截取所有分组中前三名
    val sql3 ="select area, product_id, product_name, clickcount, cityremark " +
      "from(" +
      "select area, product_id, product_name, clickcount, rank() over(partition by area order by clickcount desc) rk, cityremark " +
      "from area_product_click_count)" +
      "where rk<=3"
    sparkSession.sql(sql3).show(100,false)  //显示完全

    //, city_remark(city_name) cityremark
    //, cityremark
    //, cityremark
  }
}
