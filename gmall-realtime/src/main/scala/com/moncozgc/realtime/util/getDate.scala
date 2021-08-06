package com.moncozgc.realtime.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * 获取当前时间的前一天时间
 *
 * Created by MoncozGC on 2021/8/5
 */
object getDate {

  def main(args: Array[String]): Unit = {
    getYD("2021-08-05")
  }

  def getYD(dt: String): String = {
    //val dt = "2021-08-05"
    val myFormat = new SimpleDateFormat("yyyy-MM-dd") //使用给定的日期格式
    var date = new Date() //创建一个Date对象
    date = myFormat.parse(dt) //根据给定的日期格式去解析date
    val cal1 = Calendar.getInstance() //日历
    cal1.setTime(date) //指定日历的日期
    cal1.add(Calendar.DATE, -1) //根据日历规则，在给定的日历字段中添加或减去指定的时间量
    //println(myFormat.format(cal1.getTime())) //获取日期,将日期格式为时间字符串
    myFormat.format(cal1.getTime())
  }
}
