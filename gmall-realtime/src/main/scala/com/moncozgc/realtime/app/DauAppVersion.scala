package com.moncozgc.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.moncozgc.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

/**
 * 日活业务
 * 功能1: SparkStreaming 消费 kafka 数据 √
 *
 * Created by MoncozGC on 2022/6/8
 */
object DauAppVersion {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "moncozgc-start"
    val groupId = "gmall-dau"

    // 通过SparkStreaming程序从kafka中读取数据
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    // 直接打印ConsumerRecord的value数据, 也就是JSON字符串
    //    val jsonDStream: DStream[String] = recordDStream.map((_: ConsumerRecord[String, String]).value())

    // TODO 功能1: SparkStreaming 消费 kafka 数据
    // 对ConsumerRecord的value数据, 也就是JSON字符串数据进行处理
    val jsonDStream: DStream[JSONObject] = recordDStream.map(
      (record: ConsumerRecord[String, String]) => {
        // 获取到value数据, JSON字符串
        val jsonValue: String = record.value()
        // 利用fastjson获取一个jsonObject
        val jsonObject: JSONObject = JSON.parseObject(jsonValue)
        // 获取到json字符串中, 时间的字段
        val tsLong: lang.Long = jsonObject.getLong("ts")
        // 将时间的字段转换为 yyyy-MM-dd HH 格式
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(tsLong))
        // 将时间字段根据空格切分字符
        val dateArr: Array[String] = dateStr.split(" ")
        // 第一个字符为 年月日
        val dt: String = dateArr(0)
        // 第二个字符为 小时
        val hr: String = dateArr(1)
        // 再将两个时间添加到json字符串中
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    )

    jsonDStream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
