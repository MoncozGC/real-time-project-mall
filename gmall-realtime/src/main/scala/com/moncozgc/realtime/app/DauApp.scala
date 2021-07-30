package com.moncozgc.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.moncozgc.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日活业务
 *
 * Created by MoncozGC on 2021/7/29
 */
object DauApp {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val topic: String = "moncozgc-start"
    val groupId: String = "mall-dau"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    // 生成的启动日志, 只获取value部分
    val jsonDStream: DStream[String] = kafkaDStream.map(_.value())
    // 打印未加时间的字符串
    // jsonDStream.print(1000)

    val JSONDStream: DStream[JSONObject] = kafkaDStream.map {
      record => {
        val jsonString: String = record.value()
        // 将JSON格式字符串转化为JSON对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        val ts: lang.Long = jsonObject.getLong("ts")
        // 将时间戳转换成日期和小时 2021-07-29 23
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        // 根据空格做分割
        val dateStrArr: Array[String] = dateStr.split(" ")
        val dt: String = dateStrArr(0)
        val hr: String = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
    JSONDStream.print(1000)
    // 从kafka中读取
    // 开始执行流
    ssc.start()
    // 等待执行停止。 执行过程中发生的任何异常都会在该线程中抛出
    ssc.awaitTermination()
  }
}
