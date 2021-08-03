package com.moncozgc.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.moncozgc.realtime.bean.DauInfo
import com.moncozgc.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

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
    //jsonDStream.print(1000)

    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map {
      record => {
        val jsonString: String = record.value()
        // 将JSON格式字符串转化为JSON对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        // 时间戳为 ts字段, long类型 "ts":1627604275000
        val ts: lang.Long = jsonObject.getLong("ts")
        // 将时间戳转换成日期和小时 2021-07-29 23
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        // 根据空格做分割
        val dateStrArr: Array[String] = dateStr.split(" ")
        val dt: String = dateStrArr(0)
        val hr: String = dateStrArr(1)
        // 将时间再插入到JSON中
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
    //jsonObjDStream.print(1000)

    /** // 通过Redis 对采集到的启动日志进行去重操作 方案1  采集周期中的每条数据都要获取一次Redis连接, 连接过于频繁
     * // redis 类型 Set   key dau:2021-0801  member: mid  expire 3600*24
     * val filteredDStream: DStream[JSONObject] = jsonObjDStream.filter {
     * jsonObj => {
     * // 获取登录日期
     * val dt: String = jsonObj.getString("dt")
     * // 获取设备ID
     * val mid: String = jsonObj.getJSONObject("common").getString("mid")
     * // 拼接Redis中保存的key
     * var duaKey = "dau:" + dt
     * // 获取Jedis客户端连接Redis
     * val jedis: Jedis = MyRedisUtil.getJedisClient()
     *
     * // 添加key
     * val isFirst: lang.Long = jedis.sadd(dauKey, mid)
     * // key是否有过期时间, 没有则设置key的过期时间
     * if (jedis.ttl(dauKey) < 0) {
     *jedis.expire(dauKey, 3600 * 24)
     * }
     * // 关闭连接
     *jedis.close()
     * // 从redis中判断当前设备是否登录过
     * if (isFirst == 1L) {
     * // 说明是第一次登录
     * true
     * } else {
     * // 说明已经登录过了
     * false
     * }
     * }
     * } */

    // 通过Redis 对采集到的启动日志进行去重操作 方案2  以分区为单位对数据进行处理, 每一个分区获取一次Redis的连接
    // redis 类型 Set   key dau:2021-0801  member: mid  expire 3600*24
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      // 以分区为单位进行处理
      jsonObjItr => {
        // 每一个分区获取一次Redis的连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        // 定义一个集合, 用于存放当前分区中第一次登录的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        // 对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          // 根据json对象获取属性
          val dt: String = jsonObj.getString("dt")
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          var duaKey = "dau:" + dt

          // 添加key
          val isFirst: lang.Long = jedis.sadd(duaKey, mid)
          // 判断是否设置时间
          if (jedis.ttl(duaKey) < 0) {
            jedis.expire(duaKey, 3600 * 24)
          }
          // 判断key是否第一次添加
          if (isFirst == 1L) {
            // 说明是第一次登录
            filteredList.append(jsonObj)
          }
        }
        // 关闭连接
        jedis.close()
        filteredList.toIterator
      }
    }
    // 输出每次登录的设备
    //filteredDStream.count().print()

    // 将数据批量的保存到ES中
    filteredDStream.foreachRDD {
      rdd => {
        // 以分区为单位 对数据进行处理
        rdd.foreachPartition {
          jsonObjItr => {
            // 当前这个分区中需要保存的ES日活数据
            val dauInfList: List[DauInfo] = jsonObjItr.map {
              // 每次处理的是一个JSON对象, 将JSON对象封装为样例类
              jsonObj => {
                val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
                DauInfo(
                  commonJSONObj.getString("mid"),
                  commonJSONObj.getString("uid"),
                  commonJSONObj.getString("ar"),
                  commonJSONObj.getString("ch"),
                  commonJSONObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", // 分钟前面没有转换, 默认00
                  jsonObj.getLong("ts")
                )
              }
            }.toList
            dauInfList
            // 将数据批量保存到ES中
            // 需要传入当前日期
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfList, "gmall_dau_info" + dt)
          }
        }
      }
    }

    // 开始执行流
    ssc.start()
    // 等待执行停止。 执行过程中发生的任何异常都会在该线程中抛出
    ssc.awaitTermination()
  }
}
