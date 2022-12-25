package com.moncozgc.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.moncozgc.realtime.bean.DauInfo
import com.moncozgc.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
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
    val groupId: String = "gmall-dau"

    // TODO 功能(优化)4.1.1 从Redis中获取Kafka的偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    // 记录偏移量
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.nonEmpty) {
      // 如果Redis中存在当前消费者组对该主题的偏移量信息, 那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果Redis中没有当前消费者组对该主题的偏移量信息, 那么还是按照配置, 从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // TODO 功能(优化)4.1.2 获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    // 从 Kafka 中读取数据之后，直接就获取了偏移量的位置，因为 KafkaRDD 可以转换为HasOffsetRanges，会自动记录位置
    var offsetRanges = Array.empty[OffsetRange]
    // 将RDD的值取出来不会做任何改变, 首选transform算子, 还有foreachRDD但是现在还不用提交
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd: RDD[ConsumerRecord[String, String]] => {
        // 因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        // 转换成父类, 通过父类HasOffsetRanges定义的方法子类rdd可以自己访问, 通过调用父类使用offsetRanges
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 还是返回这个RDD
        rdd
      }
    }

    // TODO 功能(未优化3前)1 通过SparkStreaming消费Kafka中的数据, 现使用从Redis中获取Kafka的偏移量方式
    // val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    // 生成的启动日志, 只获取value部分
    val jsonDStream: DStream[String] = offsetDStream.map(_.value())
    // 打印未加时间的字符串
    //jsonDStream.print(1000)

    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
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

    // TODO 功能(新增)2 方案1  采集周期中的每条数据都要获取一次Redis连接, 连接过于频繁
    /** // 通过Redis 对采集到的启动日志进行去重操作
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
     * jedis.expire(dauKey, 3600 * 24)
     * }
     * // 关闭连接
     * jedis.close()
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

    // TODO 功能(新增)2 方案2  以分区为单位对数据进行处理, 每一个分区获取一次Redis的连接
    // 通过Redis 对采集到的启动日志进行去重操作
    // redis 类型 Set   key dau:2021-0801  member: mid  expire 3600*24
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      // 以分区为单位进行处理
      jsonObjItr => {
        // 每一个分区获取一次Redis的连接
        val jedis: Jedis = MyRedisUtil.getJedisClient
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

    // TODO 功能(新增)3 将数据批量的保存到ES中
    filteredDStream.foreachRDD {
      rdd => {
        // 以分区为单位 对数据进行处理
        rdd.foreachPartition {
          jsonObjItr => {
            // TODO 功能(优化)4.2 保证幂等性，ES数据去重
            // 通过传入mid保证幂等性 以及 当前这个分区中需要保存的ES日活数据
            val dauInfoList: List[(String, DauInfo)] = jsonObjItr.map {
              // 每次处理的是一个JSON对象, 将JSON对象封装为样例类
              jsonObj => {
                val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
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
                // 将mid写入到es中代替es文档中的id
                (dauInfo.mid, dauInfo)
              }
            }.toList
            // 将数据批量保存到ES中
            // 需要传入当前日期
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            // 指定时间
            //val dt: String = "2021-08-04"
            MyESUtil.bulkInsert(dauInfoList, "gmall_dau_info_" + dt)
          }
        }
        // TODO 功能(优化)4.1.3 提交偏移量到Redis中
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }

    // 开始执行流
    ssc.start()
    // 等待执行停止。 执行过程中发生的任何异常都会在该线程中抛出
    ssc.awaitTermination()
  }
}
