package com.moncozgc.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * 维护偏移量的工具类
 *
 * Created by MoncozGC on 2021/8/3
 */
object OffsetManagerUtil {

  /**
   * 从Redis中获取到topic的偏移量
   *
   * @param topic   主题
   * @param groupId 消费者组id
   * @return
   */
  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    // 获取Jedis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient

    // 拼接key Hash, key: offset:topic:groupId   filed: partition  key:offset
    var offsetKey: String = "offset:" + topic + ":" + groupId

    /**
     * 根据 key 从 Redis 中获取数据
     *
     * util.Map[String, String]
     * String: 分区
     * String: 分区中的偏移量
     */
    var offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

    // 关闭Jedis客户端
    jedis.close()

    // 将offsetMap java类型转换成scala来处理
    import scala.collection.JavaConverters._
    val toMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：" + partition + ":" + offset)
        // 需要返回 Map[TopicPartition, Long], 将 Redis 中保存的分区对应的偏移量进行封装
        // 放如 topic  partition offset
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
      // toMap 转换为不可变的集合
    }.toMap
    toMap

  }

  /**
   * 将偏移量保存到Redis中
   *
   * @param topic        主题
   * @param groupId      消费者组id
   * @param offsetRanges 偏移量信息, 哪一个主题、分区、偏移量范围
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 拼接Redis key
    var offsetKey: String = "offset:" + topic + ":" + groupId

    // 定义java的map集合, 用于存放每个分区对应的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    // 对应OffsetRanges进行遍历, 将数据封装offsetMap
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      // 添加到Map集合中
      offsetMap.put(partitionId.toString, untilOffset.toString)
      println("保存分区: " + partitionId + ":" + fromOffset + "------>" + untilOffset)
    }
    // 获取Jedis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient
    // 保存数据到Redis中
    jedis.hmset(offsetKey, offsetMap)
    // 关闭Jedis客户端
    jedis.close()
  }
}
