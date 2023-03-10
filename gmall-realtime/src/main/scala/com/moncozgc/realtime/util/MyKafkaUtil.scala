package com.moncozgc.realtime.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties


/**
 * 读取Kafka的工具类
 *
 * Created by MoncozGC on 2021/7/28
 */
object MyKafkaUtil {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list: String = properties.getProperty("kafka.broker.list")

  // 配置文件写法
  //  val kafkaParam: mutable.Map[String, io.Serializable] = collection.mutable.Map(
  //    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
  //    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
  //    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
  //    ConsumerConfig.GROUP_ID_CONFIG -> "gmall",
  //    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
  //    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
  //  )

  // kafka 消费者配置
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall2020_group",
    //latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
   * 接收topic和context, 使用默认的消费者组进行消费
   *
   * @param topic
   * @param ssc
   * @return
   */
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    // 创建 DStream，返回接收到的输入数据
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  /**
   * 接收topic和context, 使用指定的消费者组ID对topic进行消费
   *
   * @param topic
   * @param ssc
   * @param groupId
   * @return
   */
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String):
  InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }

  /**
   * 接收topic和context, 使用指定的消费者组ID对topic进行消费
   * 以及指定主题分区偏移量, 会从指定的偏移量处开始消费
   *
   * @param topic
   * @param ssc
   * @param offsets
   * @param groupId
   * @return
   */
  def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String)
  : InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
    dStream
  }

}
