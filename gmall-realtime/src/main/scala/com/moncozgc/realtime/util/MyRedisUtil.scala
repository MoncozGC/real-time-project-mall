package com.moncozgc.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util.Properties

/**
 * 获取Jedis客户端的工具类
 *
 * Created by MoncozGC on 2021/7/28
 */
object MyRedisUtil {
  // 定义一个连接池对象
  var jedisPool: JedisPool = _

  /**
   * 获取Jedis客户端
   *
   * @return
   */
  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  /**
   * 创建JedisPool连接池对象
   */
  def build(): Unit = {
    val config: Properties = MyPropertiesUtil.load("config.properties")
    val host: String = config.getProperty("redis.host")
    val port: String = config.getProperty("redis.port")
    val password: String = config.getProperty("redis.password")
    val db: String = config.getProperty("redis.database")
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    // 写入IP、port、timeout、pwd
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt, 1000, password, db.toInt)
  }

  def main(args: Array[String]): Unit = {
    val jedisClient: Jedis = getJedisClient
    println(jedisClient.ping())
    jedisClient.close()
  }

}
