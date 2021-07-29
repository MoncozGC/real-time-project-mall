package com.moncozgc.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * 获取Jedis客户端的工具类
 *
 * Created by MoncozGC on 2021/7/28
 */
object MyRedisUtil {
  var jedisPool: JedisPool = null

  def getJedisClient(): Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  def build(): Unit = {
    val config = MyPropertiesUtil.load("config.properties")
    val host = config.getProperty("redis.host")
    val port = config.getProperty("redis.port")
    val password = config.getProperty("redis.password")
    val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
    // 写入IP、port、timeout、pwd
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt, 1000, password)
  }

  def main(args: Array[String]): Unit = {
    val jedisClient = getJedisClient()
    println(jedisClient.ping())
    jedisClient.close()
  }

}
