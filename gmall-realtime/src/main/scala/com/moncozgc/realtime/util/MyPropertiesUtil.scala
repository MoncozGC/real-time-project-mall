package com.moncozgc.realtime.util

import java.util.Properties

/**
 * 配置文件加载
 *
 * Created by MoncozGC on 2021/7/28
 */
object MyPropertiesUtil {
  def load(propertiesName: String): Properties = {
    val properties: Properties = new Properties()
    // 加载指定的配置文件  从class加载配置文件
    properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName))
    properties
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }
}
