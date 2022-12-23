# 实时项目-SparkStreaming

## 服务器

| 主机名 | ip              | 用途                  |
| ------ | --------------- | --------------------- |
| pro01  | 192.168.153.140 | Redis/ES/Kibana/Nginx |
|        |                 |                       |
|        |                 |                       |

```txt
服务器环境以及框架版本
Centos7 3台
JDK1.8
MySQL 5.6
Hadoop 3.1.3
Zookeeper 3.5.7
Kafka2.4.1
Hbase2.2.4
Phoenix 5.1.0
Redis 3.2.5
Scala2.12
Spark 3.0.0


Nginx1.12.2
Canal1.1.4
Maxwel11.25.0
ElasticSearch 6.6.0
ClickHouse 20.4.5.36
SpringBoot
Datay
```

## 服务启动

1. Nginx 负载均衡
   - 启动: 192.168.153.140 /data/nginx/sbin/nginx
   - 停止: /data/nginx/sbin/nginx -s stop
2. gmall-logger-0.0.1-SNAPSHOT.jar `接受日志服务转发至Kafka`
   - 192.168.153.100: java -jar /export/gmall_SparkStreaming/gmall-logger-RELEASE.jar
   - 192.168.153.101: java -jar /export/gmall_SparkStreaming/gmall-logger-RELEASE.jar
   - 192.168.153.102: java -jar /export/gmall_SparkStreaming/gmall-logger-RELEASE.jar
3. kafka消费者[start、event]topic
   - 先启动zk: 三台都需要启动启动
     - 启动: /export/servers/zookeeper-3.4.5-cdh5.14.0/bin/zkServer.sh start

     - 状态: /export/servers/zookeeper-3.4.5-cdh5.14.0/bin/zkServer.sh status
       - 或直接执行一键脚本/export/servers/zookeeper-3.4.5-cdh5.14.0/startZKServers.sh 三台都会执行
   - 再启动kafka: 三台都需要启动
     - 启动: nohup /export/servers/kafka_2.11-1.0.0/bin/kafka-server-start.sh /export/servers/kafka_2.11-1.0.0/config/server.properties >> /export/servers/kafka_2.11-1.0.0/kafkaserver.log  2>&1 &
     - 启动消费者: 192.168.153.100执行, /export/servers/kafka_2.11-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server 192.168.153.100:9092 --topic moncozgc-start
4. gmall2020-mock-log-2020-05-10.jar `发送日志`
   
   - 192.168.153.100执行, java -jar /export/gmall_SparkStreaming/rollLog/gmall2020-mock-log-2020-05-10.jar
5. 启动ES
   - 192.168.153.140 切换到elasticsearch用户启动 sh elasticsearch
   - /data/elasticsearch-6.6.0/bin/elasticsearch -d
6. 启动Kibanan
   - 192.168.153.100 启动Kibanan
   - nohup /data/kibana-6.6.0-linux-x86_64/bin/kibana &

## 整体构造

```txt
├──gmall-logger工程，上报数据到kafka；启动端口：8989
	├──接收"日志发送器数据"，然后将日志根据分类发送到对应的Kafka主题中
	├──启动日志：moncozgc-start
	├──时间日志：moncozgc-event
	├──Controller
	├────LoggerController: 接收模拟器生成的数据，并对数据进行处理，分类保存数据到Kafka

├──gmall-realtime工程，处理数据；无启动端口
	├──app包
	├────DauApp类: 实现日活业务功能
	├────────jsonDStream: 打印启动日志的全部日志
	├────────jsonObjDStream: 获取时间戳, 转换为dt和hr再插入到json中
	├────────filteredDStream: 通过Redis 对采集到的启动日志进行去重操作 方案1  采集周期中的每条数据都要获取一次Redis连接, 连接过于频繁
	├────────filteredDStream: 方案2  以分区为单位对数据进行处理, 每一个分区获取一次Redis的连接
	├────────recordDStream: 记录偏移量(从redis中，redis中没有则重新获取)
	├────────offsetDStream: 得到本批次中处理数据的分区对应的偏移量起始及结束位置
	├──bean包
	├────DauInfo: 封装日活的样例类
	├──util包
	├────MyESUtil类: 写入数据到ES、查询ES数据工具类
	├────────putIndex1(): 插入单条数据, 将插入文档的数据以json的形式直接传递
	├────────putIndex2(): 插入单条数据, 将向插入的文档封装为一个样例类对象
	├────────queryIndexById(): 根据ID查询ES中的数据
	├────────queryIndexByCondition(): 根据文档查询, 使用文档模式
	├────────queryIndexByCondition2(): 根据指定查询条件, 从ES中查询多个文档, 使用SearchSourceBuilder用于构建查询的json格式字符串
	├────MyKafkaUtil类: 读取Kafka的工具类
	├────────getKafkaStream(): 接收topic和context, 使用默认的消费者组进行消费, 以及指定主题分区偏移量, 会从指定的偏移量处开始消费
	├────MyPropertiesUtil类: 配置文件加载工具类
	├────────load(): 加载配置文件
	├────MyRedisUtil类: 获取Jedis客户端的工具类
	├────────getJedisClient(): 获取Jedis客户端
	├────────build(): 创建JedisPool连接池对象
	
├──gmall-publisher工程，数据接口；启动端口：8070
	├────controller包
	├────────PublisherController类: getDauTotal，获取某天的日活数据；getDauHour，查询某天某时段的日活数
	├────service包
	├────────impl包
	├───────────ESServiceImpl类：实现ElasticSearch数据接口，getDauTotal，获取某天的日活数据；getDauHour，查询某天某时段的日活数
	├────────ESService类:查询ElasticSearch数据接口
	├────resource 配置文件
	├────────application.properties：配置Spring端口；ES服务器地址

├──dw-chart工程，获取接口的数据前端展示项目；启动端口：8090
```



## 实现的功能

- **gmall-logger**

1. 接收"日志发送器数据"，然后将日志根据分类发送到对应的Kafka主题中

- **gmall-realtime**

1. 功能 1: SparkStreaming 消费 kafka 数据 ✔
2. 功能 2: 利用 Redis 过滤当日已经计入的日活设备 ✔
3. 功能 3：把每批次新增的当日日活信息保存到 ES 中 ✔
4. 功能 4 优化：保证数据的精准一次性消费 ✔
   1. 手动提交偏移量，将偏移量保存到Redis中
   2. 保证幂等性，ES数据去重

- **gmall-publisher**

5. 功能 5 从 ES 中查询出数据，发布成数据接口，可视化工程进行调用
   1. 使用Spring项目，从ES中获取每天的日活数以及每个时段的日活数
   2. 导入前端项目(**du-chart**)，获取Spring提供的接口，展示数据



## 类的作用

- **gmall-logger - controller**

1. LoggerController类：接收模拟器生成的数据，并对数据进行处理

- **gmall-realtime - app**

1. DauApp类：日活业务

- **gmall-realtime - util**

1. MyESUtil类: 写入数据到ES、查询ES数据工具类
2. MyKafkaUtil类: 读取Kafka的工具类
3. MyPropertiesUtil类: 配置文件加载工具类
4. MyRedisUtil类: 获取Jedis客户端的工具类
5. OffSetManagerUtil类：维护偏移量的工具类
6. getDate类：获取当前时间的前几天时间

- **gmall-realtime - bean**

1. DauInfo类：封装日活的样例类

- **gmall-publisher - controller**

1. PublisherController类: 通过接口获取数据以及拦截请求进行页面展示

- **gmall-publisher - service**

1. ESService类：查询ElasticSearch数据接口
2. impl包-ESServiceImpl类：实现ElasticSearch数据接口，提供查询某天的日活数以及某天某时段的日活数

- **dw-chart**

1. 前端工程展示数据项目
2. 