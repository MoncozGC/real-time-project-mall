# 实时项目-SparkStreaming

## 整体构造

```txt
├──gmall-logger
	├──接收"日志发送器数据"，然后将日志根据分类发送到对应的Kafka主题中
	├──启动日志：moncozgc-start
	├──时间日志：moncozgc-event

├──gmall-realtime
	├──app包
	├────DauApp类: 实现日活业务功能
	├────────jsonDStream: 打印启动日志的全部日志
	├────────jsonObjDStream: 获取时间戳, 转换为dt和hr再插入到json中
	├────────filteredDStream: 通过Redis 对采集到的启动日志进行去重操作 方案1  采集周期中的每条数据都要获取一次Redis连接, 连接过于频繁
	├────────filteredDStream: 方案2  以分区为单位对数据进行处理, 每一个分区获取一次Redis的连接
	├────────recordDStream: 记录偏移量(从redis中，redis中没有则重新获取)
	├────────offsetDStream: 得到本批次中处理数据的分区对应的偏移量起始及结束位置
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
```



## DauApp类中实现功能
1. 功能 1: SparkStreaming 消费 kafka 数据 ✔
2. 功能 2: 利用 Redis 过滤当日已经计入的日活设备 ✔
3. 功能 3：把每批次新增的当日日活信息保存到 ES 中 ✔
4. 功能 4 优化：保证数据的精准一次性消费 ✔
   1. 手动提交偏移量，将偏移量保存到Redis中
   2. 保证幂等性，ES数据去重

5. 功能 5 数据展示



## util中实现的功能

1. MyESUtil类: 写入数据到ES、查询ES数据工具类
2. MyKafkaUtil类: 读取Kafka的工具类
3. MyPropertiesUtil类: 配置文件加载工具类
4. MyRedisUtil类: 获取Jedis客户端的工具类