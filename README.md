# 实时项目-SparkStreaming

- gmall-logger
   - 接收"日志发送器数据"，然后将日志根据分类发送到对应的Kafka主题中
   - 启动日志：moncozgc-start
   - 时间日志：moncozgc-event
   
- gmall-realtime
   -  util-MyESUtil: 写入数据到ES、查询ES数据