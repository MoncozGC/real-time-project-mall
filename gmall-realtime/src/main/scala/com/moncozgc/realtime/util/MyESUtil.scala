package com.moncozgc.realtime.util

import com.moncozgc.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * 操作ES的工具类
 *
 * Created by MoncozGC on 2021/7/26
 */
object MyESUtil {

  //声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null

  //提供获取Jest客户端的方法
  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      //创建Jest客户端工厂对象
      build()
    }
    jestFactory.getObject
  }

  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://192.168.153.140:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).build())
  }

  /**
   * 向ES中插入单条数据  方式1  将插入文档的数据以json的形式直接传递
   *
   */
  def putIndex1(): Unit = {
    //获取客户端连接
    val jestClient: JestClient = getJestClient()
    //定义执行的source
    var source: String =
      """
        |{
        |  "id":200,
        |  "name":"operation meigong river",
        |  "doubanScore":8.0,
        |  "actorList":[
        |	    {"id":3,"name":"zhang han yu"}
        |	  ]
        |}
      """.stripMargin
    //创建插入类 Index   Builder中的参数表示要插入到索引中的文档，底层会转换Json格式的字符串，所以也可以将文档封装为样例类对象
    // 不是直接构建的外部对象,而是通过构造者设计模式,通过内部类对象来创建对象 内部类对象就是Bulider
    /**
     * Builder设计模式的好处是我们可以随意组合类相同类型输入的参数，不仅避免了方法重载出错的问题，还不需要写过多的构造器。
     * 实现方法就是在User内部创建一个内部类，并且拥有和User一样的字段（属性），并且提供SET方法，最重要的是要提供一个能够返回User对象的方法（build），这样才能通过Builder来创建User对象。
     *
     */
    val index: Index = new Index.Builder(source)
      .index("movie_index_5")
      .`type`("movie")
      .id("1")
      .build()
    //通过客户端对象操作ES     execute参数为Action类型，Index是Action接口的实现类
    jestClient.execute(index)
    //关闭连接
    jestClient.close()
  }

  /**
   * 向ES中插入单条数据  方式2   将向插入的文档封装为一个样例类对象
   *
   */
  def putIndex2(): Unit = {
    val jestClient: JestClient = getJestClient()

    // actorList属性添加值
    val arrayList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]
    val hashMap: util.HashMap[String, Any] = new util.HashMap[String, Any]
    hashMap.put("id", 77)
    hashMap.put("name", "胡歌")
    arrayList.add(hashMap)

    // 使用样例类对象
    // actorList属性根据数据类型, 所以使用 List集合中Map类型的参数形式
    val movie = Movie(101, "琅琊榜", 9.0F, arrayList)

    val index: Index = new Index.Builder(movie)
      .index("movie_index_5").`type`("movie").id("2")
      .build()

    // 执行操作
    jestClient.execute(index)

    // 关闭链接
    jestClient.close()
  }

  /**
   * 向ES中批量插入数据
   * id中加入mid,保证幂等性
   *
   * @param dauInfList mid, 分区中需要保存的ES日活数据
   * @param indexName  索引名称
   */
  def bulkInsert(dauInfList: List[(String, DauInfo)], indexName: String): Unit = {
    if (dauInfList != null && dauInfList.size != 0) {
      // 获取客户d端
      val jestClient: JestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      // 遍历日活对象 加入mid,保证幂等性
      for ((id, dauInfo) <- dauInfList) {
        val index: Index = new Index.Builder(dauInfo)
          // TODO 功能(优化)4.2 保证幂等性，添加mid
          .index(indexName).id(id).`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }

      // 创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult: BulkResult = jestClient.execute(bulk)
      println("向ES中插入" + bulkResult.getItems.size() + "多少条")
      jestClient.close()
    }
  }

  /**
   * 根据ID查询ES中的数据
   *
   */
  def queryIndexById(): Unit = {
    val jestClient: JestClient = getJestClient

    val get = new Get.Builder("movie_index_5", "2").build()
    val result: DocumentResult = jestClient.execute(get)
    println(result.getJsonString)

    jestClient.close()
  }


  /**
   * 根据文档查询
   *
   */
  def queryIndexByCondition(): Unit = {
    val jestClient: JestClient = getJestClient()
    val query: String =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "琅琊榜"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "胡歌"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}""".stripMargin

    // 封装search
    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()

    // 执行操作
    val searchResult: SearchResult = jestClient.execute(search)
    // 只获取JSON字符串中的hits数据, 使用Java的Map集合封装内部的数据
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = searchResult.getHits(classOf[util.Map[String, Any]])
    // 要把java的List转换成JSON的List,  将数据转换成scala来进行处理
    import scala.collection.JavaConverters._

    val toList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(toList.mkString("\n"))

    jestClient.close()
  }

  /**
   * 根据指定查询条件，从ES中查询多个文档  方式2
   */
  def queryIndexByCondition2(): Unit = {
    val jestClient: JestClient = getJestClient()

    // 根据JSON对象各个参数声明对应的对象
    /**
     * {
     * "query": {
     * "bool": {
     * "must": [
     * {"match": {
     * "name": "天龙八部"
     * }}
     * ],
     * "filter": [
     * {"term": { "actorList.name.keyword": "李若彤"}}
     * ]
     * }
     * },
     * "from": 0,
     * "size": 20,
     * "sort": [
     * {
     * "doubanScore": {
     * "order": "desc"
     * }
     * }
     * ],
     * "highlight": {
     * "fields": {
     * "name": {}
     * }
     * }
     * }
     */

    // 全部输出 没有只过滤hits
    // println(query)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.must(new MatchQueryBuilder("name", "天龙八部"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", "李若彤"))
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(20)
    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    // 将JSON字符串中, bool字段的查询数据加入到search中进行查询
    val builder: SearchSourceBuilder = searchSourceBuilder.query(boolQueryBuilder)
    val query: String = builder.toString()
    // 全部输出
    //println(query)

    val search: Search = new Search.Builder(query).addIndex("movie_index_5").build()
    val result: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])

    import scala.collection.JavaConverters._
    val toList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    // 过滤hits部分输出
    println(toList.mkString("\n"))

    jestClient.close()
  }


  def main(args: Array[String]): Unit = {
    // 插入数据
    //putIndex2()
    // 查找数据
    //queryIndexByCondition()
    queryIndexByCondition2
  }
}

case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Any]])