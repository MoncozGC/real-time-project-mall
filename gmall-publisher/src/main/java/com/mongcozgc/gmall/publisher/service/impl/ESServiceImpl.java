package com.mongcozgc.gmall.publisher.service.impl;

import com.mongcozgc.gmall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实现ElasticSearch数据接口
 *
 * Created by MoncozGC on 2021/8/4
 */
@Service
public class ESServiceImpl implements ESService {

    //将ES的客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;

    /**
     * 查询某天的日活数
     *
     * 索引格式
     * GET /gmall_dau_info_2021-08-03-query/_search
     * {
     * "query": {
     * "match_all": {
     * }}}
     *
     * 数据格式:
     * "total" : 50,
     *
     * @param date 天数
     * @return 日活数
     */
    @Override
    public Long getDauTotal(String date) {
        // 利用代码的方式获取文档信息
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // query 中的 MatchAll
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        // 转换成字符串
        String query = searchSourceBuilder.toString();
        // 索引名
        String indexName = "gmall_dau_info_" + date + "-query";
        Search search = new Search.Builder(query).addIndex(indexName).build();

        // 获取日活数
        Long total = 0L;
        try {
            SearchResult result = jestClient.execute(search);
            // 获取ES文档中 total参数值
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES数据失败");
        }
        return total;
    }

    /**
     * 查询某天某时段的日活数
     *
     * 查询语句:
     * GET /gmall_dau_info_2021-08-03-query/_search
     * {
     * "aggs": {
     * "groupBy_hr": {
     * "terms": {
     * "field": "hr",
     * "size": 24
     * }}}}
     *
     * 数据格式:
     * "aggregations" : {
     * "grouBy_hr" : {
     * "doc_count_error_upper_bound" : 0,
     * "sum_other_doc_count" : 0,
     * "buckets" : [
     * {
     * "key" : "23",
     * "doc_count" : 50
     * }]}}
     *
     * @param date
     * @return <小时, 当前小时的活跃数>
     */
    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> hourMap = new HashMap<String, Long>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 编写查询的语句
        TermsAggregationBuilder termsAggregationBuilder =
                new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        // 查询语句
        String query = searchSourceBuilder.toString();
        // 索引名
        String indexName = "gmall_dau_info_" + date + "-query";
        // 获取查询语句以及对应索引名
        Search search = new Search.Builder(query).addIndex(indexName).build();
        try {
            // 执行查询语句
            SearchResult result = jestClient.execute(search);
            // 使用分组获取数据, 因为上面也有分组
            TermsAggregation termsAgg = result.getAggregations().getTermsAggregation("groupBy_hr");
            if (termsAgg != null) {
                // Entry 存储的数据就是 key和doc_count
                List<TermsAggregation.Entry> buckets = termsAgg.getBuckets();
                // 进行遍历获取数据, 添加到Map中
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(), bucket.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES数据失败");
        }
        return hourMap;
    }
}
