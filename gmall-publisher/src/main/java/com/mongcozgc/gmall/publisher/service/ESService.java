package com.mongcozgc.gmall.publisher.service;

import java.util.Map;

/**
 * 查询ElasticSearch数据接口
 *
 * Created by MoncozGC on 2021/8/4
 */
public interface ESService {
    /**
     * 查询某天的日活数
     *
     * @param date 天数
     * @return 日活数
     */
    public Long getDauTotal(String date);

    /**
     * 查询某天某时段的日活数
     *
     * @param date
     * @return <小时, 当前小时的活跃数>
     */
    public Map<String, Long> getDauHour(String date);
}
