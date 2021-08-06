package com.mongcozgc.gmall.publisher.controller;

import com.mongcozgc.gmall.publisher.service.ESService;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by MoncozGC on 2021/8/4
 */
@RestController
public class PublisherController {

    @Autowired
    ESService esService;

    /**
     * 获取当天新增的日活和设备
     * 响应地址:
     * http://publisher:8070/realtime-total?date=2021-08-03
     *
     * 处理的数据格式:
     * [{"id":"dau","name":"新增日活","value":1200},
     * {"id":"new_mid","name":"新增设备","value":233} ]
     *
     * @param dt 时间
     * @return
     */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String dt) {

        // 利用List来转载数据
        List<Map<String, Object>> rsList = new ArrayList<Map<String, Object>>();

        // 获取日活数据
        Map<String, Object> dauMap = new HashMap<String, Object>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        // 获取当天的日活数
        Long dauTotal = esService.getDauTotal(dt);
        // 如果当天日活数据未空, 则未0
        if (dauTotal == null) {
            dauMap.put("value", 0L);
        } else {
            dauMap.put("value", dauTotal);
        }
        // 添加到List中
        rsList.add(dauMap);

        // 获取设备数据
        Map<String, Object> midMap = new HashMap<String, Object>();
        midMap.put("id", "dau");
        midMap.put("name", "新增日活");
        // 获取当天的设备数
        midMap.put("value", "777");
        // 添加到List中
        rsList.add(midMap);


        return rsList;
    }

    /**
     * 响应地址:
     * http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
     *
     * 处理的数据格式:
     * {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
     * "today":{"12":38,"13":1233,"17":123,"19":688 }}
     *
     * @param id 类型
     * @param dt 时间
     * @return
     */
    @RequestMapping("/realtime-hour")
    public Object realtimeHour(String id, @RequestParam("date") String dt) {
        Map<String, Map<String, Long>> resultMap = new HashMap<String, Map<String, Long>>();

        // 获取今天的日期
        Map<String, Long> dtMap = esService.getDauHour(dt);
        resultMap.put("today", dtMap);

        // 获取昨天的日期
        String yd = getYd(dt);
        Map<String, Long> ytMap = esService.getDauHour(yd);
        resultMap.put("yesterday", ytMap);

        return resultMap;
    }

    /**
     * 根据出入的日期转换其他的时间日期
     *
     * @param dt 日期
     * @return
     */
    private String getYd(String dt) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(dt);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }
}
