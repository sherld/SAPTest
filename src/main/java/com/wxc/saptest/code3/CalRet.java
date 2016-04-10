package com.wxc.saptest.code3;

// 封装词频统计结果

import java.util.HashMap;
import java.util.Map;

public class CalRet {
    private Map<String, Integer> map;
    
    public CalRet() {
        map = new HashMap<String, Integer>();
    }
    
    public void append(Map<String, Integer> data) {
        if(data == null)
            return;
        for(Map.Entry<String, Integer> entry: data.entrySet()) {
            String key = entry.getKey();
            int num = entry.getValue();
            if(map.containsKey(key)) {
                int sum = map.get(key) + num;
                map.put(key, sum);
            } else {
                map.put(key, num);
            }
        }
    }
    
    public Map<String, Integer> getRet() {
        return map;
    }
}
