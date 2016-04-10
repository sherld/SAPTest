package com.wxc.saptest.code1;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

public class CalTask extends RecursiveTask<Map<String, Integer>>{
    private static final int LIMITLEN = 1024*1024; // 阈值设定，设置块处理最大byte长度
    private String fileName; // 文件名
    private long start; // 块起始位置
    private long end; // 块末尾位置
    
    public CalTask(String fileName, long start, long end) {
        this.fileName = fileName;
        this.start = start;
        this.end = end;
    }
    
    @Override
    protected Map<String, Integer> compute() {
        if(end - start < LIMITLEN) {
            // 获取块中词频，返回map
            return Tool.readFileDateSimple(fileName, start, end);
        } else {
            long mid = start + (end - start) / 2;
            CalTask left = new CalTask(fileName, start, mid);
            left.fork();
            CalTask right = new CalTask(fileName, mid, end);
            Map<String, Integer> retLeft = left.join();
            Map<String, Integer> retRight = right.compute();
            Map<String, Integer> ret = new HashMap<>(retLeft);
            // 合并内容
            for(Map.Entry<String, Integer> entry: retRight.entrySet()) {
                String key = entry.getKey();
                int num = entry.getValue();
                if(ret.containsKey(key)) {
                    int sum = ret.get(key) + num;
                    ret.put(key, sum);
                } else {
                    ret.put(key, num);
                }
            }
            return ret;
        }
    }
}
