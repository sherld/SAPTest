package com.wxc.saptest.code2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CalTask2 extends RecursiveTask<Map<String, Integer>>{
    private static final int LIMITLEN = 1024*1024; // 阈值设定，设置块处理最大byte长度
    private byte[] buff; // 文件数据
    private int start; // 块起始位置
    private int end; // 块末尾位置
    
    public CalTask2(byte[] buff, int start, int end) {
        this.buff = buff;
        this.start = start;
        this.end = end;
    }
    
    @Override
    protected Map<String, Integer> compute() {
        if(end - start < LIMITLEN) {
            return parseBuffer(start, end);
        } else {
            int mid = start + (end - start) / 2;
            CalTask2 left = new CalTask2(buff, start, mid);
            left.fork();
            CalTask2 right = new CalTask2(buff, mid, end);
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
    
    // 对文件块进行词频统计操作
    public Map<String, Integer> parseBuffer(int start, int end) {
        Map<String, Integer> map = new HashMap<>();
        
        int indexStart = start;
        int indexEnd = end;
        int fileLen = buff.length;
        // 除了第一块以外，如果起始位置指向一个单词中间，则移动到下一个单词开始处
        if (start != 0) {
            for (; indexStart < fileLen; indexStart++) {
                char c = (char) buff[indexStart];
                if (c == ' ') {
                    indexStart++;
                    break;
                }
            }
        }
        if(indexStart > end) {
            System.out.println("超过边界1");
            return map;
        }
        // 若结尾位置指向一个单词中间，则移动到该单词末尾
        for(; indexEnd < fileLen; indexEnd++) {
            char c = (char) buff[indexEnd];
            if(c == ' ') {
                indexEnd--;
                break;
            }
        }
        if(indexEnd == fileLen)
            indexEnd = fileLen - 1;
        
        if(indexStart > indexEnd) {
            System.out.println("超过边界2");
            return map;
        }
        int dataLen = indexEnd - indexStart + 1;

        String wordStr = new String(buff, indexStart, dataLen);
        String[] words = wordStr.split(" ");
        
//        for(String word : words) {
//            if(map.containsKey(word)) {
//                map.put(word, map.get(word) + 1);
//            } else {
//                map.put(word, 1);
//            }
//        }
        
        // 进行词频统计
        map = Arrays.stream(words).collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(e -> 1)));
        
        return map;
    }
}
