package com.wxc.saptest.code1;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Tool {
    // 读取文件中指定块内容，并统计词频
    public static Map<String, Integer> readFileDateSimple(String fileName, long start, long end) {
        Map<String, Integer> map = new HashMap<>();
        
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            long indexStart = start;
            long indexEnd = end;
            long fileLen = raf.length();
            // 除了第一块以外，如果起始位置指向一个单词，则移动到下一个单词开始处
            if (start != 0) {
                for (; indexStart < fileLen; indexStart++) {
                    raf.seek(indexStart);
                    char c = (char) raf.read();
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
                raf.seek(indexEnd);
                char c = (char) raf.read();
                if(c == ' ') {
                    indexEnd--;
                    break;
                }
            }
            if(indexStart > indexEnd) {
                System.out.println("超过边界2");
                return map;
            }
                
            if(indexEnd == fileLen)
                indexEnd = fileLen - 1;
            int arrLen = (int)(indexEnd - indexStart + 1);
            byte[] array = new byte[arrLen];
            raf.seek(indexStart);
            raf.read(array);

            String wordStr = new String(array);
            String[] words = wordStr.split(" ");

            // 进行词频统计
//            for(String word : words) {
//                if(map.containsKey(word)) {
//                    map.put(word, map.get(word) + 1);
//                } else {
//                    map.put(word, 1);
//                }
//            }
            map = Arrays.stream(words).parallel().collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(e -> 1)));
            return map;

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return map;
    }
    
    public static void main(String[] args) {
        String fileName = "content.txt";
        Map<String, Integer> map = readFileDateSimple(fileName, 0, 20);
        System.out.println(map);
    }
}
