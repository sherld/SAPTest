package com.wxc.saptest.code2;

/*
 * 该类中calWordCount函数执行文本单词计数的功能
 * 使用FileChannel的map方法将文件采用内存映射方式读取文件内容
 * 该流程一次性将文件数据读入内存数组中，并使用ForkJoinPool对数组分块进行词频统计
 */

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class CalNum2 {
    public static Map<String, Integer> calWordCount(String fileName) throws Exception{
        long startTime = System.currentTimeMillis();
        
        RandomAccessFile raf = new RandomAccessFile(fileName, "r"); 
        FileChannel channel = raf.getChannel();
        ForkJoinPool pool = ForkJoinPool.commonPool();
        
        long fileLength = raf.length();
        MappedByteBuffer mb = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength);
        
        byte buff[] = new byte[(int)fileLength];
        
        // 获取文件数据
        mb.get(buff, 0, buff.length);
        
        // 进行词频统计操作
        CalTask2 task = new CalTask2(buff, 0, buff.length-1);
        ForkJoinTask<Map<String, Integer>> fjt = pool.submit(task);
        Map<String, Integer> map = fjt.get();
        
        raf.close();
        channel.close();
        pool.shutdown();

        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间" + (endTime - startTime) + "ms");
        
        return map;
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("code2 start");
        String fileName = "content.txt";
        if(args.length > 0)
            fileName = args[0];
        System.out.println("文件名为:" + fileName);
        
        Map<String, Integer> map = calWordCount(fileName);
        
        // 结果数据输出
        try (FileWriter fw = new FileWriter("out2.txt"); BufferedWriter bw = new BufferedWriter(fw)) {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String word = entry.getKey();
                int num = entry.getValue();
                fw.write(word + '\t' + num + '\r' + '\n');
            }
        }
    }
}
