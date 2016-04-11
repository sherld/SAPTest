package com.wxc.saptest.code1;

/*
 * 该类中main函数执行文本单词计数的功能
 * 采用RandomAccessFile类进行文件指定位置内容的读写
 * 使用ForkJoinPool线程池将文件分块并行处理，每块内容使用RandomAccessFile来读取
 */

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

public class CalNum {
    public static void main(String[] args) throws Exception {
        System.out.println("code1 start");
        String fileName = "content.txt";
        if(args.length > 0)
            fileName = args[0];
        System.out.println("文件名为:" + fileName);
        long fileLength = 0;
        
        long startTime = System.currentTimeMillis();
        
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r");) {
            fileLength = raf.length();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // 调用线程池，并行处理文件内容
        ForkJoinPool pool = ForkJoinPool.commonPool();
        ForkJoinTask<Map<String, Integer>> fjt = pool.submit(new CalTask(fileName, 0, fileLength));
        Map<String, Integer> map = fjt.get();
        
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间" + (endTime - startTime) + "ms");
        
        // 结果数据输出
        try(
                FileWriter fw = new FileWriter("out.txt");
                BufferedWriter bw = new BufferedWriter(fw))
        {
            for(Map.Entry<String, Integer> entry : map.entrySet()) {
                String word = entry.getKey();
                int num = entry.getValue();
                fw.write(word + '\t' + num + '\r' + '\n');
            }
        }
    }
}
