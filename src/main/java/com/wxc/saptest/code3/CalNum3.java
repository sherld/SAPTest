package com.wxc.saptest.code3;

/*
 * 该类中calWordCount函数执行文本单词计数的功能
 * 使用FileChannel的map方法将文件采用内存映射方式读取文件内容
 * 该流程会依次将大文本文件分成几个小块，依次对每个小块再使用ForkJoinPool对其进行词频统计
 */

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

public class CalNum3 {
    public static Map<String, Integer> calWordCount(String fileName) throws Exception{
        long startTime = System.currentTimeMillis();
        
        RandomAccessFile raf = new RandomAccessFile(fileName, "r"); 
        FileChannel channel = raf.getChannel();
        ForkJoinPool pool = ForkJoinPool.commonPool();
        
        long fileLength = raf.length();
//        long channelLength = channel.size();
        MappedByteBuffer mb = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileLength);
        
        byte buff[] = new byte[512*1024*1024];
        CalRet ret = new CalRet(); // 封装数据结果
        
        int shift = 0; // 位移量，当对文件切分时若buff数组的末尾处于单词中间时，记录到前一个空格的距离
        while(mb.hasRemaining()) {
            int len = Math.min(mb.remaining(), buff.length - shift);
            // 获取文件数据
            mb.get(buff, shift, len);
            // 对最后一个文件块结尾添加空格，方便对后续词频统计操作
            if(len < buff.length - shift) {
                buff[len + shift] = ' ';
            }
            shift = 0;
            if(mb.remaining() != 0) {
                for(int i = buff.length-1; i >= 0; i--) {
                    char tmp = (char)buff[i];
                    if(tmp == ' ')
                        break;
                    shift++;
                }
            }
            CalTask3 task = new CalTask3(buff, 0, len-1-shift);
            ret.append(pool.invoke(task));
            for(int i = 0; i < shift; i++)
                buff[i] = buff[buff.length- shift + i];
        }
        
        raf.close();
        channel.close();
        pool.shutdown();

        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间" + (endTime - startTime) + "ms");
        
        return ret.getRet();
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("code3 start");
        String fileName = "content.txt";
        if(args.length > 0)
            fileName = args[0];
        System.out.println("文件名为:" + fileName);
        
        Map<String, Integer> map = calWordCount(fileName);
        
        // 结果数据输出
        try (FileWriter fw = new FileWriter("out3.txt"); BufferedWriter bw = new BufferedWriter(fw)) {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String word = entry.getKey();
                int num = entry.getValue();
                fw.write(word + '\t' + num + '\r' + '\n');
            }
        }
    }
}
