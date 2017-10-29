package com.yy.hadoop.adRecommend;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by upsmart on 17-10-27.
 */


public class AdThirdMapper extends Mapper<LongWritable,Text,Text,Text> {

    Map<String,Integer> nmap = null;
    Map<String,Integer> dfmap = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //读HDFS文件系统上的两个文件获取N和DF
        if(nmap==null && dfmap==null){
            URI[] cacheFile = context.getCacheFiles();
            if(cacheFile != null){
                for(int i=0;i<cacheFile.length;i++){
                    String path_str = cacheFile[i].getPath();
                    if(path_str.endsWith("part-r-00003")){
                        Path path = new Path(path_str);
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        FSDataInputStream fsInput = fs.open(path);
                        BufferedReader bf = new BufferedReader(new InputStreamReader(fsInput,"UTF-8"));
                        String line = null;
                        while ((line=bf.readLine()) != null){
                            if(line.contains("count")){
                                nmap = new HashMap<>();
                                String[] arr = line.split("\t");
                                nmap.put(arr[0],Integer.parseInt(arr[1]));
                            }
                        }
                        bf.close();
                    }else if(path_str.contains("part-r-00000")){
                        Path path = new Path(path_str);
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        FSDataInputStream fsInput = fs.open(path);
                        BufferedReader bf = new BufferedReader(new InputStreamReader(fsInput,"UTF-8"));
                        String line = null;
                        dfmap = new HashMap<>();
                        while ((line=bf.readLine()) != null){
                            String[] arr = line.split("\t");
                            dfmap.put(arr[0],Integer.parseInt(arr[1]));
                        }
                        bf.close();
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //first,second已经统计了TF,DF,现在就要读进来
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        if(fileSplit.getPath().getName().endsWith("part-r-00003")){
            System.out.println(value.toString());
        }else{
            String str = value.toString().trim();
            String[] arr = str.split("\t");
            int tf = Integer.parseInt(arr[1]);
            String[] blog = arr[0].split("-");
            String word = blog[0];
            String blogId = blog[1];
            int df = dfmap.get(word);
            int n = nmap.get("count");
            double W = tf*Math.log(n/df);
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setMaximumFractionDigits(5);
            context.write(new Text(blogId),new Text(word +":"+numberFormat.format(W)));
        }
    }
}
