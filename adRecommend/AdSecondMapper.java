package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by upsmart on 17-10-27.
 */


public class AdSecondMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //first已经统计了TF,第二个就是统计DF
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String splitFileName = inputSplit.getPath().getName();

        if(splitFileName.contains("part-r-00003")){
            System.out.println(value.toString()+"----------"+value);
        }else{
            String[] lines = value.toString().split("\t");
            if(lines.length == 2){
                String[] ss = lines[0].split("-");
                context.write(new Text(ss[0]),new IntWritable(1));
            }
        }
    }
}
