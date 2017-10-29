package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by upsmart on 17-10-27.
 */
public class AdFirstReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int eachWordCount = 0;

        for(IntWritable count:values){
            eachWordCount = eachWordCount + Integer.parseInt(count.toString());
        }
        if(key.toString().equals("count")){
            System.out.println(key.toString()+"------"+eachWordCount);
        }
        context.write(key,new IntWritable(eachWordCount));
    }
}
