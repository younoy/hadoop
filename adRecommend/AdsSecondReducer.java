package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by upsmart on 17-10-29.
 */
public class AdsSecondReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int df = 0;
        for(IntWritable num:values){
            df = df + Integer.parseInt(num.toString());
        }
        context.write(key,new IntWritable(df));
    }
}
