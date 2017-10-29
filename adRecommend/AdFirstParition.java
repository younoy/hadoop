package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by upsmart on 17-10-28.
 */
public class AdFirstParition extends HashPartitioner<Text,IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if(key.toString().equals("count")){
            return 3;
        }else{
            return super.getPartition(key,value,numReduceTasks-1);
        }
    }
}
