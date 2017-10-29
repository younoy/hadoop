package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by upsmart on 17-10-29.
 */
public class AdThirdReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer buff = new StringBuffer();

        for(Text value:values){
            buff.append(value.toString());
            buff.append("\t");
        }
        context.write(key,new Text(buff.toString()));
    }
}
