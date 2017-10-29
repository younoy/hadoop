package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by upsmart on 17-10-27.
 */


public class AdFirstMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] blog = value.toString().split("\\s+");
        if(blog.length >= 2){
            String blogId = blog[0].trim();
            String blogContent = blog[1].trim();

            //分词
            StringReader stringReader = new StringReader(blogContent);
            IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);
            Lexeme word = null;
            while((word = ikSegmenter.next()) != null){
                String text = word.getLexemeText();
                //每篇博客的单词出现的次数
                context.write(new Text(text+"-"+blogId),new IntWritable(1));
            }
            //博客总量
            context.write(new Text("count"),new IntWritable(1));
        }
    }
}
