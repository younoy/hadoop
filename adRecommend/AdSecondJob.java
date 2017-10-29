package com.yy.hadoop.adRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by upsmart on 17-10-29.
 */
public class AdSecondJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        Job job = Job.getInstance(conf);

        job.setUser("hadoop");

        job.setJarByClass(AdSecondJob.class);
        job.setJobName("AdSecondJob");

        FileInputFormat.addInputPath(job,new Path("/data/weibo/output1"));
        FileOutputFormat.setOutputPath(job,new Path("/data/weibo/output2"));

        job.setMapperClass(AdSecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(AdsSecondReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setNumReduceTasks(4);
//        job.setPartitionerClass(AdFirstParition.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
