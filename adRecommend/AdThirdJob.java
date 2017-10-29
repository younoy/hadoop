package com.yy.hadoop.adRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by upsmart on 17-10-29.
 */
public class AdThirdJob {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        Job job = Job.getInstance(conf);

        job.setUser("hadoop");

        job.setJarByClass(AdThirdJob.class);
        job.setJobName("AdThirdJob");

        FileInputFormat.addInputPath(job,new Path("/data/weibo/output1"));
        FileOutputFormat.setOutputPath(job,new Path("/data/weibo/output3"));

        job.setMapperClass(AdThirdMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AdThirdReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI("/data/weibo/output1/part-r-00003"));
        job.addCacheFile(new URI("/data/weibo/output2/part-r-00000"));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
