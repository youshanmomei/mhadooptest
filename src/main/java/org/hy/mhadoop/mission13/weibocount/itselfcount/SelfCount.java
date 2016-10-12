package org.hy.mhadoop.mission13.weibocount.itselfcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by andy on 2016/10/11.
 */
public class SelfCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path myPath = new Path("hdfs://hy:9000/mission/12-mysql/out2");
        FileSystem hdfs = myPath.getFileSystem(conf);
        if(hdfs.isDirectory(myPath)){
            hdfs.delete(myPath, true);
        }

        Job job = Job.getInstance(conf, "weibo self count");
        job.setJarByClass(SelfCount.class);
        job.setMapperClass(SelfcountMapper.class);
        job.setReducerClass(SelfcountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/relsemple.json"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/out2"));

//        FileInputFormat.addInputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/weibo.json"));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/out1"));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
