package org.hy.mhadoop.mission13.weibocount.relationcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by andy on 2016/10/13.
 */
public class FindDoubleStarts {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path myPath = new Path("hdfs://hy:9000/mission/12-mysql/out4");
        FileSystem hdfs = myPath.getFileSystem(conf);
        if (hdfs.isDirectory(myPath)) {
            hdfs.delete(myPath, true);
        }

        Job job = Job.getInstance(conf, "weibo double stars attention count");
        job.setJarByClass(FindDoubleStarts.class);
        job.setMapperClass(DoubleStarsCountMapper.class);
        job.setReducerClass(DoubleStartsCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/relsemple.json"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hy:9000/mission/12-mysql/out4"));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
