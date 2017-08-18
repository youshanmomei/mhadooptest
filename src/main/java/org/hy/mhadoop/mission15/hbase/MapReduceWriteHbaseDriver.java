package org.hy.mhadoop.mission15.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by andy on 2016/10/26.
 */
public class MapReduceWriteHbaseDriver {
    public static class WordCountMapperHbase extends Mapper<Object, Text, ImmutableBytesWritable, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreElements()){
                word.set(itr.nextToken());
                //输出到hbase的key类型为ImmutableBytesWritable
                context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())),one);
            }

        }
    }

    public class WordCountReducerHbase extends TableReducer< ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(ImmutableBytesWritable key, Iterable< IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Put put = new Put(key.get());//put 实例化  key代表主键，每个单词存一行
            //三个参数分别为  列簇为content，列修饰符为count，列值为词频
            put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            context.write(key , put);
        }
    }

    public static void main(String[] args)throws Exception {
        String tableName = "wordcount";//hbase 数据库表名
        Configuration conf= HBaseConfiguration.create(); //实例化Configuration
        conf.set("hbase.zookeeper.quorum", "hy1,hy2,hy3,hy4,hy5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");//端口号
        conf.set("hbase.master", "hy1:60000");

        //如果表已经存在就先删除
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor("content");
        htd.addFamily(hcd);//创建列簇
        admin.createTable(htd);//创建表

        Job job=new Job(conf,"import from hdfs to hbase");
        job.setJarByClass(MapReduceWriteHbaseDriver.class);

        job.setMapperClass(WordCountMapperHbase.class);

        //设置插入hbase时的相关操作
//        TableMapReduceUtil.initTableReducerJob(tableName, WordCountReducerHbase.class, job, null, null, null, null, false);
        TableMapReduceUtil.initTableReducerJob(tableName, WordCountReducerHbase.class, job, null, null, null, null, false);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.addInputPaths(job, "hdfs://hy:9000/idata/hy.txt");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
