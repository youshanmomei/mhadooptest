package org.hy.mhadoop.mission13.weibocount.itselfcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by andy on 2016/10/11.
 */
public class SelfcountReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //对关注度进行统计
        int sum = 0;
        for(Text t:values){
            sum += 1;
        }
        context.write(key, new IntWritable(sum));
    }
}
