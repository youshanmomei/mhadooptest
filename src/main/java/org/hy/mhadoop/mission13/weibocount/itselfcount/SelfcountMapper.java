package org.hy.mhadoop.mission13.weibocount.itselfcount;

import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by andy on 2016/10/11.
 */
public class SelfcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Gson gson;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        //初始化gson
        gson = new Gson();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        WeiboBean wb = null;
        try {
            //解析每一行数据
            wb = gson.fromJson(value.toString(), WeiboBean.class);
        } catch (Exception e) {
        }

        //对明星的被关注数进行统计
        if(wb!=null){
            for (String strKey : wb.getIds()) {
                context.write(new Text(strKey), new IntWritable(1));
            }
        }
    }
}
