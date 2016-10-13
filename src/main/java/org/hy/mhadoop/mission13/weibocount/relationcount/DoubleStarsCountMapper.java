package org.hy.mhadoop.mission13.weibocount.relationcount;

import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hy.mhadoop.mission13.weibocount.itselfcount.WeiboBean;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by andy on 2016/10/11.
 */
public class DoubleStarsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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
            System.out.println(" ------> 该行解析出错，已舍弃 ");
        }

        //判断是否同时关注了李开复，赵薇的微博
        //李开复	1197161814
        //赵薇	1656809190
        boolean isDouAtt = false;
        if (wb != null) {
            ArrayList<String> ids = wb.getIds();
            isDouAtt = ids.contains("1197161814") && ids.contains("1656809190");
        }


        //对明星的被关注数进行统计
        if(isDouAtt){
            for (String strKey : wb.getIds()) {
                context.write(new Text(strKey), new IntWritable(1));
            }
        }
    }
}
