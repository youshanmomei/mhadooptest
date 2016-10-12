package org.hy.mhadoop.mission13.weibocount.itselfcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by andy on 2016/10/11.
 */
public class SelfcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private List<OutputValue> opvs = new ArrayList<OutputValue>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            opvs.add(new OutputValue(new Text("0"), new IntWritable(0)));
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum++;
        }
        OutputValue ov = new OutputValue(new Text(key.toString()), new IntWritable(sum));//该处若不用new Text(key.toString()而是直接用key的话，传入的是key的引用而不是key的值，最后就会变成key值一样的数组
        opvs.add(ov);
        Collections.sort(opvs);
        opvs.remove(10);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (OutputValue opv : opvs) {
            context.write(opv.key, opv.value);
        }
    }

    private class OutputValue implements Comparable{
        public Text key;
        public IntWritable value;

        public OutputValue(Text key, IntWritable value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(Object o) {
            OutputValue opv_c = (OutputValue)o;
            return opv_c.value.get()-value.get();
        }
    }

}
