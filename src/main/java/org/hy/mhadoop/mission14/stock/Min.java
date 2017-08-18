package org.hy.mhadoop.mission14.stock;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by andy on 2016/10/19.
 */
public class Min extends UDF{
    public Double evaluate(Double a, Double b){
        if(a==null)a=0.0;
        if(b==null)b=0.0;

        if(a>=b)return a;
        else return b;
    }

}
