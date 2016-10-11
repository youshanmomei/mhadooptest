package org.hy.mhadoop.mission13.weibocount.itselfcount;

import java.util.ArrayList;

/**
 * Created by andy on 2016/10/11.
 * 微博数据的数据集合
 *
 */
public class WeiboBean {
    private String id;
    private ArrayList<String> ids;
    private String total_number;

    public WeiboBean() {}

    public WeiboBean(String id, ArrayList<String> ids, String total_number) {
        this.id = id;
        this.ids = ids;
        this.total_number = total_number;
    }

    public String getId() {
        return id;
    }

    public ArrayList<String> getIds() {
        return ids;
    }

    public String getTotal_number() {
        return total_number;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setIds(ArrayList<String> ids) {
        this.ids = ids;
    }

    public void setTotal_number(String total_number) {
        this.total_number = total_number;
    }
}
