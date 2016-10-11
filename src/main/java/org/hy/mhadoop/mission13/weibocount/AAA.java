package org.hy.mhadoop.mission13.weibocount;

import com.google.gson.Gson;
import org.hy.mhadoop.mission13.weibocount.itselfcount.WeiboBean;

/**
 * Created by andy on 2016/10/11.
 */
public class AAA {

    private static Gson gson;

    private static WeiboBean parseJson(String line){
        gson = new Gson();
        WeiboBean wb = gson.fromJson(line, WeiboBean.class);

        return wb;
    }

    public static void main(String[] args){
        String lineValue = "{\"id\": 1701439105,\"ids\": [2154137571,3889177061,1740623531,1818796684,3912522941,1345264660,2813165271,3787129530,3819147406,2878306635,1781794411,3045994857,1697292185,1558592103,1630850750,1648267080,2432239367,1839778957,1400287693,1981622273,3206773285,1171650805,2402464143,1784200131,1959718390,1811574125,1710406277,1401880315,2873712630,3547390265,2157221604,1841149974,2876532864,1629799443,1791382201,2878439511,2367955351,1676403145,2371422760,2908382233,1496915057,……,1663973284],\"total_number\": 493}";
        WeiboBean wb = parseJson(lineValue);

        System.out.println(wb.getIds().size());
    }
}
