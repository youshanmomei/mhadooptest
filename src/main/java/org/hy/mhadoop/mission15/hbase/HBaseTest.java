package org.hy.mhadoop.mission15.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by andy on 2016/10/26.
 */
public class HBaseTest {

    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hy1,hy2,hy3,hy4,hy5"); //设置hbase的服务器的地址
        conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置端口号
        conf.set("hbase.master", "hy1:60000");

    }

    public static void main(String[] args) throws IOException {
        runTest(4);
    }

    private static void runTest(int i) throws IOException {
        switch (i) {
            case 0:
                createTable("member");
                System.out.println("---> create table running over.");
                break;
            case 1:
                insertDataByPut("member");
                System.out.println("---> put value into table running over.");
                break;
            case 2:
                QueryByGet("member");
                System.out.println("---> get value done.");
                break;
            case 3:
                QueryByScan("member");
                System.out.println("---> scan a table done.");
                break;
            case 4:
                deleteData("member");
                System.out.println("---> delete done.");
                break;
            default:
                break;
        }
    }

    public static void createTable(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        //add family
        tableDescriptor.addFamily(new HColumnDescriptor("address"));
        tableDescriptor.addFamily(new HColumnDescriptor("info"));

        hBaseAdmin.createTable(tableDescriptor); // create table
        hBaseAdmin.close(); //release resource
    }

    public static void insertDataByPut(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);

        Put put1 = new Put(getBytes("hy"));
        put1.add(getBytes("address"), getBytes("country"), getBytes("china"));
        put1.add(getBytes("address"), getBytes("province"), getBytes("shanghai"));
        put1.add(getBytes("address"), getBytes("city"), getBytes("shanghai"));

        put1.add(getBytes("info"), getBytes("age"), getBytes("24"));
        put1.add(getBytes("info"), getBytes("birthday"), getBytes("1993-3-15"));
        put1.add(getBytes("info"), getBytes("company"), getBytes("ibm"));

        table.put(put1);
        table.close();//release resource
    }

    public static void QueryByGet(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);

        Get get_hy = new Get(getBytes("hy"));
        Result r = table.get(get_hy);

        System.out.println("the value of rowkey:" + new String(r.getRow()));
        for (KeyValue keyValue : r.raw()) {
            System.out.println(
                    "列簇：" + new String(keyValue.getFamily())
                    + " ===> 列：" + new String(keyValue.getQualifier())
                    + " ===> 值：" + new String(keyValue.getValue())
            );
        }

        table.close();
    }

    /**
     * 扫描一行特定的数据
     * @param tableName
     * @throws IOException
     */
    public static void QueryByScan(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);

        Scan scan = new Scan();
        scan.addColumn(getBytes("info"), getBytes("company"));

        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            System.out.println("the rowkey is " + new String(r.getRow()));
            for (KeyValue kv : r.raw()) {
                System.out.println(
                        "|  列簇：" + new String(kv.getFamily())
                        + " | 列：" + new String(kv.getQualifier())
                        + " | 值：" + new String(kv.getValue())
                );
            }
        }

        scanner.close();
        table.close();

    }

    public static void deleteData(String tableName) throws IOException {
        HTable table = new HTable(conf,  tableName);

        Delete delete = new Delete(getBytes("hy"));
        delete.deleteColumn(getBytes("info"), getBytes("age"));

        table.delete(delete);

        table.close();
    }

    public static byte[] getBytes(String str){
        if(str==null) str="";
        return Bytes.toBytes(str);
    }

}
