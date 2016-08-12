import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by xcoder on 8/12/16.
 */
public class HBaseOperation {
    private Configuration config;
    private HBaseAdmin admin;

    public HBaseOperation() {
    }

    public HBaseOperation(Configuration config)
            throws MasterNotRunningException, ZooKeeperConnectionException,
            IOException {
        this.config = config;
        this.admin = new HBaseAdmin(config);
    }

    /**
     * 创建表
     *
     * @param tableName   表名
     * @param colFamilies 列族名
     */
    public void createTable(String tableName, String[] colFamilies)
            throws IOException {

        HTableDescriptor desc = new HTableDescriptor(
                TableName.valueOf(tableName));
        for (int i = 0; i < colFamilies.length; i++) {
            desc.addFamily(new HColumnDescriptor(colFamilies[i]));
        }
        if (this.admin.tableExists(tableName)) {
            System.out.println("table exists!");
            System.exit(0);
        } else {
            this.admin.createTable(desc);
            System.out.println("create table success!");
        }

    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public void deleteTable(String tableName) throws IOException {
        if (this.admin.tableExists(tableName)) {
            admin.deleteTable(tableName);
            System.out.println("delete table success!");
        } else {
            System.out.println("table not exists!");
        }
    }

    /**
     * @param tableName 表名
     * @param rowkey    行键
     * @param family    列族名
     * @param qualifier 列名
     * @param value     列的值
     */
    public void insertRecord(String tableName, String rowkey, String family,
                             String qualifier, String value) throws IOException {
        HTable table = new HTable(config, tableName);// HTabel负责跟记录相关的操作如增删改查等
        Put put = new Put(Bytes.toBytes(rowkey));// 设置rowkey
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                Bytes.toBytes(value));// 将指定的列族中的某一列以及该列的值添加到put实例中
        table.put(put);
        System.out.println("insert record success！");
        table.close();
    }

    /**
     * 根据rowkey查询某一行记录
     *
     * @param tableName 表名
     * @param rowkey    行键
     * @return
     */
    public Result getRecord(String tableName, String rowkey) throws IOException {
        HTable table = new HTable(config, tableName);

        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);

        table.close();
        return result;
    }

    /**
     * 查询某一列的值
     *
     * @param tableName 表名
     * @param rowkey    行键
     * @param family    列族名
     * @param qualifier 列名
     */
    public Result getRecordByColumn(String tableName, String rowkey,
                                    String family, String qualifier) throws IOException {
        HTable table = new HTable(config, tableName);

        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);

        table.close();
        return result;
    }

    /**
     * 遍历查询所有记录
     *
     * @param tableName 表名
     */
    public List<Result> getAllRecord(String tableName) throws IOException {
        HTable table = new HTable(config, tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<Result>();
        for (Result result : scanner) {
            list.add(result);
        }
        table.close();
        return list;
    }

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowkey    行键
     */
    public void deleteRow(String tableName, String rowkey) throws IOException {
        HTable table = new HTable(config, tableName);

        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);

        System.out.println("row are deleted!");
        table.close();
    }

    public void deleteColumn(String tableName, String rowkey, String family,
                             String qualifier) throws IOException {
        HTable table = new HTable(config, tableName);

        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        table.delete(delete);

        System.out.println("clomun are deleted！");
        table.close();
    }
}
