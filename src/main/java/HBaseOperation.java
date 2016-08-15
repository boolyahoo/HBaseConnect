import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by xcoder on 8/12/16.
 */
public class HBaseOperation {
    private Configuration config;
    private Admin admin;
    private Connection conn;

    public HBaseOperation(Configuration conf) {
        try {
            config = conf;
            conn = ConnectionFactory.createConnection(config);
            admin = conn.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName   表名
     * @param colFamilies 列族名
     */
    public void createTable(String tableName, String[] colFamilies) {
        try {
            HTableDescriptor tableDes = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < colFamilies.length; i++) {
                tableDes.addFamily(new HColumnDescriptor(colFamilies[i]));
            }
            TableName table = TableName.valueOf(tableName);
            if (admin.tableExists(table)) {
                System.out.println("table exists!");
            } else {
                admin.createTable(tableDes);
                System.out.println("create table success!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public void deleteTable(String tableName) {
        try {
            TableName table = TableName.valueOf(tableName);
            if (admin.tableExists(table)) {
                admin.deleteTable(table);
                System.out.println("delete table success!");
            } else {
                System.out.println("table not exists!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param tableName 表名
     * @param rowkey    行键
     * @param family    列族名
     * @param qualifier 列名
     * @param value     列的值
     */
    public void insertRecord(String tableName, String rowkey, String family, String qualifier, byte[] value) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            // Tabel负责跟记录相关的操作如增删改查等
            Put put = new Put(Bytes.toBytes(rowkey));
            // 将指定的列族中的某一列以及该列的值添加到put实例中
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
            table.put(put);
            System.out.println("insert record success！");
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowkey查询某一行记录
     *
     * @param tableName 表名
     * @param rowkey    行键
     * @return 查询结果
     */
    public Result getRecord(String tableName, String rowkey) {
        Result result = null;
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            result = table.get(get);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
    public Result getRecordByColumn(String tableName, String rowkey, String family, String qualifier) {
        Result result = null;
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            result = table.get(get);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 遍历查询所有记录
     *
     * @param tableName 表名
     */
    public List<Result> getAllRecord(String tableName) {
        List<Result> list = new ArrayList<Result>();
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                list.add(result);
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowkey    行键
     */
    public void deleteRow(String tableName, String rowkey) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            System.out.println("row are deleted!");
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void deleteColumn(String tableName, String rowkey, String family, String qualifier) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            table.delete(delete);
            System.out.println("clomun are deleted！");
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
