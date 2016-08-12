import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * Created by xcoder on 8/12/16.
 */
public class Main {

    public static void main(String... args) throws IOException {
        // 1.初始化HBaseOperation
        Configuration config = new Configuration();
        config.set("hbase.zookeeper.quorum", "H01,H02,H03");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseOperation hbase = new HBaseOperation(config);

        // 2. 测试相应操作
        // 2.1 创建表
        String tableName = "blog";
        String colFamilies[] = {"article", "author"};
        //hbase.createTable(tableName, colFamilies);

        // 2.2 插入一条记录
        hbase.insertRecord(tableName, "row1", "article", "title", "hadoop");
        hbase.insertRecord(tableName, "row1", "author", "name", "tom");
        hbase.insertRecord(tableName, "row1", "author", "nickname", "tt");

        // 2.2 查询一条记录
        Result rs1 = hbase.getRecord(tableName, "row1");
        for (Cell cell : rs1.rawCells()) {
            System.out.println(new String(cell.getRowArray()));
            System.out.println(new String(cell.getFamilyArray()));
            System.out.println(new String(cell.getQualifierArray()));
            System.out.println(new String(cell.getValueArray()));
        }

        // 2.3 查询所有记录
        List<Result> list = hbase.getAllRecord(tableName);
        Iterator<Result> it = list.iterator();
        while (it.hasNext()) {
            Result rs2 = it.next();
            for (Cell cell : rs2.rawCells()) {
                System.out.println("row key is : "
                        + new String(cell.getRowArray()));
                System.out.println("family is  : "
                        + new String(cell.getFamilyArray()));
                System.out.println("qualifier is:"
                        + new String(cell.getQualifierArray()));
                System.out.print("timestamp is:" + cell.getTimestamp());
                System.out.println("Value  is  : "
                        + new String(cell.getValueArray()));
            }
        }

    }


}
