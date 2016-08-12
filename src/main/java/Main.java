import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by xcoder on 8/12/16.
 */
public class Main {

    public static void main(String... args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        try {

            Table table = connection.getTable(TableName.valueOf("test"));
            try {
                Put p = new Put(Bytes.toBytes("myLittleRow"));
                //p.add(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
                p.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
                table.put(p);
                Get g = new Get(Bytes.toBytes("myLittleRow"));
                Result r = table.get(g);
                byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
                String valueStr = Bytes.toString(value);
                System.out.println("GET: " + valueStr);
                Scan s = new Scan();
                s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
                ResultScanner scanner = table.getScanner(s);
                try {
                    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                        // print out the row we found and the columns we were looking for
                        System.out.println("Found row: " + rr);
                    }
                } finally {
                    scanner.close();
                }
            } finally {
                if (table != null) table.close();
            }
        } finally {
            connection.close();
        }
    }


}
