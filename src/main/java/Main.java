import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;


/**
 * Created by xcoder on 8/12/16.
 */
public class Main {

    public static void main(String[] args) {
        String fileName = "CRFSegmentModel.txt.bin";
        byte[] data = null;
        try {
            File file = new File(fileName);
            System.out.println("len = " + file.length());
            FileInputStream is = new FileInputStream(file);
            data = new byte[(int) file.length()];
            int len = is.read(data);
            is.close();
            System.out.println("len = " + len);
        } catch (Exception e) {
            e.printStackTrace();
        }


        Configuration config = new Configuration();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        HBaseOperation hbase = new HBaseOperation(config);
        /*try {
            hbase.insertRecord("blog", "dic", "author", "name", data);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        Result result = hbase.getRecord("blog", "dic");
        int len = result.getValue(Bytes.toBytes("author"), Bytes.toBytes("name")).length;
        System.out.println("len = = " + len);
    }


}
