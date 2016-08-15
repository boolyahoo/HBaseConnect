import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xcoder on 8/12/16.
 */
public class Main {

    private static HBaseOperation hbase;


    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        hbase = new HBaseOperation(config);

        String tableName = "model";
        String colFamilies[] = {"data"};
        hbase.createTable(tableName, colFamilies);

        Map<String, String> dic = new HashMap<String, String>();
        dic.put("CRFDependency:data/model/dependency/CRFDependencyModelMini.txt.bin", "dependency");
        dic.put("MaxEnt:data/model/dependency/MaxEntModel.txt.bin", "dependency");
        dic.put("NNParser:data/model/dependency/NNParserModel.txt.bin", "dependency");
        dic.put("WordNature:data/model/dependency/WordNature.txt.bin", "dependency");
        dic.put("CRFSegment:data/model/segment/CRFSegmentModel.txt.bin", "segment");
        dic.put("HMMSegment:data/model/segment/HMMSegmentModel.bin", "segment");

        loadDicIntoHBase(dic);
    }

    private static void loadDicIntoHBase(Map dic) {
        byte[] data;
        String tableName = "dictionary";
        String columnFamily = "data";
        for (Object key : dic.keySet()) {
            String rowKey = ((String) key).split(":")[0];
            String path = ((String) key).split(":")[1];
            String column = (String) dic.get(key);
            File file = new File(path);
            try {
                FileInputStream in = new FileInputStream(file);
                data = new byte[(int) file.length()];
                int len = in.read(data);
                in.close();
                System.out.println(rowKey + ":" + column);
                hbase.insertRecord(tableName, rowKey, columnFamily, column, data);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
