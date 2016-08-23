import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xcoder on 8/16/16.
 */
public class HBaseClient {
    private static HBaseOperation hbase;
    private static Logger LOG;

    static {
        Configuration config = new Configuration();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        hbase = new HBaseOperation(config);
        LOG = Logger.getLogger(HBaseClient.class.getName());
        LOG.setLevel(Level.INFO);
    }

    public static void insertDic() {
        try {
            String tableName = "dictionary";
            String colFamilies[] = {"data"};
            hbase.createTable(tableName, colFamilies);

            Map<String, String> dic = new HashMap<String, String>();
            dic.put("ngramNature:data/dictionary/core/CoreNatureDictionary.ngram.txt", "content");
            dic.put("coreNature:data/dictionary/core/CoreNatureDictionary.txt", "content");
            dic.put("organization:data/dictionary/organization/nt.txt", "content");
            dic.put("charTable:data/dictionary/other/CharTable.txt", "content");
            dic.put("charType:data/dictionary/other/CharType.dat.yes", "content");
            dic.put("person:data/dictionary/person/nr.txt", "content");
            dic.put("personForeign:data/dictionary/person/nrf.txt", "content");
            dic.put("personJap:data/dictionary/person/nrj.txt", "content");
            dic.put("pinyin:data/dictionary/pinyin/pinyin.txt", "content");
            dic.put("sytDic:data/dictionary/pinyin/SYTDictionary.txt", "content");
            dic.put("place:data/dictionary/place/ns.txt", "content");
            dic.put("stopWords:data/dictionary/stopwords/stopwords.txt", "content");
            dic.put("synonym:data/dictionary/synonym/CoreSynonym.txt", "content");
            dic.put("traditionChn:data/dictionary/tc/TraditionalChinese.txt", "content");


            byte[] data;
            for (Object key : dic.keySet()) {
                String rowKey = ((String) key).split(":")[0];
                String path = ((String) key).split(":")[1];
                String column = (String) dic.get(key);
                File file = new File(path);

                FileInputStream in = new FileInputStream(file);
                data = new byte[(int) file.length()];
                int len = in.read(data);
                in.close();
                System.out.println(rowKey + ":" + column);
                hbase.insertRecord(tableName, rowKey, colFamilies[0], column, data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertModel() {
        String tableName = "model";
        String colFamilies[] = {"data"};
        try {
            hbase.createTable(tableName, colFamilies);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, String> dic = new HashMap<String, String>();
        dic.put("CRFDependency:data/model/dependency/CRFDependencyModelMini.txt.bin", "content");
        dic.put("MaxEnt:data/model/dependency/MaxEntModel.txt.bin", "content");
        dic.put("NNParser:data/model/dependency/NNParserModel.txt.bin", "content");
        dic.put("WordNature:data/model/dependency/WordNature.txt.bin", "content");
        dic.put("CRFSegment:data/model/segment/CRFSegmentModel.txt.bin", "content");
        dic.put("HMMSegment:data/model/segment/HMMSegmentModel.bin", "content");

        dic.put("trMatrix:data/dictionary/core/CoreNatureDictionary.tr.txt", "coreNature");
        dic.put("trMatrix:data/dictionary/organization/nt.tr.txt", "organization");
        dic.put("trMatrix:data/dictionary/person/nr.tr.txt", "person");
        dic.put("trMatrix:data/dictionary/place/ns.tr.txt", "place");

        try{
            byte[] data = null;
            for (Object key : dic.keySet()) {
                String rowKey = ((String) key).split(":")[0];
                String path = ((String) key).split(":")[1];
                String column = (String) dic.get(key);
                File file = new File(path);

                FileInputStream in = new FileInputStream(file);
                data = new byte[(int) file.length()];
                int len = in.read(data);
                in.close();
                System.out.println(rowKey + ":" + column);
                hbase.insertRecord(tableName, rowKey, colFamilies[0], column, data);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }


    }


    public void readModel() {
        try {
            String tableName = "model";
            String rowKey = "HMMSegment";
            String colFamily = "data";
            String column = "segment";
            Result result = hbase.getRecordByColumn(tableName, rowKey, colFamily, column);
            byte[] data = result.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(column));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void readDictionary() {
        String tableName = "dictionary";
        String colFamily = "data";
        String rowKey = "coreNature";
        String column = "coreNature";

        Result result = null;
        try {
            result = hbase.getRecordByColumn(tableName, rowKey, colFamily, column);
        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] fileContent = result.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(column));

        int j = 0;
        for (int i = j; i < 200; i++) {

            if (fileContent[i] == '\n') {
                String s = new String(fileContent, j, i - j, Charset.forName("UTF-8"));
                System.out.println(j + ":" + s + ":" + i);
                j = i + 1;
            }
        }
    }


}
