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

        String tableName = "dictionary";
        String colFamilies[] = {"data"};
        hbase.createTable(tableName, colFamilies);

        Map<String, String> dic = new HashMap<String, String>();
        dic.put("core:data/dictionary/core/CoreNatureDictionary.ngram.txt", "ngramNature");
        dic.put("core:data/dictionary/core/CoreNatureDictionary.txt", "coreNature");
        dic.put("organization:data/dictionary/organization/nt.txt", "nt");
        dic.put("other:data/dictionary/other/CharTable.txt", "charTable");
        dic.put("other:data/dictionary/other/CharType.dat.yes", "charType");
        dic.put("person:data/dictionary/person/nr.txt", "nr");
        dic.put("person:data/dictionary/person/nrf.txt", "nrf");
        dic.put("person:data/dictionary/person/nrj.txt", "nrj");
        dic.put("pinyin:data/dictionary/pinyin/pinyin.txt", "pinyin");
        dic.put("pinyin:data/dictionary/pinyin/SYTDictionary.txt", "sytDictionary");
        dic.put("place:data/dictionary/place/ns.txt", "ns");
        dic.put("stopwords:data/dictionary/stopwords/stopwords.txt", "stopwords");
        dic.put("synonym:data/dictionary/synonym/CoreSynonym.txt", "coreSynonym");
        dic.put("tc:data/dictionary/tc/TraditionalChinese.txt", "traditionalChinese");

        loadDicIntoHBase(dic);



       /* hbase.insertRecord(tableName, "row2", "article", "title", "hadoop");
        hbase.insertRecord(tableName, "row2", "author", "name", "tom");
        hbase.insertRecord(tableName, "row2", "author", "nickname", "tt");*/


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
                hbase.insertRecord(tableName, rowKey, columnFamily, column, data);
            } catch (Exception e) {
                e.printStackTrace();
            }


            //System.out.println(column);


        }
    }


}
