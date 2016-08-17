import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
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

    public void insertModdel() {
        String tableName = "dictionary";
        String colFamilies[] = {"data"};
        hbase.createTable(tableName, colFamilies);

        Map<String, String> dic = new HashMap<String, String>();
        /*dic.put("ngramNature:data/dictionary/core/CoreNatureDictionary.ngram.txt", "ngramNature");
        dic.put("coreNature:data/dictionary/core/CoreNatureDictionary.txt", "coreNature");
        dic.put("organization:data/dictionary/organization/nt.txt", "organization");
        dic.put("charTable:data/dictionary/other/CharTable.txt", "charTable");
        dic.put("charType:data/dictionary/other/CharType.dat.yes", "charType");
        dic.put("person:data/dictionary/person/nr.txt", "person");
        dic.put("personf:data/dictionary/person/nrf.txt", "personf");
        dic.put("personj:data/dictionary/person/nrj.txt", "personj");
        dic.put("pinyin:data/dictionary/pinyin/pinyin.txt", "pinyin");
        dic.put("SYTDictionary:data/dictionary/pinyin/SYTDictionary.txt", "SYTDictionary");
        dic.put("place:data/dictionary/place/ns.txt", "place");
        dic.put("stopwords:data/dictionary/stopwords/stopwords.txt", "stopwords");
        dic.put("synonym:data/dictionary/synonym/CoreSynonym.txt", "synonym");
        dic.put("traditionalChinese:data/dictionary/tc/TraditionalChinese.txt", "traditionalChinese");*/

        dic.put("CRFDependency:data/model/dependency/CRFDependencyModelMini.txt.bin", "CRFDependency");
        dic.put("MaxEnt:data/model/dependency/MaxEntModel.txt.bin", "MaxEnt");
        dic.put("NNParser:data/model/dependency/NNParserModel.txt.bin", "NNParser");
        dic.put("WordNature:data/model/dependency/WordNature.txt.bin", "WordNature");
        dic.put("CRFSegment:data/model/segment/CRFSegmentModel.txt.bin", "CRFSegment");
        dic.put("HMMSegment:data/model/segment/HMMSegmentModel.bin", "HMMSegment");

        byte[] data;
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
                hbase.insertRecord(tableName, rowKey, colFamilies[0], column, data);
            } catch (Exception e) {
                LOG.info(e.getMessage());
            }
        }
    }

    public void readModel() {
        String tableName = "model";
        String rowKey = "HMMSegment";
        String colFamily = "data";
        String column = "segment";
        Result result = hbase.getRecordByColumn(tableName, rowKey, colFamily, column);
        byte[] data = result.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(column));



    }
}
