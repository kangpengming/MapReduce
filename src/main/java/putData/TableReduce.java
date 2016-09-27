package putData;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by kp on 16/9/26.
 */
public class TableReduce extends TableReducer<Text,Text,Text> {

    String url = "";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        url = "http://www.whitepages.com/search/FindNearby?utf8=%E2%9C%93";

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String street = null;
        String where = null;
        for(Text strurl:values){
            if(strurl.toString().contains("house_address0")){
                String[] info = strurl.toString().split("\\$&");
                street = info[2];
                street = street.replaceAll(",\\s","%2C+").replaceAll("\\s{1,4}","+").replaceAll(",","%2");

            }
            if(strurl.toString().contains("house_address1")){
                String[] info = strurl.toString().split("\\$&");
                where = info[2];
                where = where.replaceAll(",\\s","%2C+").replaceAll("\\s{1,4}","+").replaceAll(",","%2");
            }
        }

        String urlupdate = url+"&street="+street+"&"+"where="+where;

        Put putRow = new Put(key.toString().getBytes());
        putRow.add("property".getBytes(),"Ewalkurl".getBytes(),urlupdate.getBytes());

        context.write(key,putRow);
    }
}
