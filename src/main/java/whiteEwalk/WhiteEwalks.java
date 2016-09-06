package whiteEwalk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by kp on 16/9/5.
 */
public class WhiteEwalks {

    private static Configuration conf = null;

    static {
        conf = new Configuration();

        conf.set("hbase.zookeeper.quorum", "192.168.1.230");
        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.rootdir","hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");

    }


    public  void tests(String tabelName,String storeTable) throws Exception{

        Scan scan = new Scan();
        conf.set("tablename", tabelName);
        Job job = new Job(conf,storeTable);
        job.setJarByClass(WhiteEwalks.class);
        job.setReducerClass(ReduceWhitel.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TableMapReduceUtil.initTableMapperJob(tabelName.getBytes(), scan, MaperWhite.class, Text.class, Text.class, job);
        //FileOutputFormat.setOutputPath(job, new Path("/home/kp/12111school"));
        FileOutputFormat.setOutputPath(job, new Path("/home/kp/KPtest/ewalk"));
        job.waitForCompletion(true);

        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fsDataInputStream =fileSystem.open(new Path("/home/kp/KPtest/ewalk/part-r-00000"));
        FileOutputStream fileOutputStream = new FileOutputStream("/home/kp/KPtest/ewalk/ewalk.txt");
        //FileOutputStream fileOutputStream = new FileOutputStream("/Users/kp/KPtest/ewalk.txt");
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, 4096, true);

    }

    public static class MaperWhite extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Cell[] cells = value.rawCells();
            for(Cell cell:cells){
                String family = new String(CellUtil.cloneFamily(cell));
                String column = new String(CellUtil.cloneQualifier(cell));
                String valueStr = new String(CellUtil.cloneValue(cell));

                String outValue = family +"$&"+column +"$&" + valueStr;
                context.write(new Text(key.get()),new Text(outValue));
            }
        }
    }

    public static class ReduceWhitel extends Reducer<Text, Text, Text, Text> {

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
            String keystr = key.toString().split("\\$")[1];
            context.write( new Text(keystr) ,new Text(urlupdate));

        }
    }
}

