import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kp on 16/8/22.
 */
public class schlDelete extends TestCase {

    private static Configuration conf = null;

    static {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.rootdir", "hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");
       // conf.set("fs.default.name", "hdfs://localhost:9000");
    }

    public  void tests() throws Exception{

        ArrayList<Scan>  listscan = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,"12111".getBytes());
        listscan.add(scan);
        Job job = new Job(conf,"shool");
        job.setJarByClass(schlDelete.class);
        job.setReducerClass(ReduceSchool.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TableMapReduceUtil.initTableMapperJob(listscan, MaperSchool.class, Text.class, Text.class, job);
        FileOutputFormat.setOutputPath(job, new Path("/Users/kp/KPtest/12111school"));
        job.waitForCompletion(true);
    }

    public static class MaperSchool extends TableMapper<Text, Text> {
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

    public static class ReduceSchool extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value:values){
                String[] keyvalue= value.toString().split("\\$&");
                String valuestr = keyvalue[2];
                String family = keyvalue[0];
                String column = keyvalue[1];
                if(family.equals("property")){
                    if (valuestr.equals("none")){
                        count++;
                    }
                }
            }

            if(count!=6){
                System.out.println(key.toString());
                String keystr = key.toString().split("\\$")[1];
                context.write(new Text(keystr),NullWritable.get());
            }

        }
    }

}