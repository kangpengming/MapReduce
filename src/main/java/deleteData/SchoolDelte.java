package deleteData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
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

/**
 * Created by kp on 16/8/22.
 */
public class SchoolDelte {
    private static Configuration conf = null;

    static {
        conf = new Configuration();

        conf.set("hbase.zookeeper.quorum","127.0.0.1");
        conf.set("hbase.rootdir","hdfs://127.0.0.1:9000");
        conf.set("hbase.master","hdfs://master:60000");

    }

    public  void tests(String tabelName,String storeTable) throws Exception{

        Scan scan = new Scan();
        conf.set("tablename", tabelName);
        Job job = new Job(conf,storeTable);
        job.setJarByClass(SchoolDelte.class);
        job.setReducerClass(ReduceSchool.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TableMapReduceUtil.initTableMapperJob(tabelName.getBytes(), scan, MaperSchool.class, Text.class, Text.class, job);
        //FileOutputFormat.setOutputPath(job, new Path("/home/kp/12111school"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/kp/KPtest/"+storeTable+".txt"));
        job.waitForCompletion(true);
/*
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fsDataInputStream =fileSystem.open(new Path("/home/kp/12111school/part-r-00000"));
        FileOutputStream fileOutputStream = new FileOutputStream("/Users/kp/KPtest/"+storeTable+".txt");
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,4096,true);
  */
    }

    public static class MaperSchool extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Cell[] cells = value.rawCells();
            for(Cell cell:cells){
                String family = new String(CellUtil.cloneFamily(cell));
                String column = new String(CellUtil.cloneQualifier(cell));
                String valueStr = new String(CellUtil.cloneValue(cell));
                if (valueStr.equals("")){
                    valueStr = "none";
                }

                String outValue = family +"$&"+column +"$&" + valueStr;
                context.write(new Text(key.get()),new Text(outValue));
            }
        }
    }

    public static class ReduceSchool extends Reducer<Text, Text, Text, NullWritable> {
        static HTable table = null;
        static String tablename = null;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            tablename = context.getConfiguration().get("tablename");
            System.out.println(tablename+"--------------------");
            table = new HTable(conf,tablename);


        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          /*  int count = 0;
            int numProperty = 0;
            for (Text value:values){
                numProperty++;
                String[] keyvalue= value.toString().split("\\$&");
                System.out.println(new String(key.getBytes()));
                String valuestr = keyvalue[2];
                String family = keyvalue[0];
                String column = keyvalue[1];
                if(family.equals("property")){
                    if (valuestr.equals("none")){
                        count++;
                    }
                }
            }
            numProperty = numProperty-5;
            if(count != numProperty){
                String keystr = key.toString().split("\\$")[1];
                context.write(new Text(keystr),NullWritable.get());
            }else{
                System.out.println(key.toString());
                Delete delete = new Delete(key.toString().getBytes());
                table.delete(delete);
            }*/
            System.out.println(tablename);

        }
    }
}
