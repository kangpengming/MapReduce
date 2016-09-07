package TestPatrtion;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by kp on 16/9/6.
 */
/**
 * 二次排序的基本原理，使用key值与value进行排序，因为MR都是利用key进行排序，所以此时需要将key与value构造成一个复合key。
 * */
public class testPartion extends TestCase{

    private static Configuration conf = null;

    static {
        conf = new Configuration();

        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("fs.default.name","hdfs://localhost:9000");
        conf.set("hbase.rootdir","hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");

    }

    public void testpartion() throws IOException,InterruptedException,ClassNotFoundException{

        Job job = new Job(conf,"test");
        job.setJarByClass(testPartion.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Intpair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(FirstPartion.class);
        //job.setSortComparatorClass();
        job.setGroupingComparatorClass(GroupSort.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("/home/kp/KPtest/test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/kp/KPtest/ab.txt"));
        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<Text,Text,Intpair,IntWritable>{
        Intpair keyset = new Intpair();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            keyset.setFirst(key.toString());
            keyset.setSecond(Integer.parseInt(value.toString()));

            System.out.println("=============================="+keyset.getFirst());
            context.write(keyset, new IntWritable(Integer.parseInt(value.toString())));
        }
    }

    public static class Reduce extends Reducer<Intpair,IntWritable,Text,IntWritable>{
        String seperate = "----------------------";
        Text first = new Text();
        @Override
        protected void reduce(Intpair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(seperate),null);
            first.set(key.getFirst());
            for (IntWritable keyvalue : values){
                context.write(first,keyvalue);
            }

        }
    }
}
