package airbnb2one;

import Read2CSV.MappSchool;
import Read2CSV.ReduceSchool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * Created by kp on 16/8/27.
 */
public class airbnbTest {
    private static Configuration conf = null;
    static {
        conf = new Configuration();

        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.rootdir","hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");

    }
    public void airbnbManyTales(String args[]) throws Exception
    {
        String[] tableNames = {"","",""}; //在这里面填写读取的hbase中的表名
        ArrayList<Scan> list = new ArrayList<Scan>();

        for (int i = 0; i < tableNames.length; i++) {
            Scan scan = new Scan();
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,tableNames[i].getBytes());
            list.add(scan);
        }


        //生成当时的日期
        Calendar calendar = Calendar.getInstance();
        int day = calendar.get(Calendar.DATE);
        int month = calendar.get(Calendar.MONTH)+1;
        int year = calendar.get(Calendar.YEAR);

        String time = Integer.toString(year)+"-"+Integer.toString(month)+"-"+Integer.toString(day);

        String[] keys = args[1].split("%");
        //用来分割找出数据所需要的值
        conf.setStrings("keys",keys);

        conf.set("seperate",args[2]);

        Job job = new Job(conf, "word count");
        job.setJarByClass(airbnbTest.class);
        job.setReducerClass(Myreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableMapperJob(list, MyMapper.class, Text.class, Text.class, job);
        FileOutputFormat.setOutputPath(job, new Path("/home/kp/KPtest/file" + args[3] + "-" + time));
        job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsDataInputStream = fs.open(new Path("/home/kp/KPtest/file" + args[3] + "-" + time+"/part-r-00000"));
        OutputStream outputStream = new FileOutputStream("/Users/kp/KPtest/file" + args[3] + "-" + time+".csv");

        IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, true);

    }
}
