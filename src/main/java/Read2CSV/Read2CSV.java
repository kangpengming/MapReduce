package Read2CSV;

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
import java.util.Calendar;

/**
 * Created by kp on 16/8/27.
 *
 * 将hbase中的表导入本地的CSV文件中，
 * CSV文件原名是逗号分隔符，但是并不指定是逗号，也可以是别的符号作为分隔符。
 */
public class Read2CSV{
    private static Configuration conf = null;
    static{
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("hbase.rootdir","hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");
		conf.set("fs.default.name","hdfs://localhost:9000");
    }
    /**
     * args[0]为表名，分隔符为$;args[1]为每个变量值，args[2]需要设定的分隔符，args[3]为生成的表名
     * */

    public void read2CSV(String args[]) throws Exception
    {

        /**
         * 定义输入变量，读取的表名
         * 每张表需要读取的变量的名称：必须读取的数值,只需要是列名就可以了
		 * 生成表名的路径一定要对应，因为在集群中文件只能生成在集群上，所以需要利用分布式系统的文件输入输出流
		 * 将文件从集群中读出来
         * */



		Scan scan = new Scan();
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
		job.setJarByClass(Read2CSV.class);
		job.setReducerClass(ReduceSchool.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TableMapReduceUtil.initTableMapperJob(args[0].getBytes(), scan, MappSchool.class, Text.class, Text.class, job);
		FileOutputFormat.setOutputPath(job, new Path("/home/kp/KPtest/file" + args[3] + "-" + time));
		job.waitForCompletion(true);

		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream fsDataInputStream = fs.open(new Path("/home/kp/KPtest/file" + args[3] + "-" + time+"/part-r-00000"));
		OutputStream outputStream = new FileOutputStream("/Users/kp/KPtest/file" + args[3] + "-" + time+".csv");

		IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, true);

    }
}
