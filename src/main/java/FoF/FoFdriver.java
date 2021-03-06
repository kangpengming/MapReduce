package FoF;

import javafx.scene.text.Text;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by kp on 16/8/27.
 */

/**
 * fof算法，主要用于好友的推荐算法，计算出两个人之间的共同好友数，然后根据共同好友数量进行排列,其实就是朋友推荐算法，因为相互认识的朋友已经不需要推荐。
 * 所以在算法中标识为1的时候，这时候将共同好友之间认识的已经排除。
 * 计算共同好友：逆向思维的方法，以A为标签，然后去构造与A好友中的每两个好友之间的关系，则A必然是这两个好友的共同好友（C,D），同理，如果在别的人中也
 * 同时存在这两个人，那么A与该人都是C，D的共同好友。
 * 其中最主要的是用到了二次排序，现将进行分区，分区需要根据规则进行，这里面构造了复合key。对其进行分区（默认的是选取key值）。
 * 然后就是排序，排序会自动选取分区之后的数据，在对分区里面的数据进行分组，然后reduce去读取每个分区里面的数据。
 * 分区与分组的区别，分区是将相关的数据分配到reduce中，而分组是将每一个分区中的相关联的数据放置在一个迭代器中。
 * */
public class FoFdriver extends TestCase {
    static Configuration conf = new Configuration();
    static {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
        conf.set("fs.default.name","hdfs://localhost:9000");
        conf.set("hbase.rootdir","hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");
    }

    public static void main(String args[]) throws Exception{

        /*String inputfile = args[0];
        String calcOutputDir = args[1];
        String sortOutputDir = args[2];*/
        String inputfile = "/Users/kp/KPtest/friends.txt";
        String calcOutputDir = "calc-output";
        String sortOutputDir = "sort-output";
        if (runCalcJob(inputfile, calcOutputDir)) {
        runSortJob(calcOutputDir, sortOutputDir);
    }
}

    public static boolean runCalcJob(String input,String output) throws IOException,InterruptedException,ClassNotFoundException{


        Job job = new Job(conf);
        job.setJarByClass(FoFdriver.class);
        job.setMapperClass(CalcMapReduce.Map.class);
        job.setReducerClass(CalcMapReduce.Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(CalcMapReduce.TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path outpath = new Path(output);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,outpath);

        outpath.getFileSystem(conf).delete(outpath,true);

        return job.waitForCompletion(true);
    }

    public static void runSortJob(String input,String output) throws ClassNotFoundException,InterruptedException,IOException{

        Job job = new Job(conf);

        job.setJarByClass(FoFdriver.class);
        job.setMapperClass(SortJob.Map.class);
        job.setReducerClass(SortJob.Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Person.class);
        job.setMapOutputValueClass(Person.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(PersonNamePartitioner.class);
        job.setSortComparatorClass(PersonComparator.class);
        job.setGroupingComparatorClass(PersonNameComparator.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,outputPath);

        outputPath.getFileSystem(conf).delete(outputPath,true);

        job.waitForCompletion(true);
    }
}
