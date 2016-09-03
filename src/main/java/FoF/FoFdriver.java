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
 * */
public class FoFdriver extends TestCase {

    public static void main(String args[]) throws Exception{

        String inputfile = "";
        String calcOutputDir = "";
        String sortOutputDir = "";
        if (runCalcJob(inputfile, calcOutputDir)) {
        runSortJob(calcOutputDir, sortOutputDir);
    }
}

    public static boolean runCalcJob(String input,String output) throws IOException,InterruptedException,ClassNotFoundException{

        Configuration configuration = new Configuration();

        Job job = new Job(configuration);
        job.setJarByClass(FoFdriver.class);
        job.setMapperClass(CalcMapReduce.Map.class);
        job.setReducerClass(CalcMapReduce.Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(CalcMapReduce.TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path outpath = new Path(output);

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,outpath);

        outpath.getFileSystem(configuration).delete(outpath,true);

        return job.waitForCompletion(true);
    }

    public static void runSortJob(String input,String output) throws ClassNotFoundException,InterruptedException,IOException{
        Configuration configuration = new Configuration();

        Job job = new Job(configuration);

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

        outputPath.getFileSystem(configuration).delete(outputPath,true);

        job.waitForCompletion(true);
    }
}
