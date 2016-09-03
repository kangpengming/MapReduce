package PageRank;

import javafx.scene.text.Text;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;



/**
 * Created by kp on 16/8/30.
 */

/***
 * PageRank算法：
 * （1）列出n个节点，并将每个节点可以到达的其余节点组成一行
 * （2）计算出每个节点的初始PageRank，即1/n
 * （3）初始节点将自己的PageRank分给自己所对应邻接点。
 * （4）累加并按权重计算，pj=a*(p1+p2+…Pm)+(1-a)*1/n;
 * （5）如此之后进行以后的迭代
 * */
public class PageRank extends TestCase{

    public void Testcase(String... args) throws Exception{
        String inputfile = args[0];
        String outputfile = args[1];

        iterate(inputfile, outputfile);

    }

    public static void iterate(String input,String output) throws Exception{

        Configuration configuration = new Configuration();
        Path outputpath = new Path(output);
        outputpath.getFileSystem(configuration).delete(outputpath, true);
        outputpath.getFileSystem(configuration).mkdirs(outputpath);

        Path inputPath = new Path(outputpath,"input.txt");

        int numNodes = createInputFile(new Path(input),inputPath);

        int iter = 1;
        double desiredCovergence = 0.01;

        while(true){
            Path jobOutputPath = new Path(outputpath,String.valueOf(iter));

            System.out.println("===============================");
            System.out.println("= Iteration: " + iter);
            System.out.println("= Input path: " + inputPath);
            System.out.println("= Output path:" + jobOutputPath);
            System.out.println("===============================");

            if(calcPageRank(inputPath,jobOutputPath,numNodes)<desiredCovergence){
                System.out.println("Convergence is below "+desiredCovergence+"we're done");
                break;
            }
            inputPath = jobOutputPath;
            iter++;
        }

    }

    public static int createInputFile(Path file,Path targetfile) throws IOException{

        Configuration configuration = new Configuration();
        FileSystem fs = file.getFileSystem(configuration);

        int numNodes = getNumNodes(file);
        double initialPageRank = 1.0/(double)numNodes;

        OutputStream os = fs.create(targetfile);
        LineIterator iterator = IOUtils.lineIterator(fs.open(file),"UTF8");

        while (iterator.hasNext()){
            String line = iterator.nextLine();

            String[] parts = StringUtils.split(line);

            Node node = new Node().setPageRank(initialPageRank).setAdjacentNodeNames(
                    Arrays.copyOfRange(parts,1,parts.length));
            IOUtils.write(parts[0]+'\t'+node.toString()+'\n',os);

        }
        os.close();
        return numNodes;
    }

    public static int getNumNodes(Path file) throws IOException{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = file.getFileSystem(configuration);

        return IOUtils.readLines(fileSystem.open(file),"UTF8").size();
    }

    public static double calcPageRank(Path inputPath,Path outputPath,int numNodes) throws Exception{
        Configuration conf = new Configuration();

        //conf.setInt();

        Job job = new Job(conf);

        job.setJarByClass(PageRank.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        if (job.waitForCompletion(true)){
            throw new Exception("JOB FAILED");
        }
        long summedConvergence = job.getCounters().findCounter(Reduce.Counter.CONV_DELTAS).getValue();
        double convergence = ((double)summedConvergence/Reduce.CONVERGENCE_SCALING_FACTOR)/(double)numNodes;
        System.out.println("======================================");
        System.out.println("=  Num nodes:           " + numNodes);
        System.out.println("=  Summed convergence:  " + summedConvergence);
        System.out.println("=  Convergence:         " + convergence);
        System.out.println("======================================");

        return convergence;
    }
}
