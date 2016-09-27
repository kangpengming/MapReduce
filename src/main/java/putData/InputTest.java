package putData;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by kp on 16/9/26.
 */
public class InputTest {
    private static Configuration conf = null;
    static {
        conf = new Configuration();

        conf.set("hbase.zookeeper.quorum", "192.168.1.230");
        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.rootdir","hdfs://master:8020/hbase");
        conf.set("hbase.master", "hdfs://master:60000");
    }

    public void inputData() throws Exception{

        Job job = new Job(conf,"InpuData");
        Scan scan = new Scan();

        job.setJarByClass(InputTest.class);
        TableMapReduceUtil.initTableMapperJob("12111", scan, MapTable.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("12111",TableReduce.class,job);
        job.waitForCompletion(true);
    }
}
