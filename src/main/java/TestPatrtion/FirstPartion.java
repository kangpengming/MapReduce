package TestPatrtion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kp on 16/9/6.
 */
public class FirstPartion extends Partitioner<Intpair,IntWritable> {
    @Override
    public int getPartition(Intpair intpair, IntWritable intWritable, int numPartitions) {
        System.out.println("----------------");
        return Math.abs(intpair.getFirst().hashCode()&Integer.MAX_VALUE)%numPartitions;
    }
}
