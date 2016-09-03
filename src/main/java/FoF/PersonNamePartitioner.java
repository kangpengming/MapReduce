package FoF;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kp on 16/8/29.
 */
public class PersonNamePartitioner  extends Partitioner<Person,Person>{
    @Override
    public int getPartition(Person person, Person person2, int numPartitions) {
        return Math.abs(person.getName().hashCode()*127)%numPartitions;
    }
}
