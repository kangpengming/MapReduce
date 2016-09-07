package FoF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kp on 16/8/29.
 */
public class SortJob {
    public static class Map extends Mapper<Text,Text,Person, Person>{

        private Person outputkey = new Person();
        private Person outputvalue = new Person();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = StringUtils.split(value.toString());
            String name = parts[0];
            int commonFriends = Integer.valueOf(parts[1]);

            outputkey.set(name,commonFriends);
            outputvalue.set(key.toString(),commonFriends);
            context.write(outputkey,outputvalue);
        }
    }
    public static class Reduce extends Reducer<Person,Person,Text,Text>{

        private Text name = new Text();
        private Text potentialFriends = new Text();

        @Override
        protected void reduce(Person key, Iterable<Person> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            context.write(new Text("-=-----------------"),null);
            int count = 0;
            for(Person potentialFriend : values){
                    if (sb.length() > 0){
                        sb.append(",");
                    }
                sb.append(potentialFriend.getName()).append(":").append(potentialFriend.getCommonFriends());

                if(++count==10){
                    break;
                }
            }
            name.set(key.getName());
            potentialFriends.set(sb.toString());
            context.write(name,potentialFriends);
        }
    }
}
