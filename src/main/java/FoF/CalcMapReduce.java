package FoF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kp on 16/8/27.
 */
public class CalcMapReduce {

    public static class TextPair extends Text{
        public static char seperate = '\t';

        public TextPair(){
            super();
        }

        public TextPair(String person1,String person2){
            super(joinPersonsLexicographically(person1, person2));
        }

        public void set(String person1,String person2){
            super.set(joinPersonsLexicographically(person1,person2));
        }

        public  static String joinPersonsLexicographically(String person1,String person2){
            if (person1.compareTo(person2) < 0){
                return person1 + seperate + person2;
            }
            return person2 + seperate +person1;
        }
    }

    public class Map extends Mapper<Text,Text,TextPair,IntWritable>{

        private  TextPair pair = new TextPair();
        private IntWritable one = new IntWritable(1);
        private IntWritable two = new IntWritable(2);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] friends = StringUtils.split(value.toString());

            for (int i = 0; i < friends.length; i++) {

                pair.set(key.toString(),friends[i]);
                context.write(pair,one);

                for (int j = i + 1; j <friends.length; j++) {
                    pair.set(friends[i],friends[j]);
                    context.write(pair,two);
                }
            }
        }
    }

    public class Reduce extends Reducer<TextPair,IntWritable,TextPair,IntWritable>{

        private IntWritable friendsInCommon = new IntWritable();

        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int commonfriends = 0;
            boolean alreadyFriends = false;

            for (IntWritable hops:values){
                if (hops.get() == 1){
                    alreadyFriends = true;
                    break;
                }
                commonfriends++;
            }
            if(!alreadyFriends){
                friendsInCommon.set(commonfriends);
                context.write(key,friendsInCommon);
            }
        }
    }
}
