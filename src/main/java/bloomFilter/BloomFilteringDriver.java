package bloomFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by kp on 16/9/2.
 */

/**
 * 利用map将大表中的数据进行过滤，因为大表中的数据过于大，会导致无法存储到内存当中，这时候也需要将每个小表的文件写入到各个mapTask中。
 *但是布隆过滤器是有错误率的，所以这时候需要在reduce阶段进行join，即联合阶段，因为在map阶段已经将大表不存在的数据已经进行过滤。
 * */
public class BloomFilteringDriver {
    public class bloomMap extends Mapper<Object, Text, Text, NullWritable> {


        private BloomFilter bloomFilter = new BloomFilter();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;

            try {
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                for (Path path : paths) {
                    if (path.toString().contains("bloom.bin")) {
                        DataInputStream strm = new DataInputStream(new FileInputStream(path.toString()));
                        bloomFilter.readFields(strm);
                        strm.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String comment = value.toString();

            if (comment == null || comment.isEmpty()) {
                return;
            }
            //构造一个字符串的标识器，默认是Tab
            StringTokenizer tokenizer = new StringTokenizer(comment);
            //循环标识器里面的字符串
            while (tokenizer.hasMoreTokens()) {
                String cleanWord = tokenizer.nextToken().replaceAll("'", "")
                        //^符号在[]外面则标识以。。。开头，在里面标识匹配不包含。。。（在这里指不包含字母的字符进行匹配）
                        .replaceAll("[^a-zA-Z]", " ");
                if (cleanWord.length() > 0 && bloomFilter.membershipTest(new Key(cleanWord.getBytes()))) {
                    context.write(new Text(cleanWord), NullWritable.get());
                }
            }
        }
    }

    public static void main(String... strings) throws Exception{

        Configuration configuration = new Configuration();
        //GenericOptionsParser主要目的是将命令行的参数写入到配置文件中，因为每次运行一个程序就需要重写一次MR程序，写配置、编译、打包浪费时间。
        // 使用这个方法可以直接将需要的参数写入到配置的文件中
        /**
         * 例如，将设置里面reduce的数目：bin/hadoop jar MyJob.jar com.xxx.MyJobDriver -Dmapred.reduce.tasks=5
         * 其它常用的参数还有”-libjars”和-“files”：bin/hadoop jar MyJob.jar com.xxx.MyJobDriver -Dmapred.reduce.tasks=5 \
         -files ./dict.conf  \
         -libjars lib/commons-beanutils-1.8.3.jar,lib/commons-digester-2.1.jar
         * */
        String[] otherArgs = new GenericOptionsParser(configuration,strings).getRemainingArgs();

        System.out.println("===============" + strings[0]);
        if(otherArgs.length != 3){
            System.err.println("Usage: BloomFultering <in> <out>");
            System.exit(1);
        }

        FileSystem.get(configuration).delete(new Path(otherArgs[2]), true);

        Job job = new Job(configuration,"TestBloomFiltering");
        job.setJarByClass(BloomFilteringDriver.class);
        job.setMapperClass(bloomMap.class);
        job.setNumReduceTasks(0);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //"/tmp/decli/bloom.bin"
        DistributedCache.addCacheFile(new Path("").toUri(),configuration);

        //主要功能是报出是否是异常。
        System.exit(job.waitForCompletion(true)?0:1);
    }
}