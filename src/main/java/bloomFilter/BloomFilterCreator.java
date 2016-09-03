package bloomFilter;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * Created by kp on 16/9/2.
 *
 * 布隆滤波器：
 * （1）初始化m个位数组，将每个数组标识为0
 * （2）集合中的n个元素
 * （3）k个相互独立的hash函数
 * （4）将每个元素进行hash，可以得到一组索引值，
 * 然后根据索引值将上述的位数组的位置标记为1.不同元素如果位数组的位置一样，则进行覆盖。
 * （5）用途：判断一个数据是否在该集合中，如果对应的k个hash值没有对应的索引1，则一定不在；
 * 否则不一定存在，因为都落在了1点，但是元素不在集合中。
 * 确定hash的个数：K=ln2*(m/n);
 * 在给定错误率的情况下：m>=(log(e))2*(n*log2(1/E)),然后确定hash函数的个数
 *优势：利用空间与时间的转换来提高效率
 */

/***
 * 该类的主要意义是将小表中的数据进行训练，作为布隆滤波器的对比样本。
 * */
public class BloomFilterCreator extends TestCase {
    //根据错误率确位数组的个数
    public static int getOptiomalBloomFilterSize(int numRecords,float falsePosRate){
        //这里面的log是指以e为底的对数
        int size = (int)(-numRecords * (float)Math.log(falsePosRate)/Math.pow(Math.log(2),2));
        return size;
    }

    public static int getOptimalK(float numMembers,float vectorSize){
        return (int)Math.round(vectorSize/numMembers * Math.log(2));
    }

    public void TestBloom(String... strings) throws IOException{
        //      "/tmp/decli/user1.txt"
        Path inputFile = new Path("");
        //定义位数组
        int numMembers = Integer.parseInt("10");
        //定义错误率
        float falsePosRate = Float.parseFloat("0.01");
        //      "/tmp/decli/bloom.bin"
        Path bfFile = new Path("");
        //位数组数
        int vectorSize = getOptiomalBloomFilterSize(numMembers, falsePosRate);
        //hash的个数
        int nbHash = getOptimalK(numMembers, vectorSize);

        BloomFilter bloomFilter = new BloomFilter(vectorSize,nbHash, Hash.MURMUR_HASH);

        //读取分布式系统上面的文件
        System.out.println("Training Bloom filter of size " + vectorSize
                        + " with " + nbHash + " hash functions, " + numMembers
                        + " approximate number of records, and " + falsePosRate
                        + " false positive rate");

        String line = null;
        int numRecords = 0;
        FileSystem fs = FileSystem.get(new Configuration());
        //FileStatus记录下每个文件的信息。
        for (FileStatus status : fs.listStatus(inputFile)){
            BufferedReader rdr;
            if(status.getPath().getName().endsWith("gz")){
                rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));
            }else{
                rdr = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            }

            System.out.println("Reading"+status.getPath());
            while((line = rdr.readLine())!=null){
                bloomFilter.add(new Key(line.getBytes()));
                ++numRecords;
            }

            rdr.close();
        }

        System.out.println("Trained Bloom filter with " + numRecords + " entries.");
        System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
        //a |= b; a= a|b;
        FSDataOutputStream stream = fs.create(bfFile);
        bloomFilter.write(stream);

        stream.flush();
        stream.close();

        System.out.println("Done training Bloom filter.");
    }
}
/**
 * 测试方式与结果：
 * user1.txt

 test
 xiaowang
 xiao
 wang
 test

 user2.txt

 test xiaowang
 xiao wang test
 test1 2xiaowang
 1xiao wa2ng atest


 运行命令：

 hadoop jar trainbloom.jar TrainingBloomfilter
 hadoop jar bloom.jar BloomFilteringDriver /tmp/decli/user2.txt /tmp/decli/result

 结果：

 root@master 192.168.120.236 ~/lijun06 >
 hadoop fs -cat /tmp/decli/result/p*
 test
 xiaowang
 xiao
 wang
 test
 * */