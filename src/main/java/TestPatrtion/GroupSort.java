package TestPatrtion;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by kp on 16/9/6.
 */
public  class GroupSort extends WritableComparator{
    //为什么要进行复写？？？
    protected GroupSort(){
        super(Intpair.class,true);
    }


    //compare(Object a,Object b)
    //而返回值是compare(WritableComparable a, WritableComparable b)，看源代码即可明白。

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Intpair a1 = (Intpair)a;
        Intpair b1 = (Intpair)b;

        return a1.getFirst().compareTo(b1.getFirst());
}
}


/*public class GroupSort implements RawComparator<Intpair>{

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);

    }

    public int compare(Intpair o1, Intpair o2) {

        return o1.getFirst().compareTo(o1.getFirst());
    }
}*/
