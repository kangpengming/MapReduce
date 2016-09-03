package FoF;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by kp on 16/8/29.
 */
public class PersonComparator extends WritableComparator {

    protected PersonComparator(){
        super(Person.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Person p1 = (Person)a;
        Person p2 = (Person)b;

        int cmp = p1.getName().compareTo(p2.getName());

        if(cmp != 0){
            return  cmp;
        }
        return p1.getCommonFriends() == p2.getCommonFriends() ? 0 : (p1
        .getCommonFriends()>p2.getCommonFriends() ? -1 : 1);
    }
}
