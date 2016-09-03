package FoF;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by kp on 16/8/29.
 */
public class PersonNameComparator extends WritableComparator {

    protected PersonNameComparator(){
        super(Person.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        Person p1 = (Person)a;
        Person p2 = (Person)b;
        return p1.getName().compareTo(p2.getName());
    }
}
