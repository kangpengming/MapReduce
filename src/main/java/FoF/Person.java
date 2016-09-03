package FoF;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kp on 16/8/29.
 */
public  class Person implements WritableComparable<Person> {
    private String name = "";
    private long commonFriends = 0;

    public Person(){

    }

    public Person(String name,long commonFriends){
        this.name = name;
        this.commonFriends = commonFriends;
    }

    public void set(String name,long commonFriends){
        this.name = name;
        this.commonFriends = commonFriends;
    }

    public String getName(){
        return this.name;
    }

    public long getCommonFriends(){
        return this.commonFriends;
    }

    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.commonFriends = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
            out.writeUTF(name);
            out.writeLong(this.commonFriends);
    }

    public int compareTo(Person other) {
        if(this.name.compareTo(other.name)!=0){
            return  this.name.compareTo(other.name);
        }else if(this.commonFriends != other.commonFriends){
            return commonFriends<other.commonFriends?-1:1;
        }else{
        return 0;
        }
    }
    public  static class PersonKeyComparator extends WritableComparator{

        public PersonKeyComparator(){
            super(Person.class);
        }

        @Override
        public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2, int l2) {
            return super.compare(b1,s1,l1, b2,s2,l2);
        }
    }
    static {
        WritableComparator.define(Person.class,new PersonKeyComparator());
    }
}

