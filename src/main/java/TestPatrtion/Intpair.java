package TestPatrtion;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kp on 16/9/6.
 */
public  class Intpair implements WritableComparable<Intpair> {
        private String first = "";
        private int second = 0;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.first);
        out.writeInt(this.second);
    }

    public int compareTo(Intpair o) {
        if(this.first.compareTo(o.first)!=0){
            return this.first.compareTo(o.first);
        }else{
            if(this.second != o.second){
                return this.second - o.second;
            }else{
                return 0;
            }
        }
    }
}
