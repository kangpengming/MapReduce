package Read2CSV;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kp on 16/8/18.
 */
public class MappSchool extends TableMapper<Text, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        Cell[] cells = value.rawCells();
        for (Cell cell:cells) {

            String cloumn = new String(CellUtil.cloneQualifier(cell));
            String valuestr = new String(CellUtil.cloneValue(cell));

                String ouput = cloumn+"$&"+valuestr;

            context.write(new Text(key.get()),new Text(ouput));
        }

    }
}
