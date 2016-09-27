package putData;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by kp on 16/9/26.
 */
public class MapTable extends TableMapper<Text,Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Cell[] cells = value.rawCells();
        for(Cell cell:cells){
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String valueStr = new String(CellUtil.cloneValue(cell));

            String outValue = family +"$&"+column +"$&" + valueStr;
            context.write(new Text(key.get()),new Text(outValue));
        }
    }
}
