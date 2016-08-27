package airbnb2one;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MyMapper extends TableMapper<Text, Text>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//context.
	//	super.map(key, value, context);
		
		
		String strValue = null;
		String family = null;
		String quality = null;
		String str = null;
		String keybak = null;

		Cell[] cells = value.rawCells();
		for(Cell cell :cells){
			if (new String(key.get()).contains("100020"))
				System.out.println(new String(key.get()));
			keybak = Bytes.toString(key.get()).split("\\$")[1];

			family = new String(CellUtil.cloneFamily(cell));
			quality = new String(CellUtil.cloneQualifier(cell));
			strValue =  new String(CellUtil.cloneValue(cell));
			if (strValue.equals("")){
				strValue = "none";
			}
			str = quality+"$&"+strValue;
			if(family.equals("property")){
				context.write(new Text(keybak), new Text(str));
			}
			}
	}
}
