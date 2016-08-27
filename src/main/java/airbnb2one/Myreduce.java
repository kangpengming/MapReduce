package airbnb2one;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.KeyStroke;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Myreduce extends Reducer<Text, Text, Text, Text>{
	
	Map<String,String> map = new HashedMap();
	String[] keystr =  {"airbnb_commentnum","airbnb_edu","airbnb_locality",
			"airbnb_name"};


	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		for(int i = 0;i < keystr.length;i++){
			map.put(keystr[i],"");
		}

		HashSet<String> hostlist = new HashSet<String>();//9111
		HashSet<String> guestlist = new HashSet<String>();//6111

		ArrayList<String> list = new ArrayList<String>();
		for(Text text : arg1){
			String[] keyvalue = text.toString().split("\\$&");
			//System
			if(keyvalue[0].contains("host_comment")){
				if(!keyvalue[1].equals("none")){
					//System.out.println(arg0.toString());
					keyvalue[1]=keyvalue[1]+"\"";
				hostlist.add(keyvalue[1]);
				}
			}else{
			if(keyvalue[0].matches("comment[\\d]")){
				if(!keyvalue[1].equals("none"))
				{
					keyvalue[1]=keyvalue[1]+"\"";
					guestlist.add(keyvalue[1]);
				}
			}else{
			//	System.out.println(arg0.toString()+"========"+text.toString());
				keyvalue[1]=keyvalue[1]+"\"";
			map.put(keyvalue[0], keyvalue[1]);
			}
			}
		}
		
		for(String str : keystr){
			if(str.equals("airbnb_commentnum")){
				String commentNum = map.get(str).replace("(", "").replace(")", "");
				if (commentNum.equals("")){
					commentNum = "none\"";
				}
				list.add(commentNum);
			}else{
				String flagNull = "";
				if((flagNull=map.get(str)).equals(""))
					flagNull = "none\"";
				list.add(flagNull);
			}
		}

		String info = "";
		String host = "";
		String guest = "";

		for(int i = 0;i < list.size();i++){
			info = info + list.get(i);
		}
		for(String str : hostlist){
			host = host+str;
		}
		for(String str : guestlist){
			guest = guest+str;
		}
		String stringkey = arg0.toString() + "\"";
		String information = info + host +guest;
		information = information.substring(0,information.length()-1);
		context.write(new Text(stringkey), new Text(information));
	}
		
}
