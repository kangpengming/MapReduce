package Read2CSV;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kp on 16/8/18.
 */
public class ReduceSchool extends Reducer<Text,Text,Text,Text> {

     String[] stringProperty = null;
     String  seperate = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        stringProperty = context.getConfiguration().getStrings("keys");
        seperate = context.getConfiguration().get("seperate");
    }

    /*String[] stringProperty = {
                        "Student/Teacher Ratio", "Phone", "Magnet", "Hispanic", "Institution Name"
                        ,"District and Couty","Two or More Races","Charter","Type","Total Teachers (FTE)","White, non-Hispanic",
                        "American Indian/Alaska Native","Asian/Pacific Islander*","Institution Type","Black, non-Hispanic","NCES District and school ID",
                        "Total Students","Locale","Mailing Address"
                };*/

        HashMap<String,String> map = new HashMap<String, String>();
        ArrayList<String> list = new ArrayList<String>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        String keystr = "";
        if (key.toString().contains("$")){
            keystr = key.toString().split("\\$")[1];
        }else{
            keystr = key.toString();
        }
        list.clear();
        for (int i = 0; i < stringProperty.length; i++) {
            map.put(stringProperty[i],"");
        }
        String string  = "";
        String[] keyvalue = null;

        for (Text str : values){

           keyvalue = str.toString().split("\\$&");
            map.put(keyvalue[0],keyvalue[1]);
        }
        for (int i = 0;i < stringProperty.length;i++){
            list.add(map.get(stringProperty[i]));
        }
        for (int i = 0; i < list.size(); i++) {
            string = string +seperate+list.get(i);
        }
        context.write(new Text(keystr), new Text(string));
    }
}
