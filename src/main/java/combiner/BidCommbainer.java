package combiner;

import CustomType.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class BidCommbainer 	extends Reducer<CustomKey,Text,CustomKey,Text> {

@Override
protected void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count=0;
        String name ="";


        for(Text value: values) {
        if ( !value.toString().matches("[0-9]") ) {
        name = value.toString();
        } else if (value.toString().matches("[0-9]")) {
        count += Integer.parseInt(value.toString());
        }
        }
        name += " " + count.toString();

        context.write(new CustomKey(key.getCityId()),new Text(name));
        }

        }