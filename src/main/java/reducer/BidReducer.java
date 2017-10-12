package reducer;

import CustomType.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



public class BidReducer  extends Reducer<CustomKey,Text,Text,IntWritable> {


    protected void reduce(CustomKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count = 0;
        String name = null;
        for(Text value: values) {
            if ( !value.toString().matches("[0-9]") ) {
                name = value.toString();
            } else if (value.toString().matches("[0-9]")) {
                count += Integer.parseInt(value.toString());
            }
        }
        if(count!=0) context.write(new Text(name),new IntWritable(count));

    }
}