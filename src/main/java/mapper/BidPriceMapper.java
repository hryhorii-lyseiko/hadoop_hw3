package mapper;

import CustomType.CustomKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import eu.bitwalker.useragentutils.UserAgent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class BidPriceMapper extends Mapper<LongWritable, Text, CustomKey, Text> {

    CustomKey customKey ;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            List<String> values = Arrays.asList(line.split("\t"));

            int cityId = Integer.parseInt(values.get(7));
            int bid = Integer.parseInt(values.get(19));

            //UserAgent userAgent = new UserAgent(values[4].toString());
            //String os_type = userAgent.getOperatingSystem().getName();


            if (bid >= 250) {
                customKey = new CustomKey(cityId);
                context.write(customKey, new Text("1"));
            }

    }
}

