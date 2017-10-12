package mapper;

import CustomType.CustomKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CityMapper extends Mapper<LongWritable, Text, CustomKey, Text> {



    CustomKey customKey;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String values = value.toString();
        List<String> cities = Arrays.asList(values.split("\t"));

        if (cities.size() == 2){
            customKey = new CustomKey((Integer.parseInt(cities.get(0))));
            context.write(customKey,new Text(cities.get(1)));
        }
    }

}