import CustomType.CustomKey;
import combiner.BidCommbainer;
import mapper.BidPriceMapper;
import mapper.CityMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.BidReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MRTest {
    MapDriver<LongWritable, Text, CustomKey, Text> mapDriver1;
    MapDriver<LongWritable, Text, CustomKey, Text> mapDriver2;
    ReduceDriver<CustomKey,Text,Text,IntWritable> reduceDriver;
    ReduceDriver<CustomKey,Text,CustomKey,Text> combineDriver;
    MapReduceDriver<LongWritable, Text, CustomKey, Text,CustomKey,Text> mapReduceDriver;

    @Before
    public void setUp() {
        BidPriceMapper mapper1= new  BidPriceMapper();
        CityMapper mapper2= new  CityMapper();
        BidReducer reducer = new BidReducer();
        BidCommbainer combiner = new BidCommbainer();
        mapDriver1 = MapDriver.newMapDriver(mapper1);
        mapDriver2 = MapDriver.newMapDriver(mapper2);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        combineDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombiner1() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("1"));
        values.add(new Text("1"));
        combineDriver.withInput(new CustomKey(11), values);
        combineDriver.withOutput(new CustomKey(11), new Text(" 2"));
        combineDriver.runTest();
    }

    @Test
    public void testCombiner2() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("Hirosima"));
        combineDriver.withInput(new CustomKey(11), values);
        combineDriver.withOutput(new CustomKey(11), new Text("Hirosima 0"));
        combineDriver.runTest();
    }


    @Test
    public void testMapper1() throws IOException {

        mapDriver1.withInput(new LongWritable(1), new Text( "1\t2\t3\t4\t5\t6\t2\t2\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t19\t300\t21"));
        mapDriver1.withOutput(new CustomKey(2), new Text("1"));
        mapDriver1.runTest();
    }

    @Test
    public void testMapper2() throws IOException {

        mapDriver2.withInput(new LongWritable(0), new Text("2\ttianjin"));
        mapDriver2.withOutput(new CustomKey(2), new Text("tianjin"));
        mapDriver2.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("Hirosima"));
        values.add(new Text("2"));
        reduceDriver.withInput(new CustomKey(2), values);
        reduceDriver.withOutput(new Text("Hirosima"), new IntWritable(2));
        reduceDriver.runTest();
    }

}