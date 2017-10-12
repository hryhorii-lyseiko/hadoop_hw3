package partitioner;

import CustomType.CustomKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomKey, Text> {

    @Override
    public int getPartition(CustomKey key, Text value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}