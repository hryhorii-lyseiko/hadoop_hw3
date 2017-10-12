import CustomType.CustomKey;
import CustomType.GroupComparator;
import combiner.BidCommbainer;
import mapper.BidPriceMapper;
import mapper.CityMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import partitioner.CustomPartitioner;
import reducer.BidReducer;

public class AppDriver extends Configured implements Tool {

    public int run (String[]args) throws Exception {

        if (args.length != 3) {
            System.err.printf("Usage: %s <input1> <input2> <outputfolder>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Configuration c = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(AppDriver.class);
        job.setJobName("Bid price count per city");

        // input paths
        Configuration conf = new Configuration();
        FileSystem fs= FileSystem.get(conf);
        FileInputFormat.setInputDirRecursive(job,true);
        FileStatus[] status_list = fs.listStatus(new Path(args[0]));
        if(status_list != null){
            for(FileStatus status : status_list){
                MultipleInputs.addInputPath(job, status.getPath(), TextInputFormat.class, BidPriceMapper.class);
            }
        }

        FileStatus[] status_list1 = fs.listStatus(new Path(args[1]));
        if(status_list != null){
            for(FileStatus status : status_list1){
                MultipleInputs.addInputPath(job, status.getPath(), TextInputFormat.class, CityMapper.class);
            }
        }
        //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CityMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setCombinerKeyGroupingComparatorClass(GroupComparator.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setCombinerClass(BidCommbainer.class);
        job.setReducerClass(BidReducer.class);
        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(4);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new AppDriver(), args);
        System.exit(res);
    }
}