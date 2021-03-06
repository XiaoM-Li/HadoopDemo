package hadoop.totalorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
import java.io.IOException;
 
public class TotalSortV3 extends Configured implements Tool {
    static class SimpleMapper extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            IntWritable intWritable = new IntWritable(Integer.parseInt(key.toString()));
            context.write(key, intWritable);
        }
    }
 
    static class SimpleReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            for (IntWritable value : values)
                context.write(value, NullWritable.get());
        }
    }
 
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(Text.class, true);
        }
 
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            int v1 = Integer.parseInt(w1.toString());
            int v2 = Integer.parseInt(w2.toString());
 
            return v1 - v2;
        }
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Total Order Sorting");
        job.setJarByClass(TotalSortV3.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setSortComparatorClass(KeyComparator.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
 
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
        InputSampler.writePartitionFile(job, sampler);
 
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);
 
        job.setJobName("iteblog");
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TotalSortV3(), args);
        System.exit(exitCode);
    }
}