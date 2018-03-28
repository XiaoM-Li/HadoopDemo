package hadoop.totalorder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalSortV2 {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(TotalSortV2.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		InputSampler.Sampler<Text, Text> sampler=new InputSampler.RandomSampler<>(0.1, 1000, 10);
		TotalOrderPartitioner.setPartitionFile(conf, new Path(args[2]));
		InputSampler.writePartitionFile(job, sampler);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setNumReduceTasks(4);

		job.waitForCompletion(true);
	}

}
