package hadoop.wordcount.differentInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiDemo {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance(conf);
		job.setJarByClass(MultiDemo.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SpaceMapper.class);//不同的文件设置不同的mapper
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TMapper.class);
		
		job.setReducerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);

	}

}
