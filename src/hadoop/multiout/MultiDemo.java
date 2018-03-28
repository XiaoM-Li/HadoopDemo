package hadoop.multiout;
/*
 * 这个程序主要是将reduce输出到多个文件，利用MultipleOutputs实现
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiDemo {

	public static void main(String[] args) throws Exception {
		
		Job job=Job.getInstance(new Configuration());
		job.setJarByClass(MultiDemo.class);
		
		job.setMapperClass(SpaceMapper.class);
		job.setReducerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
