package hadoop.keyvalueInput;
/*
 * MapReduce≥Ã–Ú ‰»ÎsequenceFile
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeyValueInputDemo {

	public static void main(String[] args) throws Exception {
		
		Job job=Job.getInstance(new Configuration());
		
		job.setJarByClass(KeyValueInputDemo.class);
		
		job.setMapperClass(KVMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("d://seqfile"));
		FileOutputFormat.setOutputPath(job, new Path("d://result"));
		
		job.waitForCompletion(true);

	}

}
