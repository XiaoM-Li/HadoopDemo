package hadoop.keyvalueInput;
/*
 * MapReduce程序输入sequenceFile
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
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://itcast00:9000/r2/part-r-00000"));//这个文件是每个小文件转换为一条记录，最终存为顺序文件
		FileOutputFormat.setOutputPath(job, new Path("d://result"));
		
		job.waitForCompletion(true);

	}

}
