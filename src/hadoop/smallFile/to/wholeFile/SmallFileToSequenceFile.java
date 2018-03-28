package hadoop.smallFile.to.wholeFile;
/*
 * 这个程序的功能是将一系列小文件转换为一个顺序文件，以文件名作为键，文件内容作为值
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SmallFileToSequenceFile {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job=Job.getInstance(new Configuration());
		
		job.setJarByClass(SmallFileToSequenceFile.class);
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		WholeFileInputFormat.setInputPaths(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.waitForCompletion(true);
	}
	public static class MyMapper extends Mapper<NullWritable, Text, Text, Text>{
		private Text fileNameKey;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			InputSplit inputSplit = context.getInputSplit();
			FileSplit fileSplit=(FileSplit) inputSplit;
			String filePath = fileSplit.getPath().toString();
			fileNameKey=new Text(filePath);
		}
		@Override
		protected void map(NullWritable key, Text value,Context context)throws IOException, InterruptedException {
			context.write(fileNameKey, value);
		}
	}

}
