package hadoop.smallFile.to.wholeFileV2;
/*
 * 这个程序的功能是将一系列小文件转换为一个顺序文件，以文件名作为键，文件内容作为值
 * 继承的是CombineFileInputFormat，将小文件先组成一个块，这样map任务可以一次处理多个文件
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SmallFileToSequenceFile {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job=Job.getInstance(new Configuration());
		
		job.setJarByClass(SmallFileToSequenceFile.class);
		
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

}
