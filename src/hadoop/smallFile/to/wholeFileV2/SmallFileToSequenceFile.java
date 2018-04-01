package hadoop.smallFile.to.wholeFileV2;
/*
 * �������Ĺ����ǽ�һϵ��С�ļ�ת��Ϊһ��˳���ļ������ļ�����Ϊ�����ļ�������Ϊֵ
 * �̳е���CombineFileInputFormat����С�ļ������һ���飬����map�������һ�δ������ļ�
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
