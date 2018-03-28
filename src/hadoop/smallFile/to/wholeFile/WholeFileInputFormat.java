package hadoop.smallFile.to.wholeFile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, Text>{

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		WholeFileRecordReader reader=new WholeFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO 自动生成的方法存根
		return false;//文件不可切分
	}



}
