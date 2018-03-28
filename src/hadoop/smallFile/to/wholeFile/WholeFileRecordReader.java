package hadoop.smallFile.to.wholeFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<NullWritable, Text>{//将一个文件作为一条记录读出

	private FileSplit fileSpit;
	private Configuration conf;
	private Text value=new Text();
	private boolean processed=false;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSpit=(FileSplit) split;
		this.conf=context.getConfiguration();		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		if(!processed){
			byte[] contents=new byte[(int) fileSpit.getLength()];
			Path file=fileSpit.getPath();
			FileSystem fs=file.getFileSystem(conf);
			FSDataInputStream inputStream =null;
			try{
				inputStream= fs.open(file);
				IOUtils.readFully(inputStream, contents, 0,contents.length);
				value.set(contents, 0, contents.length);
			}finally {
				IOUtils.closeStream(inputStream);
			}
			processed=true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		return processed?1.0f:0.0f;
	}

	@Override
	public void close() throws IOException {
		// TODO 自动生成的方法存根
		
	}

}
