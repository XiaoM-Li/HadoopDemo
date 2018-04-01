package hadoop.smallFile.to.wholeFileV2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class WholeFileRecordReader extends RecordReader<Text, Text>{

	private CombineFileSplit combinesplit;
	private Configuration conf;
	private Path[] Paths;
	private int fileNumber;
	private int position=0;
	
	private Text key=new Text();
	private Text value=new Text();
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.combinesplit=(CombineFileSplit) split;
		this.conf=context.getConfiguration();
		this.Paths=combinesplit.getPaths();
		this.fileNumber=combinesplit.getNumPaths();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		if(position<fileNumber){
			byte[] contents=new byte[(int) combinesplit.getLength(position)];
			FileSystem fs=FileSystem.get(conf);
			FSDataInputStream inputStream = fs.open(Paths[position]);
			IOUtils.readFully(inputStream, contents, 0, contents.length);
			key.set(Paths[position].toString());
			value.set(contents, 0, contents.length);
			
			position++;
			return true;
		}
		
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO 自动生成的方法存根
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO 自动生成的方法存根
		
	}

}
