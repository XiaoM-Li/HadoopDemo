package hadoop.multiout;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private MultipleOutputs<Text,IntWritable> output;
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		output=new MultipleOutputs<>(context);
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable v:values){
			sum+=v.get();
		}
		output.write(key, new IntWritable(sum),key.toString());
//		context.write(key, new IntWritable(sum));
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		output.close();
	}
}
