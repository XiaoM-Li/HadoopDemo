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
		//采用name-m（或r）-nnnnn形式的文件名，name由程序任意设定的名字，nnnnn是一个指明块号的整数（00000开始）
		//output.write(key, new IntWritable(sum),key.toString());
		String basePath=String.format("%s/part/",key.toString());//可以包含文件路径分隔符(/)，可以创建任意深度的子目录
		output.write(key, new IntWritable(sum),basePath);
//		context.write(key, new IntWritable(sum));
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		output.close();
	}
}
