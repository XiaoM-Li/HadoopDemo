package hadoop.joinDemos;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SimpleJoinV2 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new SimpleJoinV2(), args);

	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.addCacheFile(new URI(args[0]));//或者使用-files配置也可以
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)?1:0;
	}
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			String line=value.toString();
			String[] fields=line.split("\t");
			context.write(new Text(fields[0]), new Text(fields[1]));
		}
	}
	
	public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
		private Map<Text, Text> studentMap=new HashMap<>();
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream("student_info.txt")));
			String line;
			while((line=reader.readLine())!=null){
				String[] fields=line.split("\t");
				studentMap.put(new Text(fields[1]),new Text(fields[0]));
			}
			
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			// TODO 自动生成的方法存根
			Text name=studentMap.get(key);
			for(Text v:values){
				context.write(key, new Text(name.toString()+"\t"+v.toString()));
				
			}
			
		}
	}

	

}
