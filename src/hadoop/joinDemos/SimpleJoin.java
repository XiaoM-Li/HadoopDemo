package hadoop.joinDemos;
/*
 * 这个程序主要作用是把student_info.txt与class_info.txt进行连接，二者共同的一个字段是ID。
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimpleJoin {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		Job job=Job.getInstance(configuration);
		
		job.setJarByClass(SimpleJoin.class);
		
		job.setMapperClass(JoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);

	}
	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		FileSplit fileSplit=(FileSplit) context.getInputSplit();
		String filePath=fileSplit.getPath().toString();
		String fileType="";
		String[] fields=value.toString().split("\t");
		Text k=null;
		Text v=null;
		if(StringUtils.contains(filePath,"student_info.txt")){
			fileType="name";
			k=new Text(fields[1]);
			v=new Text(fields[0]+"\t"+fileType);			
		}
		else{
			fileType="class";
			k=new Text(fields[0]);
			v=new Text(fields[1]+"\t"+fileType);	
		}
		context.write(k, v);
		}
	}
	public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			ArrayList<String> list=new ArrayList<String>();
			String name="";
			for(Text value:values){
				String[] fields=value.toString().split("\t");
				String type=fields[1];
				if(type.contains("name")){
					name=fields[0];
				}
				if(type.contains("class")){
					list.add(fields[0]);
				}
			}
			for(String s:list){
				context.write(new Text(name), new Text(s));
			}
		}
	}

}
