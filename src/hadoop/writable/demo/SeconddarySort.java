package hadoop.writable.demo;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SeconddarySort {

	public static void main(String[] args) throws Exception {
		
		Job job=Job.getInstance(new Configuration());
		job.setJarByClass(SeconddarySort.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(2);
		job.setSortComparatorClass(MyCompare.class);
		job.setGroupingComparatorClass(GroupSort.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		

		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
	
	static class MyMapper extends Mapper<LongWritable,Text, TextPair, Text>{
		

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			System.out.println("-------进入map函数----------");
			String line=value.toString();
			String[] fields=StringUtils.split(line, "\t");
			TextPair keyOut=new TextPair(fields[0], fields[1]);
			Text valueOut=new Text(fields[1]);
			context.write(keyOut, valueOut);
			System.out.println("-------结束map函数-----------");
			
		}
	}
	
	static class MyReducer extends Reducer<TextPair, Text, Text, Text>{
		@Override
		protected void reduce(TextPair key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			System.out.println("------进入reduce函数-------");
			StringBuffer stringBuffer=new StringBuffer();
			for(Text value:values){
				context.write(key.getFirst(), value);
				stringBuffer.append(value+" ");
			}
			
			System.out.println("----------"+key.toString()+"\t"+stringBuffer.toString());
			System.out.println("------结束reduce函数-------");
			
		}
	}
	
	static class MyPartitioner extends Partitioner<TextPair, Text>{

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			System.out.println("-------进入自定义分区-------");
			System.out.println("-------结束自定义分区-------");
//			return (key.getFirst().hashCode()&Integer.MAX_VALUE)%numPartitions;
			return Integer.parseInt(key.getFirst().toString()) % 2;
		}

	}
}
