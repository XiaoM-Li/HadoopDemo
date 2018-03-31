package hadoop.compress.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
//		conf.set("mapreduce.output.fileoutputformat.compress", "true");
//		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
//		conf.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");//Ĭ����ѹ��һ����¼��RECORD
		//��conf��ֱ�������Ƿ�ѹ�����Լ�ѹ�������ͣ�����ʹ������ĸ��򵥵ķ��������о��ǿͻ������г����ʱ��Ӳ���
		
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
		
//		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
//		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
//		
		Job job=Job.getInstance(conf);

		job.setJarByClass(WordCount.class);

		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("d://a.txt"));
		FileOutputFormat.setOutputPath(job, new Path("d://result"));

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);//Ĭ�����ÿ����¼ѹ��

		job.waitForCompletion(true);

	}
	
	static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] strings=line.split(" ");
			for(String s:strings){
				context.write(new Text(s), new IntWritable(1));
			}
			
		}
			
		
		
	}
	
	static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			// TODO �Զ����ɵķ������
			int sum=0;
			for(IntWritable v:values){
				sum=sum+v.get();
			}
			context.write(key, new IntWritable(sum));
		}
				
	}
	

}
