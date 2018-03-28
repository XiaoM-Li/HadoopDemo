package hadoop.totalorder;
/*
 * 这个包里的程序是实现全排序的几种方法
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
 
public class TotalSortV1 {
    static class SimpleMapper extends
            Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            IntWritable intWritable = new IntWritable(Integer.parseInt(value.toString().trim()));
            context.write(intWritable, intWritable);
        }
    }
 
    static class SimplePartioner extends Partitioner<IntWritable, IntWritable>{

		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			int number=key.get();
			if(number<10000){
				return 0;
			}else if(number>=10000&&number<20000){
				return 1;
			}else{
				return 2;
			}
		}
    	
    }
    
    static class SimpleReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            for (IntWritable value : values)
                context.write(value, NullWritable.get());
        }
    }
 
 
    public static void main(String[] args) throws Exception {
    	 Job job = Job.getInstance(new Configuration());
         job.setJarByClass(TotalSortV1.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
         job.setMapperClass(SimpleMapper.class);
         job.setReducerClass(SimpleReducer.class);
         job.setPartitionerClass(SimplePartioner.class);
         job.setMapOutputKeyClass(IntWritable.class);
         job.setMapOutputValueClass(IntWritable.class);
         job.setOutputKeyClass(IntWritable.class);
         job.setOutputValueClass(NullWritable.class);
         job.setNumReduceTasks(3);
         
         job.waitForCompletion(true);
    }
}