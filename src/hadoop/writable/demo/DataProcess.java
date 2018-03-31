package hadoop.writable.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 跟踪数据排序分区分组的流程
 */

public class DataProcess {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(DataProcess.class);

		job.setMapperClass(DataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setPartitionerClass(DataPartitioner.class);
		job.setNumReduceTasks(2);
		job.setSortComparatorClass(DataCompare.class);//这里如果不设置比较函数的话，就会使用键自带的比较函数
		job.setGroupingComparatorClass(GroupDataCompare.class);//没有设置分组比较，使用上面设置的比较函数，上面的没有设置就用键的比较函数

		FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);

	}

	static class DataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("进入map函数,处理数据为:" + key + ":" + value);
			context.write(value, NullWritable.get());
			System.out.println("结束map函数");
		}
	}

	static class DataReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("进入reduce函数,处理数据为:" + key);
			context.write(key, NullWritable.get());
			System.out.println("结束reduce函数");
		}
	}

	static class DataPartitioner extends Partitioner<Text, NullWritable> {

		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			System.out.println("对" + key.toString() + "进行分区");
			System.out.println("分区结果:" + Integer.parseInt(key.toString()) % 2);
			System.out.println("结束分区");
			return Integer.parseInt(key.toString()) % 2;
		}

	}

	static class DataCompare extends WritableComparator {

		protected DataCompare() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			Text t1 = (Text) a;
			Text t2 = (Text) b;
			System.out.println("比较" + t1 + "和" + t2);
			System.out.println(t1.compareTo(t2));
			System.out.println("结束比较");
			return t1.compareTo(t2);
		}
	}

	static class GroupDataCompare extends WritableComparator {

		protected GroupDataCompare() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			Text t1 = (Text) a;
			Text t2 = (Text) b;
			System.out.println("分组比较" + t1 + "和" + t2);
			System.out.println(t1.compareTo(t2));
			System.out.println("结束比较");
			return t1.compareTo(t2);
		}
	}

}
