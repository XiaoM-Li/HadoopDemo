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
 * ������������������������
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
		job.setSortComparatorClass(DataCompare.class);//������������ñȽϺ����Ļ����ͻ�ʹ�ü��Դ��ıȽϺ���
		job.setGroupingComparatorClass(GroupDataCompare.class);//û�����÷���Ƚϣ�ʹ���������õıȽϺ����������û�����þ��ü��ıȽϺ���

		FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);

	}

	static class DataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("����map����,��������Ϊ:" + key + ":" + value);
			context.write(value, NullWritable.get());
			System.out.println("����map����");
		}
	}

	static class DataReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.println("����reduce����,��������Ϊ:" + key);
			context.write(key, NullWritable.get());
			System.out.println("����reduce����");
		}
	}

	static class DataPartitioner extends Partitioner<Text, NullWritable> {

		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			System.out.println("��" + key.toString() + "���з���");
			System.out.println("�������:" + Integer.parseInt(key.toString()) % 2);
			System.out.println("��������");
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
			System.out.println("�Ƚ�" + t1 + "��" + t2);
			System.out.println(t1.compareTo(t2));
			System.out.println("�����Ƚ�");
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
			System.out.println("����Ƚ�" + t1 + "��" + t2);
			System.out.println(t1.compareTo(t2));
			System.out.println("�����Ƚ�");
			return t1.compareTo(t2);
		}
	}

}
