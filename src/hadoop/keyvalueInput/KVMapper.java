package hadoop.keyvalueInput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVMapper extends Mapper<IntWritable, Text, IntWritable, Text>{
	@Override
	protected void map(IntWritable key, Text value,Context context)throws IOException, InterruptedException {
		context.write(key, value);
	}

}
