package hadoop.wordcount.differentInput;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String line=value.toString();
		String[] words=StringUtils.split(line,"\t");
		for(String word:words){
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
