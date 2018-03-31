package hadoop.mrunit.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.test.MapredTestDriver;
import org.junit.Test;

public class MRUnitTest {

	@Test
	public void testMapper() throws IOException {
		Text value = new Text("hello world");
		MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.withMapper(new WCMapper());
		mapDriver.withInput(new LongWritable(0), value);
		List<Pair<Text, IntWritable>> outputRecords=new ArrayList<>();
		Pair<Text, IntWritable> pair1=new Pair<Text, IntWritable>(new Text("hello"), new IntWritable(1));
		Pair<Text, IntWritable> pair2=new Pair<Text, IntWritable>(new Text("world"), new IntWritable(1));
		outputRecords.add(pair1);
		outputRecords.add(pair2);
		// mapDriver.withOutput(new Text("hello"),new IntWritable(1));
		// mapDriver.withOutput(new Text("world"), new IntWritable(1));
		mapDriver.withAllOutput(outputRecords);
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException{
		Text key=new Text("hello");
		List<IntWritable> list = Arrays.asList(new IntWritable(1),new IntWritable(2),new IntWritable(3));
		ReduceDriver<Text, IntWritable,Text, IntWritable> reduceDriver=new ReduceDriver<>();
		reduceDriver.withReducer(new WCReducer());
		reduceDriver.withInput(key, list);
		reduceDriver.withOutput(new Text("hello"),new IntWritable(6));
		reduceDriver.runTest();
	}

}
