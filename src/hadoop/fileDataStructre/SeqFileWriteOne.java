package hadoop.fileDataStructre;
/*
 * 这个程序主要是SequenceFile的写操作
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SeqFileWriteOne {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] DATA = { "one,two,three", "four,five,six", "seven,nine,ten", "hello,wolrd", "good,morning" };
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);//如果项目中有配置文件的话configuration就会读取配置文件，找到相应的文件系统配置，没有的话默认使用本地文件系统,或者使用另外的方法配置文件系统实例
		IntWritable key=new IntWritable();
		Text value=new Text();
		SequenceFile.Writer writer=SequenceFile.createWriter(fs, conf, new Path("d://seqfile"), IntWritable.class, Text.class);
		for(int i=0;i<DATA.length;i++){
			key.set(i);
			value.set(DATA[i]);
			writer.append(key, value);
		}
		
		IOUtils.closeStream(writer);//不加这一句的话可能看不到结果，没有把缓冲区的结果刷出来
	}

}
