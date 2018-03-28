package hadoop.fileDataStructre;
/*
 * ���������Ҫ��SequenceFile��д����
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
		FileSystem fs=FileSystem.get(conf);//�����Ŀ���������ļ��Ļ�configuration�ͻ��ȡ�����ļ����ҵ���Ӧ���ļ�ϵͳ���ã�û�еĻ�Ĭ��ʹ�ñ����ļ�ϵͳ,����ʹ������ķ��������ļ�ϵͳʵ��
		IntWritable key=new IntWritable();
		Text value=new Text();
		SequenceFile.Writer writer=SequenceFile.createWriter(fs, conf, new Path("d://seqfile"), IntWritable.class, Text.class);
		for(int i=0;i<DATA.length;i++){
			key.set(i);
			value.set(DATA[i]);
			writer.append(key, value);
		}
		
		IOUtils.closeStream(writer);//������һ��Ļ����ܿ����������û�аѻ������Ľ��ˢ����
	}

}
