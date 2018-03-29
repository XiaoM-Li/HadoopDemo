package hadoop.someHdfsDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class GetLocal {

	public static void main(String[] args) throws IOException {
		FileSystem fSystem=null;
		Configuration conf=new Configuration();
		fSystem=FileSystem.getLocal(conf);//��ȡ��һ�������ļ���ʵ��,������ֱ��get(conf)
		FSDataInputStream open = fSystem.open(new Path("d://a.txt"));
		IOUtils.copyBytes(open, System.out, 4096, false);
		IOUtils.closeStream(open);

	}

}
