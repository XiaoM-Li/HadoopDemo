package hadoop.someHdfsDemo;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemDoubleCat {

	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://itcast00:9000"), configuration);
		FSDataInputStream inputStream = fs.open(new Path("/input.txt"));
		IOUtils.copyBytes(inputStream, System.out, 4096,false);
		
		
		inputStream.seek(0);//seek()�����ƶ����ļ�������һ������λ��
		IOUtils.copyBytes(inputStream, System.out, 4096, false);
		IOUtils.closeStream(inputStream);
		
		fs.copyToLocalFile(new Path("/input.txt"), new Path("d://input.txt"));
		//��windows������Ĭ�ϵı����ļ�ϵͳ��Windows�����ļ�ϵͳ
		

	}

}
