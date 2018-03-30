package hadoop.compress.demo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class CompressionDemo4 {

	public static void main(String[] args) throws Exception {
		// TODO �Զ����ɵķ������
		InputStream in=new FileInputStream("D://join.sql");
		CompressionCodec codec=new GzipCodec();
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://itcast00:9000"), conf);
		FSDataOutputStream outputStream = fs.create(new Path("/join.gz"));
		CompressionOutputStream compressionOutputStream = codec.createOutputStream(outputStream);
		IOUtils.copyBytes(in, compressionOutputStream, 4096, false);
		compressionOutputStream.finish();//Ҫ��һ��finish��Ҫ��ѹ��������ɵ�ѹ���������ķ�����

	}

}
