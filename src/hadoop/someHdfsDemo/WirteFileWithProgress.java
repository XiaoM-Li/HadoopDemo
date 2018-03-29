package hadoop.someHdfsDemo;
/*
 * 写文件，并且显示进度
 */
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class WirteFileWithProgress {

	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://itcast00:9000"), conf);
		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/progress.txt"),new Progressable() {//匿名内部类实现
			
			@Override
			public void progress() {
				System.out.print(".");
			}
		});
		InputStream in=new FileInputStream("D://a.txt");
		IOUtils.copyBytes(in, fsDataOutputStream, conf);
				
			
	}

}
