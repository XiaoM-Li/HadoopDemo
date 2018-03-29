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
		fSystem=FileSystem.getLocal(conf);//获取到一个本地文件的实例,或者是直接get(conf)
		FSDataInputStream open = fSystem.open(new Path("d://a.txt"));
		IOUtils.copyBytes(open, System.out, 4096, false);
		IOUtils.closeStream(open);

	}

}
