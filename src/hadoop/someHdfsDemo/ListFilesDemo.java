package hadoop.someHdfsDemo;
/*
 * 这个程序主要是列出文件系统中的文件
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class ListFilesDemo {
	private FileSystem fs=null;
	
	@Before
	public void init() throws Exception{
		Configuration configuration=new Configuration();
		fs=FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration);
	}
	
	@Test
	public void list() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);//改为false的话不进行迭代，只列出文件
		LocatedFileStatus fileStatus=null;
		while(listFiles.hasNext()){
			fileStatus=listFiles.next();
			String name = fileStatus.getPath().toString();
			System.out.println(name);
		}
	}

}
