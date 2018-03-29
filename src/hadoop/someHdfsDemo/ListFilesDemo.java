package hadoop.someHdfsDemo;
/*
 * ���������Ҫ���г��ļ�ϵͳ�е��ļ�
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
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
		fs=FileSystem.get(new URI("hdfs://itcast00:9000"), configuration);
	}
	
	@Test
	public void list() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/1940"),true);//��Ϊfalse�Ļ������е�����ֻ�г��ļ�
		LocatedFileStatus fileStatus=null;
		while(listFiles.hasNext()){
			fileStatus=listFiles.next();
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			String name = fileStatus.getPath().toString();
			System.out.println(name);
			for(int i=0;i<blockLocations.length;i++){
				System.out.println(blockLocations[i].toString());
			}
			
		}
	}

}
