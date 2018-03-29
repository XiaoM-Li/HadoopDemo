package hadoop.someHdfsDemo;
/*
 * ���������Ҫ���г��ļ�ϵͳ�е��ļ�
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class ListFilesDemo {
	private FileSystem fs = null;

	@Before
	public void init() throws Exception {
		Configuration configuration = new Configuration();
		fs = FileSystem.get(new URI("hdfs://itcast00:9000"), configuration);
	}

	@Test
	public void list() throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/1940"), true);// ��Ϊfalse�Ļ������е�����ֻ�г��ļ�
		LocatedFileStatus fileStatus = null;
		while (listFiles.hasNext()) {
			fileStatus = listFiles.next();
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			String name = fileStatus.getPath().toString();
			System.out.println(name);
			for (int i = 0; i < blockLocations.length; i++) {
				System.out.println(blockLocations[i].toString());
			}
		}
	}

	@Test
	//��ȡ�ļ���Ϣ
	public void fileStatusForFile() throws Exception {
		SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		FileStatus fileStatus = fs.getFileStatus(new Path("/1940"));
		// FileStatus�����ļ����ȡ����С���������޸�ʱ���Լ�Ȩ����Ϣ
		long accessTime = fileStatus.getAccessTime();
		String path = fileStatus.getPath().toString();
		long modificationTime = fileStatus.getModificationTime();
		String owner = fileStatus.getOwner();
		String group = fileStatus.getGroup();
		String permission = fileStatus.getPermission().toString();

		System.out.println(simpleDateFormat.format(new Date(accessTime)));
		System.out.println(path);
		System.out.println(simpleDateFormat.format(new Date(modificationTime)));
		System.out.println(owner);
		System.out.println(group);
		System.out.println(permission);
		System.out.println(fileStatus.getBlockSize()/1024/1024);//128M

	}
	
	@Test
	//��ȡ�ļ���λ����Ϣ
	public void fileLocation() throws Exception{
		FileSystem fs = FileSystem.get(new URI("hdfs://itcast00:9000"), new Configuration());
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/1940"), true);
		while(listFiles.hasNext()){
			BlockLocation[] blockLocations = listFiles.next().getBlockLocations();
			for(BlockLocation blockLocation:blockLocations){
				long offset = blockLocation.getOffset();
				System.out.println(offset);
				String[] hosts = blockLocation.getHosts();
				for(String host:hosts){
					System.out.println(host);
				}
			}
		}
		
	}

}
