package hadoop.someHdfsDemo;
/*
 * 这个程序主要是列出符合要求的文件，带有文件名过滤器
 */

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ListFilesWithPathFilter {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://hadoop01:9000"), conf);
		FileStatus[] fileStatus = fs.listStatus(new Path("/"));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
//		for(Path path:paths){
//			System.out.println(path.getName());
//		}
		FileStatus[] listStatus = fs.listStatus(new Path("/"), new PathFilter() {
			
			@Override
			public boolean accept(Path path) {
				
				if(path.toString().endsWith(".txt")){
				return true;
				}
				else{
					return false;
				}
			}
		});
		
		Path[] paths2=FileUtil.stat2Paths(listStatus);
		for(Path path:paths2){
			System.out.println(path);
		}
	}

}
