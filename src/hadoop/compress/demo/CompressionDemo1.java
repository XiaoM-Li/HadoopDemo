package hadoop.compress.demo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressionDemo1 {

	public static void main(String[] args) throws Exception {
		
		String className = "org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codecClass = Class.forName(className);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		FileSystem fs = FileSystem.get(new URI("hdfs://itcast00:9000"), conf);
		FSDataInputStream inputStream = fs.open(new Path("/1901.gz"));
		CompressionInputStream compressionInputStream = codec.createInputStream(inputStream);
		IOUtils.copyBytes(compressionInputStream, System.out, conf);
	}

}
