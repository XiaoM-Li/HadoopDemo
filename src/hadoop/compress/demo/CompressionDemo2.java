package hadoop.compress.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class CompressionDemo2 {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://itcast00:9000"), conf);
		FSDataInputStream inputStream = fs.open(new Path("/1901.gz"));
		CompressionCodecFactory factory=new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(new Path("/1901.gz"));
		CompressionInputStream createInputStream = codec.createInputStream(inputStream);
		IOUtils.copyBytes(createInputStream, System.out, conf);

	}

}
