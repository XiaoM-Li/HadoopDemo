package hadoop.compress.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressionDemo3 {
	
	public static void main(String[] args) throws Exception {
		String codecClassname="org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codeClass=Class.forName(codecClassname);
		Configuration configuration=new Configuration();
		CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(codeClass, configuration);
		CompressionOutputStream out=codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.close();//这个程序有点问题，Failed to load/initialize native-zlib library，在Linux上可以运行
	}
//	@Test
//	public void readAndCompress() throws IOException, ClassNotFoundException{
//		
//		String codecClassname="org.apache.hadoop.io.compress.GzipCodec";
//		Class<?> codeClass=Class.forName(codecClassname);
//		Configuration configuration=new Configuration();
//		CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(codeClass, configuration);
//		CompressionOutputStream out=codec.createOutputStream(System.out);
//		IOUtils.copyBytes(System.in, out, 4096, false);
//		out.close();//这个程序有点问题，Failed to load/initialize native-zlib library，在Linux上应该可以运行
//	}

}
