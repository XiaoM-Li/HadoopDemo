package hadoop.fileDataStructre;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SeqFileRead {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://hadoop01:9000"), conf);
		SequenceFile.Reader reader=new SequenceFile.Reader(fs, new Path("/123Seqfile"),conf);
		Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value=(Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while(reader.next(key, value)){
			System.out.println(key+"  "+value);
		}
	}

}
