package hadoop.fileDataStructre;
/*
 * 这个程序主要是把一个文件夹下的所有文件按以下处理，文件名当做键，整个文件内容当做值，写入到顺序文件中
 */

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SeqFileWriteTwo {

	public static void main(String[] args) throws IOException, URISyntaxException {
		
		FileSystem fs=FileSystem.get(new URI("hdfs://itcast00:9000"), new Configuration());
		SequenceFile.Writer writer=SequenceFile.createWriter(fs, new Configuration(), new Path("/abcSeqfile"), Text.class, Text.class);
		
		FileStatus[] fileStatus=fs.listStatus(new Path("/seqFile"));//123file是一个文件夹，下面包含1、2、3三个文件
		for(FileStatus fStatus:fileStatus){
			byte[] content=new byte[(int)fStatus.getLen()];
			FSDataInputStream inputStream=fs.open(fStatus.getPath());
			IOUtils.readFully(inputStream, content, 0, content.length);
			writer.append(new Text(fStatus.getPath().getName()), new Text(content));
		}

		IOUtils.closeStream(writer);
	}

}
