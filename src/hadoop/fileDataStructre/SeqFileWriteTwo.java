package hadoop.fileDataStructre;
/*
 * ���������Ҫ�ǰ�һ���ļ����µ������ļ������´����ļ����������������ļ����ݵ���ֵ��д�뵽˳���ļ���
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
		
		FileStatus[] fileStatus=fs.listStatus(new Path("/seqFile"));//123file��һ���ļ��У��������1��2��3�����ļ�
		for(FileStatus fStatus:fileStatus){
			byte[] content=new byte[(int)fStatus.getLen()];
			FSDataInputStream inputStream=fs.open(fStatus.getPath());
			IOUtils.readFully(inputStream, content, 0, content.length);
			writer.append(new Text(fStatus.getPath().getName()), new Text(content));
		}

		IOUtils.closeStream(writer);
	}

}
