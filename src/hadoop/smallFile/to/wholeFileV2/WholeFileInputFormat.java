package hadoop.smallFile.to.wholeFileV2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

public class WholeFileInputFormat extends CombineFileInputFormat<Text, Text>{

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		// TODO �Զ����ɵķ������
		WholeFileRecordReader recordReader=new WholeFileRecordReader();
		try {
			recordReader.initialize(split, context);
		} catch (InterruptedException e) {
			// TODO �Զ����ɵ� catch ��
			e.printStackTrace();
		}
		return recordReader;
	}

}
