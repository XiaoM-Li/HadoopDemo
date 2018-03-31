package hadoop.writable.demo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupSort extends WritableComparator{
	protected GroupSort(){
		super(TextPair.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		System.out.println("-----�����Զ������------");
		TextPair tp1=(TextPair) a;
		TextPair tp2=(TextPair) b;
		System.out.println("-----������------"+tp1.getFirst().compareTo(tp2.getFirst()));
		
		System.out.println("-----�������------");
		
		return tp1.getFirst().compareTo(tp2.getFirst());
	}

}
