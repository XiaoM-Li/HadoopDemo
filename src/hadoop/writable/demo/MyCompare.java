package hadoop.writable.demo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyCompare extends WritableComparator{
	
	protected  MyCompare() {
		// TODO Auto-generated constructor stub
		super(TextPair.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		TextPair tp1=(TextPair) a;
		TextPair tp2=(TextPair) b;
		System.out.println("-----自定义排序------");
		int cmp=tp1.getFirst().compareTo(tp2.getFirst());
		if(cmp!=0){
			System.out.println("----结束自定义排序-----");
			return cmp;
		}
		System.out.println("----结束自定义排序-----");
		return tp1.getSecond().compareTo(tp2.getSecond());
	}

}
