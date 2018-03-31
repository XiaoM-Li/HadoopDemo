package hadoop.writable.demo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{
	private Text first = new Text();
	private Text second = new Text();

	public TextPair() {
		
	}

	public TextPair(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public TextPair(String first,String second){
		setFirst(new Text(first));
		setSecond(new Text(second));
	}
	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		first.write(out);
		second.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		first.readFields(in);
		second.readFields(in);
		
	}

	@Override
	public int compareTo(TextPair o) {
		
		System.out.println("------ø™ º≈≈–Ú---------");
		int cmp=this.first.compareTo(o.first);
		if(cmp!=0){
			System.out.println("---------≈≈–ÚΩ· ¯-------");
			return cmp;
		}
		System.out.println("---------≈≈–ÚΩ· ¯-------");
		return this.second.compareTo(o.second);
		
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return first+"\t"+second;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode()*163+second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {

		if(obj instanceof TextPair){
			TextPair tp=(TextPair) obj;
			return this.first.equals(tp.first)&&this.second.equals(tp.second);
		}
		return false;
	}
	
}
