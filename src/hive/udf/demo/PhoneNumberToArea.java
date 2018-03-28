package hive.udf.demo;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

public class PhoneNumberToArea extends UDF{
	
	private static HashMap<String, String> area=new HashMap<>();
	static{
		area.put("133","Beijing");
		area.put("134", "Tianjing");
		area.put("135","Henan");
		area.put("136", "Hunan");
		area.put("137", "Sichuan");
	}
	
	public String evaluate(String phoneNumber){
		String prefix=phoneNumber.substring(0, 3);
		String areaStr=area.get(prefix)!=null?area.get(prefix):"Other area";
		String result=phoneNumber+"\t"+areaStr;
		return result;
	}

}
