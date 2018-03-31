package hadoop.configuration.demo;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ConfigurationPrinterOne {
	
	Configuration conf=new Configuration();
	
	@Test
	public void test1(){
		conf.addResource("conf1.xml");
		System.out.println(conf.get("color"));
		System.out.println(conf.get("size"));
		System.out.println(conf.get("weight"));
		System.out.println(conf.get("size-weight"));
		System.out.println(conf.get("no","123"));
		
		System.out.println(conf.get("fs.defaultFS"));
	}
	

}
