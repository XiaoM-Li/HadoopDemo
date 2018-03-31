package hadoop.configuration.demo;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * GenericOptionsParser是一个类，用来解释常用的Hadoop命令行选项，并根据需要，为Configuration对象设定相应的取值。
 * 更方便的方式是实现Tool接口，通过ToolRunner来运行程序。
 * ToolRunner还使用了GenericOptionsParser来获取在命令行方式中指定所有标准的选项，然后在Configuration实例上进行设置。
 * hadoop jar print.jar hadoop.configuration.demo.ConfigurationPrinterTwo 
 * -conf conf_file | grep hadoop.tmp.dir=
 * 
 * hadoop hadoop.configuration.demo.ConfigurationPrinterTwo -D color=yellow | grep color=
 */

public class ConfigurationPrinterTwo extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new ConfigurationPrinterTwo(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO 自动生成的方法存根
		Configuration conf=getConf();//要用这个方法
		for(Entry<String,String> entry:conf){
			System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());
		}
		return 0;
	}
	
	

}
