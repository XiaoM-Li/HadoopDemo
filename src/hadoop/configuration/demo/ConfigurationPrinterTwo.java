package hadoop.configuration.demo;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * GenericOptionsParser��һ���࣬�������ͳ��õ�Hadoop������ѡ���������Ҫ��ΪConfiguration�����趨��Ӧ��ȡֵ��
 * ������ķ�ʽ��ʵ��Tool�ӿڣ�ͨ��ToolRunner�����г���
 * ToolRunner��ʹ����GenericOptionsParser����ȡ�������з�ʽ��ָ�����б�׼��ѡ�Ȼ����Configurationʵ���Ͻ������á�
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
		// TODO �Զ����ɵķ������
		Configuration conf=getConf();//Ҫ���������
		for(Entry<String,String> entry:conf){
			System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());
		}
		return 0;
	}
	
	

}
