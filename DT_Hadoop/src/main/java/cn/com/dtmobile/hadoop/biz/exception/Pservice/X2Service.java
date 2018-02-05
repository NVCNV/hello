package cn.com.dtmobile.hadoop.biz.exception.Pservice;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import cn.com.dtmobile.hadoop.util.StringUtils;

/** 
 * @完成功能:
 * @创建时间:2017年3月27日 上午11:03:09 
 * @param    
 */
public class X2Service {

	public Text x2Service(Text value){
		
		StringBuffer sb = new StringBuffer();
		String[] words = value.toString().split(
				StringUtils.DELIMITER_COMMA);
		for (int i = 0; i < words.length; i++) {
			sb.append(words[i]);
			sb.append(StringUtils.DELIMITER_VERTICAL);
		}
		if ("1".equals(words[8]) && ("1".equals(words[11])||"255".equals(words[11]))
				&& (!"1000".equals(words[20]) || "".equals(words[20]))) {
			sb.append("10");
		} else {
			sb.append("0");
		}
		return new Text(sb.toString());
			
	}
	
	
}
