package cn.com.dtmobile.hadoop.biz.exception.Pservice;

import org.apache.hadoop.io.Text;

import cn.com.dtmobile.hadoop.util.StringUtils;

/** 
 * @完成功能:
 * @创建时间:2017年3月27日 上午10:26:00 
 * @param    
 */
public class RxgxService {

	public Text rxgxService(Text value){
		
		StringBuffer sb = new StringBuffer();
		String[] words = value.toString().split(StringUtils.DELIMITER_INNER_ITEM,-1);
		for (int i = 0; i < words.length; i++) {
			sb.append(words[i]);
			sb.append(StringUtils.DELIMITER_VERTICAL);
		}
		if ("4".equals(words[8])
				&& "26".equals(words[2])
				&& ("0".equals(words[21]) || "1".equals(words[21])
						|| "2".equals(words[21]) || "4".equals(words[21]))) {
			if ("0".equals(words[20])) {
				sb.append("5");
			} else if ("1".equals(words[20])) {
				sb.append("7");
			} else {
				sb.append("0");
			}
		} else {
			sb.append("0");
		}
		return  new Text(sb.toString());
	}
	
	
}
