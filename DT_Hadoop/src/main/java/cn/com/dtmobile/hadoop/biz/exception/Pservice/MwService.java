package cn.com.dtmobile.hadoop.biz.exception.Pservice;

import org.apache.hadoop.io.Text;

import cn.com.dtmobile.hadoop.util.StringUtils;

/** 
 * @完成功能:
 * @创建时间:2017年3月27日 上午10:56:09 
 * @param    
 */
public class MwService {

	public Text MwService(Text value){
		
		StringBuffer sb = new StringBuffer();
		String[] words = value.toString().split(StringUtils.DELIMITER_INNER_ITEM,-1);
		for (int i = 0; i < 67; i++) {
			sb.append(words[i]);
			sb.append(StringUtils.DELIMITER_VERTICAL);
		}
		if ("1".equals(words[8]) && "14".equals(words[2])
				&& ("3".equals(words[12])||"2".equals(words[12]) || "4".equals(words[12]))) {    
			sb.append("1");    /* 流程状态:1：成功；2：失败；3：取消；4：超时 */
		} else if ("5".equals(words[8]) && "14".equals(words[2])&& (StringUtils.isBlank(words[56]) || "255".equals(words[56]))) {  
			if ("1".equals(words[11])) {
				/*Service Type  1: 语音呼叫/Voice call  2: 视频呼叫/Video call  3: 数据业务/Data service  10: IMS PTT呼叫  11: IMS会议*/
				sb.append("4");
			} else if ("2".equals(words[11])) {
				sb.append("6");
			}
			else {
				sb.append("0");
			}
		} else {
			sb.append("0");
		}
		 return new Text(sb.toString()) ;
	}
	
}
