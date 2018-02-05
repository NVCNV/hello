package cn.com.dtmobile.hadoop.biz.exception.Pservice;


import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

/**
 * @完成功能:
 * @创建时间:2017年3月27日 上午10:42:30
 * @param
 */
public class S1mmeService {

	 public Text s1mmeService(Text values, ArrayList<Text> rows) {

	  StringBuffer sb = new StringBuffer();

	  String word = values.toString();
	  word = word.replaceAll(StringUtils.DELIMITER_COMMA,StringUtils.DELIMITER_VERTICAL);

	  String[] words = word.split(StringUtils.DELIMITER_INNER_ITEM,-1);
	  for (int i = 0; i < words.length; i++) {
	   sb.append(words[i]);
	   sb.append(StringUtils.DELIMITER_VERTICAL);
	  }

	  StringBuffer s = new StringBuffer();
	  for (int i = 0; i < 16 * 8 - Integer.parseInt(StringUtils.isNotBlank(words[37])?words[37]:"0") * 8; i++) {
	   s.append(StringUtils.DELIMITER_VERTICAL);
	  }



		sb.append(s);

		if ("1".equals(words[8])
				&& ("1".equals(words[11]) || "255".equals(words[11]))) {
			sb.append("2");
		} else if ("2".equals(words[8])
				&& ("1".equals(words[11]) || "255".equals(words[11]))) {
			sb.append("3");
		} else if ("5".equals(words[8])
				&& ("1".equals(words[11]) || "255".equals(words[11]))) {
			sb.append("8");
		} else if ("20".equals(words[8])
				&& "0".equals(words[14])
				&& (!"2".equals(words[12]) && !"20".equals(words[12])
						&& !"23".equals(words[12]) && !"24".equals(words[12])
						&& !"28".equals(words[12]) && !"512".equals(words[12]) && !"514"
							.equals(words[12]))) {
			sb.append("9");
		} else if ("16".equals(words[8]) && "3".equals(words[14])) {
			if ("1".equals(words[11]) || "255".equals(words[11])) {
				sb.append("13");
			} else {
				sb.append("14");
			}
		} else if ("16".equals(words[8]) && "1".equals(words[14])) {
			if ("1".equals(words[11]) || "255".equals(words[11])) {
				sb.append("10");
			} else if ("0".equals(words[11])) {
				int num = 0;
				for (Text value_t : rows) {
					String[] words_t = value_t.toString().split(StringUtils.DELIMITER_INNER_ITEM);
					if ("20".equals(words_t[8])
							&& "2".equals(words_t[12])
							&& words[33].equals(words_t[33])
							&& (ParseUtils.parseLong(words_t[9]) > ParseUtils
									.parseLong(words[9]) && ParseUtils
									.parseLong(words_t[9])
									- ParseUtils.parseLong(words[9]) < 5000)) {
						num++;
						break;
					}
				}
				if (num > 0) {
					sb.append("0");
				} else {
					sb.append("10");
				}
			} else {
				sb.append("0");
			}
		} else if ("20".equals(words[8])
				&& ("21".equals(words[12]) || "26".equals(words[12]) || "28"
						.equals(words[12]))) {
			int num = 0;
			for (Text value_t : rows) {
				String[] words_t = value_t.toString().split(StringUtils.DELIMITER_INNER_ITEM);
				if (("1".equals(words_t[8]) || "2".equals(words_t[8]) || "5"
						.equals(words_t[8]))
						&& (ParseUtils.parseLong(words_t[9]) > ParseUtils
								.parseLong(words[9]) && ParseUtils
								.parseLong(words_t[9])
								- ParseUtils.parseLong(words[9]) < 3000)) {
					num++;
					break;
				}
			}
			if (num > 0) {
				sb.append("0");
			} else {
				sb.append("14");
			}
		} else {
			sb.append("0");
		}

//		 }
	
		if (sb != null) {
			return new Text(sb.toString());
		} else {
			return null;
		}
		
		
		
	}
}
