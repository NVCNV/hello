package cn.com.dtmobile.hadoop.util;

import org.apache.commons.lang.StringUtils;

public class ParseUtils {

	public static Long  parseLong(String val){
		if("\\N".equals(val)||"null".equals(val)||"NULL".equals(val)){
			return -1l;
		}
		if(StringUtils.isNotEmpty(val)){
			return Long.valueOf(val);
		}
		return -1l;
	}
	
	public static Double  parseDouble(String val){
		if("\\N".equals(val)||"null".equals(val)||"NULL".equals(val)){
			return -1.0;
		}
		if(StringUtils.isNotEmpty(val)){
			return Double.valueOf(val);
		}
		return -1.0;
	}
	
	public static Integer  parseInteger(String val){
		if("\\N".equals(val)||"null".equals(val)||"NULL".equals(val)){
			return -1;
		}
		if(StringUtils.isNotEmpty(val)){
			return Integer.valueOf(val);
		}
		return -1;
	}
	
	public static Integer null2Zero(Integer num){
		if(num == null){
			num = 0;
		}
		return num;
	}
	
}