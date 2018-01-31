package cn.com.dtmobile.hadoop.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class ReadConfig {

	public static Map<String,String> readConfig(String path){
		Properties prop = new Properties();
		Map<String,String> config = new HashMap<String, String>() ;
		try {
			// 读取属性文件properties
			InputStream in = new BufferedInputStream(new FileInputStream(path)) ;
			prop.load(in); // /加载属性列表
			Iterator<String> it = prop.stringPropertyNames().iterator() ;
			while (it.hasNext()) {
				String key = it.next() ;
				config.put(key, prop.getProperty(key)) ;
			}
			in.close() ;

		} catch (Exception e) {
			e.printStackTrace() ;
		}
		return config ;
	}
}
