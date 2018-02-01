package cn.com.dtmobile.hadoop.util;
/** 
 * @完成功能:
 * @创建时间:2017年7月17日 上午9:17:37 
 * @param    
 */
public class WhetherIsNull {

	public static String IsNUll(Object data){
		if(data==null){
		   return	String.valueOf("") ;
		}else{
			return	String.valueOf(data) ;
		}
	}	
	
}
