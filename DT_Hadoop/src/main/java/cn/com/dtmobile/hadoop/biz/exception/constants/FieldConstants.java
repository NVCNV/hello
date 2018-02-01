package cn.com.dtmobile.hadoop.biz.exception.constants;

public class FieldConstants {

	public static final String procedureStatus_Success = "1";
	public static final String procedureStatus_Fail = "2";
	public static final String procedureStatus_OverTime = "3";
	
	public static final String procedureStatus_Success_Change = "0";
	public static final String procedureStatus_Fail_Change = "1";
	public static final String procedureStatus_OverTime_Change = "255";
	
	public static final String procedureType_Paging = "4";
	public static final String procedureType_Change = "20";
	public static final String procedureType_Change_1 = "21";
	public static final String procedureType_Reset = "22";
	public static final String procedureType_SMS = "32";
	public static final String procedureType_Detach = "6";
	/**
	 * 将共享平台的proceudrestatus转换为自定义通用 避免业务代码修改
	 * @param procedureStatus
	 * @return
	 */
	public static String changeProcedureStatus(String procedureStatus){
		String result="";
		if(procedureStatus != null){
			if(procedureStatus_Success.equals(procedureStatus)){
				result = procedureStatus_Success_Change;
			}else if(procedureStatus_Fail.equals(procedureStatus)){
				result = procedureStatus_Fail_Change;
			}else if(procedureStatus_OverTime.equals(procedureStatus)){
				result = procedureStatus_OverTime_Change;
			}
		}
		return result;
		
	}
	
	public static Integer getKeyWord1(String procedureType,Integer keyWord){
		if(keyWord != null){
			if(procedureType_Paging.equals(procedureType)){
				keyWord --;
			}else if(procedureType_Change.equals(procedureType) || procedureType_Change_1.equals(procedureType)){
				keyWord --;
			}else if(procedureType_Reset.equals(procedureType)){
				keyWord --;
			}else if(procedureType_SMS.equals(procedureType)){
				keyWord --;
			}
		}
		return keyWord;
	}
	
	public static int getKeyWord2(String procedureType,Integer keyWord){
		if(keyWord != null){
			if(procedureType_Detach.equals(procedureType)){
				keyWord -- ;
			}
		}
		return keyWord;
	}
	
	public static Integer changeAppType(Integer appType){
		if(appType == null){
			appType = 0;
		}
		if(110 == appType){
			appType = 108;
		}else if(108 == appType){
			appType = 109;
		}else if(109 == appType){
			appType = null;
		}
		return appType;
	}
}
