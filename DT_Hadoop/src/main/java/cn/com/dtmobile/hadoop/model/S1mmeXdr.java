package cn.com.dtmobile.hadoop.model;



import cn.com.dtmobile.hadoop.biz.exception.constants.FieldConstants;
import cn.com.dtmobile.hadoop.util.FailCauseMapping;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class S1mmeXdr {
	private CommXdr commXdr;
	private String procedureType;
	private Long procedureStartTime;
	private Long procedureEndTime;
	private String startLon;
	private String startLat;
	private Integer locationSource;
	private Integer msgFlag;
	private String procedureStatus;
	private Integer reqCauseGroup; 
	private Integer requestCauseT; 
	private Integer failCauseGroup; 
	private Integer failureCauseT;
	private String requestCause;
	private String failureCause;
	private String keyword1;
	private String keyword2;
	private String keyword3;
	private String keyword4;
	private String mmeUeS1apId;
	private String oldMmeGroupId;
	private String oldMmeCode;
	private String oldMtmsi;
	private Integer oldGutiType;
	private String mmeGroupId;
	private String mmeCode;
	private String mtmsi;
	private String tmsi;
	private String userIpv4;
	private String userIpv6;
	private String mmeIpAdd;
	private String enbIpAdd;
	private String mmePort;
	private String enbPort;
	private String tac;
	private String cellId;
	private String otherTac;
	private String otherEci;
	private String apn;
	private Integer epsBearerNumber;
	private String[] bearerArr;
	private String rangeTime;

	public S1mmeXdr() {
	}
	public S1mmeXdr(String[] values,int isShare) {
		commXdr = new CommXdr(values,1);
		this.procedureType = values[12];
		this.procedureStartTime = ParseUtils.parseLong(values[13]);
		this.procedureEndTime = ParseUtils.parseLong(values[14]);
		this.startLon = values[15];
		this.startLat = values[16];
		this.locationSource = ParseUtils.parseInteger(values[17]);
		this.msgFlag = ParseUtils.parseInteger(values[18]);
		this.procedureStatus = values[19];
		this.reqCauseGroup = ParseUtils.parseInteger(values[20]);
		this.requestCauseT = ParseUtils.parseInteger(values[21]); 
		this.failCauseGroup = ParseUtils.parseInteger(values[22]);
		this.failureCauseT = ParseUtils.parseInteger(values[23]);
//		this.requestCause = values[24];
//		this.failureCause = values[25];
		this.keyword1 = values[24];
		this.keyword2 = values[25];
		this.keyword3 = values[26];
		this.keyword4 = values[27];
		this.mmeUeS1apId = values[28];
		this.oldMmeGroupId = values[29];
		this.oldMmeCode = values[30];
		this.oldMtmsi = values[31];
		this.oldGutiType = ParseUtils.parseInteger(values[32]);
		this.mmeGroupId = values[33];
		this.mmeCode = values[34];
		this.mtmsi = values[35];
		this.tmsi = values[36];
		this.userIpv4 = values[37];
		this.userIpv6 = values[38];
		this.mmeIpAdd = values[39];
		this.enbIpAdd = values[40];
		this.mmePort = values[41];
		this.enbPort = values[42];
		this.tac = values[43];
		this.cellId = values[44];
		this.otherTac = values[45];
		this.otherEci = values[46];
		this.apn = values[47];
		this.epsBearerNumber = ParseUtils.parseInteger(values[48]);
		Integer start = 49;
		Integer step = 9;
		if(epsBearerNumber>0){
			bearerArr = new String[epsBearerNumber] ;
			for (Integer i = 0; i < this.epsBearerNumber; i++) {
				StringBuffer sb = new StringBuffer();
				int k = 0;
				for (Integer j = start; j <= start + step; j++) {
					k++;
					if(k == 5){
						sb.append(FailCauseMapping.getFailCauseMapping(ParseUtils.parseInteger(values[j]), ParseUtils.parseInteger(values[j+1]), ParseUtils.parseInteger(procedureType)));
						sb.append(StringUtils.DELIMITER_VERTICAL);
						sb.append(FailCauseMapping.getFailCauseMapping(ParseUtils.parseInteger(values[j+2]), ParseUtils.parseInteger(values[j+3]), ParseUtils.parseInteger(procedureType)));
						sb.append(StringUtils.DELIMITER_VERTICAL);
						j = j+4;
					}
					
					sb.append(values[j]);
					sb.append(StringUtils.DELIMITER_VERTICAL);
				}
				if(start < 49 + bearerArr.length - step){
					start = start + step;
				}
				sb.deleteCharAt(sb.length() - 1);
				try{
				    this.bearerArr[i] = sb.toString();
				}
				catch(Exception e){
					this.bearerArr[i] = sb.toString();
				}
			}

		}
	}
	
	public S1mmeXdr(String[] values) {
		commXdr = new CommXdr(values);
		this.procedureType = values[8];
		this.procedureStartTime = ParseUtils.parseLong(values[9]);
		this.procedureEndTime = ParseUtils.parseLong(values[10]);
		this.procedureStatus = values[11];
		this.requestCause = values[12];
		this.failureCause = values[13];
		this.keyword1 = values[14];
		this.keyword2 = values[15];
		this.keyword3 = values[16];
		this.keyword4 = values[17];
		this.mmeUeS1apId = values[18];
		this.oldMmeGroupId = values[19];
		this.oldMmeCode = values[20];
		this.oldMtmsi = values[21];
		this.mmeGroupId = values[22];
		this.mmeCode = values[23];
		this.mtmsi = values[24];
		this.tmsi = values[25];
		this.userIpv4 = values[26];
		this.userIpv6 = values[27];
		this.mmeIpAdd = values[28];
		this.enbIpAdd = values[29];
		this.mmePort = values[30];
		this.enbPort = values[31];
		this.tac = values[32];
		this.cellId = values[33];
		this.otherTac = values[34];
		this.otherEci = values[35];
		this.apn = values[36];
		this.epsBearerNumber = ParseUtils.parseInteger(values[37]);
		Integer start = 38;
		Integer step = 8;
		if(epsBearerNumber>0){
			bearerArr = new String[epsBearerNumber*8] ;
		for (Integer i = 0; i < this.epsBearerNumber; i++) {
			StringBuffer sb = new StringBuffer();
			for (Integer j = start; j <= start + step; j++) {
				sb.append(values[j]);
				sb.append(StringUtils.DELIMITER_COMMA);
			}
			start = start + step;
			sb.deleteCharAt(sb.length() - 1);
			try
			{
			    this.bearerArr[i] = sb.toString();
			}
			catch(Exception e)
			{
				this.bearerArr[i] = sb.toString();
			}
		}
		}
	}

	
	public String toStringShare(){
		StringBuffer sb = new StringBuffer();
		sb.append(commXdr.toString());
		sb.append(procedureType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureStartTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureEndTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(FieldConstants.changeProcedureStatus(procedureStatus));
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(FailCauseMapping.getFailCauseMapping(reqCauseGroup,requestCauseT , ParseUtils.parseInteger(procedureType)));
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(FailCauseMapping.getFailCauseMapping(failCauseGroup,failureCauseT , ParseUtils.parseInteger(procedureType)));
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(FieldConstants.getKeyWord1(procedureType,StringUtils.isNotEmpty(keyword1)?Integer.parseInt(keyword1):null));
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(FieldConstants.getKeyWord2(procedureType,StringUtils.isNotEmpty(keyword2)?Integer.parseInt(keyword2):null));
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword3);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword4);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeUeS1apId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(oldMmeGroupId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(oldMmeCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(oldMtmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeGroupId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mtmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(userIpv4);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(userIpv6);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeIpAdd);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enbIpAdd);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmePort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enbPort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(cellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(otherTac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(otherEci);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(apn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(epsBearerNumber);
		if(bearerArr != null && bearerArr.length>0){
			for(String val : this.bearerArr){
				sb.append(StringUtils.DELIMITER_VERTICAL);
				sb.append(val);
			}
		}
		if(epsBearerNumber < 16){
			for (int i = epsBearerNumber; i < 16; i++) {
				for (int j = 1; j <= 8 ; j++) {
					sb.append(StringUtils.DELIMITER_VERTICAL);
					sb.append("");
				}
			}
		}
//		sb.append(commXdr.getLocalProvince());
//		sb.append(StringUtils.DELIMITER_VERTICAL);
//		sb.append(commXdr.getOwnerProvince());
//		sb.append(StringUtils.DELIMITER_VERTICAL);
//		sb.append(commXdr.getOwnerCity());
//		sb.append(StringUtils.DELIMITER_VERTICAL);
//		sb.append(commXdr.getRoamingType());
		return sb.toString();
	}
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(commXdr.toString());
		sb.append(procedureType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureStartTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureEndTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureStatus);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(requestCause);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(failureCause);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword1);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword2);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword3);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword4);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeUeS1apId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(oldMmeGroupId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(oldMmeCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(oldMtmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeGroupId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mtmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tmsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(userIpv4);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(userIpv6);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmeIpAdd);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enbIpAdd);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mmePort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enbPort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(cellId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(otherTac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(otherEci);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(apn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(epsBearerNumber);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		for(String val : this.bearerArr == null ? new String [0] : bearerArr){
			sb.append(val);
			sb.append(StringUtils.DELIMITER_VERTICAL);
		}
		return sb.toString();
	}
	
	
	public CommXdr getCommXdr() {
		return commXdr;
	}

	public void setCommXdr(CommXdr commXdr) {
		this.commXdr = commXdr;
	}

	public String getProcedureType() {
		return procedureType;
	}

	public void setProcedureType(String procedureType) {
		this.procedureType = procedureType;
	}

	public Long getProcedureStartTime() {
		return procedureStartTime;
	}

	public void setProcedureStartTime(Long procedureStartTime) {
		this.procedureStartTime = procedureStartTime;
	}

	public Long getProcedureEndTime() {
		return procedureEndTime;
	}

	public void setProcedureEndTime(Long procedureEndTime) {
		this.procedureEndTime = procedureEndTime;
	}

	public String getProcedureStatus() {
		return procedureStatus;
	}
	
	public String getShareProcedureStatus() {
		return FieldConstants.changeProcedureStatus(procedureStatus);
	}
	
	public void setProcedureStatus(String procedureStatus) {
		this.procedureStatus = procedureStatus;
	}

	public String getRequestCause() {
		return requestCause;
	}

	public void setRequestCause(String requestCause) {
		this.requestCause = requestCause;
	}

	public String getFailureCause() {
		return failureCause;
	}

	public void setFailureCause(String failureCause) {
		this.failureCause = failureCause;
	}

	public String getKeyword1() {
		return keyword1;
	}

	public void setKeyword1(String keyword1) {
		this.keyword1 = keyword1;
	}

	public String getKeyword2() {
		return keyword2;
	}

	public void setKeyword2(String keyword2) {
		this.keyword2 = keyword2;
	}

	public String getKeyword3() {
		return keyword3;
	}

	public void setKeyword3(String keyword3) {
		this.keyword3 = keyword3;
	}

	public String getKeyword4() {
		return keyword4;
	}

	public void setKeyword4(String keyword4) {
		this.keyword4 = keyword4;
	}

	public String getMmeUeS1apId() {
		return mmeUeS1apId;
	}

	public void setMmeUeS1apId(String mmeUeS1apId) {
		this.mmeUeS1apId = mmeUeS1apId;
	}

	public String getOldMmeGroupId() {
		return oldMmeGroupId;
	}

	public void setOldMmeGroupId(String oldMmeGroupId) {
		this.oldMmeGroupId = oldMmeGroupId;
	}

	public String getOldMmeCode() {
		return oldMmeCode;
	}

	public void setOldMmeCode(String oldMmeCode) {
		this.oldMmeCode = oldMmeCode;
	}

	public String getOldMtmsi() {
		return oldMtmsi;
	}

	public void setOldMtmsi(String oldMtmsi) {
		this.oldMtmsi = oldMtmsi;
	}

	public String getMmeGroupId() {
		return mmeGroupId;
	}

	public void setMmeGroupId(String mmeGroupId) {
		this.mmeGroupId = mmeGroupId;
	}

	public String getMmeCode() {
		return mmeCode;
	}

	public void setMmeCode(String mmeCode) {
		this.mmeCode = mmeCode;
	}

	public String getMtmsi() {
		return mtmsi;
	}
	public void setMtmsi(String mtmsi) {
		this.mtmsi = mtmsi;
	}
	public String getTmsi() {
		return tmsi;
	}

	public void setTmsi(String tmsi) {
		this.tmsi = tmsi;
	}

	public String getUserIpv4() {
		return userIpv4;
	}

	public void setUserIpv4(String userIpv4) {
		this.userIpv4 = userIpv4;
	}

	public String getUserIpv6() {
		return userIpv6;
	}

	public void setUserIpv6(String userIpv6) {
		this.userIpv6 = userIpv6;
	}

	public String getMmeIpAdd() {
		return mmeIpAdd;
	}

	public void setMmeIpAdd(String mmeIpAdd) {
		this.mmeIpAdd = mmeIpAdd;
	}

	public String getEnbIpAdd() {
		return enbIpAdd;
	}

	public void setEnbIpAdd(String enbIpAdd) {
		this.enbIpAdd = enbIpAdd;
	}

	public String getMmePort() {
		return mmePort;
	}

	public void setMmePort(String mmePort) {
		this.mmePort = mmePort;
	}

	public String getEnbPort() {
		return enbPort;
	}

	public void setEnbPort(String enbPort) {
		this.enbPort = enbPort;
	}

	public String getTac() {
		return tac;
	}

	public void setTac(String tac) {
		this.tac = tac;
	}

	public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

	public String getOtherTac() {
		return otherTac;
	}

	public void setOtherTac(String otherTac) {
		this.otherTac = otherTac;
	}

	public String getOtherEci() {
		return otherEci;
	}

	public void setOtherEci(String otherEci) {
		this.otherEci = otherEci;
	}

	public String getApn() {
		return apn;
	}

	public void setApn(String apn) {
		this.apn = apn;
	}

	public Integer getEpsBearerNumber() {
		return epsBearerNumber;
	}

	public void setEpsBearerNumber(Integer epsBearerNumber) {
		this.epsBearerNumber = epsBearerNumber;
	}

	public String[] getBearerArr() {
		return bearerArr;
	}

	public void setBearerArr(String[] bearerArr) {
		this.bearerArr = bearerArr;
	}

	public String getRangeTime() {
		return rangeTime;
	}
	public void setRangeTime(String rangeTime) {
		this.rangeTime = rangeTime;
	}
	public String getStartLon() {
		return startLon;
	}
	public void setStartLon(String startLon) {
		this.startLon = startLon;
	}
	public String getStartLat() {
		return startLat;
	}
	public void setStartLat(String startLat) {
		this.startLat = startLat;
	}
	public Integer getLocationSource() {
		return locationSource;
	}
	public void setLocationSource(Integer locationSource) {
		this.locationSource = locationSource;
	}
	public Integer getMsgFlag() {
		return msgFlag;
	}
	public void setMsgFlag(Integer msgFlag) {
		this.msgFlag = msgFlag;
	}
	public Integer getReqCauseGroup() {
		return reqCauseGroup;
	}
	public void setReqCauseGroup(Integer reqCauseGroup) {
		this.reqCauseGroup = reqCauseGroup;
	}
	public Integer getRequestCauseT() {
		return requestCauseT;
	}
	public void setRequestCauseT(Integer requestCauseT) {
		this.requestCauseT = requestCauseT;
	}
	public Integer getFailCauseGroup() {
		return failCauseGroup;
	}
	public void setFailCauseGroup(Integer failCauseGroup) {
		this.failCauseGroup = failCauseGroup;
	}
	public Integer getFailureCauseT() {
		return failureCauseT;
	}
	public void setFailureCauseT(Integer failureCauseT) {
		this.failureCauseT = failureCauseT;
	}
	public Integer getOldGutiType() {
		return oldGutiType;
	}
	public void setOldGutiType(Integer oldGutiType) {
		this.oldGutiType = oldGutiType;
	}
	public static void main(String[] args) {
		String value="245|240|0415|0|000|2|5|000005ff587c00f800000004b9730c44|6|460028425418904|862263036814433|8615842508191|18|1498486508332|1498486508371|124.08094700|40.45841800|1|255|1|255|255|255|255|255|255|255|255|62985227|65535|255|4294967295|255|515|198|3230291070|4294967295|||100.74.251.13|100.74.115.159|36412|36412|16817|69765632|65535|4294967295||1|5|1|9|1|255|255|255|255|2233733278|233087891";
		S1mmeXdr s1mme = new S1mmeXdr(value.split("\\|"), 1);
		System.out.println(s1mme.toStringShare());
		System.out.println(s1mme.toStringShare().split("\\|",-1).length);
	}
}
