package cn.com.dtmobile.hadoop.biz.hangzhoufilter.model;

public class S1mmeXdr {
	private String length;
	private String city ;
	private String interfaces;
	private String xdrId;
	private String rat;
	private String imsi ;
	private String imei ;
	private String msisdn;
	private String procedureType;
	private String procedureStartTime ;
	private String procedureEndTime;
	private String procedureStatus ;
	private String requestCause ;
	private String failureCause ;
	private String keyword ;
	private String mmeUeS1apId;
	private String mmeGroupId;
	private String mmeCode ;
	private String mTmsi;
	private String tmsi ;
	private String userIpv4;
	private String userIpv6;
	private String mmeIpAdd ;
	private String enbIpAdd ;
	private String mmePort ;
	private String enbPort ;
	private String tac;
	private String cellId;
	private String otherTac;
	private String otherEci;
	private String apn;
	private String eps1earerNumber ;
	private String bearer1Id ;
	private String bearer1Type;
	private String bearer1Qci;
	private String bearer1Status;
	private String bearer1EnbGtpTeid;
	private String bearer1SgwGtpTeid;
	private String bearerNId ;
	private String bearerNType;
	private String bearerNQci;
	private String bearerNStatus;
	private String bearerNEnbGtpTeid;
	private String bearerNSgwGtpTeid;
	private String bearerNTemp01;
	private String bearerNTemp02;
	private String bearerNTemp03;
	private String bearerNTemp04;
	
	public S1mmeXdr(String values[]) {
		this.length  = values[0];
		this.city= values[1];
		this.interfaces= values[2];
		this.xdrId= values[3];
		this.rat = values[4];
		this.imsi = values[5];
		this.imei= values[6];
		this.msisdn  = values[7];
		this.procedureType  = values[8];
		this.procedureStartTime= values[9];
		this.procedureEndTime  = values[10];
		this.procedureStatus= values[11];
		this.requestCause= values[12];
		this.failureCause= values[13];
		this.keyword = values[14];
		this.mmeUeS1apId= values[15];
		this.mmeGroupId = values[16];
		this.mmeCode = values[17];
		this.mTmsi = values[18];
		this.tmsi = values[19];
		this.userIpv4 = values[20];
		this.userIpv6 = values[21];
		this.mmeIpAdd  = values[22];
		this.enbIpAdd  = values[23];
		this.mmePort = values[24];
		this.enbPort = values[25];
		this.tac = values[26];
		this.cellId  = values[27];
		this.otherTac= values[28];
		this.otherEci= values[29];
		this.apn = values[30];
		this.eps1earerNumber= values[31];
		this.bearer1Id  = values[32];
		this.bearer1Type= values[33];
		this.bearer1Qci = values[34];
		this.bearer1Status  = values[35];
		this.bearer1EnbGtpTeid = values[36];
		this.bearer1SgwGtpTeid = values[37];
		this.bearerNId  = values[38];
		this.bearerNType= values[39];
		this.bearerNQci = values[40];
		this.bearerNStatus  = values[41];
		this.bearerNEnbGtpTeid = values[42];
		this.bearerNSgwGtpTeid = values[43];
		this.bearerNTemp01  = values[44];
		this.bearerNTemp02  = values[45];
		this.bearerNTemp03  = values[46];
		this.bearerNTemp04  = values[47];
	}
	public String getLength() {
		return length;
	}
	public void setLength(String length) {
		this.length = length;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getInterfaces() {
		return interfaces;
	}
	public void setInterfaces(String interfaces) {
		this.interfaces = interfaces;
	}
	public String getXdrId() {
		return xdrId;
	}
	public void setXdrId(String xdrId) {
		this.xdrId = xdrId;
	}
	public String getRat() {
		return rat;
	}
	public void setRat(String rat) {
		this.rat = rat;
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public String getMsisdn() {
		return msisdn;
	}
	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}
	public String getProcedureType() {
		return procedureType;
	}
	public void setProcedureType(String procedureType) {
		this.procedureType = procedureType;
	}
	public String getProcedureStartTime() {
		return procedureStartTime;
	}
	public void setProcedureStartTime(String procedureStartTime) {
		this.procedureStartTime = procedureStartTime;
	}
	public String getProcedureEndTime() {
		return procedureEndTime;
	}
	public void setProcedureEndTime(String procedureEndTime) {
		this.procedureEndTime = procedureEndTime;
	}
	public String getProcedureStatus() {
		return procedureStatus;
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
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public String getMmeUeS1apId() {
		return mmeUeS1apId;
	}
	public void setMmeUeS1apId(String mmeUeS1apId) {
		this.mmeUeS1apId = mmeUeS1apId;
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
	public String getmTmsi() {
		return mTmsi;
	}
	public void setmTmsi(String mTmsi) {
		this.mTmsi = mTmsi;
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
	public String getEps1earerNumber() {
		return eps1earerNumber;
	}
	public void setEps1earerNumber(String eps1earerNumber) {
		this.eps1earerNumber = eps1earerNumber;
	}
	public String getBearer1Id() {
		return bearer1Id;
	}
	public void setBearer1Id(String bearer1Id) {
		this.bearer1Id = bearer1Id;
	}
	public String getBearer1Type() {
		return bearer1Type;
	}
	public void setBearer1Type(String bearer1Type) {
		this.bearer1Type = bearer1Type;
	}
	public String getBearer1Qci() {
		return bearer1Qci;
	}
	public void setBearer1Qci(String bearer1Qci) {
		this.bearer1Qci = bearer1Qci;
	}
	public String getBearer1Status() {
		return bearer1Status;
	}
	public void setBearer1Status(String bearer1Status) {
		this.bearer1Status = bearer1Status;
	}
	public String getBearer1EnbGtpTeid() {
		return bearer1EnbGtpTeid;
	}
	public void setBearer1EnbGtpTeid(String bearer1EnbGtpTeid) {
		this.bearer1EnbGtpTeid = bearer1EnbGtpTeid;
	}
	public String getBearer1SgwGtpTeid() {
		return bearer1SgwGtpTeid;
	}
	public void setBearer1SgwGtpTeid(String bearer1SgwGtpTeid) {
		this.bearer1SgwGtpTeid = bearer1SgwGtpTeid;
	}
	public String getBearerNId() {
		return bearerNId;
	}
	public void setBearerNId(String bearerNId) {
		this.bearerNId = bearerNId;
	}
	public String getBearerNType() {
		return bearerNType;
	}
	public void setBearerNType(String bearerNType) {
		this.bearerNType = bearerNType;
	}
	public String getBearerNQci() {
		return bearerNQci;
	}
	public void setBearerNQci(String bearerNQci) {
		this.bearerNQci = bearerNQci;
	}
	public String getBearerNStatus() {
		return bearerNStatus;
	}
	public void setBearerNStatus(String bearerNStatus) {
		this.bearerNStatus = bearerNStatus;
	}
	public String getBearerNEnbGtpTeid() {
		return bearerNEnbGtpTeid;
	}
	public void setBearerNEnbGtpTeid(String bearerNEnbGtpTeid) {
		this.bearerNEnbGtpTeid = bearerNEnbGtpTeid;
	}
	public String getBearerNSgwGtpTeid() {
		return bearerNSgwGtpTeid;
	}
	public void setBearerNSgwGtpTeid(String bearerNSgwGtpTeid) {
		this.bearerNSgwGtpTeid = bearerNSgwGtpTeid;
	}
	public String getBearerNTemp01() {
		return bearerNTemp01;
	}
	public void setBearerNTemp01(String bearerNTemp01) {
		this.bearerNTemp01 = bearerNTemp01;
	}
	public String getBearerNTemp02() {
		return bearerNTemp02;
	}
	public void setBearerNTemp02(String bearerNTemp02) {
		this.bearerNTemp02 = bearerNTemp02;
	}
	public String getBearerNTemp03() {
		return bearerNTemp03;
	}
	public void setBearerNTemp03(String bearerNTemp03) {
		this.bearerNTemp03 = bearerNTemp03;
	}
	public String getBearerNTemp04() {
		return bearerNTemp04;
	}
	public void setBearerNTemp04(String bearerNTemp04) {
		this.bearerNTemp04 = bearerNTemp04;
	}
	
}
