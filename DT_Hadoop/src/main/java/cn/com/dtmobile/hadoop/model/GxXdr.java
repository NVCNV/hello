package cn.com.dtmobile.hadoop.model;

import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

/**
 * GX\RX 接口
 * @author zhangchao15
 * @version 2016-11-14
 */
public class GxXdr {
	private CommXdr commXdr;
	private Integer procedureType;          
	private Long procedureStartTime;     
	private Long procedureEndTime;    
	private String icid;                   
	private String originRealm;           
	private String destinationRealm;      
	private String originHost;      
	private String destinationHost;       
	private String sgsnSgwSigIp;      
	private String afAppId;        
	private String ccRequestType;        
	private String rxRequestType;       
	private String mediaType;       
	private String abortCause;            
	private String resultCode;           
	private String experimentalResultCode;
	private String sessionReleaseCause;
	private Integer ruleFailureCode;
	private String sessionid;
	private String calledStationId;
	private String frameduIpv6Prefix;
	private String framedIpAddress;
	private Long sourceEci;
	private Integer sourceTac;
	private String sourceNeIp;
	private Integer sourceNePort;
	private String destinationNeIp;
	private Integer destinationNePort;
	private Integer qci;

	//辅助字段 由于本身没有cellid 需要取S1MME接口的cellid
	private String cellId;
	private String mmeGroupId;
	private String mmeCode;
	private String rangeTime;
	public GxXdr(String [] values) {
		this.commXdr = new CommXdr(values);
		this.procedureType = ParseUtils.parseInteger(values[8]);
		this.procedureStartTime = ParseUtils.parseLong(values[9]);
		this.procedureEndTime = ParseUtils.parseLong(values[10]);
		this.icid = values[11];
		this.originRealm = values[12];
		this.destinationRealm = values[13];
		this.originHost = values[14];
		this.destinationHost = values[15];
		this.sgsnSgwSigIp = values[16];
		this.afAppId = values[17];
		this.ccRequestType = values[18];
		this.rxRequestType = values[19];
		this.mediaType = values[20];
		this.abortCause = values[21];
		this.resultCode = values[22];
		this.experimentalResultCode = values[23]; 
		this.sessionReleaseCause = values[24];
		this.ruleFailureCode = ParseUtils.parseInteger(values[25]);
		this.sessionid = values[26];
		this.calledStationId = values[27];
		this.frameduIpv6Prefix = values[28];
		this.framedIpAddress = values[29];
		this.sourceEci =  ParseUtils.parseLong(values[30]);
		this.cellId = values[30] ;
		this.sourceTac = ParseUtils.parseInteger(values[31]);
		this.sourceNeIp = values[32];
		this.sourceNePort =  ParseUtils.parseInteger(values[33]);
		this.destinationNeIp = values[34];
		this.destinationNePort =  ParseUtils.parseInteger(values[35]);
		this.qci =  ParseUtils.parseInteger(values[36]);
//		this.rangeTime = values[values.length-1];
	}
	
	
	public GxXdr(String [] values,int share) {
		this.commXdr = new CommXdr(values);
		this.procedureType = ParseUtils.parseInteger(values[8]);
		this.procedureStartTime = ParseUtils.parseLong(values[9]);
		this.procedureEndTime = ParseUtils.parseLong(values[10]);
		this.icid = values[11];
		this.originRealm = values[12];
		this.destinationRealm = values[13];
		this.originHost = values[14];
		this.destinationHost = values[15];
		this.sgsnSgwSigIp = values[16];
		this.afAppId = values[17];
		this.ccRequestType = values[18];
		this.rxRequestType = values[19];
		this.mediaType = values[20];
		this.abortCause = values[21];
		this.resultCode = values[22];
		this.experimentalResultCode = values[23]; 
		this.sessionReleaseCause = values[24];
		this.ruleFailureCode = ParseUtils.parseInteger(values[25]);
		this.sessionid = values[26];
		this.calledStationId = values[27];
		this.frameduIpv6Prefix = values[28];
		this.framedIpAddress = values[29];
		this.sourceEci =  ParseUtils.parseLong(values[30]);
		this.sourceTac = ParseUtils.parseInteger(values[31]);
		this.sourceNeIp = values[32];
		this.sourceNePort =  ParseUtils.parseInteger(values[33]);
		this.destinationNeIp = values[34];
		this.destinationNePort =  ParseUtils.parseInteger(values[35]);
		this.qci =  ParseUtils.parseInteger(values[36]);
//		this.rangeTime = values[values.length-1];
	}
	
	
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(commXdr.toString());
		sb.append(procedureType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureStartTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(procedureEndTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(icid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(originRealm);   
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(destinationRealm);    
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(originHost);      
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(destinationHost);       
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sgsnSgwSigIp);      
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(afAppId);        
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ccRequestType);        
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(rxRequestType);       
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(mediaType);       
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(abortCause);            
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(resultCode);           
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(experimentalResultCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sessionReleaseCause);
//		sb.append(rangeTime);
//		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ruleFailureCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sessionid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(calledStationId);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(frameduIpv6Prefix);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(framedIpAddress);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceEci);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceTac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceNeIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sourceNePort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(destinationNeIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(destinationNePort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(qci);
		return sb.toString();
	}
	

	public CommXdr getCommXdr() {
		return commXdr;
	}

	public void setCommXdr(CommXdr commXdr) {
		this.commXdr = commXdr;
	}

	/**
	 * @return the procedureType
	 */
	public Integer getProcedureType() {
		return procedureType;
	}
	/**
	 * @param procedureType the procedureType to set
	 */
	public void setProcedureType(Integer procedureType) {
		this.procedureType = procedureType;
	}
	/**
	 * @return the procedureStartTime
	 */
	public Long getProcedureStartTime() {
		return procedureStartTime;
	}
	/**
	 * @param procedureStartTime the procedureStartTime to set
	 */
	public void setProcedureStartTime(Long procedureStartTime) {
		this.procedureStartTime = procedureStartTime;
	}
	/**
	 * @return the procedureEndTime
	 */
	public Long getProcedureEndTime() {
		return procedureEndTime;
	}
	/**
	 * @param procedureEndTime the procedureEndTime to set
	 */
	public void setProcedureEndTime(Long procedureEndTime) {
		this.procedureEndTime = procedureEndTime;
	}
	/**
	 * @return the icid
	 */
	public String getIcid() {
		return icid;
	}
	/**
	 * @param icid the icid to set
	 */
	public void setIcid(String icid) {
		this.icid = icid;
	}
	/**
	 * @return the originRealm
	 */
	public String getOriginRealm() {
		return originRealm;
	}
	/**
	 * @param originRealm the originRealm to set
	 */
	public void setOriginRealm(String originRealm) {
		this.originRealm = originRealm;
	}
	/**
	 * @return the destinationRealm
	 */
	public String getDestinationRealm() {
		return destinationRealm;
	}
	/**
	 * @param destinationRealm the destinationRealm to set
	 */
	public void setDestinationRealm(String destinationRealm) {
		this.destinationRealm = destinationRealm;
	}
	/**
	 * @return the originHost
	 */
	public String getOriginHost() {
		return originHost;
	}
	/**
	 * @param originHost the originHost to set
	 */
	public void setOriginHost(String originHost) {
		this.originHost = originHost;
	}
	/**
	 * @return the destinationHost
	 */
	public String getDestinationHost() {
		return destinationHost;
	}
	/**
	 * @param destinationHost the destinationHost to set
	 */
	public void setDestinationHost(String destinationHost) {
		this.destinationHost = destinationHost;
	}
	/**
	 * @return the sgsnSgwSigIp
	 */
	public String getSgsnSgwSigIp() {
		return sgsnSgwSigIp;
	}
	/**
	 * @param sgsnSgwSigIp the sgsnSgwSigIp to set
	 */
	public void setSgsnSgwSigIp(String sgsnSgwSigIp) {
		this.sgsnSgwSigIp = sgsnSgwSigIp;
	}
	/**
	 * @return the afAppId
	 */
	public String getAfAppId() {
		return afAppId;
	}
	/**
	 * @param afAppId the afAppId to set
	 */
	public void setAfAppId(String afAppId) {
		this.afAppId = afAppId;
	}
	/**
	 * @return the ccRequestType
	 */
	public String getCcRequestType() {
		return ccRequestType;
	}
	/**
	 * @param ccRequestType the ccRequestType to set
	 */
	public void setCcRequestType(String ccRequestType) {
		this.ccRequestType = ccRequestType;
	}
	/**
	 * @return the rxRequestType
	 */
	public String getRxRequestType() {
		return rxRequestType;
	}
	/**
	 * @param rxRequestType the rxRequestType to set
	 */
	public void setRxRequestType(String rxRequestType) {
		this.rxRequestType = rxRequestType;
	}
	/**
	 * @return the mediaType
	 */
	public String getMediaType() {
		return mediaType;
	}
	/**
	 * @param mediaType the mediaType to set
	 */
	public void setMediaType(String mediaType) {
		this.mediaType = mediaType;
	}
	/**
	 * @return the abortCause
	 */
	public String getAbortCause() {
		return abortCause;
	}
	/**
	 * @param abortCause the abortCause to set
	 */
	public void setAbortCause(String abortCause) {
		this.abortCause = abortCause;
	}
	/**
	 * @return the resultCode
	 */
	public String getResultCode() {
		return resultCode;
	}
	/**
	 * @param resultCode the resultCode to set
	 */
	public void setResultCode(String resultCode) {
		this.resultCode = resultCode;
	}
	/**
	 * @return the experimentalResultCode
	 */
	public String getExperimentalResultCode() {
		return experimentalResultCode;
	}
	/**
	 * @param experimentalResultCode the experimentalResultCode to set
	 */
	public void setExperimentalResultCode(String experimentalResultCode) {
		this.experimentalResultCode = experimentalResultCode;
	}
	/**
	 * @return the sessionReleaseCause
	 */
	public String getSessionReleaseCause() {
		return sessionReleaseCause;
	}
	/**
	 * @param sessionReleaseCause the sessionReleaseCause to set
	 */
	public void setSessionReleaseCause(String sessionReleaseCause) {
		this.sessionReleaseCause = sessionReleaseCause;
	}

	/**
	 * @return the cellId
	 */
	public String getCellId() {
		return cellId;
	}

	/**
	 * @param cellId the cellId to set
	 */
	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

	/**
	 * @return the mmeGroupId
	 */
	public String getMmeGroupId() {
		return mmeGroupId;
	}

	/**
	 * @param mmeGroupId the mmeGroupId to set
	 */
	public void setMmeGroupId(String mmeGroupId) {
		this.mmeGroupId = mmeGroupId;
	}

	/**
	 * @return the mmeCode
	 */
	public String getMmeCode() {
		return mmeCode;
	}

	/**
	 * @param mmeCode the mmeCode to set
	 */
	public void setMmeCode(String mmeCode) {
		this.mmeCode = mmeCode;
	}

	public String getRangeTime() {
		return rangeTime;
	}

	public void setRangeTime(String rangeTime) {
		this.rangeTime = rangeTime;
	}

	public String getSessionid() {
		return sessionid;
	}

	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}

	public String getCalledStationId() {
		return calledStationId;
	}

	public void setCalledStationId(String calledStationId) {
		this.calledStationId = calledStationId;
	}

	public String getFrameduIpv6Prefix() {
		return frameduIpv6Prefix;
	}

	public void setFrameduIpv6Prefix(String frameduIpv6Prefix) {
		this.frameduIpv6Prefix = frameduIpv6Prefix;
	}

	public String getFramedIpAddress() {
		return framedIpAddress;
	}

	public void setFramedIpAddress(String framedIpAddress) {
		this.framedIpAddress = framedIpAddress;
	}

	public String getSourceNeIp() {
		return sourceNeIp;
	}

	public void setSourceNeIp(String sourceNeIp) {
		this.sourceNeIp = sourceNeIp;
	}

	public String getDestinationNeIp() {
		return destinationNeIp;
	}

	public void setDestinationNeIp(String destinationNeIp) {
		this.destinationNeIp = destinationNeIp;
	}

	public Integer getRuleFailureCode() {
		return ruleFailureCode;
	}

	public void setRuleFailureCode(Integer ruleFailureCode) {
		this.ruleFailureCode = ruleFailureCode;
	}

	public Long getSourceEci() {
		return sourceEci;
	}

	public void setSourceEci(Long sourceEci) {
		this.sourceEci = sourceEci;
	}

	public Integer getSourceTac() {
		return sourceTac;
	}

	public void setSourceTac(Integer sourceTac) {
		this.sourceTac = sourceTac;
	}

	public Integer getSourceNePort() {
		return sourceNePort;
	}

	public void setSourceNePort(Integer sourceNePort) {
		this.sourceNePort = sourceNePort;
	}

	public Integer getDestinationNePort() {
		return destinationNePort;
	}

	public void setDestinationNePort(Integer destinationNePort) {
		this.destinationNePort = destinationNePort;
	}

	public Integer getQci() {
		return qci;
	}

	public void setQci(Integer qci) {
		this.qci = qci;
	}

}
