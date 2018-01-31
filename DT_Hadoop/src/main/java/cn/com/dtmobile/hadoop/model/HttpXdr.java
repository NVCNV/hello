package cn.com.dtmobile.hadoop.model;

import cn.com.dtmobile.hadoop.biz.exception.constants.FieldConstants;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class HttpXdr {
	private CommXdr commXdr;
	private Integer machineIpAddType ;
	private String sgwIp;
	private String enb_ip;
	private String pgwAdd;
	private Integer sgwPort;
	private Integer enbPort;
	private Integer pgw_Port;
	private Long enbUteid ;
	private Long sgwUteid ;
	private Integer tac;
	private Long eci ;
	private String apn ;
	private Integer xdrType;
	private Long startTime ;
	private Long endTime ;
	private String longitude;
	private String latitude ;
	private Integer protocoltype ;
	private Integer appType;
	private Integer appSubtype;
	private Integer appContent;
	private Integer appStatus ;
	private String ipAddressType ;
	private String useripv4 ;
	private String useripv6 ;
	private Integer userport;
	private Integer l4Protocol;
	private String appserverIpv4;
	private String appserverIpv6;
	private Integer appserverPort;
	private Long ulthroughput;
	private Long dlthroughput;
	private Long ulpackets;
	private Long dlpackets;
	private Long updura;
	private Long downdura ;
	private Long ultcpDisorderPackets;
	private Long dltcpDisorderPackets;
	private Long ultcpRetransferPackets;
	private Long dltcpRetransferPackets;
	private Long ultcpResponseTime;
	private Long dltcpResponseTime;
	private Long ultcpFlagPackets ;
	private Long dltcpFlagPackets ;
	private Long tcplinkResponseTime1;
	private Long tcplinkResponseTime2;
	private Long windowsize ;
	private Long msssize;
	private Integer tcplinkCount;
	private Integer tcplinkState;
	private Integer sessionEnd;
	private Integer tcpSynAckMum ;
	private Integer tcpAckNum;
	private Integer tcp12Status ;
	private Integer tcp23Status ;
	private Integer repetition ;
	private Integer version ;
	private Integer transType ;
	private Integer httpStatusCode;
	private Long responseTime ;
	private Long lastpacketDelay;
	private Long lastpacketAckDelay ;
	private String host;
	private Integer urilength;
	private String uri ;
	private String xOnlineHost ;
	private Integer useragentlength ;
	private String useragent;
	private String contentType;
	private Integer referUrilength ;
	private String referUri;
	private Integer cookielength ;
	private String cookie;
	private Long contentlength ;
	private String keyword;
	private Integer action;
	private Integer finish;
	private Long delay ;
	private Integer browseTool;
	private Integer portals ;
	private Integer locationlength;
	private String location;
	private Integer firstrequest ;
	private String useraccount;
	
	public HttpXdr(String [] values) {
		commXdr = new CommXdr(values, 1);
		this.machineIpAddType = ParseUtils.parseInteger(values[12]);
		this.sgwIp = values[13];
		this.enb_ip = values[14];
		this.pgwAdd = values[15];
		this.sgwPort = ParseUtils.parseInteger(values[16]);
		this.enbPort = ParseUtils.parseInteger(values[17]);
		this.pgw_Port = ParseUtils.parseInteger(values[18]);
		this.enbUteid= ParseUtils.parseLong(values[19]);
		this.sgwUteid= ParseUtils.parseLong(values[20]);
		this.tac = ParseUtils.parseInteger(values[21]);
		this.eci= ParseUtils.parseLong(values[22]);
		this.apn= values[23];
		this.xdrType = ParseUtils.parseInteger(values[24]);
		this.startTime= ParseUtils.parseLong(values[25]);
		this.endTime= ParseUtils.parseLong(values[26]);
		this.longitude = values[27];
		this.latitude= values[28];
		this.protocoltype= ParseUtils.parseInteger(values[29]);
		this.appType = ParseUtils.parseInteger(values[30]);
		this.appSubtype = ParseUtils.parseInteger(values[31]);
		this.appContent = ParseUtils.parseInteger(values[32]);
		this.appStatus= ParseUtils.parseInteger(values[33]);
		this.ipAddressType= values[34];
		this.useripv4= values[35];
		this.useripv6= values[36];
		this.userport = ParseUtils.parseInteger(values[37]);
		this.l4Protocol = ParseUtils.parseInteger(values[38]);
		this.appserverIpv4 = values[39];
		this.appserverIpv6 = values[40];
		this.appserverPort = ParseUtils.parseInteger(values[41]);
		this.ulthroughput = ParseUtils.parseLong(values[42]);
		this.dlthroughput = ParseUtils.parseLong(values[43]);
		this.ulpackets = ParseUtils.parseLong(values[44]);
		this.dlpackets = ParseUtils.parseLong(values[45]);
		this.updura = ParseUtils.parseLong(values[46]);
		this.downdura= ParseUtils.parseLong(values[47]);
		this.ultcpDisorderPackets = ParseUtils.parseLong(values[48]);
		this.dltcpDisorderPackets = ParseUtils.parseLong(values[49]);
		this.ultcpRetransferPackets = ParseUtils.parseLong(values[50]);
		this.dltcpRetransferPackets = ParseUtils.parseLong(values[51]);
		this.ultcpResponseTime = ParseUtils.parseLong(values[52]);
		this.dltcpResponseTime = ParseUtils.parseLong(values[53]);
		this.ultcpFlagPackets= ParseUtils.parseLong(values[54]);
		this.dltcpFlagPackets= ParseUtils.parseLong(values[55]);
		this.tcplinkResponseTime1 = ParseUtils.parseLong(values[56]);
		this.tcplinkResponseTime2 = ParseUtils.parseLong(values[57]);
		this.windowsize= ParseUtils.parseLong(values[58]);
		this.msssize = ParseUtils.parseLong(values[59]);
		this.tcplinkCount = ParseUtils.parseInteger(values[60]);
		this.tcplinkState = ParseUtils.parseInteger(values[61]);
		this.sessionEnd = ParseUtils.parseInteger(values[62]);
		this.tcpSynAckMum= ParseUtils.parseInteger(values[63]);
		this.tcpAckNum = ParseUtils.parseInteger(values[64]);
		this.tcp12Status= ParseUtils.parseInteger(values[65]);
		this.tcp23Status= ParseUtils.parseInteger(values[66]);
		this.repetition= ParseUtils.parseInteger(values[67]);
		this.version= ParseUtils.parseInteger(values[68]);
		this.transType= ParseUtils.parseInteger(values[69]);
		this.httpStatusCode = ParseUtils.parseInteger(values[70]);
		this.responseTime= ParseUtils.parseLong(values[71]);
		this.lastpacketDelay = ParseUtils.parseLong(values[72]);
		this.lastpacketAckDelay= ParseUtils.parseLong(values[73]);
		this.host = values[74];
		this.urilength = ParseUtils.parseInteger(values[75]);
		this.uri= values[76];
		this.xOnlineHost= values[77];
		this.useragentlength= ParseUtils.parseInteger(values[78]);
		this.useragent = values[79];
		this.contentType = values[80];
		this.referUrilength= ParseUtils.parseInteger(values[81]);
		this.referUri = values[82];
		this.cookielength= ParseUtils.parseInteger(values[83]);
		this.cookie = values[84];
		this.contentlength = ParseUtils.parseLong(values[85]);
		this.keyword = values[86];
		this.action = ParseUtils.parseInteger(values[87]);
		this.finish = ParseUtils.parseInteger(values[88]);
		this.delay = ParseUtils.parseLong(values[89]);;
		this.browseTool = ParseUtils.parseInteger(values[90]);
		this.portals = ParseUtils.parseInteger(values[91]);
		this.locationlength = ParseUtils.parseInteger(values[92]);
		this.location = values[93];
		this.firstrequest = ParseUtils.parseInteger(values[94]);
		this.useraccount = values[95];
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(commXdr.toString());
		sb.append(machineIpAddType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sgwIp);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enb_ip);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sgwPort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enbPort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(enbUteid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sgwUteid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tac);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(eci);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(apn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(FieldConstants.changeAppType(xdrType));
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(startTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(endTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(protocoltype);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appSubtype);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appContent);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appStatus);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(useripv4);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(useripv6);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(userport);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(l4Protocol);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appserverIpv4);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appserverIpv6);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(appserverPort);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ulthroughput);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dlthroughput);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ulpackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dlpackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ultcpDisorderPackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dltcpDisorderPackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ultcpRetransferPackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dltcpRetransferPackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ultcpResponseTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dltcpResponseTime);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(ultcpFlagPackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(dltcpFlagPackets);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tcplinkResponseTime1);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tcplinkResponseTime2);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(windowsize);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(msssize);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tcplinkCount);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(tcplinkState);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(sessionEnd);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(version);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(transType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(httpStatusCode);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(firstrequest);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(lastpacketDelay);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(lastpacketAckDelay);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(host);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(uri);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(xOnlineHost);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(useragent);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(contentType);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(referUri);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(cookie);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(contentlength);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append("");
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append("");
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append("");
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append("");
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(keyword);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(action);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(finish);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(delay);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(browseTool);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(portals);
		return sb.toString();
		
	}
	public CommXdr getCommXdr() {
		return commXdr;
	}
	public void setCommXdr(CommXdr commXdr) {
		this.commXdr = commXdr;
	}
	
	public Integer getMachineIpAddType() {
		return machineIpAddType;
	}
	public void setMachineIpAddType(Integer machineIpAddType) {
		this.machineIpAddType = machineIpAddType;
	}
	public String getSgwIp() {
		return sgwIp;
	}
	public void setSgwIp(String sgwIp) {
		this.sgwIp = sgwIp;
	}
	public String getEnb_ip() {
		return enb_ip;
	}
	public void setEnb_ip(String enb_ip) {
		this.enb_ip = enb_ip;
	}
	public String getPgwAdd() {
		return pgwAdd;
	}
	public void setPgwAdd(String pgwAdd) {
		this.pgwAdd = pgwAdd;
	}
	public Integer getSgwPort() {
		return sgwPort;
	}
	public void setSgwPort(Integer sgwPort) {
		this.sgwPort = sgwPort;
	}
	public Integer getEnbPort() {
		return enbPort;
	}
	public void setEnbPort(Integer enbPort) {
		this.enbPort = enbPort;
	}
	public Integer getPgw_Port() {
		return pgw_Port;
	}
	public void setPgw_Port(Integer pgw_Port) {
		this.pgw_Port = pgw_Port;
	}
	public Long getEnbUteid() {
		return enbUteid;
	}
	public void setEnbUteid(Long enbUteid) {
		this.enbUteid = enbUteid;
	}
	public Long getSgwUteid() {
		return sgwUteid;
	}
	public void setSgwUteid(Long sgwUteid) {
		this.sgwUteid = sgwUteid;
	}
	public Integer getTac() {
		return tac;
	}
	public void setTac(Integer tac) {
		this.tac = tac;
	}
	public Long getEci() {
		return eci;
	}
	public void setEci(Long eci) {
		this.eci = eci;
	}
	public String getApn() {
		return apn;
	}
	public void setApn(String apn) {
		this.apn = apn;
	}
	public Integer getXdrType() {
		return xdrType;
	}
	public void setXdrType(Integer xdrType) {
		this.xdrType = xdrType;
	}
	public Long getStartTime() {
		return startTime;
	}
	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}
	public Long getEndTime() {
		return endTime;
	}
	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public Integer getProtocoltype() {
		return protocoltype;
	}
	public void setProtocoltype(Integer protocoltype) {
		this.protocoltype = protocoltype;
	}
	public Integer getAppType() {
		
		return FieldConstants.changeAppType(appType);
	}
	public void setAppType(Integer appType) {
		this.appType = appType;
	}
	public Integer getAppSubtype() {
		return appSubtype;
	}
	public void setAppSubtype(Integer appSubtype) {
		this.appSubtype = appSubtype;
	}
	public Integer getAppContent() {
		return appContent;
	}
	public void setAppContent(Integer appContent) {
		this.appContent = appContent;
	}
	public Integer getAppStatus() {
		return appStatus;
	}
	public void setAppStatus(Integer appStatus) {
		this.appStatus = appStatus;
	}
	public String getIpAddressType() {
		return ipAddressType;
	}
	public void setIpAddressType(String ipAddressType) {
		this.ipAddressType = ipAddressType;
	}
	public String getUseripv4() {
		return useripv4;
	}
	public void setUseripv4(String useripv4) {
		this.useripv4 = useripv4;
	}
	public String getUseripv6() {
		return useripv6;
	}
	public void setUseripv6(String useripv6) {
		this.useripv6 = useripv6;
	}
	public Integer getUserport() {
		return userport;
	}
	public void setUserport(Integer userport) {
		this.userport = userport;
	}
	public Integer getL4Protocol() {
		return l4Protocol;
	}
	public void setL4Protocol(Integer l4Protocol) {
		this.l4Protocol = l4Protocol;
	}
	public String getAppserverIpv4() {
		return appserverIpv4;
	}
	public void setAppserverIpv4(String appserverIpv4) {
		this.appserverIpv4 = appserverIpv4;
	}
	public String getAppserverIpv6() {
		return appserverIpv6;
	}
	public void setAppserverIpv6(String appserverIpv6) {
		this.appserverIpv6 = appserverIpv6;
	}
	public Integer getAppserverPort() {
		return appserverPort;
	}
	public void setAppserverPort(Integer appserverPort) {
		this.appserverPort = appserverPort;
	}
	public Long getUlthroughput() {
		return ulthroughput;
	}
	public void setUlthroughput(Long ulthroughput) {
		this.ulthroughput = ulthroughput;
	}
	public Long getDlthroughput() {
		return dlthroughput;
	}
	public void setDlthroughput(Long dlthroughput) {
		this.dlthroughput = dlthroughput;
	}
	public Long getUlpackets() {
		return ulpackets;
	}
	public void setUlpackets(Long ulpackets) {
		this.ulpackets = ulpackets;
	}
	public Long getDlpackets() {
		return dlpackets;
	}
	public void setDlpackets(Long dlpackets) {
		this.dlpackets = dlpackets;
	}
	public Long getUpdura() {
		return updura;
	}
	public void setUpdura(Long updura) {
		this.updura = updura;
	}
	public Long getDowndura() {
		return downdura;
	}
	public void setDowndura(Long downdura) {
		this.downdura = downdura;
	}
	public Long getUltcpDisorderPackets() {
		return ultcpDisorderPackets;
	}
	public void setUltcpDisorderPackets(Long ultcpDisorderPackets) {
		this.ultcpDisorderPackets = ultcpDisorderPackets;
	}
	public Long getDltcpDisorderPackets() {
		return dltcpDisorderPackets;
	}
	public void setDltcpDisorderPackets(Long dltcpDisorderPackets) {
		this.dltcpDisorderPackets = dltcpDisorderPackets;
	}
	public Long getUltcpRetransferPackets() {
		return ultcpRetransferPackets;
	}
	public void setUltcpRetransferPackets(Long ultcpRetransferPackets) {
		this.ultcpRetransferPackets = ultcpRetransferPackets;
	}
	public Long getDltcpRetransferPackets() {
		return dltcpRetransferPackets;
	}
	public void setDltcpRetransferPackets(Long dltcpRetransferPackets) {
		this.dltcpRetransferPackets = dltcpRetransferPackets;
	}
	public Long getUltcpResponseTime() {
		return ultcpResponseTime;
	}
	public void setUltcpResponseTime(Long ultcpResponseTime) {
		this.ultcpResponseTime = ultcpResponseTime;
	}
	public Long getDltcpResponseTime() {
		return dltcpResponseTime;
	}
	public void setDltcpResponseTime(Long dltcpResponseTime) {
		this.dltcpResponseTime = dltcpResponseTime;
	}
	public Long getUltcpFlagPackets() {
		return ultcpFlagPackets;
	}
	public void setUltcpFlagPackets(Long ultcpFlagPackets) {
		this.ultcpFlagPackets = ultcpFlagPackets;
	}
	public Long getDltcpFlagPackets() {
		return dltcpFlagPackets;
	}
	public void setDltcpFlagPackets(Long dltcpFlagPackets) {
		this.dltcpFlagPackets = dltcpFlagPackets;
	}
	public Long getTcplinkResponseTime1() {
		return tcplinkResponseTime1;
	}
	public void setTcplinkResponseTime1(Long tcplinkResponseTime1) {
		this.tcplinkResponseTime1 = tcplinkResponseTime1;
	}
	
	public Long getTcplinkResponseTime2() {
		return tcplinkResponseTime2;
	}
	public void setTcplinkResponseTime2(Long tcplinkResponseTime2) {
		this.tcplinkResponseTime2 = tcplinkResponseTime2;
	}
	public Long getWindowsize() {
		return windowsize;
	}
	public void setWindowsize(Long windowsize) {
		this.windowsize = windowsize;
	}
	public Long getMsssize() {
		return msssize;
	}
	public void setMsssize(Long msssize) {
		this.msssize = msssize;
	}
	public Integer getTcplinkCount() {
		return tcplinkCount;
	}
	public void setTcplinkCount(Integer tcplinkCount) {
		this.tcplinkCount = tcplinkCount;
	}
	public Integer getTcplinkState() {
		return tcplinkState;
	}
	public void setTcplinkState(Integer tcplinkState) {
		this.tcplinkState = tcplinkState;
	}
	public Integer getSessionEnd() {
		return sessionEnd;
	}
	public void setSessionEnd(Integer sessionEnd) {
		this.sessionEnd = sessionEnd;
	}
	public Integer getTcpSynAckMum() {
		return tcpSynAckMum;
	}
	public void setTcpSynAckMum(Integer tcpSynAckMum) {
		this.tcpSynAckMum = tcpSynAckMum;
	}
	public Integer getTcpAckNum() {
		return tcpAckNum;
	}
	public void setTcpAckNum(Integer tcpAckNum) {
		this.tcpAckNum = tcpAckNum;
	}
	public Integer getTcp12Status() {
		return tcp12Status;
	}
	public void setTcp12Status(Integer tcp12Status) {
		this.tcp12Status = tcp12Status;
	}
	public Integer getTcp23Status() {
		return tcp23Status;
	}
	public void setTcp23Status(Integer tcp23Status) {
		this.tcp23Status = tcp23Status;
	}
	public Integer getRepetition() {
		return repetition;
	}
	public void setRepetition(Integer repetition) {
		this.repetition = repetition;
	}
	public Integer getVersion() {
		return version;
	}
	public void setVersion(Integer version) {
		this.version = version;
	}
	public Integer getTransType() {
		return transType;
	}
	public void setTransType(Integer transType) {
		this.transType = transType;
	}
	public Integer getHttpStatusCode() {
		return httpStatusCode;
	}
	public void setHttpStatusCode(Integer httpStatusCode) {
		this.httpStatusCode = httpStatusCode;
	}
	public Long getResponseTime() {
		return responseTime;
	}
	public void setResponseTime(Long responseTime) {
		this.responseTime = responseTime;
	}
	public Long getLastpacketDelay() {
		return lastpacketDelay;
	}
	public void setLastpacketDelay(Long lastpacketDelay) {
		this.lastpacketDelay = lastpacketDelay;
	}
	public Long getLastpacketAckDelay() {
		return lastpacketAckDelay;
	}
	public void setLastpacketAckDelay(Long lastpacketAckDelay) {
		this.lastpacketAckDelay = lastpacketAckDelay;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public Integer getUrilength() {
		return urilength;
	}
	public void setUrilength(Integer urilength) {
		this.urilength = urilength;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getxOnlineHost() {
		return xOnlineHost;
	}
	public void setxOnlineHost(String xOnlineHost) {
		this.xOnlineHost = xOnlineHost;
	}
	public Integer getUseragentlength() {
		return useragentlength;
	}
	public void setUseragentlength(Integer useragentlength) {
		this.useragentlength = useragentlength;
	}
	public String getUseragent() {
		return useragent;
	}
	public void setUseragent(String useragent) {
		this.useragent = useragent;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public Integer getReferUrilength() {
		return referUrilength;
	}
	public void setReferUrilength(Integer referUrilength) {
		this.referUrilength = referUrilength;
	}
	public String getReferUri() {
		return referUri;
	}
	public void setReferUri(String referUri) {
		this.referUri = referUri;
	}
	public Integer getCookielength() {
		return cookielength;
	}
	public void setCookielength(Integer cookielength) {
		this.cookielength = cookielength;
	}
	public String getCookie() {
		return cookie;
	}
	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	public Long getContentlength() {
		return contentlength;
	}
	public void setContentlength(Long contentlength) {
		this.contentlength = contentlength;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public Integer getAction() {
		return action;
	}
	public void setAction(Integer action) {
		this.action = action;
	}
	public Integer getFinish() {
		return finish;
	}
	public void setFinish(Integer finish) {
		this.finish = finish;
	}
	public Long getDelay() {
		return delay;
	}
	public void setDelay(Long delay) {
		this.delay = delay;
	}
	public Integer getBrowseTool() {
		return browseTool;
	}
	public void setBrowseTool(Integer browseTool) {
		this.browseTool = browseTool;
	}
	public Integer getPortals() {
		return portals;
	}
	public void setPortals(Integer portals) {
		this.portals = portals;
	}
	public Integer getLocationlength() {
		return locationlength;
	}
	public void setLocationlength(Integer locationlength) {
		this.locationlength = locationlength;
	}
	
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public Integer getFirstrequest() {
		return firstrequest;
	}
	public void setFirstrequest(Integer firstrequest) {
		this.firstrequest = firstrequest;
	}
	public String getUseraccount() {
		return useraccount;
	}
	public void setUseraccount(String useraccount) {
		this.useraccount = useraccount;
	}
	
	
}
