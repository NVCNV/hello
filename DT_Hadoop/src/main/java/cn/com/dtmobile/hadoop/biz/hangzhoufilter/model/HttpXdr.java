package cn.com.dtmobile.hadoop.biz.hangzhoufilter.model;

public class HttpXdr {
	private String length;
	private String city;
	private String interfaces;
	private String xdrId ;
	private String rat;
	private String imsi;
	private String imei;
	private String msisdn;
	private String machineIpAddType;
	private String sgwGgsnIpAdd;
	private String enbSgsnIpIdd;
	private String sgwGgsnPort ;
	private String enbSgsnPort ;
	private String enbSgsnGtpTeid ;
	private String sgwGgsnGtpTeid ;
	private String tac;
	private String cellId;
	private String apn;
	private String appTypeCode ;
	private String procedureStartTime;
	private String procedureEndTime;
	private String protocolType;
	private String appType;
	private String appSubType;
	private String appContent;
	private String appStatus;
	private String userIpv4 ;
	private String userIpv6 ;
	private String userPort ;
	private String l4Protocal;
	private String appServerIpIpv4;
	private String appServerIpIpv6;
	private String appServerPort;
	private String ulData;
	private String dlData;
	private String ulIpPacket;
	private String dlIpPacket;
	private String tcpOutorderMesscntUplink;
	private String tcpOutorderMesscntDownlink ;
	private String tcpRetransMesscntUplink ;
	private String tcpRetransMesscntDownlink;
	private String tcpResponseTdelay ;
	private String tcpIffTdelay;
	private String ulIpFragPackets;
	private String dlIpFragPackets;
	private String tcpSucWorkRequTdelay ;
	private String workRequRespacketTdelay ;
	private String winBs ;
	private String mssBs ;
	private String tcpTryFreq;
	private String tcpStateMark;
	private String whetherSessionendFlag;
	private String httpVersion ;
	private String workType ;
	private String httpWapWorkState;
	private String fstHttpRespacketTdelay;
	private String lastHttpContpacketTdelay;
	private String lastIckIffpacketTdelay;
	private String host;
	private String uri;
	private String xOnlineHost ;
	private String userIgent;
	private String httpContentType;
	private String referUri ;
	private String cookie;
	private String contentLength;
	private String targetIction;
	private String wtpBraekType;
	private String wtpBreakReason ;
	private String title ;
	private String keyWord;
	private String busiCondMark;
	private String busiFinishFlag ;
	private String busiTdelay;
	private String browseTool;
	private String portalIpplitCllt;
	private String httpContent ;
	private String pageId;
	private String serviceType ;
	
	public HttpXdr(String values[]) {
		this.length = values[0];
		this.city = values[1];
		this.interfaces = values[2];
		this.xdrId  = values[3];
		this.rat = values[4];
		this.imsi = values[5];
		this.imei = values[6];
		this.msisdn = values[7];
		this.machineIpAddType = values[8];
		this.sgwGgsnIpAdd = values[9];
		this.enbSgsnIpIdd = values[10];
		this.sgwGgsnPort  = values[11];
		this.enbSgsnPort  = values[12];
		this.enbSgsnGtpTeid  = values[13];
		this.sgwGgsnGtpTeid  = values[14];
		this.tac = values[15];
		this.cellId = values[16];
		this.apn = values[17];
		this.appTypeCode  = values[18];
		this.procedureStartTime = values[19];
		this.procedureEndTime = values[20];
		this.protocolType = values[21];
		this.appType = values[22];
		this.appSubType = values[23];
		this.appContent = values[24];
		this.appStatus = values[25];
		this.userIpv4  = values[26];
		this.userIpv6  = values[27];
		this.userPort  = values[28];
		this.l4Protocal = values[29];
		this.appServerIpIpv4 = values[30];
		this.appServerIpIpv6 = values[31];
		this.appServerPort = values[32];
		this.ulData = values[33];
		this.dlData = values[34];
		this.ulIpPacket = values[35];
		this.dlIpPacket = values[36];
		this.tcpOutorderMesscntUplink = values[37];
		this.tcpOutorderMesscntDownlink  = values[38];
		this.tcpRetransMesscntUplink  = values[39];
		this.tcpRetransMesscntDownlink = values[40];
		this.tcpResponseTdelay  = values[41];
		this.tcpIffTdelay = values[42];
		this.ulIpFragPackets = values[43];
		this.dlIpFragPackets = values[44];
		this.tcpSucWorkRequTdelay  = values[45];
		this.workRequRespacketTdelay  = values[46];
		this.winBs  = values[47];
		this.mssBs  = values[48];
		this.tcpTryFreq = values[49];
		this.tcpStateMark = values[50];
		this.whetherSessionendFlag = values[51];
		this.httpVersion  = values[52];
		this.workType  = values[53];
		this.httpWapWorkState = values[54];
		this.fstHttpRespacketTdelay = values[55];
		this.lastHttpContpacketTdelay = values[56];
		this.lastIckIffpacketTdelay = values[57];
		this.host = values[58];
		this.uri = values[59];
		this.xOnlineHost  = values[60];
		this.userIgent = values[61];
		this.httpContentType = values[62];
		this.referUri  = values[63];
		this.cookie = values[64];
		this.contentLength = values[65];
		this.targetIction = values[66];
		this.wtpBraekType = values[67];
		this.wtpBreakReason  = values[68];
		this.title  = values[69];
		this.keyWord = values[70];
		this.busiCondMark = values[71];
		this.busiFinishFlag  = values[72];
		this.busiTdelay = values[73];
		this.browseTool = values[74];
		this.portalIpplitCllt = values[75];
		this.httpContent  = values[76];
		this.pageId = values[77];
		this.serviceType  = values[78];
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
	public String getMachineIpAddType() {
		return machineIpAddType;
	}
	public void setMachineIpAddType(String machineIpAddType) {
		this.machineIpAddType = machineIpAddType;
	}
	public String getSgwGgsnIpAdd() {
		return sgwGgsnIpAdd;
	}
	public void setSgwGgsnIpAdd(String sgwGgsnIpAdd) {
		this.sgwGgsnIpAdd = sgwGgsnIpAdd;
	}
	public String getEnbSgsnIpIdd() {
		return enbSgsnIpIdd;
	}
	public void setEnbSgsnIpIdd(String enbSgsnIpIdd) {
		this.enbSgsnIpIdd = enbSgsnIpIdd;
	}
	public String getSgwGgsnPort() {
		return sgwGgsnPort;
	}
	public void setSgwGgsnPort(String sgwGgsnPort) {
		this.sgwGgsnPort = sgwGgsnPort;
	}
	public String getEnbSgsnPort() {
		return enbSgsnPort;
	}
	public void setEnbSgsnPort(String enbSgsnPort) {
		this.enbSgsnPort = enbSgsnPort;
	}
	public String getEnbSgsnGtpTeid() {
		return enbSgsnGtpTeid;
	}
	public void setEnbSgsnGtpTeid(String enbSgsnGtpTeid) {
		this.enbSgsnGtpTeid = enbSgsnGtpTeid;
	}
	public String getSgwGgsnGtpTeid() {
		return sgwGgsnGtpTeid;
	}
	public void setSgwGgsnGtpTeid(String sgwGgsnGtpTeid) {
		this.sgwGgsnGtpTeid = sgwGgsnGtpTeid;
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
	public String getApn() {
		return apn;
	}
	public void setApn(String apn) {
		this.apn = apn;
	}
	public String getAppTypeCode() {
		return appTypeCode;
	}
	public void setAppTypeCode(String appTypeCode) {
		this.appTypeCode = appTypeCode;
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
	public String getProtocolType() {
		return protocolType;
	}
	public void setProtocolType(String protocolType) {
		this.protocolType = protocolType;
	}
	public String getAppType() {
		return appType;
	}
	public void setAppType(String appType) {
		this.appType = appType;
	}
	public String getAppSubType() {
		return appSubType;
	}
	public void setAppSubType(String appSubType) {
		this.appSubType = appSubType;
	}
	public String getAppContent() {
		return appContent;
	}
	public void setAppContent(String appContent) {
		this.appContent = appContent;
	}
	public String getAppStatus() {
		return appStatus;
	}
	public void setAppStatus(String appStatus) {
		this.appStatus = appStatus;
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
	public String getUserPort() {
		return userPort;
	}
	public void setUserPort(String userPort) {
		this.userPort = userPort;
	}
	public String getL4Protocal() {
		return l4Protocal;
	}
	public void setL4Protocal(String l4Protocal) {
		this.l4Protocal = l4Protocal;
	}
	public String getAppServerIpIpv4() {
		return appServerIpIpv4;
	}
	public void setAppServerIpIpv4(String appServerIpIpv4) {
		this.appServerIpIpv4 = appServerIpIpv4;
	}
	public String getAppServerIpIpv6() {
		return appServerIpIpv6;
	}
	public void setAppServerIpIpv6(String appServerIpIpv6) {
		this.appServerIpIpv6 = appServerIpIpv6;
	}
	public String getAppServerPort() {
		return appServerPort;
	}
	public void setAppServerPort(String appServerPort) {
		this.appServerPort = appServerPort;
	}
	public String getUlData() {
		return ulData;
	}
	public void setUlData(String ulData) {
		this.ulData = ulData;
	}
	public String getDlData() {
		return dlData;
	}
	public void setDlData(String dlData) {
		this.dlData = dlData;
	}
	public String getUlIpPacket() {
		return ulIpPacket;
	}
	public void setUlIpPacket(String ulIpPacket) {
		this.ulIpPacket = ulIpPacket;
	}
	public String getDlIpPacket() {
		return dlIpPacket;
	}
	public void setDlIpPacket(String dlIpPacket) {
		this.dlIpPacket = dlIpPacket;
	}
	public String getTcpOutorderMesscntUplink() {
		return tcpOutorderMesscntUplink;
	}
	public void setTcpOutorderMesscntUplink(String tcpOutorderMesscntUplink) {
		this.tcpOutorderMesscntUplink = tcpOutorderMesscntUplink;
	}
	public String getTcpOutorderMesscntDownlink() {
		return tcpOutorderMesscntDownlink;
	}
	public void setTcpOutorderMesscntDownlink(String tcpOutorderMesscntDownlink) {
		this.tcpOutorderMesscntDownlink = tcpOutorderMesscntDownlink;
	}
	public String getTcpRetransMesscntUplink() {
		return tcpRetransMesscntUplink;
	}
	public void setTcpRetransMesscntUplink(String tcpRetransMesscntUplink) {
		this.tcpRetransMesscntUplink = tcpRetransMesscntUplink;
	}
	public String getTcpRetransMesscntDownlink() {
		return tcpRetransMesscntDownlink;
	}
	public void setTcpRetransMesscntDownlink(String tcpRetransMesscntDownlink) {
		this.tcpRetransMesscntDownlink = tcpRetransMesscntDownlink;
	}
	public String getTcpResponseTdelay() {
		return tcpResponseTdelay;
	}
	public void setTcpResponseTdelay(String tcpResponseTdelay) {
		this.tcpResponseTdelay = tcpResponseTdelay;
	}
	public String getTcpIffTdelay() {
		return tcpIffTdelay;
	}
	public void setTcpIffTdelay(String tcpIffTdelay) {
		this.tcpIffTdelay = tcpIffTdelay;
	}
	public String getUlIpFragPackets() {
		return ulIpFragPackets;
	}
	public void setUlIpFragPackets(String ulIpFragPackets) {
		this.ulIpFragPackets = ulIpFragPackets;
	}
	public String getDlIpFragPackets() {
		return dlIpFragPackets;
	}
	public void setDlIpFragPackets(String dlIpFragPackets) {
		this.dlIpFragPackets = dlIpFragPackets;
	}
	public String getTcpSucWorkRequTdelay() {
		return tcpSucWorkRequTdelay;
	}
	public void setTcpSucWorkRequTdelay(String tcpSucWorkRequTdelay) {
		this.tcpSucWorkRequTdelay = tcpSucWorkRequTdelay;
	}
	public String getWorkRequRespacketTdelay() {
		return workRequRespacketTdelay;
	}
	public void setWorkRequRespacketTdelay(String workRequRespacketTdelay) {
		this.workRequRespacketTdelay = workRequRespacketTdelay;
	}
	public String getWinBs() {
		return winBs;
	}
	public void setWinBs(String winBs) {
		this.winBs = winBs;
	}
	public String getMssBs() {
		return mssBs;
	}
	public void setMssBs(String mssBs) {
		this.mssBs = mssBs;
	}
	public String getTcpTryFreq() {
		return tcpTryFreq;
	}
	public void setTcpTryFreq(String tcpTryFreq) {
		this.tcpTryFreq = tcpTryFreq;
	}
	public String getTcpStateMark() {
		return tcpStateMark;
	}
	public void setTcpStateMark(String tcpStateMark) {
		this.tcpStateMark = tcpStateMark;
	}
	public String getWhetherSessionendFlag() {
		return whetherSessionendFlag;
	}
	public void setWhetherSessionendFlag(String whetherSessionendFlag) {
		this.whetherSessionendFlag = whetherSessionendFlag;
	}
	public String getHttpVersion() {
		return httpVersion;
	}
	public void setHttpVersion(String httpVersion) {
		this.httpVersion = httpVersion;
	}
	public String getWorkType() {
		return workType;
	}
	public void setWorkType(String workType) {
		this.workType = workType;
	}
	public String getHttpWapWorkState() {
		return httpWapWorkState;
	}
	public void setHttpWapWorkState(String httpWapWorkState) {
		this.httpWapWorkState = httpWapWorkState;
	}
	public String getFstHttpRespacketTdelay() {
		return fstHttpRespacketTdelay;
	}
	public void setFstHttpRespacketTdelay(String fstHttpRespacketTdelay) {
		this.fstHttpRespacketTdelay = fstHttpRespacketTdelay;
	}
	public String getLastHttpContpacketTdelay() {
		return lastHttpContpacketTdelay;
	}
	public void setLastHttpContpacketTdelay(String lastHttpContpacketTdelay) {
		this.lastHttpContpacketTdelay = lastHttpContpacketTdelay;
	}
	public String getLastIckIffpacketTdelay() {
		return lastIckIffpacketTdelay;
	}
	public void setLastIckIffpacketTdelay(String lastIckIffpacketTdelay) {
		this.lastIckIffpacketTdelay = lastIckIffpacketTdelay;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
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
	public String getUserIgent() {
		return userIgent;
	}
	public void setUserIgent(String userIgent) {
		this.userIgent = userIgent;
	}
	public String getHttpContentType() {
		return httpContentType;
	}
	public void setHttpContentType(String httpContentType) {
		this.httpContentType = httpContentType;
	}
	public String getReferUri() {
		return referUri;
	}
	public void setReferUri(String referUri) {
		this.referUri = referUri;
	}
	public String getCookie() {
		return cookie;
	}
	public void setCookie(String cookie) {
		this.cookie = cookie;
	}
	public String getContentLength() {
		return contentLength;
	}
	public void setContentLength(String contentLength) {
		this.contentLength = contentLength;
	}
	public String getTargetIction() {
		return targetIction;
	}
	public void setTargetIction(String targetIction) {
		this.targetIction = targetIction;
	}
	public String getWtpBraekType() {
		return wtpBraekType;
	}
	public void setWtpBraekType(String wtpBraekType) {
		this.wtpBraekType = wtpBraekType;
	}
	public String getWtpBreakReason() {
		return wtpBreakReason;
	}
	public void setWtpBreakReason(String wtpBreakReason) {
		this.wtpBreakReason = wtpBreakReason;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getKeyWord() {
		return keyWord;
	}
	public void setKeyWord(String keyWord) {
		this.keyWord = keyWord;
	}
	public String getBusiCondMark() {
		return busiCondMark;
	}
	public void setBusiCondMark(String busiCondMark) {
		this.busiCondMark = busiCondMark;
	}
	public String getBusiFinishFlag() {
		return busiFinishFlag;
	}
	public void setBusiFinishFlag(String busiFinishFlag) {
		this.busiFinishFlag = busiFinishFlag;
	}
	public String getBusiTdelay() {
		return busiTdelay;
	}
	public void setBusiTdelay(String busiTdelay) {
		this.busiTdelay = busiTdelay;
	}
	public String getBrowseTool() {
		return browseTool;
	}
	public void setBrowseTool(String browseTool) {
		this.browseTool = browseTool;
	}
	public String getPortalIpplitCllt() {
		return portalIpplitCllt;
	}
	public void setPortalIpplitCllt(String portalIpplitCllt) {
		this.portalIpplitCllt = portalIpplitCllt;
	}
	public String getHttpContent() {
		return httpContent;
	}
	public void setHttpContent(String httpContent) {
		this.httpContent = httpContent;
	}
	public String getPageId() {
		return pageId;
	}
	public void setPageId(String pageId) {
		this.pageId = pageId;
	}
	public String getServiceType() {
		return serviceType;
	}
	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
	
	
}
