package cn.com.dtmobile.hadoop.model;

import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class CommXdr {
	private Integer length;
	private String localProvince;
	private String city;
	private String ownerProvince;
	private String ownerCity;
	private Integer roamingType;
	private Integer interfaces;
	private String xdrid;
	private Integer rat;
	private String imsi;
	private String imei;
	private String msisdn;

	public CommXdr() {
		
	}
	public CommXdr(String[] values) {
		this.length = ParseUtils.parseInteger(values[0]);
		this.city = values[1];
		this.interfaces = ParseUtils.parseInteger(values[2]);
		this.xdrid = values[3];
		this.rat = ParseUtils.parseInteger(values[4]);
		this.imsi = values[5];
		this.imei = values[6];
		this.msisdn = values[7];
	}
	
	public CommXdr(String[] values,int isShare) {
		this.length = ParseUtils.parseInteger(values[0]);
		this.localProvince = values[1];
		this.city = values[2];
		this.ownerProvince = values[3];
		this.ownerCity = values[4];
		this.roamingType =  ParseUtils.parseInteger(values[5]);
		this.interfaces = ParseUtils.parseInteger(values[6]);
		this.xdrid = values[7];
		this.rat = ParseUtils.parseInteger(values[8]);
		this.imsi = values[9];
		this.imei = values[10];
		this.msisdn = values[11];
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(length);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(city);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(interfaces);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(xdrid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(rat);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(imsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(imei);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(msisdn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		return sb.toString();
	}
	
	public String toStringShare() {
		StringBuffer sb = new StringBuffer();
		sb.append(length);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		//sb.append(localProvince);
		//sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(city);
		sb.append(StringUtils.DELIMITER_VERTICAL);
//		sb.append(ownerProvince);
//		sb.append(StringUtils.DELIMITER_VERTICAL);
//		sb.append(ownerCity);
//		sb.append(StringUtils.DELIMITER_VERTICAL);
//		sb.append(roamingType);
//		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(interfaces);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(xdrid);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(rat);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(imsi);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(imei);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		sb.append(msisdn);
		sb.append(StringUtils.DELIMITER_VERTICAL);
		return sb.toString();
	}
	public String getLocalProvince() {
		return localProvince;
	}

	public void setLocalProvince(String localProvince) {
		this.localProvince = localProvince;
	}

	public String getOwnerProvince() {
		return ownerProvince;
	}

	public void setOwnerProvince(String ownerProvince) {
		this.ownerProvince = ownerProvince;
	}

	public String getOwnerCity() {
		return ownerCity;
	}

	public void setOwnerCity(String ownerCity) {
		this.ownerCity = ownerCity;
	}

	public Integer getRoamingType() {
		return roamingType;
	}

	public void setRoamingType(Integer roamingType) {
		this.roamingType = roamingType;
	}

	public Integer getLength() {
		return length;
	}

	public void setLength(Integer length) {
		this.length = length;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Integer getInterfaces() {
		return interfaces;
	}

	public void setInterfaces(Integer interfaces) {
		this.interfaces = interfaces;
	}

	public String getXdrid() {
		return xdrid;
	}

	public void setXdrid(String xdrid) {
		this.xdrid = xdrid;
	}

	public Integer getRat() {
		return rat;
	}

	public void setRat(Integer rat) {
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

}
