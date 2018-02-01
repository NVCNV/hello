package cn.com.dtmobile.hadoop.biz.hangzhoufilter.model;

import cn.com.dtmobile.hadoop.util.ParseUtils;

public class ResSite {
	private Integer oid;
	private Integer enodebid;
	private Integer manufacturer ;
	private String city ;
	private String sitename;
	private String longitude ;
	private String latitude;
	private String sitetype;
	private String area ;
	private String sectornum ;
	private Integer sectortype;
	private Integer sectorlocationtype ;
	private Integer maxtxpower;
	private Double frequencybandtype;
	private Integer frequency ;
	private Double groupid ;
	
	public ResSite(String [] values) {
		this.oid = ParseUtils.parseInteger(values[0]);
		this.enodebid = ParseUtils.parseInteger(values[1]);
		this.manufacturer  = ParseUtils.parseInteger(values[2]);
		this.city  = values[3];
		this.sitename = values[4];
		this.longitude  = values[5];
		this.latitude = values[6];
		this.sitetype = values[7];
		this.area  = values[8];
		this.sectornum  = values[9];
		this.sectortype = ParseUtils.parseInteger(values[10]);
		this.sectorlocationtype  = ParseUtils.parseInteger(values[11]);
		this.maxtxpower = ParseUtils.parseInteger(values[12]);
		this.frequencybandtype = ParseUtils.parseDouble(values[13]);
		this.frequency  = ParseUtils.parseInteger(values[14]);
		this.groupid  = ParseUtils.parseDouble(values[15]);
	}

	public Integer getOid() {
		return oid;
	}

	public void setOid(Integer oid) {
		this.oid = oid;
	}

	public Integer getEnodebid() {
		return enodebid;
	}

	public void setEnodebid(Integer enodebid) {
		this.enodebid = enodebid;
	}

	public Integer getManufacturer() {
		return manufacturer;
	}

	public void setManufacturer(Integer manufacturer) {
		this.manufacturer = manufacturer;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getSitename() {
		return sitename;
	}

	public void setSitename(String sitename) {
		this.sitename = sitename;
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

	public String getSitetype() {
		return sitetype;
	}

	public void setSitetype(String sitetype) {
		this.sitetype = sitetype;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getSectornum() {
		return sectornum;
	}

	public void setSectornum(String sectornum) {
		this.sectornum = sectornum;
	}

	public Integer getSectortype() {
		return sectortype;
	}

	public void setSectortype(Integer sectortype) {
		this.sectortype = sectortype;
	}

	public Integer getSectorlocationtype() {
		return sectorlocationtype;
	}

	public void setSectorlocationtype(Integer sectorlocationtype) {
		this.sectorlocationtype = sectorlocationtype;
	}

	public Integer getMaxtxpower() {
		return maxtxpower;
	}

	public void setMaxtxpower(Integer maxtxpower) {
		this.maxtxpower = maxtxpower;
	}

	public Double getFrequencybandtype() {
		return frequencybandtype;
	}

	public void setFrequencybandtype(Double frequencybandtype) {
		this.frequencybandtype = frequencybandtype;
	}

	public Integer getFrequency() {
		return frequency;
	}

	public void setFrequency(Integer frequency) {
		this.frequency = frequency;
	}

	public Double getGroupid() {
		return groupid;
	}

	public void setGroupid(Double groupid) {
		this.groupid = groupid;
	}
	
	
}
