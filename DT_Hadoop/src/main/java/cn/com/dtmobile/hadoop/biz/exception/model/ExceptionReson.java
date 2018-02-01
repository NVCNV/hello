package cn.com.dtmobile.hadoop.biz.exception.model;

public class ExceptionReson {
	private String proInterface = "";        // 问题接口
	private String statusCode;        // 失败原因 CODE
//	private String means; 						//meaning
	private String cellType;                 // 问题网元类型, 从exceptionMap 中获�?
//	private String cellRegion = "";  //归类
//	private String description;    //描述
//	private String optAdvise;   //优化建议
	private String presentDetaile;  //呈现详情
	private String key;     //key
	
	
	
	
	
	public ExceptionReson(String[] word) {
		super();
		this.proInterface = word[0];
		this.statusCode = word[1];
		this.cellType = word[3];
		this.presentDetaile = word[7];
		this.key = word[8];
	}
	public String getProInterface() {
		return proInterface;
	}
	public void setProInterface(String proInterface) {
		this.proInterface = proInterface;
	}
	
	public String getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}
	public String getCellType() {
		return cellType;
	}
	public void setCellType(String cellType) {
		this.cellType = cellType;
	}
	public String getPresentDetaile() {
		return presentDetaile;
	}
	public void setPresentDetaile(String presentDetaile) {
		this.presentDetaile = presentDetaile;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
}
