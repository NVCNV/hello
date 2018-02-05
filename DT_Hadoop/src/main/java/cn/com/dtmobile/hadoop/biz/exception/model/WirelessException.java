package cn.com.dtmobile.hadoop.biz.exception.model;
/** 
 * @完成功能:
 * @创建时间:2017年7月14日 下午1:23:22 
 * @param    
 */
public class WirelessException {
	
	private String exceptionCode = "" ;          //无线原因值  
	private String xdrId = "";                   //XDRID
	private String excertionType = "" ;          //关联异常事件类型
	private String interfaceType = "" ;          //关联异常事件接口类型
	private String exceptionStartTime = "" ;     //关联异常事件开始时间
	private String exceptionXdrId = "" ;         //关联异常事件XDRID
	private Long  pci;         //主小区pci
	private Double cellRSRP  ;                  //主小区RSRP
	private Double rip ;                         //主小区rip   
	private Double phr ;						//主小区phr
	private Double upSinr ;                     //上行SINR
	private Long highestCellId ;                 //最强邻区CellID
	private Double highestCellRSRP ;            //最强邻区RSRP
	private Long secondCellid ;                  //次强邻区CellID
	private Double secondCellRSRP ;             //次强邻区RSRP
	private Long thirdCellId ;                   //三强邻区CellID
	private Double thirdCellRSRP ;              //三强邻区RSRP
	private Double actualRange;                    //最强邻区与主小区实际距离（km）
	private Double equivalentDistance ;            //最强邻区与主小区等效距离（km）
	private Long modelThreeNCellId ;                  //主小区模3干扰邻小区CellID
	private Long modelThreeNPci ; 			//主小区模3干扰邻小区PCI
	private Double modelThreeNRSRP ;             //主小区模3干扰邻小区RSRP
	private Double modelThreeActualRange ;       	//模3干扰邻区与主小区实际距离（km
	private Double modelThreeEquivDistance ;     	//模3干扰邻区与主小区等效距离（km）
	
	private Long  targetPci;  
	private Double targetCellRip ;             		//目标小区rip

	private Double targetCellRSRP ; 				//目标小区RSRP
	private Long targetHighestCellId ;                 //目标小区最强邻区CellID
	private Double targetHighestCellRSRP ;            //目标小区最强邻区RSRP
	private Long targetSecondCellId ;                  //目标小区次强邻区CellID
	private Double targetSecondCellRSRP ;             //目标小区次强邻区RSRP
	private Long targetThirdCellId ;                   //目标小区三强邻区CellID
	private Double targetThirdCellRSRP ;              //目标小区三强邻区RSRP
	private Double targetActualRange;                    //目标小区最强邻区与目标小区实际距离（km）
	private Double targetEquivalentDistance ;            //目标小区最强邻区与目标小区等效距离（km）
	private Long targetModelThreeNCellId ;                  //目标小区模3干扰邻小区CellID
	private Long targetModelThreeNPci ; 			//目标小区模3干扰邻小区PCI
	private Double targetModelThreeNRSRP ;             //目标小区模3干扰邻小区RSRP
	private Double targetModelThreeActualRange ;       //目标小区模3干扰邻区与目标小区实际距离（km
	private Double targetModelThreeEquivDistance ;     //目标小区模3干扰邻区与目标小区等效距离（km）
	private String MRType;
	public String getExceptionCode() {
		return exceptionCode;
	}
	public void setExceptionCode(String exceptionCode) {
		this.exceptionCode = exceptionCode;
	}
	public String getXdrId() {
		return xdrId;
	}
	public void setXdrId(String xdrId) {
		this.xdrId = xdrId;
	}
	public String getExcertionType() {
		return excertionType;
	}
	public void setExcertionType(String excertionType) {
		this.excertionType = excertionType;
	}
	public String getInterfaceType() {
		return interfaceType;
	}
	public void setInterfaceType(String interfaceType) {
		this.interfaceType = interfaceType;
	}
	public String getExceptionStartTime() {
		return exceptionStartTime;
	}
	public void setExceptionStartTime(String exceptionStartTime) {
		this.exceptionStartTime = exceptionStartTime;
	}
	public String getExceptionXdrId() {
		return exceptionXdrId;
	}
	public void setExceptionXdrId(String exceptionXdrId) {
		this.exceptionXdrId = exceptionXdrId;
	}

	public Double getCellRSRP() {
		return cellRSRP;
	}
	public void setCellRSRP(Double cellRSRP) {
		this.cellRSRP = cellRSRP;
	}
	public Double getRip() {
		return rip;
	}
	public void setRip(Double rip) {
		this.rip = rip;
	}
	public Double getPhr() {
		return phr;
	}
	public void setPhr(Double phr) {
		this.phr = phr;
	}
	public Double getUpSinr() {
		return upSinr;
	}
	public void setUpSinr(Double upSinr) {
		this.upSinr = upSinr;
	}
	public Long getHighestCellId() {
		return highestCellId;
	}
	public void setHighestCellId(Long highestCellId) {
		this.highestCellId = highestCellId;
	}

	
	public Double getHighestCellRSRP() {
		return highestCellRSRP;
	}
	public void setHighestCellRSRP(Double highestCellRSRP) {
		this.highestCellRSRP = highestCellRSRP;
	}
	public Long getSecondCellid() {
		return secondCellid;
	}
	public void setSecondCellid(Long secondCellid) {
		this.secondCellid = secondCellid;
	}


	public Double getSecondCellRSRP() {
		return secondCellRSRP;
	}
	public void setSecondCellRSRP(Double secondCellRSRP) {
		this.secondCellRSRP = secondCellRSRP;
	}
	public Long getThirdCellId() {
		return thirdCellId;
	}
	public void setThirdCellId(Long thirdCellId) {
		this.thirdCellId = thirdCellId;
	}
	public Double getThirdCellRSRP() {
		return thirdCellRSRP;
	}
	public void setThirdCellRSRP(Double thirdCellRSRP) {
		this.thirdCellRSRP = thirdCellRSRP;
	}
	public Double getActualRange() {
		return actualRange;
	}
	public void setActualRange(Double actualRange) {
		this.actualRange = actualRange;
	}
	public Double getEquivalentDistance() {
		return equivalentDistance;
	}
	public void setEquivalentDistance(Double equivalentDistance) {
		this.equivalentDistance = equivalentDistance;
	}
	public Double getModelThreeActualRange() {
		return modelThreeActualRange;
	}
	public void setModelThreeActualRange(Double modelThreeActualRange) {
		this.modelThreeActualRange = modelThreeActualRange;
	}
	public Double getModelThreeEquivDistance() {
		return modelThreeEquivDistance;
	}
	
	public Double getTargetCellRip() {
		return targetCellRip;
	}
	public void setTargetCellRip(Double targetCellRip) {
		this.targetCellRip = targetCellRip;
	}
	public void setModelThreeEquivDistance(Double modelThreeEquivDistance) {
		this.modelThreeEquivDistance = modelThreeEquivDistance;
	}
	public Long getModelThreeNCellId() {
		return modelThreeNCellId;
	}
	public void setModelThreeNCellId(Long modelThreeNCellId) {
		this.modelThreeNCellId = modelThreeNCellId;
	}
	public Long getModelThreeNPci() {
		return modelThreeNPci;
	}
	public void setModelThreeNPci(Long modelThreeNPci) {
		this.modelThreeNPci = modelThreeNPci;
	}
	public Double getModelThreeNRSRP() {
		return modelThreeNRSRP;
	}
	public void setModelThreeNRSRP(Double modelThreeNRSRP) {
		this.modelThreeNRSRP = modelThreeNRSRP;
	}
	public Double getTargetCellRSRP() {
		return targetCellRSRP;
	}
	public void setTargetCellRSRP(Double targetCellRSRP) {
		this.targetCellRSRP = targetCellRSRP;
	}
	public Long getTargetHighestCellId() {
		return targetHighestCellId;
	}
	public void setTargetHighestCellId(Long targetHighestCellId) {
		this.targetHighestCellId = targetHighestCellId;
	}
	public Double getTargetHighestCellRSRP() {
		return targetHighestCellRSRP;
	}
	public void setTargetHighestCellRSRP(Double targetHighestCellRSRP) {
		this.targetHighestCellRSRP = targetHighestCellRSRP;
	}
	public Long getTargetSecondCellId() {
		return targetSecondCellId;
	}
	public void setTargetSecondCellId(Long targetSecondCellId) {
		this.targetSecondCellId = targetSecondCellId;
	}
	public Double getTargetSecondCellRSRP() {
		return targetSecondCellRSRP;
	}
	public void setTargetSecondCellRSRP(Double targetSecondCellRSRP) {
		this.targetSecondCellRSRP = targetSecondCellRSRP;
	}
	public Long getTargetThirdCellId() {
		return targetThirdCellId;
	}
	public void setTargetThirdCellId(Long targetThirdCellId) {
		this.targetThirdCellId = targetThirdCellId;
	}
	public Double getTargetThirdCellRSRP() {
		return targetThirdCellRSRP;
	}
	public void setTargetThirdCellRSRP(Double targetThirdCellRSRP) {
		this.targetThirdCellRSRP = targetThirdCellRSRP;
	}
	public Double getTargetActualRange() {
		return targetActualRange;
	}
	public void setTargetActualRange(Double targetActualRange) {
		this.targetActualRange = targetActualRange;
	}
	public Double getTargetEquivalentDistance() {
		return targetEquivalentDistance;
	}
	public void setTargetEquivalentDistance(Double targetEquivalentDistance) {
		this.targetEquivalentDistance = targetEquivalentDistance;
	}
	public Long getTargetModelThreeNCellId() {
		return targetModelThreeNCellId;
	}
	public void setTargetModelThreeNCellId(Long targetModelThreeNCellId) {
		this.targetModelThreeNCellId = targetModelThreeNCellId;
	}
	public Long getTargetModelThreeNPci() {
		return targetModelThreeNPci;
	}
	public void setTargetModelThreeNPci(Long targetModelThreeNPci) {
		this.targetModelThreeNPci = targetModelThreeNPci;
	}
	public Double getTargetModelThreeNRSRP() {
		return targetModelThreeNRSRP;
	}
	public void setTargetModelThreeNRSRP(Double targetModelThreeNRSRP) {
		this.targetModelThreeNRSRP = targetModelThreeNRSRP;
	}
	public Double getTargetModelThreeActualRange() {
		return targetModelThreeActualRange;
	}
	public void setTargetModelThreeActualRange(Double targetModelThreeActualRange) {
		this.targetModelThreeActualRange = targetModelThreeActualRange;
	}
	public Double getTargetModelThreeEquivDistance() {
		return targetModelThreeEquivDistance;
	}
	public void setTargetModelThreeEquivDistance(Double targetModelThreeEquivDistance) {
		this.targetModelThreeEquivDistance = targetModelThreeEquivDistance;
	}
	public String getMRType() {
		return MRType;
	}
	public void setMRType(String mRType) {
		MRType = mRType;
	}
	public Long getPci() {
		return pci;
	}
	public void setPci(Long pci) {
		this.pci = pci;
	}
	public Long getTargetPci() {
		return targetPci;
	}
	public void setTargetPci(Long targetPci) {
		this.targetPci = targetPci;
	}
	
}
