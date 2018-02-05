package cn.com.dtmobile.hadoop.biz.exception.model;

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.List;




import java.util.Map;

import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.S1mmeXdrNew;
import cn.com.dtmobile.hadoop.util.DateUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;
import cn.com.dtmobile.hadoop.util.WhetherIsNull;


public class EventMesage {
	private String eventName = "";           // 事件名称
	private Long procedureStarttime;         // xdr 开始时�?
	private String imsi = "";
	private Integer procedureType;
	private Integer etype;
	private Long cellid;
	private Long targetCellid;
	private String failureCause = "";        // 失败原因 CODE
	private String cellType = "";                 // 问题网元类型, 从exceptionMap 中获�?
	private String cellRegion = "";          // 问题归属�? 参考代�? Exception4GUtils.getCellRegion();
	private String cellKey = "";             // 问题网元标识 参�? U.MORPHO异常事件需求分析报�?docx,
	private Integer inteface;                    // xdr 接口
	private String proInterface = "";        // 问题接口
	private String rangetime = "";
	private Double elong;                    // 经度
	private Double elat;                     // 纬度
	private Integer eupordown;                   // 上下行标�?
	private WirelessException wirelessException ;  //无线异常
	private String resultDesc; //信令问题描述
	private String imei ;
	private String wirelessResultDesc; //无线问题描述
	
	static private Map<String, ExceptionReson> exceptionResonMap;
	static private Map<String, String> ltecellMap;
	
	public EventMesage(Map<String, ExceptionReson> expResonMap , Map<String, String> cellMap) {
		super();
		if(wirelessException == null){
			wirelessException = new WirelessException() ;
		}
		exceptionResonMap = expResonMap;
		ltecellMap = cellMap;
	}
	
	public EventMesage() {
		super();
		if(wirelessException == null){
			wirelessException = new WirelessException() ;
		}
		
	}

	public void setCellKeyByCellType_S1MME(S1mmeXdrNew s1mmeXdr) {
		if (this.cellType == null) {
			this.cellKey = "";
		} else if ("UE".equals(this.cellType)) {
			this.cellKey = s1mmeXdr.getS1mmeXdr().getCommXdr().getImsi();
		} else if ("CELL".equals(this.cellType)
				|| "目标CELL".equals(this.cellType)) {
			this.cellKey = s1mmeXdr.getS1mmeXdr().getCellId();
		} else if ("MME".equals(this.cellType)) {
			this.cellKey = s1mmeXdr.getS1mmeXdr().getMmeGroupId() + "_"
					+ s1mmeXdr.getS1mmeXdr().getMmeCode();
		}
	}

	@Override
	public String toString() {
		doTransValue();
		setResultDesc();
		
		StringBuffer sb = new StringBuffer();
		sb.append(eventName);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(procedureStarttime));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(imsi);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(procedureType));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(etype));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(cellid));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(targetCellid));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(failureCause);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(cellType);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(cellRegion);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(cellKey);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(inteface));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(proInterface);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(rangetime);
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(elong));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(elat));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(eupordown));

		
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getExceptionCode())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getXdrId()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(wirelessException.getExcertionType()) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(wirelessException.getInterfaceType()) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(wirelessException.getExceptionStartTime()) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(wirelessException.getExceptionXdrId()) ;
		
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getPci())) ;
		
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getCellRSRP())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getRip())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getPhr())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getUpSinr())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getHighestCellId())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getHighestCellRSRP())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getSecondCellid())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getSecondCellRSRP())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getThirdCellId())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getThirdCellRSRP())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getActualRange())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getEquivalentDistance())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getModelThreeNCellId())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getModelThreeNPci())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getModelThreeNRSRP())) ;	
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getModelThreeActualRange())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getModelThreeEquivDistance())) ;	
		
		
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetPci())) ;
		
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetCellRip())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetCellRSRP()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetHighestCellId()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetHighestCellRSRP()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetSecondCellId()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetSecondCellRSRP()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetThirdCellId()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetThirdCellRSRP()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetActualRange()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetEquivalentDistance()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetModelThreeNCellId()));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetModelThreeNPci())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetModelThreeNRSRP())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetModelThreeActualRange())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getTargetModelThreeEquivDistance())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessException.getMRType())) ;
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(resultDesc));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(WhetherIsNull.IsNUll(wirelessResultDesc));
		sb.append(StringUtils.DELIMITER_COMMA);
		sb.append(imei);
		return sb.toString();
	}


	
	
	public void setResultDesc() {
		resultDesc = setResultDescByKey(this.getProInterface()+StringUtils.DELIMITER_COMMA + this.getFailureCause());  //信令原因
		wirelessResultDesc = setResultDescByKey("UU" + StringUtils.DELIMITER_COMMA + this.getWirelessException().getExceptionCode());  //无线原因
	}

	public String setResultDescByKey(String KeyValue) {

		
		ExceptionReson ER1 = exceptionResonMap.get(KeyValue);
		if (ER1==null){
			return null;
		}

		String resultDesc1 = ER1.getPresentDetaile();
		if(resultDesc1 == null){
			return null;
		}
		
		String key1 = ER1.getKey();
		if (key1 == null) {
			return resultDesc1;
		} else {
			String[] keys = key1.split(";");
			Object[] messagekey = new Object[keys.length];
			if(keys.length == 0){
				return null ;
			}
			for (int i = 0; i < keys.length; i++) {
				Method method;
				try {
					method = this.getClass().getMethod(
							"get" + keys[i].substring(0, 1).toUpperCase()+ keys[i].substring(1, keys[i].length()));
				
					messagekey[i] = method.invoke(this);
					
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			
			
			return MessageFormat.format(resultDesc1, messagekey);
		}
	}
	
	
	public void setLonglat(List<LteMroSourceNew> ueMroSourceList, EventMesage em) {

		Long curTime = 0L;
		Long minTime = Long.MAX_VALUE;
		double longtitude=0.0;
		double late=0.0;
		for (LteMroSourceNew mrs : ueMroSourceList) {
			if (mrs.getLtemrosource().getVid() == 1
					&& mrs.getLtemrosource().getMrtime() >= em.getProcedureStarttime() - 20000
					&& mrs.getLtemrosource().getMrtime() <= em.getProcedureStarttime() + 1000
					&& StringUtils.isNotBlank(mrs.getLtemrosource().getGridcenterlatitude().toString())
					&& StringUtils.isNotBlank(mrs.getLtemrosource().getGridcenterlongitude().toString())
					&& mrs.getLtemrosource().getGridcenterlongitude() > -1.0
					&& mrs.getLtemrosource().getGridcenterlatitude() > -1.0) {

				curTime = Math.abs(em.getProcedureStarttime()
						- mrs.getLtemrosource().getMrtime());
				if (minTime > curTime) {
					minTime = curTime;
					
					if(mrs.getLtemrosource().getGridcenterlongitude()!=null && mrs.getLtemrosource().getGridcenterlatitude()!=null){
						longtitude = mrs.getLtemrosource().getGridcenterlongitude();
						late = mrs.getLtemrosource().getGridcenterlatitude();
					}else{
						longtitude=0.0 ;
						late=0.0 ;
					}	
					
				}
			}
		}
		
		
		
		if(longtitude ==0.0 && late ==0.0 && em.getCellid() != null){
			String cellInfo = ltecellMap.get(em.getCellid().toString()) ;
			if(cellInfo != null){
				String[] word = cellInfo.split(StringUtils.DELIMITER_COMMA) ;
				if(word != null){
					longtitude = Double.parseDouble(word[2]) ;
					late = Double.parseDouble(word[3]) ;
				}
				
			}
			
		}
		
		
			em.setElong(longtitude);
			em.setElat(late);
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	public Long getProcedureStarttime() {
		return procedureStarttime;
	}

	public void setProcedureStarttime(Long procedureStarttime) {
		this.procedureStarttime = procedureStarttime;
		this.rangetime = DateUtils.getDateTime(procedureStarttime);
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public Integer getProcedureType() {
		return procedureType;
	}

	public void setProcedureType(Integer procedureType) {
		this.procedureType = procedureType;
	}

	public Integer getEtype() {
		return etype;
	}

	public void setEtype(Integer etype) {
		this.etype = etype;
	}

	public Long getCellid() {
		return cellid;
	}

	public void setCellid(Long cellid) {
		this.cellid = cellid;
	}

	public Long getTargetCellid() {
		return targetCellid;
	}

	public void setTargetCellid(Long targetCellid) {
		this.targetCellid = targetCellid;
	}

	public String getFailureCause() {
		return failureCause;
	}

	public void setFailureCause(String failureCause) {
		this.failureCause = failureCause;
	}

	public String getCellType() {
		return cellType;
	}

	public void setCellType(String cellType) {
		this.cellType = cellType;
	}

	public String getCellRegion() {
		return cellRegion;
	}

	public void setCellRegion(String cellRegion) {
		this.cellRegion = cellRegion;
	}

	public String getCellKey() {
		return cellKey;
	}

	public void setCellKey(String cellKey) {
		this.cellKey = cellKey;
	}

	public Integer getInteface() {
		return inteface;
	}

	public void setInteface(Integer inteface) {
		this.inteface = inteface;
	}

	public String getProInterface() {
		return proInterface;
	}

	public void setProInterface(String proInterface) {
		this.proInterface = proInterface;
	}

	public String getRangetime() {
		return rangetime;
	}

	public void setRangetime(String rangetime) {
		this.rangetime = rangetime;
	}

	public Double getElong() {
		return elong;
	}

	public void setElong(Double elong) {
		this.elong = elong;
	}

	public Double getElat() {
		return elat;
	}

	public void setElat(Double elat) {
		this.elat = elat;
	}

	public Integer getEupordown() {
		return eupordown;
	}

	public void setEupordown(Integer eupordown) {
		this.eupordown = eupordown;
	}

	public String getResultDesc() {
		return resultDesc;
	}

	public void setResultDesc(String resultDesc) {
		this.resultDesc = resultDesc;
	}

	public String getWirelessResultDesc() {
		return wirelessResultDesc;
	}

	public void setWirelessResultDesc(String wirelessResultDesc) {
		this.wirelessResultDesc = wirelessResultDesc;
	}

	public WirelessException getWirelessException() {
		return wirelessException;
	}

	public void setWirelessException(WirelessException wirelessException) {
		this.wirelessException = wirelessException;
	}
	


	
	
	
	private String getCellNameByCellId(Long cellId) {
		String cellName = null;
		
		if(cellId == null){
			return null;
		}
		
		String word = ltecellMap.get(cellId.toString());
		if(word != null){
			String[] tmp = word.split(StringUtils.DELIMITER_COMMA) ;
			cellName = tmp[5];
		}
		return cellName;
	}
	
	//cellName
	public String getCellName() {
	
		return this.getCellNameByCellId(this.getCellid());
	}
		
	//highestCellName
	public String getHighestCellName() {
		
		return this.getCellNameByCellId(this.getWirelessException().getHighestCellId());
	}
	
	//secondCellName
	public String getSecondCellName() {
		
		return getCellNameByCellId(getWirelessException().getSecondCellid());
	}
	
	//thirdCellName
	public String getThirdCellName() {
		
		return getCellNameByCellId(getWirelessException().getThirdCellId());
	}
	
	//TargetCellName
	public String getTargetCellName() {
	
		return getCellNameByCellId(getTargetCellid());
	}
	
	//TargetHighestCellName
	public String getTargetHighestCellName() {
	
		return getCellNameByCellId(getWirelessException().getTargetHighestCellId());
	}
	
	//modelThreeNCellName
	public String getModelThreeNCellName() {
		
		return getCellNameByCellId(getWirelessException().getModelThreeNCellId());
	}
	
	//TargetModelThreeNCellName
	public String getTargetModelThreeNCellName() {
		
		return getCellNameByCellId(getWirelessException().getTargetModelThreeNCellId());
	}
	
	//phr
	public Double getPhrValue() {
		return getWirelessException().getPhr();
	}
	
	
	public Double getCellRSRP(){
		return getWirelessException().getCellRSRP() ;
	}

	public Double getSecondCellRSRP(){
		return getWirelessException().getSecondCellRSRP() ;
	}
	
	public Double getHighestCellRSRP(){
		return getWirelessException().getHighestCellRSRP() ;
	}
	
	public Double getTargetCellRSRP(){
		return getWirelessException().getTargetCellRSRP() ;
	}
	
	public Double getTargetHighestCellRSRP(){
		return getWirelessException().getTargetHighestCellRSRP() ;
	}
	
	public Double getTargetSecondCellRSRP(){
		return getWirelessException().getTargetSecondCellRSRP() ;
	}
	
	public Double getTargetThirdCellRSRP(){
		return getWirelessException().getTargetThirdCellRSRP() ;
	}
	
	public Double getThirdCellRSRP(){
		return getWirelessException().getThirdCellRSRP() ;
	}
	
	public Double getRip(){
		return getWirelessException().getRip() ;
	}
	
	public Double getUpSinr(){
		return getWirelessException().getUpSinr() ;
	}
	
	public Double getActualRange(){
		return getWirelessException().getActualRange() ;
	}
	
	public Double getEquivalentDistance(){
		return getWirelessException().getEquivalentDistance() ;
	}
	
	public Double getTargetCellRip(){
		return getWirelessException().getTargetCellRip() ;
	}
	
	public Long getPci(){
		return getWirelessException().getPci() ;
	}
	
	public Long getModelThreeNPci(){
		return getWirelessException().getModelThreeNPci() ;
	}
	
	public Double getModelThreeNRSRP(){
		return getWirelessException().getModelThreeNRSRP() ;
	}
	
	public Double getModelThreeActualRange(){
		return getWirelessException().getModelThreeActualRange() ;
	}
	
	public Double getModelThreeEquivDistance(){
		return getWirelessException().getModelThreeEquivDistance() ;
	}
	
	public Long getTargetPci(){
		return getWirelessException().getTargetPci() ;
	}
	
	public Long getTargetModelThreeNPci(){
		return getWirelessException().getTargetModelThreeNPci();
	}
	
	public Double getTargetModelThreeNRSRP(){
		return getWirelessException().getTargetModelThreeNRSRP() ;
	}
	
	public Double getTargetModelThreeActualRange(){
		return getWirelessException().getTargetModelThreeActualRange() ;
	}
	
	public Double getTargetModelThreeEquivDistance(){
		return getWirelessException().getTargetModelThreeEquivDistance() ;
	}
	
	
	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	//协议值转为实际值
	private void doTransValue(){
		WirelessException Exp =  getWirelessException();
		
		if(Exp == null){
			return ;
		}
		
		
		//RSRP
		Exp.setCellRSRP(Exp.getCellRSRP()==null ||Exp.getCellRSRP()==-1.0  ?null:Exp.getCellRSRP() - 141);
		
		Exp.setHighestCellRSRP(Exp.getHighestCellRSRP()==null  || Exp.getHighestCellRSRP()==-1.0 ?null:Exp.getHighestCellRSRP() - 141);
		Exp.setSecondCellRSRP(Exp.getSecondCellRSRP()==null || Exp.getSecondCellRSRP() ==-1.0   ?null:Exp.getSecondCellRSRP() - 141);
		Exp.setThirdCellRSRP(Exp.getThirdCellRSRP()==null || Exp.getThirdCellRSRP()==-1.0  ?null:Exp.getThirdCellRSRP() - 141);
		Exp.setModelThreeNRSRP(Exp.getModelThreeNRSRP()==null || Exp.getModelThreeNRSRP() == -1.0    ?null:Exp.getModelThreeNRSRP() - 141);
		Exp.setTargetCellRSRP(Exp.getTargetCellRSRP()==null || Exp.getTargetCellRSRP() == -1.0     ?null:Exp.getTargetCellRSRP() - 141);
		Exp.setTargetHighestCellRSRP(Exp.getTargetHighestCellRSRP()==null || Exp.getTargetHighestCellRSRP()==-1.0    ?null:Exp.getTargetHighestCellRSRP() - 141);
		Exp.setTargetSecondCellRSRP(Exp.getTargetSecondCellRSRP()==null || Exp.getTargetSecondCellRSRP() == -1.0   ?null:Exp.getTargetSecondCellRSRP() - 141);
		Exp.setTargetThirdCellRSRP(Exp.getTargetThirdCellRSRP()==null || Exp.getTargetThirdCellRSRP() ==-1.0    ?null:Exp.getTargetThirdCellRSRP() - 141);
		Exp.setTargetModelThreeNRSRP(Exp.getTargetModelThreeNRSRP()==null || Exp.getTargetModelThreeNRSRP() == -1.0    ?null:Exp.getTargetModelThreeNRSRP() - 141);
	
		//rip
		Exp.setRip(Exp.getRip()==null || Exp.getRip() == -1.0  ?null:Exp.getRip() * 0.1 - 126.1);
		Exp.setTargetCellRip(Exp.getTargetCellRip()==null || Exp.getTargetCellRip()==-1.0  ?null:Exp.getTargetCellRip() * 0.1 - 126.1);
		
		//phr
		Exp.setPhr(Exp.getPhr()==null || Exp.getPhr() == -1.0  ?null:46 - Exp.getPhr());
		
		//Sinr
		Exp.setUpSinr(Exp.getUpSinr()==null || Exp.getUpSinr() ==-1.0 ?null:Exp.getUpSinr() - 11);
	}
	
	
	
	
	
	
	
	
	
	
	

}
