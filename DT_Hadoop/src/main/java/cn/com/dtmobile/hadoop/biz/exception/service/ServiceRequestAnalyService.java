package cn.com.dtmobile.hadoop.biz.exception.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.com.dtmobile.hadoop.biz.exception.constants.ExceptionConstnts;
import cn.com.dtmobile.hadoop.biz.exception.constants.InterfaceConstants;
import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.exception.model.ExceptionReson;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.S1mmeXdrNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class ServiceRequestAnalyService {

	public EventMesage eventExceptionMessage=null;
	List<LteMroSourceNew> lteMroSourceListtmp=null;
	public LteMroSourceNew miniUeMr;
	public LteMroSourceNew miniUe6Mr;
	public LteMroSourceNew miniUe8Mr;
	public LteMroSourceNew miniCellMr ;
	/**
	 * ServiceRequest\u5931\u8d25\u5206\u6790\u6d41\u7a0b
	 * @param s1mmeXdrList
	 * @param lteMroSourceList
	 * @param cellXdrMap
	 * @param exceptionMap
	 * @return
	 */
	public List<EventMesage> volteServiceRequestService(List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap,
			Map<String, ExceptionReson>  exceptionMap,Map<String, String> lteCellMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		List<EventMesage> s1ExceptionMessageList = new ArrayList<EventMesage>();
		lteMroSourceListtmp=lteMroSourceList;
		if (s1mmeXdrList.size() > 0) {
			
			for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
				if(ExceptionConstnts.SVR_REQ_FAIL == s1mmeXdr.getEtype()){
					//\u6b65\u9aa41
					eventExceptionMessage =  this._ServiceRequestAnalyOne(s1mmeXdr, s1mmeXdrList, lteMroSourceList, cellXdrMap, exceptionMap,lteCellMap,uUXDR,nCellinfoMap,configPara);
					if(eventExceptionMessage != null){
						s1ExceptionMessageList.add(eventExceptionMessage);
						continue;
					}		
				}
			}
		}
		return s1ExceptionMessageList;
	}
	public EventMesage getEventMesage(S1mmeXdrNew S1mmeXdr,String failcode,Map<String, ExceptionReson> exceptionMap,
			Map<String, String> ltecellMap,Map<String, List<String>> cellXdrMap,List<UuXdrNew> uUXDR,List<LteMroSourceNew> lteMroSourceList,
			Map<String,String> nCellinfoMap,Map<String, String> configPara)
	{
		EventMesage em=new EventMesage(); 
		switch (failcode) {
		case "01":
			if(S1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255")){
                em.setFailureCause("接口超时");
             }else{
                em.setFailureCause(S1mmeXdr.getS1mmeXdr().getFailureCause());
             }
			em.setProInterface(InterfaceConstants.NAS);
			break;
		case "02":
			if(S1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255")){
                em.setFailureCause("接口超时");
             }else{
                em.setFailureCause(S1mmeXdr.getS1mmeXdr().getFailureCause());
             }
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		case "03":
			em.setFailureCause("1");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "04":
			em.setFailureCause("2");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "05":
			em.setFailureCause("3");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "06":
			em.setFailureCause("4");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "07":
			em.setFailureCause("65535");
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		case "08":
			em.setFailureCause("2003");
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		default:
			break;
		}
//		em.setCellType(exceptionMap.get(em.getProInterface()+StringUtils.DELIMITER_COMMA+em.getFailureCause()).getCellType());
		em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

		em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
		em.setEventName("Service Request失败");
		em.setProcedureStarttime(S1mmeXdr.getS1mmeXdr().getProcedureStartTime());
		em.setImsi(S1mmeXdr.getS1mmeXdr().getCommXdr().getImsi());
		em.setImei(S1mmeXdr.getS1mmeXdr().getCommXdr().getImei());
		em.setProcedureType(ParseUtils.parseInteger(S1mmeXdr.getS1mmeXdr().getProcedureType()));
		em.setEtype(S1mmeXdr.getEtype());
		em.setCellid(ParseUtils.parseLong(S1mmeXdr.getS1mmeXdr().getCellId()));//cellid long
		em.setInteface(S1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces());
//		em.setRangetime(S1mmeXdr.getS1mmeXdr().getRangeTime());
		em.setEupordown(S1mmeXdr.getEupordown());//无Eupordown
		//em.setElong(S1mmeXdr.getElong());
		//em.setElat(S1mmeXdr.getElat());
		if("UE".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey(em.getImsi());
		}
		else if("CELL".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey(S1mmeXdr.getS1mmeXdr().getCellId());
		}
		else if("MME".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey(S1mmeXdr.getS1mmeXdr().getMmeGroupId()+"_"+S1mmeXdr.getS1mmeXdr().getMmeCode());
		}
		else if("DPI采集".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey("DPI采集");
			em.setCellRegion("DPI采集");
		}
		
		em.setLonglat(lteMroSourceListtmp, em);
		em.getWirelessException().setXdrId(S1mmeXdr.getS1mmeXdr().getCommXdr().getXdrid());
		WirelessAnalyseService wirelessanalyseservice=new WirelessAnalyseService();
		return wirelessanalyseservice.wirelessAnalyse(ltecellMap, cellXdrMap,nCellinfoMap,configPara,uUXDR, lteMroSourceList, em);

	}
	private EventMesage _ServiceRequestAnalyOne(S1mmeXdrNew s1mmeXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap,
			Map<String, ExceptionReson>  exceptionMap,Map<String, String> ltecellMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara){
		
		if("1".equals(s1mmeXdr.getS1mmeXdr().getProcedureStatus())){
			return this.getEventMesage(s1mmeXdr, "01",exceptionMap,ltecellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);//
		}else {
			return this._ServiceRequestAnalyTwo(s1mmeXdr, s1mmeXdrList, lteMroSourceList, cellXdrMap, exceptionMap,ltecellMap,uUXDR,nCellinfoMap,configPara);
		}
	}
	
	private EventMesage _ServiceRequestAnalyTwo(S1mmeXdrNew s1mmeXdr,List<S1mmeXdrNew>s1mmeXdrList,List<LteMroSourceNew> lteMroSourceList,Map<String, List<String>> cellXdrMap,Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> ltecellMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara){
		if (s1mmeXdrList.size() > 0) {			
			for (S1mmeXdrNew s1mme : s1mmeXdrList) {
				if(s1mme.getS1mmeXdr().getProcedureStartTime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()-1000 
						&& s1mme.getS1mmeXdr().getProcedureStartTime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()+5000
						&& "18".equals(s1mme.getS1mmeXdr().getProcedureType()) 
						&& s1mme.getS1mmeXdr().getCellId().equals(s1mmeXdr.getS1mmeXdr().getCellId())){
					if("1".equals(s1mme.getS1mmeXdr().getProcedureStatus())){
						return this.getEventMesage(s1mmeXdr, "02",exceptionMap,ltecellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);//
					}else if("255".equals(s1mme.getS1mmeXdr().getProcedureStatus())){
						this.getEventMesage(s1mmeXdr, "01",exceptionMap,ltecellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
					}else {
						return this.getEventMesage(s1mmeXdr,"08",exceptionMap,ltecellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
					}					
				}
			}
			return this.getEventMesage(s1mmeXdr,"07",exceptionMap,ltecellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
		}
		return null;
		//return new S1mmeExceptionMessage(s1mmeXdr, "65535", InterfaceConstants.S1AP, exceptionMap);
	}

//	private EventMesage _ServiceRequestAnalyFour(S1mmeXdrNew s1mmeXdr,
//			List<LteMroSourceNew> lteMroSourceList,
//			Map<String, List<String>> cellXdrMap,
//			Map<String, ExceptionReson>  exceptionMap){
//		List<LteMroSourceNew>ueMr1List = new ArrayList<LteMroSourceNew>();
//		List<LteMroSourceNew>ueMr6List = new ArrayList<LteMroSourceNew>();
//		List<LteMroSourceNew>ueMr8List = new ArrayList<LteMroSourceNew>();
//		//\u6b65\u9aa44
//		if (lteMroSourceList.size() > 0) {
//			for (LteMroSourceNew lteMroSource : lteMroSourceList) {
//				if(lteMroSource.getLtemrosource().getMrtime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()-20000 
//						&& lteMroSource.getLtemrosource().getMrtime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()+1000
//						&& lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()))
//						&& lteMroSource.getLtemrosource().getImsi().equals(s1mmeXdr.getS1mmeXdr().getCommXdr().getImsi())
//						&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource.getLtemrosource().getMrname())
//						&& 1 == lteMroSource.getLtemrosource().getVid() ){//主小区
//					if(lteMroSource.getLtemrosource().getKpi1() !=-1.0){
//						ueMr1List.add(lteMroSource);
//					}	
//				}
//					if(lteMroSource.getLtemrosource().getMrtime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()-20000 
//							&& lteMroSource.getLtemrosource().getMrtime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()+1000
//						    && lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId())))//主小区+邻区
//					{
//					if(lteMroSource.getLtemrosource().getKpi6() !=-1.0){
//						ueMr6List.add(lteMroSource);
//					}
//					if(lteMroSource.getLtemrosource().getKpi8() !=-1.0){
//						ueMr8List.add(lteMroSource);
//					}	
//					}
//			}
//			miniUeMr = this.getMinKpi(s1mmeXdr, ueMr1List);	
//			miniUe6Mr = this.getMinKpi(s1mmeXdr, ueMr6List);
//			miniUe8Mr = this.getMinKpi(s1mmeXdr, ueMr8List);
//			double rip = this.getAvgKpi(s1mmeXdr, cellXdrMap);
//			
//			if(miniUeMr != null && miniUeMr.getLtemrosource().getKpi1() < 31 ){
//				return this.getEventMesage(s1mmeXdr, "03",exceptionMap);
//			}else if(rip > 111){
//				return this.getEventMesage(s1mmeXdr, "04",exceptionMap);
//			}else if(miniUe6Mr.getLtemrosource().getKpi6() < 25){
//				return this.getEventMesage(s1mmeXdr, "02",exceptionMap);
//			}
//			else if(miniUe8Mr.getLtemrosource().getKpi8() >= 22){
//					return this.getEventMesage(s1mmeXdr, "05",exceptionMap);
//				}
//			else if(miniUe8Mr.getLtemrosource().getKpi8() < 22){
//				    return this.getEventMesage(s1mmeXdr, "06",exceptionMap);
//				}
//			else{
//				return this.getEventMesage(s1mmeXdr, "02",exceptionMap);
//			}
//			}
//		return null;//lteMroSourceList.size()=0
//	}
//			
//	public LteMroSourceNew getMinKpi(S1mmeXdrNew s1mmeXdr, List<LteMroSourceNew> lteMroSourceList) {
//		Long minTime = Long.MAX_VALUE;
//		Long curTime = 0L;
//		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
//		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
//			curTime = Math.abs(s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - lteMroSource.getLtemrosource().getMrtime());
//			if (minTime > curTime) {
//				minTime = curTime;	
//				LteMroSourceMap.put(minTime, lteMroSource);
//			}
//		}
//		return LteMroSourceMap.get(minTime);
//	}
//	
//	@SuppressWarnings("rawtypes")
//	public Double getAvgKpi(S1mmeXdrNew s1mme, Map<String, List<String>> cellXdrMap) {
//		Long minTime = Long.MAX_VALUE;
//		Long curTime = 0L;
//		Iterator iter = cellXdrMap.entrySet().iterator();
//		Double kpiAvg = (double) 0;
//		while (iter.hasNext()) {
//			Map.Entry entry = (Map.Entry) iter.next();
//			if (entry.getKey().equals(s1mme.getS1mmeXdr().getCellId())) {
//				String[] valueCell = entry.getValue().toString().split(StringUtils.DELIMITER_COMMA);
//				
//				if((s1mme.getS1mmeXdr().getProcedureStartTime()- Long.valueOf(valueCell[1]))<-20000 &&
//						(s1mme.getS1mmeXdr().getProcedureStartTime()- Long.valueOf(valueCell[1])>1000))
//					{
//						continue;
//					}
//				curTime = Math
//						.abs(s1mme.getS1mmeXdr().getProcedureStartTime() - Long.valueOf(valueCell[1]));
//				if (minTime > curTime) {
//					minTime = curTime;
//					kpiAvg = Double.valueOf(valueCell[0]);
//				}
//			}
//		}
//		return kpiAvg;
//	}
}
