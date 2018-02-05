package cn.com.dtmobile.hadoop.biz.exception.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.com.dtmobile.hadoop.biz.exception.constants.InterfaceConstants;
import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.exception.model.ExceptionReson;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.MwNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class IMSRegistFailAnalyService {
	
	EventMesage eventExceptionMessage=null;
	List<LteMroSourceNew> lteMroSourceListtmp=null;
	public List<EventMesage> imsRegistFailAnalyService(
			List<MwNew> mwXdrList, List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,List<UuXdrNew> uUXDR,Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		List<EventMesage> mwExceptionMessageList = new ArrayList<EventMesage>();
		List<MwNew> iscList = new ArrayList<MwNew>();
		lteMroSourceListtmp=lteMroSourceList;
		if (mwXdrList.size() > 0) {

			for (MwNew mwXdr : mwXdrList) {
				if (mwXdr.getMwXdr().getInterfaces() == 18) {
					iscList.add(mwXdr);
				}
			}
			// enter the interface 14
			for (MwNew mwXdr : mwXdrList) {
				if (mwXdr.getMwXdr().getInterfaces() == 14 
						&& mwXdr.getMwXdr().getProceduretype().equals("1")
						&& !mwXdr.getMwXdr().getProcedurestatus().equals("0")
						&& mwXdr.getEtype()==1) {
					// step 1
					eventExceptionMessage = this.stepOne(mwXdr, iscList,
							lteMroSourceList, cellXdrMap, exceptionMap,
							lteCellMap,uUXDR,nCellinfoMap,configPara);
					if (eventExceptionMessage != null) {
						mwExceptionMessageList.add(eventExceptionMessage);
						continue;
					}
				} 
			}
		}
		return mwExceptionMessageList;
	}

	public EventMesage getEventMesage(MwNew mwXdr,String failcode, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> ltecellMap,Map<String, List<String>> cellXdrMap,List<UuXdrNew> uUXDR,List<LteMroSourceNew> lteMroSourceList,
			Map<String,String> nCellinfoMap,Map<String, String> configPara)
	{
		EventMesage em = new EventMesage();
		switch (failcode) {
		case "01":
			if(mwXdr.getMwXdr().getProcedurestatus().equals("255")){
                em.setFailureCause("接口超时");
             }else{
                em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
             }
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "02":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "03":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "04":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "05":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		default:
			break;
		}
//		em.setCellType(exceptionMap.get(em.getProInterface()+StringUtils.DELIMITER_COMMA+em.getFailureCause()).getCellType());
		em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

		em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
		em.setEventName("IMS注册失败");
		em.setProcedureStarttime(mwXdr.getMwXdr().getProcedurestarttime());
		em.setImsi(mwXdr.getMwXdr().getImsi());
		em.setImei(mwXdr.getMwXdr().getImei());
		
		em.setProcedureType(ParseUtils.parseInteger(mwXdr.getMwXdr().getProceduretype()));
		em.setEtype(mwXdr.getEtype());
		em.setCellid(ParseUtils.parseLong(mwXdr.getMwXdr().getSource_eci()));
		em.setInteface(mwXdr.getMwXdr().getInterfaces());
//		em.setRangetime(mwXdr.getMwXdr().getRangeTime());
		em.setEupordown(mwXdr.getEupordown());
		//em.setElong(mwXdr.getElong());
		//em.setElat(mwXdr.getElat());
		if("UE".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey(em.getImsi());
		}
		else if("CELL".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey(mwXdr.getMwXdr().getSource_eci());
		}
		else if("IMS".toUpperCase().equals(em.getCellType()))
		{
			em.setCellKey(mwXdr.getMwXdr().getSource_ne_ip());
		}
		em.setLonglat(lteMroSourceListtmp, em);
		em.getWirelessException().setXdrId(mwXdr.getMwXdr().getXdrid());
		WirelessAnalyseService wirelessanalyseservice=new WirelessAnalyseService();
		return wirelessanalyseservice.wirelessAnalyse(ltecellMap, cellXdrMap,nCellinfoMap,configPara,uUXDR,lteMroSourceList, em);

	}
	public EventMesage stepOne(MwNew mwXdr, List<MwNew> iscList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,List<UuXdrNew> uUXDR,Map<String,String> nCellinfoMap,Map<String, String> configPara) 
	
	{
		if ("401".equals(mwXdr.getMwXdr().getResponse_code())) {
			return this.getEventMesage(mwXdr,"01",exceptionMap,
					lteCellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
		}
		// step 2 
		else if(!("408".equals(mwXdr.getMwXdr().getResponse_code())
				|| "480".equals(mwXdr.getMwXdr().getResponse_code())
				|| "500".equals(mwXdr.getMwXdr().getResponse_code())
				|| "503".equals(mwXdr.getMwXdr().getResponse_code())
				|| "487".equals(mwXdr.getMwXdr().getResponse_code())))
		{
				return this.getEventMesage(mwXdr,"02",exceptionMap,lteCellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
		} 
		else {
				return this.stepThree(mwXdr, iscList, lteMroSourceList, cellXdrMap, exceptionMap, lteCellMap,uUXDR,nCellinfoMap,configPara);
			}
	}


	public EventMesage stepThree(MwNew mwXdr, List<MwNew> iscList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,List<UuXdrNew> uUXDR,Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		for (MwNew isc : iscList) {
			if (isc.getMwXdr().getInterfaces() == 18
					&& isc.getMwXdr().getProcedurestarttime() >= mwXdr.getMwXdr()
							.getProcedurestarttime() - 1000
					&& isc.getMwXdr().getProcedurestarttime() <= mwXdr.getMwXdr()
							.getProcedurestarttime() + 5000
					&& isc.getMwXdr().getImsi().equals(mwXdr.getMwXdr().getImsi())
					&& !"0".equals(isc.getMwXdr().getProcedurestatus())
							) 
			{
					return this.getEventMesage(mwXdr,"04",exceptionMap,lteCellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
			}
		}
		return this.getEventMesage(mwXdr,"03",exceptionMap,lteCellMap,cellXdrMap,uUXDR,lteMroSourceList,nCellinfoMap,configPara);
//		return this.stepFour(mwXdr, lteMroSourceList, cellXdrMap, exceptionMap, lteCellMap);
		
//		return this.stepFour(mwXdr, lteMroSourceList, cellXdrMap,exceptionMap, lteCellMap);
	}

//	public EventMesage stepFour(MwNew mwXdr,
//			List<LteMroSourceNew> lteMroSourceList,
//			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
//			Map<String, String> lteCellMap) {
//		List<LteMroSourceNew> ueMr1List = new ArrayList<LteMroSourceNew>();
//		List<LteMroSourceNew> ueMr6List = new ArrayList<LteMroSourceNew>();
//		List<LteMroSourceNew> ueMr8List = new ArrayList<LteMroSourceNew>();
//
//		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
//			if (lteMroSource.getLtemrosource().getMrtime() >= mwXdr.getMwXdr().getProcedurestarttime() - 20000
//					&& lteMroSource.getLtemrosource().getMrtime() <= mwXdr.getMwXdr().getProcedurestarttime() + 1000
//					&& lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(mwXdr.getMwXdr().getSource_eci()))
//					&& lteMroSource.getLtemrosource().getImsi().equals(mwXdr.getMwXdr().getImsi())
//					&& lteMroSource.getLtemrosource().getVid() == 1
//					&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource.getLtemrosource()
//							.getMrname()))//主小区
//				{
//					if (lteMroSource.getLtemrosource().getKpi1() != -1.0 ) {
//						ueMr1List.add(lteMroSource);
//					} 
//					
//				}
//			if(lteMroSource.getLtemrosource().getMrtime() >= mwXdr.getMwXdr().getProcedurestarttime() - 20000
//					&& lteMroSource.getLtemrosource().getMrtime() <= mwXdr.getMwXdr().getProcedurestarttime() + 1000
//					&& lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(mwXdr.getMwXdr().getSource_eci()))
//					&& lteMroSource.getLtemrosource().getImsi().equals(mwXdr.getMwXdr().getImsi())
//					&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource.getLtemrosource()
//							.getMrname()))//主小区+邻小区
//			{
//				if (lteMroSource.getLtemrosource().getKpi6() != -1.0 ) {
//					ueMr6List.add(lteMroSource);
//				} 
//				if (lteMroSource.getLtemrosource().getKpi8() != -1.0 ) {
//					ueMr8List.add(lteMroSource);
//				} 
//				
//			}
//			}
//		
//
//		if (ueMr1List.size()>0 && this.getMinKpi(mwXdr, ueMr1List).getLtemrosource().getKpi1() < 31)
//			{
//				return this.getEventMesage(mwXdr,"02",exceptionMap);
//			} 
//		else {
//				double avg = this.getAvgKpi(mwXdr, cellXdrMap);
//				LteMroSourceNew ue6MrSource = this.getMinKpi(mwXdr, ueMr6List);
//				LteMroSourceNew ue8MrSource = this.getMinKpi(mwXdr, ueMr8List);
//				if (avg > 111) {
//					return this.getEventMesage(mwXdr,"03",exceptionMap);
//				} else {
//					if (ueMr6List.size() > 0 && ue6MrSource.getLtemrosource().getKpi6() < 25) {
//							return this.getEventMesage(mwXdr,"01",exceptionMap);
//						}
//					else if(ue8MrSource !=null && ue8MrSource.getLtemrosource().getKpi8() >= 22){
//						return this.getEventMesage(mwXdr,"04",exceptionMap);
//					}
//					else if(ue8MrSource !=null && ue8MrSource.getLtemrosource().getKpi8() < 22){
//						return this.getEventMesage(mwXdr,"05",exceptionMap);
//					}
//					else{
//						return this.getEventMesage(mwXdr,"01",exceptionMap);
//					}
//					}
//				}
//	}
//
//	public LteMroSourceNew getMinKpi(MwNew mwXdr,
//			List<LteMroSourceNew> lteMroSourceList) {
//		Long minTime = Long.MAX_VALUE;
//		Long curTime = 0L;
//		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
//		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
//			curTime = Math.abs(mwXdr.getMwXdr().getProcedurestarttime()
//					- lteMroSource.getLtemrosource().getMrtime());
//			if (minTime > curTime) {
//				minTime = curTime;
//				LteMroSourceMap.put(minTime, lteMroSource);
//			}
//		}
//		return LteMroSourceMap.get(minTime);
//	}
//
//	@SuppressWarnings("rawtypes")
//	public Double getAvgKpi(MwNew mwXdr, Map<String, List<String>> cellXdrMap) {
//		Long minTime = Long.MAX_VALUE;
//		Long curTime = 0L;
//		Iterator iter = cellXdrMap.entrySet().iterator();
//		Double kpiAvg = 0.0;
//		while (iter.hasNext()) {
//			Map.Entry entry = (Map.Entry) iter.next();
//			if (entry.getKey().equals(mwXdr.getMwXdr().getSource_eci())) {
//				String[] valueCell = entry.getValue().toString().split(StringUtils.DELIMITER_COMMA);
//				
//				if((mwXdr.getMwXdr().getProcedurestarttime()- Long.valueOf(valueCell[1]))<-20000 &&
//					(mwXdr.getMwXdr().getProcedurestarttime()- Long.valueOf(valueCell[1])>1000))
//				{
//					continue;
//				}
//				curTime = Math
//						.abs(mwXdr.getMwXdr().getProcedurestarttime()
//								- Long.valueOf(valueCell[1]));
//				if (minTime > curTime) {
//					minTime = curTime;
//					kpiAvg = Double.valueOf(valueCell[0]);
//				}
//			}
//		}
//		return kpiAvg;
//	}
}
