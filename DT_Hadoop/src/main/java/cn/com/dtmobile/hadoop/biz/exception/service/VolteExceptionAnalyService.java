package cn.com.dtmobile.hadoop.biz.exception.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.com.dtmobile.hadoop.biz.exception.constants.AttributeConstnts;
import cn.com.dtmobile.hadoop.biz.exception.constants.ExceptionConstnts;
import cn.com.dtmobile.hadoop.biz.exception.constants.InterfaceConstants;
import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.exception.model.ExceptionReson;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.MwNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.S1mmeXdrNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class VolteExceptionAnalyService {

	
	public EventMesage getEventMesage(MwNew mwXdr, S1mmeXdrNew s1mmeXdr,String failcode,
			Map<String, String> lteGMMap, Map<String, ExceptionReson>  exceptionMap,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, List<UuXdrNew> uUXDR,Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		EventMesage em = new EventMesage();
		switch (failcode) {
		case "01":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "02":
			if(s1mmeXdr!=null ){
				if (s1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255")) {
					em.setFailureCause("接口超时");
				} else {
					em.setFailureCause(s1mmeXdr.getS1mmeXdr().getFailureCause());
				}
			}
			em.setProInterface(InterfaceConstants.NAS);
			break;
		case "03":
			if(s1mmeXdr!=null ){
				if (s1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255")) {
					em.setFailureCause("接口超时");
				} else {
					em.setFailureCause(s1mmeXdr.getS1mmeXdr().getFailureCause());
				}
			}
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		case "04":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "05":
			em.setFailureCause("1");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "06":
			em.setFailureCause("2");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "07":
			em.setFailureCause("3");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "08":
			em.setFailureCause("4");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "09":
			em.setFailureCause("2006");
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		case "10":
			em.setFailureCause("2005");
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		default:
			break;
		}
		// em.setCellType(exceptionMap.get(em.getProInterface()
		// +StringUtils.DELIMITER_COMMA+em.getFailureCause()));
//		em.setCellType(exceptionMap.get(em.getProInterface()
//				+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType());
		
		em.setEtype(mwXdr.getEtype());
		em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));
		em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
		if (em.getEtype() == 4) {
			em.setEventName("VOLTE语音未接通(主叫)");
		} else {
			em.setEventName("VOLTE视频未接通(主叫)");
		}
		
		em.setProcedureStarttime(mwXdr.getMwXdr().getProcedurestarttime());
		em.setImsi(mwXdr.getMwXdr().getImsi());
		em.setImei(mwXdr.getMwXdr().getImei());
		em.setProcedureType(ParseUtils.parseInteger(mwXdr.getMwXdr().getProceduretype()));
		em.setInteface(mwXdr.getMwXdr().getInterfaces());
		em.setEupordown(mwXdr.getEupordown());
		em.getWirelessException().setXdrId(mwXdr.getMwXdr().getXdrid());

		
		if(failcode.equals("02") || failcode.equals("03")){
			if (s1mmeXdr != null && "1".equals(s1mmeXdr.getS1mmeXdr().getCellId())) {
				em.setCellid(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()));
			} else if ( s1mmeXdr != null && "2".equals(s1mmeXdr.getS1mmeXdr().getCellId())) {
				em.setCellid(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()));
			}
			
			if ("UE".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(em.getImsi());
			} else if ("CELL".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(s1mmeXdr.getS1mmeXdr().getCellId());
			} else if ("IMS".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(mwXdr.getMwXdr().getSource_ne_ip());
			} else if ("MME".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(lteGMMap.get(failcode));
			}
		
		}else{
			if ("1".equals(mwXdr.getMwXdr().getCall_side())) {
				em.setCellid(ParseUtils.parseLong(mwXdr.getMwXdr().getSource_eci()));
			} else if ("2".equals(mwXdr.getMwXdr().getCall_side())) {
				em.setCellid(ParseUtils.parseLong(mwXdr.getMwXdr().getDest_eci()));
			}
		
			if ("UE".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(em.getImsi());
			} else if ("CELL".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(mwXdr.getMwXdr().getSource_eci());
			} else if ("IMS".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(mwXdr.getMwXdr().getSource_ne_ip());
			} else if ("MME".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(lteGMMap.get(failcode));
			}
		}
			
		// em.setCellid(ParseUtils.parseLong(((MwNew)mwXdr).getMwXdr().getSource_eci()));	
		// em.setRangetime(mwXdr.getMwXdr().getRangeTime());
		// em.setElong(mwXdr.getElong());
		// em.setElat(mwXdr.getElat());
		
		 em.setLonglat(lteMroSourceList, em);
		 
        WirelessAnalyseService wirelessanalyseservice=new WirelessAnalyseService();
         return wirelessanalyseservice.wirelessAnalyse(lteGMMap,cellXdrMap,nCellinfoMap,configPara,uUXDR,lteMroSourceList, em);
	}

	/*
	 * @param mwXdrList:mw\u63a5\u53e3\u96c6\u5408
	 * 
	 * @param s1mmeXdrList:s1mme\u63a5\u53e3\u96c6\u5408
	 * 
	 * @param lteMroSourceList:lteMroSource\u63a5\u53e3\u96c6\u5408
	 * 
	 * @param cellXdrMap:cellXdrMap \u5b58\u50a8\u683c\u5f0f key:cellid
	 * value:time+","+kpiavg
	 * 
	 * @param exceptionMap:\u9519\u8bef\u539f\u56e0\u4ee3\u7801
	 * 
	 * @return
	 * List<MwExceptionMessage>:\u8fd4\u56de\u5bfc\u5e38\u5206\u6790\u7ed3\u679c
	 */
	// MwExceptionMessage mwExceptionMessage = null;
	EventMesage eventMesage = null;
	Map<String, String> lteGMMap = new HashMap<String, String>();
	List<S1mmeXdrNew> nigeList = new ArrayList<S1mmeXdrNew>();

	public List<EventMesage> volteNoConnService(List<MwNew> mwXdrList,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteGMMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		List<EventMesage> eventMesageList = new ArrayList<EventMesage>();
		// List<MwNew> iscMgMwXdrList = new ArrayList<MwNew>();
		if (mwXdrList.size() > 0) {
			for (MwNew mwXdr : mwXdrList) {
				// MW\u53e3\uff0c ETYPE= 4\u30016\u4e14call_side=0
				if (mwXdr.getMwXdr().getInterfaces() == 14
						&& (ExceptionConstnts.VOLET_VOICE_CALL_FAIL == mwXdr.getEtype() 
							|| ExceptionConstnts.VOLET_VIEDO_CALL_FAIL == mwXdr.getEtype())
						&& AttributeConstnts.MW_CALLSIDE_SOURCE.equals(mwXdr.getMwXdr().getCall_side())) {
					// \u6b65\u9aa41
					eventMesage = volteNoConnAnalyOne(mwXdr, mwXdrList,s1mmeXdrList, ueMroSourceList, cellXdrMap,exceptionMap, lteGMMap,uUXDR,nCellinfoMap,configPara);
					if (eventMesage != null) {
						eventMesageList.add(eventMesage);
						continue;
					}
				}
			}
		}
		return eventMesageList;
	}

	public EventMesage volteNoConnAnalyOne(MwNew mwXdr, List<MwNew> mwXdrList,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteGMMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		if (!("408".equals(mwXdr.getMwXdr().getResponse_code())
				|| "480".equals(mwXdr.getMwXdr().getResponse_code())
				|| "487".equals(mwXdr.getMwXdr().getResponse_code())
				|| "500".equals(mwXdr.getMwXdr().getResponse_code())
				|| "503".equals(mwXdr.getMwXdr().getResponse_code())
				|| "504".equals(mwXdr.getMwXdr().getResponse_code()) 
				|| "580".equals(mwXdr.getMwXdr().getResponse_code()))) {
			return this.getEventMesage(mwXdr,null, "01", lteGMMap, exceptionMap,
					ueMroSourceList,cellXdrMap,uUXDR,nCellinfoMap,configPara);
			// new MwExceptionMessage(mwXdr,
			// mwXdr.getMwXdr().getResponse_code(), InterfaceConstants.SIP,
			// exceptionMap, lteGMMap);
		} else {
			return this.volteNoConnAnalyTwo(mwXdr, mwXdrList, s1mmeXdrList,
					ueMroSourceList, cellXdrMap, exceptionMap, lteGMMap,uUXDR
					,nCellinfoMap,configPara);
		}
	}

	public EventMesage volteNoConnAnalyTwo(MwNew mwXdr, List<MwNew> mwXdrList,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteGMMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		for (MwNew mwXdrTmp : mwXdrList) {
			if ((mwXdrTmp.getMwXdr().getInterfaces() == 14
					|| mwXdrTmp.getMwXdr().getInterfaces() == 15 
					|| mwXdrTmp.getMwXdr().getInterfaces() == 18
				)
				&& mwXdrTmp.getMwXdr().getProcedurestarttime() >= mwXdr.getMwXdr().getProcedurestarttime()
				&& mwXdrTmp.getMwXdr().getProcedurestarttime() <= mwXdr.getMwXdr().getProcedureendtime()
				&& mwXdrTmp.getMwXdr().getImsi().equals(mwXdr.getMwXdr().getImsi())
				&& !"0".equals(mwXdrTmp.getMwXdr().getProcedurestatus())) {
				
					return this.getEventMesage(mwXdr,null, "04", lteGMMap, exceptionMap,
							ueMroSourceList,cellXdrMap,uUXDR, nCellinfoMap, configPara);
					// 根据存储过程修改�?4 new MwExceptionMessage(mwXdr,
					// mwXdrTmp.getMwXdr().getResponse_code(),
					// InterfaceConstants.SIP,
					// exceptionMap, lteGMMap);
			}
		}
		return this.volteNoConnAnalyThree(mwXdr, s1mmeXdrList, ueMroSourceList,
				cellXdrMap, exceptionMap, lteGMMap,uUXDR
				,nCellinfoMap,configPara);

	}

	public EventMesage volteNoConnAnalyThree(MwNew mwXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteGMMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {

		boolean isExist13 = false;
		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr.getMwXdr().getProcedurestarttime()
					&& s1mmeXdr.getS1mmeXdr().getProcedureEndTime() <= mwXdr.getMwXdr().getProcedureendtime()
					&& "13".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())
					&& "5".equals(s1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces())) {
				isExist13 = true;
				
				//step four
				if (!"0".equals(s1mmeXdr.getS1mmeXdr().getProcedureStatus())) {
					lteGMMap.put("02", s1mmeXdr.getS1mmeXdr().getMmeGroupId()
							+ StringUtils.DELIMITER_INNER_ITEM1
							+ s1mmeXdr.getS1mmeXdr().getMmeCode());
					return this.getEventMesage(mwXdr,s1mmeXdr, "02", lteGMMap,
							exceptionMap, ueMroSourceList,cellXdrMap,uUXDR, nCellinfoMap, configPara);
				}
				
			}
			
			/*if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr.getMwXdr().getProcedurestarttime()
					&& s1mmeXdr.getS1mmeXdr().getProcedureEndTime() <= mwXdr.getMwXdr().getProcedureendtime()) {
				nigeList.add(s1mmeXdr);
			} else {
				nigeList.add(s1mmeXdr);
			}*/
		}
		return this.volteNoConnAnalyFive(mwXdr, s1mmeXdrList, isExist13,
				ueMroSourceList, cellXdrMap, exceptionMap, lteGMMap,uUXDR,nCellinfoMap,configPara);
	}

	public EventMesage volteNoConnAnalyFive(MwNew mwXdr,
			List<S1mmeXdrNew> s1mmeXdrList, boolean isExist13,
			List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteGMMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr.getMwXdr().getProcedurestarttime()
					&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr.getMwXdr().getProcedureendtime()
					&& "21".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())
					&& "5".equals(s1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces())) {
				return this.getEventMesage(mwXdr,s1mmeXdr, "03", lteGMMap,
						exceptionMap, ueMroSourceList,cellXdrMap,uUXDR, nCellinfoMap, configPara);
			}
		}
		
		return this.volteNoConnAnalySix(mwXdr, s1mmeXdrList, isExist13, ueMroSourceList, cellXdrMap, exceptionMap, lteGMMap, uUXDR, nCellinfoMap, configPara);
	}
	
	public EventMesage volteNoConnAnalySix(MwNew mwXdr,
			List<S1mmeXdrNew> s1mmeXdrList, boolean isExist13,
			List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteGMMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		
		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr.getMwXdr().getProcedurestarttime()
					&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr.getMwXdr().getProcedureendtime()
					&& "13".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())
					&& "5".equals(s1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces())) {
				// 返回 CO10
				return this.getEventMesage(mwXdr,s1mmeXdr, "10", lteGMMap,
						exceptionMap, ueMroSourceList,cellXdrMap,uUXDR, nCellinfoMap, configPara);
			}
		}
		//返回CO9
		return this.getEventMesage(mwXdr, null,"09", lteGMMap,
				exceptionMap, ueMroSourceList,cellXdrMap,uUXDR, nCellinfoMap, configPara);
		
//		return eventMesage;
		
	}
	/*
	 * @param mwXdr
	 * 
	 * @param ueMroSourceList:uemr\u96c6\u5408
	 * 
	 * @param cellXdrMap:cellXdrMap \u5b58\u50a8\u683c\u5f0f key:cellid
	 * value:time+","+kpiavg
	 */
		/**
		 * 
	public EventMesage volteNoConnAnalySeven(MwNew mwXdr,
			String eRABReleaseCause, List<LteMroSourceNew> ueMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, String> exceptionMap,
			// Map<String, String> lteGMMap,
			S1mmeXdrNew s1mmeXdr20) {
		List<LteMroSourceNew> ueMrKpi1List = new ArrayList<LteMroSourceNew>();
		List<LteMroSourceNew> ueMrKpi6List = new ArrayList<LteMroSourceNew>();

		for (LteMroSourceNew ueMroSource : ueMroSourceList) {
			String cellid = null;
			if (AttributeConstnts.MW_CALLSIDE_DEST.equals(mwXdr.getMwXdr()
					.getCall_side())) {
				cellid = mwXdr.getMwXdr().getDest_eci();
			} else {
				cellid = mwXdr.getMwXdr().getSource_eci();
			}

			if (ueMroSource.getLtemrosource().getMrtime() >= mwXdr.getMwXdr()
					.getProcedurestarttime() - 20000
					&& ueMroSource.getLtemrosource().getMrtime() <= mwXdr
							.getMwXdr().getProcedurestarttime() + 1000
					&& ueMroSource.getLtemrosource().getCellid()
							.equals(ParseUtils.parseLong(cellid))
					&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(ueMroSource
							.getLtemrosource().getMrname())
					&& 1 == ueMroSource.getLtemrosource().getVid()) {
				// kpi1\u4e0d\u4e3a\u7a7a
				if (ueMroSource.getLtemrosource().getKpi1() != -1.0) {
					ueMrKpi1List.add(ueMroSource);
					// kpi6\u4e0d\u4e3a\u7a7a
				}
				if (ueMroSource.getLtemrosource().getKpi6() != -1.0) {
					ueMrKpi6List.add(ueMroSource);
				}
			}
		}
		// KPI1\u4e0d\u4e3a\u7a7a\u7684UE_MR,\u6700\u5c0fKPI1\uff08\u4e3b\u5c0f\u533aRSRP\uff09<31\uff0c\u8fd4\u56deO5
		if (ueMrKpi1List.size() > 0) {
			if (getMinKpi(mwXdr, ueMrKpi1List).getLtemrosource().getKpi1() < 31) { // 31存储过程没有
				return this.getEventMesage(mwXdr, "05", lteGMMap, exceptionMap,
						ueMroSourceList);
				// new MwExceptionMessage(mwXdr, "1", InterfaceConstants.UU,
				// exceptionMap, lteGMMap);
			}
		}
		Long rip = getAvgKpi(mwXdr, cellXdrMap);
		if (rip > 111) {
			return this.getEventMesage(mwXdr, "06", lteGMMap, exceptionMap,
					ueMroSourceList);
			// new MwExceptionMessage(mwXdr, "2", InterfaceConstants.UU,
			// exceptionMap, lteGMMap);
		}
		// UE_MR,kpi6kpi6\u4e0d\u4e3a\u7a7a
		if (ueMrKpi6List.size() > 0) {
			// \u53d6\u6700\u8fd1UE_MR
			LteMroSourceNew ueMroSource = getMinKpi(mwXdr, ueMrKpi6List);
			if (ueMroSource.getLtemrosource().getKpi6() < 25) {
				// kpi8\uff08UL SINR\uff09>=22,\u8fd4\u56deO7
				if (ueMroSource.getLtemrosource().getKpi8() >= 22) {
					return this.getEventMesage(mwXdr, "07", lteGMMap,
							exceptionMap, ueMroSourceList);
					// new MwExceptionMessage(mwXdr, "3", InterfaceConstants.UU,
					// exceptionMap, lteGMMap);
				} else {
					// kpi8\uff08UL SINR\uff09<22,\u8fd4\u56deO8
					return this.getEventMesage(mwXdr, "08", lteGMMap,
							exceptionMap, ueMroSourceList);
					// new MwExceptionMessage(mwXdr, "4", InterfaceConstants.UU,
					// exceptionMap, lteGMMap);
				}
			}
		}
		lteGMMap.put("10", s1mmeXdr2.getS1mmeXdr().getMmeGroupId()
				+ StringUtils.DELIMITER_INNER_ITEM1
				+ s1mmeXdr2.getS1mmeXdr().getMmeCode());
		return this.getEventMesage(s1mmeXdr2, "03", lteGMMap, exceptionMap,
				ueMroSourceList);

		// new MwExceptionMessage(mwXdr, eRABReleaseCause,
		// InterfaceConstants.S1AP, exceptionMap, lteGMMap);
	}

	@Deprecated
	public EventMesage volteNoConnAnalyLteMroSou(MwNew mwXdr,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, String> exceptionMap, Map<String, String> lteGMMap) {
		List<LteMroSourceNew> ueMrKpi1List = new ArrayList<LteMroSourceNew>();
		List<LteMroSourceNew> ueMrKpi6List = new ArrayList<LteMroSourceNew>();
		List<LteMroSourceNew> cellMrList = new ArrayList<LteMroSourceNew>();
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			// starttime-20s\uff0cstarttime+1s ,lteMroSource.cellid=mw.cellid
			if (lteMroSource.getLtemrosource().getMrtime() >= mwXdr.getMwXdr()
					.getProcedurestarttime() - 20000
					&& lteMroSource.getLtemrosource().getMrtime() <= mwXdr
							.getMwXdr().getProcedurestarttime() + 1000
					&& lteMroSource
							.getLtemrosource()
							.getCellid()
							.equals(ParseUtils.parseLong(mwXdr.getMwXdr()
									.getSource_eci()))) {
				// ue_mr
				if (TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource
						.getLtemrosource().getMrname())
						&& lteMroSource.getLtemrosource().getVid() == 1) {
					// kpi1\u4e0d\u4e3a\u7a7a
					if (lteMroSource.getLtemrosource().getKpi1() != 0) {
						ueMrKpi1List.add(lteMroSource);
						// kpi6\u4e0d\u4e3a\u7a7a
					} else if (lteMroSource.getLtemrosource().getKpi6() != 0) {
						ueMrKpi6List.add(lteMroSource);
					}
					// cell_mr
				} else if (TablesConstants.TABLE_CELL_MR_XDR_FLAG
						.equals(lteMroSource.getLtemrosource().getMrname())) {
					cellMrList.add(lteMroSource);
				}
			}
		}
		// KPI1\u4e0d\u4e3a\u7a7a\u7684UE_MR,\u6700\u5c0fKPI1\uff08\u4e3b\u5c0f\u533aRSRP\uff09<31\uff0c\u8fd4\u56deO5
		if (ueMrKpi1List.size() > 0) {
			if (getMinKpi(mwXdr, ueMrKpi1List).getLtemrosource().getKpi1() < 31) {
				return this.getEventMesage(mwXdr, "05", lteGMMap, exceptionMap,
						lteMroSourceList);
				// new MwExceptionMessage(mwXdr, "1", InterfaceConstants.UU,
				// exceptionMap, lteGMMap);
			}
		}
		// \u6700\u8fd1CELL_MR,avg(KPI1~KPI10)>111,\u8fd4\u56deO6
		if (cellMrList.size() > 0) {
			// \u53d6\u6700\u8fd1CELL_MR
			LteMroSourceNew cellMrSource = getMinKpi(mwXdr, cellMrList);
			Double sum = cellMrSource.getLtemrosource().getKpi1()
					+ cellMrSource.getLtemrosource().getKpi2()
					+ cellMrSource.getLtemrosource().getKpi3()
					+ cellMrSource.getLtemrosource().getKpi4()
					+ cellMrSource.getLtemrosource().getKpi5()
					+ cellMrSource.getLtemrosource().getKpi6()
					+ cellMrSource.getLtemrosource().getKpi7()
					+ cellMrSource.getLtemrosource().getKpi8()
					+ cellMrSource.getLtemrosource().getKpi9()
					+ cellMrSource.getLtemrosource().getKpi10();
			Double avg = (double) (sum / 10);
			if (avg > 111) {
				return this.getEventMesage(mwXdr, "06", lteGMMap, exceptionMap,
						lteMroSourceList);
				// new MwExceptionMessage(mwXdr, "2", InterfaceConstants.UU,
				// exceptionMap, lteGMMap);
			}
		}
		// UE_MR,kpi6kpi6\u4e0d\u4e3a\u7a7a
		if (ueMrKpi6List.size() > 0) {
			// \u53d6\u6700\u8fd1UE_MR
			LteMroSourceNew ueMroSource = getMinKpi(mwXdr, ueMrKpi6List);

			if (ueMroSource.getLtemrosource().getKpi6() < 25) {
				// kpi8\uff08UL SINR\uff09>=22,\u8fd4\u56deO7
				if (ueMroSource.getLtemrosource().getKpi8() >= 22) {
					return this.getEventMesage(mwXdr, "07", lteGMMap,
							exceptionMap, lteMroSourceList);
					// new MwExceptionMessage(mwXdr, "3", InterfaceConstants.UU,
					// exceptionMap, lteGMMap);
				} else {
					// kpi8\uff08UL SINR\uff09<22,\u8fd4\u56deO8
					return this.getEventMesage(mwXdr, "08", lteGMMap,
							exceptionMap, lteMroSourceList);
					// new MwExceptionMessage(mwXdr, "4", InterfaceConstants.UU,
					// exceptionMap, lteGMMap);
				}
			} else {
				// kpi6>=25,\u8fd4\u56deO3
				return this.getEventMesage(mwXdr, "03", lteGMMap, exceptionMap,
						lteMroSourceList);
				// new MwExceptionMessage(mwXdr,
				// mwXdr.getMwXdr().getResponse_code(), InterfaceConstants.NAS,
				// exceptionMap,
				// lteGMMap);
			}
		}
		return eventMesage;
	}


	 */
	public LteMroSourceNew getMinKpi(MwNew mwXdr,
			List<LteMroSourceNew> lteMroSourceList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			curTime = Math.abs(mwXdr.getMwXdr().getProcedurestarttime()
					- lteMroSource.getLtemrosource().getMrtime());
			if (minTime > curTime) {
				minTime = curTime;
				LteMroSourceMap.put(minTime, lteMroSource);
			}
		}
		return LteMroSourceMap.get(minTime);
	}

	// cellXdrMap key:cellid value:time+","+kpiavg
	@SuppressWarnings("rawtypes")
	public Long getAvgKpi(MwNew mwXdr, Map<String, List<String>> cellXdrMap) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Iterator iter = cellXdrMap.entrySet().iterator();
		Long kpiAvg = (long) 0.0;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			if (entry.getKey().equals(mwXdr.getMwXdr().getSource_eci())) {
				String[] valueCell = entry.getValue().toString()
						.split(StringUtils.DELIMITER_COMMA);

				if ((mwXdr.getMwXdr().getProcedurestarttime() - Long
						.valueOf(valueCell[1])) < -20000
						&& (mwXdr.getMwXdr().getProcedurestarttime()
								- Long.valueOf(valueCell[1]) > 1000)) {
					continue;
				}
				curTime = Math.abs(mwXdr.getMwXdr().getProcedurestarttime()
						- Long.valueOf(valueCell[1]));
				if (minTime > curTime) {
					minTime = curTime;
					kpiAvg = Long.valueOf(valueCell[0]);
				}
			}
		}
		return kpiAvg;
	}
}
