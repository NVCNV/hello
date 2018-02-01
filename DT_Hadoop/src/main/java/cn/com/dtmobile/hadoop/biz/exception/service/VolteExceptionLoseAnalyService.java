package cn.com.dtmobile.hadoop.biz.exception.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.com.dtmobile.hadoop.biz.exception.constants.ExceptionConstnts;
import cn.com.dtmobile.hadoop.biz.exception.constants.InterfaceConstants;
import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.exception.model.ExceptionReson;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.GxRxNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.S1mmeXdrNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

/**
 * VOLTE\u6389\u8bdd\u5206\u6790\u6d41\u7a0b
 * 
 * @author zhangchao15
 * @version 2016-11-14
 * 
 */
public class VolteExceptionLoseAnalyService {
	EventMesage eventMesage = null;
	LteMroSourceNew miniUeMr;
	LteMroSourceNew miniUe6Mr;
	LteMroSourceNew miniCellMr;
	private Map<String, List<String>> g_cellXdrMap = null;
	private Map<String, String> g_lteCellMap = null;
	private List<UuXdrNew> g_uUXDR = null;
	private Map<String,String> g_nCellinfoMap = null;
	private Map<String, String> g_configPara = null;
	

	public List<EventMesage> volteNoConnService(List<GxRxNew> gxXdrList,
			List<S1mmeXdrNew> s1mmeXdrList, List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,List<UuXdrNew> uUXDR,
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		
		g_cellXdrMap = cellXdrMap;
		g_lteCellMap = lteCellMap;
		g_uUXDR = uUXDR;
		g_nCellinfoMap = nCellinfoMap;
		g_configPara = configPara;
		
		List<EventMesage> eventMesageList = new ArrayList<EventMesage>();
		if (gxXdrList.size() > 0) {

			for (GxRxNew gxXdr : gxXdrList) {
				if (26 == gxXdr.getGxXdr().getCommXdr().getInterfaces() && 
						(ExceptionConstnts.VOLET_VOICE_OFFLINE_FAIL == gxXdr.getEtype()	
						|| ExceptionConstnts.VOLET_VIEDO_OFFLINE_FAIL == gxXdr.getEtype())) {
					// \u6b65\u9aa41
					eventMesage = this._volteNoConnAnalyOne(gxXdr,
							s1mmeXdrList, lteMroSourceList, cellXdrMap,
							exceptionMap);
					if (eventMesage != null) {
						eventMesageList.add(eventMesage);
						continue;
					}

				}
			}
		}
		return eventMesageList;
	}

	private EventMesage _volteNoConnAnalyOne(GxRxNew gxXdr,
			List<S1mmeXdrNew> s1mmeXdrList, List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap) {
		boolean isExist21 = false;
		String s1apfailureCause = "";
//		S1mmeXdrNew s1mme_1 = null;
		
		S1mmeXdrNew s1mmeLeast = this.getMinS1mme(gxXdr, s1mmeXdrList); //3s内最近的的s1mme
//		if ( s1mmeLeast != null ) {
//			gxXdr.getGxXdr().setCellId(s1mmeLeast.getS1mmeXdr().getCellId());
//			gxXdr.getGxXdr().setMmeCode(s1mmeLeast.getS1mmeXdr().getMmeCode());
//			gxXdr.getGxXdr().setMmeGroupId(s1mmeLeast.getS1mmeXdr().getMmeGroupId());
//		}

		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			//步骤1:
			//[procedure starttime-3s，procedure starttime]为查询时间范围，
			//相同IMSI条件下，在 interface=5（S1-MME接口），是否出现procedure type为21的情况
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= (gxXdr.getGxXdr().getProcedureStartTime()-3000)
					&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= gxXdr.getGxXdr().getProcedureStartTime()
				    && "21".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())) {
				//步骤2:
				//interface=5（S1-MME接口），Procedure Type =21的 Bearer 1 request  cause 是否in(3,6,21,26)
				isExist21 = true;
				break; //wyq: 新算法里面不考虑bearReleasecause;
////				s1apfailureCause = s1mmeXdr.getS1mmeXdr().getFailureCause();
////				s1mme_1 = s1mmeXdr;
//
//				String[] sb = s1mmeXdr.getS1mmeXdr().getBearerArr();
//				String bearReleaseCause = null;
//				if(sb.length>0){
//					for (Integer i = 0; i < sb.length; i++) { //是否只取第一个？？？
//						if ( sb[i] == null ) break;
//						bearReleaseCause = sb[i].split(StringUtils.DELIMITER_COMMA)[4];
//						if("3".equals(bearReleaseCause) || "6".equals(bearReleaseCause) 
//								||"21".equals(bearReleaseCause) ||"26".equals(bearReleaseCause)){
//							//步骤3
//							return this._volteNoConnAnalyThree(gxXdr,bearReleaseCause,s1mmeXdr,
//									s1mmeXdrList,lteMroSourceList,cellXdrMap,exceptionMap,s1mmeLeast);							
//						}
//					}				
//				}
////				break; //只分析第一个s1_mme
			}
		}

		//步骤2输出O1		
		if(isExist21 == true ){
			if ( s1mmeLeast != null ){
				if(s1mmeLeast.getS1mmeXdr().getProcedureStatus().equals("255")){
					s1apfailureCause = "接口超时";
                 }else{
                	 s1apfailureCause = s1mmeLeast.getS1mmeXdr().getFailureCause();
                 }
			}
			return NewGxRxEventMesage(gxXdr, s1mmeLeast, s1apfailureCause,
					InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList); //01
		}else {
			//步骤4
			for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
				if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= gxXdr.getGxXdr().getProcedureStartTime() - 3000
						&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= gxXdr.getGxXdr().getProcedureStartTime()
					    && "1".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())) {
					//(prcedureStartTime-3s，prcedureStartTime）时间范围内，同IMSI条件下，是否出现interface =5, procedure type=1。如果是，进入步骤5
						// 步骤5
						return this._volteNoConnAnalyFive(gxXdr, s1mmeXdrList,
								exceptionMap,s1mmeLeast, lteMroSourceList);
				} 
			}
		
			//(prcedureStartTime-3s，prcedureStartTime）时间范围内，同IMSI条件下，是否出现interface =5, procedure type=1。如果不是，输出O7
			return NewGxRxEventMesage(gxXdr, null, "2005",InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList);	 //07
		}		
	}

	private EventMesage _volteNoConnAnalyThree(GxRxNew gxXdr,
			String bearReleaseCause,
			S1mmeXdrNew bearReleases1mmeXdr, 
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, 
			Map<String, ExceptionReson>  exceptionMap, 
			S1mmeXdrNew s1mmeLeast) {
		List<LteMroSourceNew> ueMrList = new ArrayList<LteMroSourceNew>();
		List<LteMroSourceNew> ueMr6List = new ArrayList<LteMroSourceNew>();

//		S1mmeXdrNew s1mmeLeast = this.getMinS1mme(gxXdr, s1mmeXdrList); //这里需要判断时间，如果时间不在范围内，应该不能设置？？？
//		gxXdr.getGxXdr().setCellId(s1mmeLeast.getS1mmeXdr().getCellId());
//		gxXdr.getGxXdr().setMmeCode(s1mmeLeast.getS1mmeXdr().getMmeCode());
//		gxXdr.getGxXdr().setMmeGroupId(s1mmeLeast.getS1mmeXdr().getMmeGroupId());
//
		//[starttime-20s，starttime+1s]为查询时间范围，查找满足以下条件的UE_MR
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			if (lteMroSource.getLtemrosource().getMrtime() >= gxXdr.getGxXdr().getProcedureStartTime() - 20000
					&& lteMroSource.getLtemrosource().getMrtime() <= gxXdr.getGxXdr().getProcedureStartTime() + 1000
					&& lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(gxXdr.getGxXdr().getCellId())) // 取最近的cellid???
					&& 1 == lteMroSource.getLtemrosource().getVid()
					&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource.getLtemrosource().getMrname())) {
				if (lteMroSource.getLtemrosource().getKpi1() != -1.0) {
					ueMrList.add(lteMroSource); //用于判断Kpi1
				} 
				if (lteMroSource.getLtemrosource().getKpi6() != -1.0) {
					ueMr6List.add(lteMroSource); //用于判断Kpi6
				}
			}		
		}
		
		miniUeMr = this.getMinKpi(gxXdr, ueMrList); //用于判断Kpi1
		
		if (miniUeMr != null && miniUeMr.getLtemrosource().getKpi1() < 31) {
			if ("6".equals(bearReleaseCause)) 
			{//若最近一个UE_MR满足KPI1（主小区RSRP）<31（-110dbm），且 Bearer 1 request  cause=6则输出O10
				return NewGxRxEventMesage(gxXdr, s1mmeLeast, "14",
						InterfaceConstants.UU, "VOLTE掉话", exceptionMap, lteMroSourceList); //010
			} else { //否则输出O3
				return NewGxRxEventMesage(gxXdr, s1mmeLeast, "1",
						InterfaceConstants.UU, "VOLTE掉话", exceptionMap, lteMroSourceList); //03
			}
		} else {
			long rip = this.getAvgKpi(gxXdr, cellXdrMap); //时间是按照gx的
			if (rip > 111) { //最近一个CELL_XDR满足avg(KPI1~KPI10)（eNB Received Interfere均值）>111，则输出O4
				return NewGxRxEventMesage(gxXdr, s1mmeLeast, "2",
						InterfaceConstants.UU, "VOLTE掉话", exceptionMap, lteMroSourceList); //04
			}else{
				if (ueMr6List.size() > 0) {
					miniUe6Mr = this.getMinKpi(gxXdr, ueMr6List);
					if (miniUe6Mr.getLtemrosource().getKpi6() < 25) {
						if (miniUe6Mr.getLtemrosource().getKpi8() >= 22) { //kpi6<25, kpi8>=22,则输出O5
							return NewGxRxEventMesage(gxXdr, s1mmeLeast, "3",
									InterfaceConstants.UU, "VOLTE掉话", exceptionMap, lteMroSourceList); //05
						} else {  //kpi6<25, kpi8<22,则输出O6
							return NewGxRxEventMesage(gxXdr, s1mmeLeast, "4",
									InterfaceConstants.UU, "VOLTE掉话", exceptionMap, lteMroSourceList); //06
						}
					}
					//else //所有kpi6（PHR）不为空的UE_MR，若最近一个XDR满足kpi6（PHR）<25，则进一步判断下面的SINR；否则输出O1。
				}
				return NewGxRxEventMesage(gxXdr, bearReleases1mmeXdr, bearReleases1mmeXdr.getS1mmeXdr().getFailureCause(),
						InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList); //这里的cellid等取bearS1，还是取最近的那个？？？
			}
		}
	}

	private EventMesage _volteNoConnAnalyFive(GxRxNew gxXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			Map<String, ExceptionReson>  exceptionMap, 
			S1mmeXdrNew s1mmeLeast, 
			List<LteMroSourceNew> lteMroSourceList) {
		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= gxXdr.getGxXdr().getProcedureStartTime() - 5000
					&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= gxXdr.getGxXdr().getProcedureStartTime())
			{//prcedureStartTime-5s，prcedureStartTime） procedure type=2或5，且Procedure Status=1，Faliurecause=10。如果是，进入步骤6
				if (("2".equals(s1mmeXdr.getS1mmeXdr().getProcedureType()) || "5".equals(s1mmeXdr.getS1mmeXdr()
						.getProcedureType()))
						&& "1".equals(s1mmeXdr.getS1mmeXdr().getProcedureStatus())
						&& "10".equals(s1mmeXdr.getS1mmeXdr().getFailureCause())) {
					// \u6b65\u9aa46
					return this._volteNoConnAnalySix(gxXdr, s1mmeXdrList,
							exceptionMap, s1mmeLeast, lteMroSourceList);
				}
			}
		}
		//否则，输出O8
		return NewGxRxEventMesage(gxXdr, s1mmeLeast, "2008",InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList); //08
	}

	private EventMesage _volteNoConnAnalySix(GxRxNew gxXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			Map<String, ExceptionReson>  exceptionMap, 
			S1mmeXdrNew s1mmeLeast, 
			List<LteMroSourceNew> lteMroSourceList) {
		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime()  >= gxXdr.getGxXdr().getProcedureStartTime() - 60000
					&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= gxXdr.getGxXdr().getProcedureStartTime())
			{//prcedureStartTime-60s，prcedureStartTime,procedure type=5，且Procedure Status=255。如果是，进入步骤7
				if ("5".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())
						&& "255".equals(s1mmeXdr.getS1mmeXdr().getProcedureStatus())) {
					// \u6b65\u9aa47
					return this._volteNoConnAnalySeven(gxXdr, s1mmeXdr, s1mmeXdrList,
							exceptionMap, s1mmeLeast, lteMroSourceList);
				} 					
			}
		}
		//否则，输出O8
		return NewGxRxEventMesage(gxXdr, s1mmeLeast, "2008",InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList);
	}

	private EventMesage _volteNoConnAnalySeven(GxRxNew gxXdr, 
			S1mmeXdrNew s1mmeXdr, List<S1mmeXdrNew> s1mmeXdrList,
			Map<String, ExceptionReson>  exceptionMap, 
			S1mmeXdrNew s1mmeLeast, 
			List<LteMroSourceNew> lteMroSourceList) {
		for (S1mmeXdrNew s1xdr : s1mmeXdrList) {
			if (s1xdr.getS1mmeXdr().getProcedureStartTime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()
					&& s1xdr.getS1mmeXdr().getProcedureStartTime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime() + 300 
					&& "14".equals(s1xdr.getS1mmeXdr().getProcedureType())) 
			{//TAU事件后面300ms内有无X2切换（interface=5，procedureType=14）。如果是，则输出O2
				return NewGxRxEventMesage(gxXdr, s1xdr, "2002",InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList);
			} 
		}
		//否则，输出O9
		return NewGxRxEventMesage(gxXdr, s1mmeLeast, "2007",InterfaceConstants.S1AP, "VOLTE掉话", exceptionMap, lteMroSourceList);
	} 

	public LteMroSourceNew getMinKpi(GxRxNew gxXdr,
			List<LteMroSourceNew> lteMroSourceList) {
		Long minTime = 10000000L;
		Long curTime = 0L;
		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			curTime = Math.abs(gxXdr.getGxXdr().getProcedureStartTime()
					- lteMroSource.getLtemrosource().getMrtime());
			if (minTime > curTime) {
				minTime = curTime;
			}
			LteMroSourceMap.put(minTime, lteMroSource);
		}
		return LteMroSourceMap.get(minTime);
	}

	public S1mmeXdrNew getMinS1mme(GxRxNew gxXdr, List<S1mmeXdrNew> s1mmeXdrList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, S1mmeXdrNew> S1mmeXdrMap = new HashMap<Long, S1mmeXdrNew>();
		for (S1mmeXdrNew s1mme : s1mmeXdrList) {
			if ( s1mme.getS1mmeXdr().getProcedureStartTime() > gxXdr.getGxXdr().getProcedureStartTime() )
			{ //保证在gx之前
				continue;
			}
			curTime = Math.abs(gxXdr.getGxXdr().getProcedureStartTime()
					- s1mme.getS1mmeXdr().getProcedureStartTime());
			if (minTime > curTime && curTime <= 3000) { //保证在3s内
				minTime = curTime;			
				S1mmeXdrMap.put(minTime, s1mme);
			}
		}
		return S1mmeXdrMap.get(minTime);
	}

	@SuppressWarnings("rawtypes")
	public Long getAvgKpi(GxRxNew gxXdr, Map<String, List<String>> cellXdrMap) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Iterator iter = cellXdrMap.entrySet().iterator();
		Long kpiAvg = 0L;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			if (entry.getKey().equals(gxXdr.getGxXdr().getCellId())) {
				String[] valueCell = entry.getValue().toString().split(StringUtils.DELIMITER_COMMA);
				if(gxXdr.getGxXdr().getProcedureStartTime()- Long.valueOf(valueCell[1])<-20000 &&
						gxXdr.getGxXdr().getProcedureStartTime()- Long.valueOf(valueCell[1])>1000)
				{
					continue;
				}

				curTime = Math.abs(gxXdr.getGxXdr().getProcedureStartTime()
								- Long.valueOf(valueCell[1]));
				if (minTime > curTime) {
					minTime = curTime;
					kpiAvg = Long.valueOf(valueCell[0]);
				}
			}
		}
		return kpiAvg;
	}
	
	public EventMesage NewGxRxEventMesage(GxRxNew gxXdr,S1mmeXdrNew s1,String failureCause, 
			String exceptionInterface, String eventName,
			Map<String, ExceptionReson>  exceptionMap, List<LteMroSourceNew> lteMroSourceList){
		EventMesage eventMesage = new EventMesage();
//		eventMesage.setCellType(exceptionMap.get(exceptionInterface + StringUtils.DELIMITER_COMMA + failureCause).getCellType());
		
		eventMesage.setFailureCause(failureCause);
		eventMesage.setProInterface(exceptionInterface);
		eventMesage.setCellType(exceptionMap.get(exceptionInterface+ StringUtils.DELIMITER_COMMA +failureCause)==null?null:(exceptionMap.get(exceptionInterface+ StringUtils.DELIMITER_COMMA + failureCause).getCellType()));
		eventMesage.setEventName(eventName);
		eventMesage.setCellRegion(Exception4GUtils.getCellRegion(eventMesage.getCellType()));
		eventMesage.setProcedureStarttime(gxXdr.getGxXdr().getProcedureStartTime());
		eventMesage.setImsi(gxXdr.getGxXdr().getCommXdr().getImsi());
		eventMesage.setImei(gxXdr.getGxXdr().getCommXdr().getImei());
		eventMesage.setProcedureType(gxXdr.getGxXdr().getProcedureType());
		eventMesage.setEtype(gxXdr.getEtype());
		if ( gxXdr.getGxXdr().getCellId() != null ){
			eventMesage.setCellid(ParseUtils.parseLong(gxXdr.getGxXdr().getCellId()));
		}
		
		if (eventMesage.getCellType() == null) {
			eventMesage.setCellKey("");
		}else if ("UE".equals(eventMesage.getCellType())) {			
			eventMesage.setCellKey(gxXdr.getGxXdr().getCommXdr().getImsi());
		}else if ("CELL".equals(eventMesage.getCellType()) || "目标CELL".equals(eventMesage.getCellType()) && s1 != null){
			eventMesage.setCellKey(s1.getS1mmeXdr().getCellId());
		}else if ("MME".equals(eventMesage.getCellType()) && s1 != null) {			
			eventMesage.setCellKey(s1.getS1mmeXdr().getMmeGroupId()+"_"+s1.getS1mmeXdr().getMmeCode());
		}
		
		eventMesage.setInteface(gxXdr.getGxXdr().getCommXdr().getInterfaces());
		eventMesage.setLonglat(lteMroSourceList, eventMesage);
//		eventMesage.setRangetime(gxXdr.getGxXdr().getRangeTime());
//		eventMesage.setElong(gxXdr.getElong());
//		eventMesage.setElat(gxXdr.getElat());
		eventMesage.setEupordown(gxXdr.getEupordown());		
		
		eventMesage.getWirelessException().setXdrId(gxXdr.getGxXdr().getCommXdr().getXdrid());
		WirelessAnalyseService wirelessanalyseservice=new WirelessAnalyseService();
		eventMesage = wirelessanalyseservice.wirelessAnalyse(g_lteCellMap, g_cellXdrMap,g_nCellinfoMap,
				g_configPara,g_uUXDR, lteMroSourceList, eventMesage);
		return eventMesage;
	}
}
