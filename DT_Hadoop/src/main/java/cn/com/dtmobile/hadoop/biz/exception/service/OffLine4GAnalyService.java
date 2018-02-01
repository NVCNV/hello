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

//4G脱网
public class OffLine4GAnalyService {

	EventMesage eventExceptionMessage = null;
	//public S1mmeExceptionMessage s1mmeExceptionMessage;
	//private LteMroSourceNew miniUeMr;
	//private LteMroSourceNew miniUe6Mr;
	//private LteMroSourceNew miniCellMr ;
	
	/** 4G脱网分析流程
	 * @param s1mmeXdrList 
	 * @param lteMroSourceList
	 * @param uuXdrList
	 * @param cellXdrMap
	 * @param exceptionMap
	 * @return
	 */
	public List<EventMesage> OffLine4GService(List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			List<UuXdrNew> uuXdrList, 
			Map<String, String> ltecellMap,
			Map<String, List<String>> cellXdrMap,
			Map<String, ExceptionReson>  exceptionMap, Map<String,String> nCellinfoMap, Map<String, String> configPara) {
		List<EventMesage> s1ExceptionMessageList = new ArrayList<EventMesage>();
		if (s1mmeXdrList.size() > 0) {			
			for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
				
				
				if ( ExceptionConstnts.ESC_NET_FAIL == s1mmeXdr.getEtype()) 
				{					
					eventExceptionMessage = this._4GOffLineAnalyOne(s1mmeXdr,s1mmeXdrList,
							lteMroSourceList, ltecellMap, cellXdrMap, exceptionMap, nCellinfoMap, configPara, uuXdrList);
					
					//无线环境分析					
					if (eventExceptionMessage != null) 
					{
						s1ExceptionMessageList.add(eventExceptionMessage);						
						continue;
					}
				}
			}
		}
		
		return s1ExceptionMessageList;
	}
	
	
	private EventMesage getEventMesage(S1mmeXdrNew s1mmeXdr, String failcode, Map<String, ExceptionReson>  exceptionMap, List<LteMroSourceNew> lteMroSourceList,
			Map<String, String> lteCellMap,
			Map<String, List<String>> cellXdrMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
	{
		EventMesage em = new EventMesage();
		
		switch (failcode) 
		{
			case "C01":
				em.setProInterface(InterfaceConstants.S1AP);   //问题接口
	            if(s1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255"))
	            { //CODE
	                em.setFailureCause("接口超时");
	            }
	            else
	            {
	                em.setFailureCause(s1mmeXdr.getS1mmeXdr().getRequestCause());
	            }
				break;
			case "C02":
				em.setProInterface(InterfaceConstants.UU);   //问题接口
				em.setFailureCause("10"); //CODE
				break;
			case "C07":
				em.setProInterface(InterfaceConstants.S1AP);
	            if(s1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255"))
	            { //CODE
	                em.setFailureCause("接口超时");
	            }
	            else
	            {
	                em.setFailureCause("28");
	            }
				break;
			default:
				break;
		}
		
		//=========================================
		//事件名称
		em.setEventName("4G脱网");
		
		//Range Time
//		em.setRangetime(s1mmeXdr.getS1mmeXdr().getRangeTime());
		
		//=========================================	
		//问题网元类型
//		em.setCellType(exceptionMap.get(em.getProInterface() + StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType());
		em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

		//问题归属�?
		em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));

		//问题网元标识
		em.setCellKeyByCellType_S1MME(s1mmeXdr);
			
		//Start Time
		em.setProcedureStarttime(s1mmeXdr.getS1mmeXdr().getProcedureStartTime());
		
		//Imsi
		em.setImsi(s1mmeXdr.getS1mmeXdr().getCommXdr().getImsi());
		
		em.setImei(s1mmeXdr.getS1mmeXdr().getCommXdr().getImei());
		
		//Inteface
		em.setInteface(s1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces());
		
		//ProcedureType
		em.setProcedureType(ParseUtils.parseInteger(s1mmeXdr.getS1mmeXdr().getProcedureType()));
		
		//EType
		em.setEtype(s1mmeXdr.getEtype());
		
		//Cell ID
		em.setCellid(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()));		
		
		//经度
		em.setLonglat(lteMroSourceList, em);
		//em.setElong(s1mmeXdr.getElong());
		
		//纬度
		//em.setElat(s1mmeXdr.getElat());
		
		em.getWirelessException().setXdrId(s1mmeXdr.getS1mmeXdr().getCommXdr().getXdrid());
        WirelessAnalyseService wirelessanalyseservice = new WirelessAnalyseService();
        return wirelessanalyseservice.wirelessAnalyse(lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList, lteMroSourceList, em);
	}
	
	
	//步骤1
    private EventMesage _4GOffLineAnalyOne(S1mmeXdrNew s1mmeXdr,
				List<S1mmeXdrNew> s1mmeXdrList,
				List<LteMroSourceNew> lteMroSourceList,
				Map<String, String> ltecellMap,				
				Map<String, List<String>> cellXdrMap, 
				Map<String, ExceptionReson>  exceptionMap,
				Map<String,String> nCellinfoMap,
				Map<String,String> configPara,
				List<UuXdrNew> uuXdrList)
		{

    	//interface = 5 就是s1mme  XDR
		if("16".equals(s1mmeXdr.getS1mmeXdr().getProcedureType()) && "3".equals(s1mmeXdr.getS1mmeXdr().getKeyword1()))
		{
			return getEventMesage(s1mmeXdr, "C02", exceptionMap, lteMroSourceList, ltecellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
		}

		//步骤2
		return this._4GOffLineAnalyTwo(s1mmeXdr, s1mmeXdrList,	lteMroSourceList, ltecellMap, cellXdrMap, exceptionMap, nCellinfoMap, configPara, uuXdrList);
	}
	
	//步骤2
	private EventMesage _4GOffLineAnalyTwo(S1mmeXdrNew s1mmeXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, String> ltecellMap,				
			Map<String, List<String>> cellXdrMap, 
			Map<String, ExceptionReson>  exceptionMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
	{
		//interface = 5 就是s1mme  XDR
		if("20".equals(s1mmeXdr.getS1mmeXdr().getProcedureType()))
		{
			if("28".equals(s1mmeXdr.getS1mmeXdr().getRequestCause()))
			{
				return getEventMesage(s1mmeXdr, "C07", exceptionMap, lteMroSourceList, ltecellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
			}
			else
			{				
				return getEventMesage(s1mmeXdr, "C01", exceptionMap, lteMroSourceList, ltecellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
			}				
		}
		else
		{
		    return null;
		}		
	}

/*
	//步骤3
	private EventMesage _4GOffLineAnalyThree(S1mmeXdrNew s1mmeXdr,
			List<LteMroSourceNew> lteMroSourceList, 
			Map<String, List<String>> cellXdrMap , 
			Map<String, ExceptionReson>  exceptionMap){
		List<LteMroSourceNew>ueMr1List = new ArrayList<LteMroSourceNew>();
		List<LteMroSourceNew>ueMr6List = new ArrayList<LteMroSourceNew>();
		
		if (lteMroSourceList != null) {
			for (LteMroSourceNew lteMroSource : lteMroSourceList) {
				if(lteMroSource.getLtemrosource().getVid() == 1
						// "MR.LteScRSRP" 已经相等�?
						// imsi 已经相等�?
						&& lteMroSource.getLtemrosource().getMrtime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - 20000 
						&& lteMroSource.getLtemrosource().getMrtime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime() + 1000
						&& lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()))
						&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource.getLtemrosource().getMrname())
						&& 1 == lteMroSource.getLtemrosource().getVid()){
					if(lteMroSource.getLtemrosource().getKpi1() != -1.0){
						ueMr1List.add(lteMroSource);
					}
					if(lteMroSource.getLtemrosource().getKpi6() != -1.0){
						ueMr6List.add(lteMroSource);
					}
				}				
			}
					
			miniUeMr = this.getMinKpi(s1mmeXdr, ueMr1List);
			if (miniUeMr != null) {
				if (miniUeMr.getLtemrosource().getKpi1() < 31){				
					if("6".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())){
						//弱覆蓋伴隨切換失�?
						return getEventMesage(s1mmeXdr, "08", exceptionMap, lteMroSourceList);
					}else {						
						//弱覆�?
						return getEventMesage(s1mmeXdr, "03", exceptionMap, lteMroSourceList);
					}
				}else{
					double avg = this.getAvgKpi(s1mmeXdr, cellXdrMap);
					if (avg > 111) {
						//上行干擾
						return getEventMesage(s1mmeXdr, "04", exceptionMap, lteMroSourceList);
					}else{
						miniUe6Mr = this.getMinKpi(s1mmeXdr, ueMr6List);
						if (miniUe6Mr != null) {
							if (miniUe6Mr.getLtemrosource().getKpi6() < 25) {
								if (miniUe6Mr.getLtemrosource().getKpi8() >= 22) {
									return getEventMesage(s1mmeXdr, "05", exceptionMap, lteMroSourceList);
								} else {
									return getEventMesage(s1mmeXdr, "06", exceptionMap, lteMroSourceList);
								}
							}
						}
					}
				}
			}
		}
		
		return getEventMesage(s1mmeXdr, "01", exceptionMap, lteMroSourceList);		
	}


	private LteMroSourceNew getMinKpi(S1mmeXdrNew s1mmeXdr, List<LteMroSourceNew> lteMroSourceList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			curTime = Math.abs(s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - lteMroSource.getLtemrosource().getMrtime());
			if (minTime > curTime) {
				minTime = curTime;
				LteMroSourceMap.put(minTime, lteMroSource);
			}
		}
		return LteMroSourceMap.get(minTime);
	}
	

	@SuppressWarnings("rawtypes")
	private Double getAvgKpi(S1mmeXdrNew s1mme, Map<String, List<String>> cellXdrMap) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Iterator iter = cellXdrMap.entrySet().iterator();
		Double kpiAvg = (double) 0;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			if (entry.getKey().equals(s1mme.getS1mmeXdr().getCellId())) {
				curTime = Math
						.abs(s1mme.getS1mmeXdr().getProcedureStartTime() - Long.valueOf(entry.getValue().toString()));
				if (minTime > curTime) {
					minTime = curTime;
					kpiAvg = Double.valueOf(entry.getValue().toString());
				}
			}
		}
		return kpiAvg;
	}
	
	private Double getAvgKpi(S1mmeXdrNew s1mme, Map<String, List<String>> cellXdrMap) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Iterator<Entry<String, String>> iter = cellXdrMap.entrySet().iterator();
		Double kpiAvg = (double) 0;

		while (iter.hasNext()) {
			Entry<String,String> entry =iter.next();
			String[] value=entry.getValue().split(",");
			if((s1mme.getS1mmeXdr().getProcedureStartTime()- Long.valueOf(value[1]))<-20000 &&
					(s1mme.getS1mmeXdr().getProcedureStartTime()- Long.valueOf(value[1])>1000))
				{
					continue;
				}
			if (entry.getKey().equals(s1mme.getS1mmeXdr().getCellId())) {
				curTime = Math
						.abs(s1mme.getS1mmeXdr().getProcedureStartTime() - Long.valueOf(value[1]));
				if (minTime > curTime) {
					minTime = curTime;
					kpiAvg = Double.valueOf(value[0]);
				}
			}
		}
		return kpiAvg;
	}
	

	private S1mmeXdrNew getMinS1mmeXDR(S1mmeXdrNew s1mmeXdr,
			List<S1mmeXdrNew> s1mmeXdrList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, S1mmeXdrNew> s1mmeXdrMap = new HashMap<Long, S1mmeXdrNew>();
		for (S1mmeXdrNew s1mmeXdri : s1mmeXdrList) {
			curTime = Math.abs(s1mmeXdr.getS1mmeXdr().getProcedureStartTime()
					- s1mmeXdri.getS1mmeXdr().getProcedureStartTime());
			if (minTime > curTime) {
				minTime = curTime;
				s1mmeXdrMap.put(minTime, s1mmeXdri);
			}
		}
		return s1mmeXdrMap.get(minTime);
	}
	*/
}
