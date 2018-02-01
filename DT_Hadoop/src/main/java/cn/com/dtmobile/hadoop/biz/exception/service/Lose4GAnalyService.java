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

//4G掉线
public class Lose4GAnalyService {

	EventMesage eventExceptionMessage = null;

	//private LteMroSourceNew miniUeMr;
	//private LteMroSourceNew miniUe6Mr;
	//private LteMroSourceNew miniCellMr;
	
	private List<EventMesage> x2EMList = null;
	private List<EventMesage> s1mmeEMList = null;
	
	/**
	 * 4G掉线
	 * 
	 * @param s1mmeXdrList
	 * @param lteMroSourceList
	 * @param uuXdrList
	 * @param cellXdrMap
	 * @param exceptionMap
	 * @return
	 */
	public List<EventMesage> Lose4GService(
			List<S1mmeXdrNew> s1mmeXdrList, List<LteMroSourceNew> lteMroSourceList,
			List<UuXdrNew> uuXdrList, Map<String, String> ltecellMap, Map<String, List<String>> cellXdrMap,
			Map<String, ExceptionReson>  exceptionMap, Map<String,String> nCellinfoMap, Map<String, String> configPara,
			List<EventMesage> x2ExceptionMessageList,List<EventMesage> s1mmeExceptionMessageList) {
		
		x2EMList = x2ExceptionMessageList;
		s1mmeEMList = s1mmeExceptionMessageList;
		
		List<EventMesage> s1ExceptionMessageList = new ArrayList<EventMesage>();
		if (s1mmeXdrList.size() > 0) {

			for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
				if (ExceptionConstnts.FG_OFFLINE_FAIL == s1mmeXdr.getEtype()) 
				{					
					eventExceptionMessage = this._4GLoseAnalyOne(s1mmeXdr,s1mmeXdrList,
							lteMroSourceList, cellXdrMap, exceptionMap, ltecellMap, nCellinfoMap, configPara, uuXdrList);
					
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
			EventMesage x2_s1_em,
			Map<String, String> lteCellMap,
			Map<String, List<String>> cellXdrMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
	{
		//不需要无线环境分析
		boolean no_WirelessAnalyse = false;
		
		EventMesage em = new EventMesage();
		
		switch (failcode) {
		case "C01":
		case "C02":
		case "C03":
		case "C05":
			em.setProInterface(InterfaceConstants.S1AP);   //问题接口
            if(s1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255")){ //CODE
                em.setFailureCause("接口超时");
             }else{
                em.setFailureCause(s1mmeXdr.getS1mmeXdr().getRequestCause());
             }
			break;	
		
		case "C04":
		case "C06":
			if(x2_s1_em != null){
				//遍历X2切换结果，找x2EMList里面, 距离当前时间最近的EventMessage			
				em.setTargetCellid(x2_s1_em.getTargetCellid());
				em.setFailureCause(x2_s1_em.getFailureCause());			
				em.setCellType(x2_s1_em.getCellType());
				em.setCellRegion(x2_s1_em.getCellRegion());
				em.setCellKey(x2_s1_em.getCellKey());
				em.setProInterface(x2_s1_em.getProInterface());   //问题接口
				
				
				//===============================
				em.getWirelessException().setExceptionCode(x2_s1_em.getWirelessException().getExceptionCode());	//无线原因值
				em.getWirelessException().setXdrId(s1mmeXdr.getS1mmeXdr().getCommXdr().getXdrid());	//异常事件XDRID
				em.getWirelessException().setExcertionType(x2_s1_em.getEtype().toString()); //关联异常事件类型
				em.getWirelessException().setInterfaceType(x2_s1_em.getInteface().toString()); //关联异常事件接口类型
				em.getWirelessException().setExceptionStartTime(x2_s1_em.getProcedureStarttime().toString()); //关联异常事件开始时间
				em.getWirelessException().setExceptionXdrId(x2_s1_em.getWirelessException().getXdrId()); //关联异常事件XDRID
				
				//===============================
				//主小区
				em.getWirelessException().setCellRSRP(x2_s1_em.getWirelessException().getCellRSRP()); //主小区RSRP			
				em.getWirelessException().setRip(x2_s1_em.getWirelessException().getRip());   //主小区RIP
				em.getWirelessException().setPhr(x2_s1_em.getWirelessException().getPhr());//主小区PHR
				em.getWirelessException().setUpSinr(x2_s1_em.getWirelessException().getUpSinr());//主小区上行SINR
				em.getWirelessException().setHighestCellId(x2_s1_em.getWirelessException().getHighestCellId()); //最强邻区CellID
				em.getWirelessException().setHighestCellRSRP(x2_s1_em.getWirelessException().getHighestCellRSRP()); //最强邻区RSRP
				em.getWirelessException().setSecondCellid(x2_s1_em.getWirelessException().getSecondCellid()); //次强邻区CellID
				em.getWirelessException().setSecondCellRSRP(x2_s1_em.getWirelessException().getSecondCellRSRP()); //次强邻区RSRP
				em.getWirelessException().setThirdCellId(x2_s1_em.getWirelessException().getThirdCellId()); //三强邻区CellID
				em.getWirelessException().setThirdCellRSRP(x2_s1_em.getWirelessException().getThirdCellRSRP()); //三强邻区RSRP
				em.getWirelessException().setActualRange(x2_s1_em.getWirelessException().getActualRange()); //最强邻区与主小区实际距离（km）
				em.getWirelessException().setEquivalentDistance(x2_s1_em.getWirelessException().getEquivalentDistance()); //最强邻区与主小区等效距离（km）
				em.getWirelessException().setModelThreeNCellId(x2_s1_em.getWirelessException().getModelThreeNCellId()); //主小区模3干扰邻小区CellID
				em.getWirelessException().setModelThreeNPci(x2_s1_em.getWirelessException().getModelThreeNPci()); //主小区模3干扰邻小区PCI
				em.getWirelessException().setModelThreeNRSRP(x2_s1_em.getWirelessException().getModelThreeNRSRP()); //主小区模3干扰邻小区RSRP
				em.getWirelessException().setModelThreeActualRange(x2_s1_em.getWirelessException().getModelThreeActualRange()); //模3干扰邻区与主小区实际距离（km
				em.getWirelessException().setModelThreeEquivDistance(x2_s1_em.getWirelessException().getModelThreeEquivDistance()); //模3干扰邻区与主小区等效距离（km）
				
				//===============================
				//目标小区
				em.getWirelessException().setTargetCellRip(x2_s1_em.getWirelessException().getTargetCellRip());  //目标小区rip
				em.getWirelessException().setTargetCellRSRP(x2_s1_em.getWirelessException().getTargetCellRSRP());  //目标小区RSRP
				em.getWirelessException().setTargetHighestCellId(x2_s1_em.getWirelessException().getTargetHighestCellId());  //目标小区最强邻区CellID
				em.getWirelessException().setTargetHighestCellRSRP(x2_s1_em.getWirelessException().getTargetHighestCellRSRP());   //目标小区最强邻区RSRP
				em.getWirelessException().setTargetSecondCellId(x2_s1_em.getWirelessException().getTargetSecondCellId()); //目标小区次强邻区CellID
				em.getWirelessException().setTargetSecondCellRSRP(x2_s1_em.getWirelessException().getTargetSecondCellRSRP()); //目标小区次强邻区RSRP
				em.getWirelessException().setTargetThirdCellId(x2_s1_em.getWirelessException().getTargetThirdCellId()); //目标小区三强邻区CellID
				em.getWirelessException().setTargetThirdCellRSRP(x2_s1_em.getWirelessException().getTargetThirdCellRSRP()); //目标小区三强邻区RSRP
				em.getWirelessException().setTargetActualRange(x2_s1_em.getWirelessException().getTargetActualRange());  //目标小区最强邻区与目标小区实际距离（km）
				em.getWirelessException().setTargetEquivalentDistance(x2_s1_em.getWirelessException().getTargetEquivalentDistance()); //目标小区最强邻区与目标小区等效距离（km）
				em.getWirelessException().setTargetModelThreeNCellId(x2_s1_em.getWirelessException().getTargetModelThreeNCellId()); //目标小区模3干扰邻小区CellID
				em.getWirelessException().setTargetModelThreeNPci(x2_s1_em.getWirelessException().getTargetModelThreeNPci()); //目标小区模3干扰邻小区PCI
				em.getWirelessException().setTargetModelThreeNRSRP(x2_s1_em.getWirelessException().getTargetModelThreeNRSRP()); //目标小区模3干扰邻小区RSRP
				em.getWirelessException().setTargetModelThreeActualRange(x2_s1_em.getWirelessException().getTargetModelThreeActualRange()); //目标小区模3干扰邻区与目标小区实际距离（km
				em.getWirelessException().setTargetModelThreeEquivDistance(x2_s1_em.getWirelessException().getTargetModelThreeEquivDistance()); //目标小区模3干扰邻区与目标小区等效距离（km）
				
				em.getWirelessException().setMRType(x2_s1_em.getWirelessException().getMRType()); //目标小区模3干扰邻区与目标小区等效距离（km）
				
				//===============================
				no_WirelessAnalyse = true;
			}
			break;	
		default:
			break;
		}
		
		//========================================= 
		//事件名称
		em.setEventName("4G掉线");
			
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
		
		/*
	    System.out.println("eventName: " + em.getEventName()); 
	    System.out.println("getProcedureStarttime: " + em.getProcedureStarttime());
	    System.out.println("getImsi: " + em.getImsi());
	    System.out.println("getProcedureType: " + em.getProcedureType());
	    System.out.println("getEtype: " + em.getEtype());
	    System.out.println("getCellid: " + em.getCellid());
	    System.out.println("getFailureCause: " + em.getFailureCause());
	    System.out.println("getCellType: " + em.getCellType());
	    System.out.println("getCellRegion: " + em.getCellRegion());
	    System.out.println("getCellKey: " + em.getCellKey());
	    System.out.println("getInteface: " + em.getInteface());
	    System.out.println("proInterface: " + em.getProInterface());	    
	    System.out.println("getRangetime: " + em.getRangetime()); 
	    */
		
		//经度
		em.setLonglat(lteMroSourceList, em);
		
		if(!no_WirelessAnalyse)
		{
		    em.getWirelessException().setXdrId(s1mmeXdr.getS1mmeXdr().getCommXdr().getXdrid());
		    WirelessAnalyseService wirelessanalyseservice = new WirelessAnalyseService();
		    return wirelessanalyseservice.wirelessAnalyse(lteCellMap, cellXdrMap,nCellinfoMap,configPara,uuXdrList,lteMroSourceList, em);
		}
		else
		{
		    return em;
		}
	}
	
	
	
	//Step 1
	private EventMesage _4GLoseAnalyOne(S1mmeXdrNew s1mmeXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
		{
		
		String requestCause = s1mmeXdr.getS1mmeXdr().getRequestCause();
		
		if ("1".equals(requestCause)
				|| "8".equals(requestCause)				
				|| "10".equals(requestCause)
				|| "12".equals(requestCause)
				|| "25".equals(requestCause)) 
		{			
			//Step 2
			return this._4GLoseAnalyTwo(s1mmeXdr, lteMroSourceList, cellXdrMap, exceptionMap, lteCellMap, nCellinfoMap, configPara, uuXdrList);
		}
		else
		{
			return getEventMesage(s1mmeXdr, "C01", exceptionMap, lteMroSourceList, null, lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
		}		
	}
	
	//Step 2
	private EventMesage _4GLoseAnalyTwo(S1mmeXdrNew s1mmeXdr,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
	{
		
		String requestCause = s1mmeXdr.getS1mmeXdr().getRequestCause();
		
		if ("10".equals(requestCause)
				|| "12".equals(requestCause)
				|| "25".equals(requestCause)) 
		{
			//Step 2
			return getEventMesage(s1mmeXdr, "C02", exceptionMap, lteMroSourceList, null, lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
		}
		else
		{
			return _4GLoseAnalyThree(s1mmeXdr, lteMroSourceList, cellXdrMap, exceptionMap, lteCellMap, nCellinfoMap, configPara, uuXdrList);
		}	
	}
	
	private EventMesage getX2EventMessage(S1mmeXdrNew s1mmeXdr) {
		
		if(x2EMList == null)
		{
			return null;
		}
		
		Long ulMin_ProcedureStarttime = Long.MAX_VALUE;
		Long ulCur_ProcedureStarttime = 0L;
		EventMesage min_em = null;
		
		for (EventMesage em : x2EMList) 
		{
			if(
					10   == em.getEtype() 
					&& 2 == em.getInteface()
					&& em.getCellid().equals(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()))
					&& em.getProcedureStarttime() >= (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - 5000) 
					&& em.getProcedureStarttime() <= (s1mmeXdr.getS1mmeXdr().getProcedureStartTime())
			  )
			{
				ulCur_ProcedureStarttime = 	Math.abs(s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - em.getProcedureStarttime());
				if (ulMin_ProcedureStarttime > ulCur_ProcedureStarttime) 
				{
					ulMin_ProcedureStarttime = ulCur_ProcedureStarttime;
					min_em = em;
				}
			}
		}
		
		return min_em;
	}

	
	//Step 3
	private EventMesage _4GLoseAnalyThree(S1mmeXdrNew s1mmeXdr,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
		{
		
		String requestCause = s1mmeXdr.getS1mmeXdr().getRequestCause();
		
		if ("1".equals(requestCause))
		{	
			EventMesage em = getX2EventMessage(s1mmeXdr);
			if(em != null)
			{
				return getEventMesage(s1mmeXdr, "C04", exceptionMap, lteMroSourceList, em, lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
			}
			else
			{
			    return getEventMesage(s1mmeXdr, "C03", exceptionMap, lteMroSourceList, null, lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
			}
		}
		else
		{
			return _4GLoseAnalyFour(s1mmeXdr, lteMroSourceList, cellXdrMap, exceptionMap, lteCellMap, nCellinfoMap, configPara, uuXdrList);
		}
	}

	private EventMesage getS1EventMessage(S1mmeXdrNew s1mmeXdr) {
		
		if(s1mmeEMList == null)
		{
			return null;
		}
		
		Long ulMin_ProcedureStarttime = Long.MAX_VALUE;
		Long ulCur_ProcedureStarttime = 0L;
		EventMesage min_em = null;
		
		for (EventMesage em : s1mmeEMList) 
		{
			if(
					10   == em.getEtype() 
					&& 5 == em.getInteface()
					&& em.getCellid().equals(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()))
					&& em.getProcedureStarttime() >= (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - 5000) 
					&& em.getProcedureStarttime() <= (s1mmeXdr.getS1mmeXdr().getProcedureStartTime())
			)
			{
				ulCur_ProcedureStarttime = 	Math.abs(s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - em.getProcedureStarttime());
				if (ulMin_ProcedureStarttime > ulCur_ProcedureStarttime) 
				{
					ulMin_ProcedureStarttime = ulCur_ProcedureStarttime;
					min_em = em;
				}
			}
		}
		
		return min_em;
	}
	
	//Step 4
	private EventMesage _4GLoseAnalyFour(S1mmeXdrNew s1mmeXdr,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara,
			List<UuXdrNew> uuXdrList)
	{
		
		String requestCause = s1mmeXdr.getS1mmeXdr().getRequestCause();
		
		if ("8".equals(requestCause))
		{	
			EventMesage em = getS1EventMessage(s1mmeXdr);
			if(em != null)
			{
				return getEventMesage(s1mmeXdr, "C06", exceptionMap, lteMroSourceList, em, lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
			}
			else
			{
			    return getEventMesage(s1mmeXdr, "C05", exceptionMap, lteMroSourceList, null, lteCellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList);
			}
		}
		
		return null;
	}
	
	
	
/*
	private EventMesage _4GLoseAnalyOne(S1mmeXdrNew s1mmeXdr,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap) {
		
		//Step 1
		if (StringUtils.isNotEmpty(s1mmeXdr.getS1mmeXdr().getRequestCause())
				&& Integer.parseInt(s1mmeXdr.getS1mmeXdr().getRequestCause()) > 256) {
			return getEventMesage(s1mmeXdr, "01", exceptionMap, lteMroSourceList);
		} else if ("10".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())
				|| "12".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())
				|| "25".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())) {
			return getEventMesage(s1mmeXdr, "02", exceptionMap, lteMroSourceList);
		} else if ("1".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())) {
			return getEventMesage(s1mmeXdr, "07", exceptionMap, lteMroSourceList);
		} else if ("8".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())) {
			return getEventMesage(s1mmeXdr, "08", exceptionMap, lteMroSourceList);
		//Step 2
		} else if ("3".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())
				|| "6".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())
				|| "21".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())
				|| "26".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())) {
			return this._4GLoseAnalyTwo(s1mmeXdr, lteMroSourceList, cellXdrMap, exceptionMap);
		}
		else {
			return getEventMesage(s1mmeXdr, "01", exceptionMap, lteMroSourceList);
		}
		
	}



	private EventMesage _4GLoseAnalyTwo(S1mmeXdrNew s1mmeXdr,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap) {
		List<LteMroSourceNew> ueMr1List = new ArrayList<LteMroSourceNew>();
		List<LteMroSourceNew> ueMr6List = new ArrayList<LteMroSourceNew>();

		if (lteMroSourceList.size() > 0) {
			for (LteMroSourceNew lteMroSource : lteMroSourceList) {
				if (lteMroSource.getLtemrosource().getVid() == 1
						// "MR.LteScRSRP" 已经相等�?
						// imsi 已经相等�?
						&& lteMroSource.getLtemrosource().getMrtime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime() - 20000
						&& lteMroSource.getLtemrosource().getMrtime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime() + 1000
						&& lteMroSource.getLtemrosource().getCellid().equals(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()))) {
					if (TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource.getLtemrosource().getMrname())) {
						if (lteMroSource.getLtemrosource().getKpi1() != -1.0) {
							ueMr1List.add(lteMroSource);
						} 
						if (lteMroSource.getLtemrosource().getKpi6() != -1.0) {
							ueMr6List.add(lteMroSource);
						}
					}
				}
			}

			miniUeMr = this.getMinKpi(s1mmeXdr, ueMr1List);
			if (miniUeMr != null) {
				if (miniUeMr.getLtemrosource().getKpi1() < 31){
					
					if("6".equals(s1mmeXdr.getS1mmeXdr().getRequestCause())){
						//弱覆蓋伴隨切換失�?
						return getEventMesage(s1mmeXdr, "09", exceptionMap, lteMroSourceList);
					}else{									
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

	
	
	private LteMroSourceNew getMinKpi(S1mmeXdrNew s1mmeXdr,
			List<LteMroSourceNew> lteMroSourceList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			curTime = Math.abs(s1mmeXdr.getS1mmeXdr().getProcedureStartTime()
					- lteMroSource.getLtemrosource().getMrtime());
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
						.abs(s1mme.getS1mmeXdr().getProcedureStartTime()
								- Long.valueOf(entry.getValue().toString()));
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
	
	/*
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
