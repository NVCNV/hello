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
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.SvNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class EsrvccFailAnalyService {
	EventMesage eventMessage = null;

	public List<EventMesage> esrvccFailAnalyService(List<SvNew> svList,
			List<UuXdrNew> uuXdrList, List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, ExceptionReson>  exceptionMap,
			Map<String, List<String>> cellXdrMap,
			Map<String, String> lteCellMap,Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		List<EventMesage> eventMessageList = new ArrayList<EventMesage>();
		// enter the interface 13
		if (s1mmeXdrList.size() > 0) {

			for (S1mmeXdrNew s1Xdr : s1mmeXdrList) {
				if (ExceptionConstnts.ESRVCC_FAIL == s1Xdr.getEtype()) {
					// step 1
					eventMessage = this.stepOne(s1Xdr,
							lteMroSourceList, uuXdrList, svList, exceptionMap,cellXdrMap,lteCellMap,nCellinfoMap,configPara);
					if (eventMessage != null) {
						eventMessageList.add(eventMessage);
						continue;
					}
				} 
			}
		}
		return eventMessageList;
	}

	// step 1
	public EventMesage stepOne(S1mmeXdrNew s1Xdr,
			List<LteMroSourceNew> lteMroSourceList, List<UuXdrNew> uuList,
			List<SvNew> svList, Map<String, ExceptionReson>  exceptionMap,
			Map<String, List<String>> cellXdrMap,
			Map<String, String> lteCellMap,Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		
		for (SvNew svXdr : svList) {
			if (svXdr.getSv().getProcedureStartTime() >= s1Xdr.getS1mmeXdr().getProcedureStartTime()
					&& svXdr.getSv().getProcedureStartTime() <= s1Xdr.getS1mmeXdr().getProcedureEndTime()) {
				if (svXdr.getSv().getCommXdr().getInterfaces() == 19
						&& "1".equals(svXdr.getSv().getProcedureType())) {
					if (!"0".equals(svXdr.getSv().getResult())) {
						return NewS1EventMesage(s1Xdr,
								svXdr.getSv().getSvCause(), 
								InterfaceConstants.SV, "ESRVCC失败", exceptionMap, lteMroSourceList,cellXdrMap,uuList,lteCellMap,nCellinfoMap,configPara); //o1
					}
				}
			}
		}
		
		return this.stepTwo(uuList, lteMroSourceList, s1Xdr,
				exceptionMap,cellXdrMap,lteCellMap,nCellinfoMap,configPara);
		
		
	}

	public EventMesage stepTwo(List<UuXdrNew> uuList,
			List<LteMroSourceNew> lteMroSourceList, S1mmeXdrNew s1Xdr,
			Map<String, ExceptionReson>  exceptionMap,
			Map<String, List<String>> cellXdrMap,
			Map<String, String> lteCellMap,Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		if (uuList != null) {
			for (UuXdrNew uuXdr : uuList) {
				if (uuXdr.getUuXdr().getProcedureStartTime() <= s1Xdr.getS1mmeXdr().getProcedureStartTime() - 1000
						&& uuXdr.getUuXdr().getProcedureStartTime() >= s1Xdr.getS1mmeXdr().getProcedureEndTime() + 1000) {
					if (uuXdr.getUuXdr().getCommXdr().getInterfaces() == 1
							&& "10".equals(uuXdr.getUuXdr().getProcedureType())
							&& "0".equals(uuXdr.getUuXdr().getProcedureStatus())) {
						// step 2
						return NewS1EventMesage(s1Xdr,
								s1Xdr.getS1mmeXdr().getRequestCause(), //requestCause,
								InterfaceConstants.S1AP, "ESRVCC失败", exceptionMap, lteMroSourceList,cellXdrMap,uuList,lteCellMap,nCellinfoMap,configPara); //02
					}
				}
			}
			
			// step 3
			if ("6".equals(s1Xdr.getS1mmeXdr().getRequestCause())
					&& "5".equals(s1Xdr.getS1mmeXdr().getCommXdr().getInterfaces())) {
				return NewS1EventMesage(s1Xdr, "13",
						InterfaceConstants.UU, "ESRVCC失败", exceptionMap, lteMroSourceList,cellXdrMap,uuList,lteCellMap,nCellinfoMap,configPara); //04
			}else{
				// step 4
				if (!("8".equals(s1Xdr.getS1mmeXdr().getRequestCause())
						&& "5".equals(s1Xdr.getS1mmeXdr().getCommXdr().getInterfaces()))) {
					return NewS1EventMesage(s1Xdr,
							"8", 
							InterfaceConstants.S1AP, "ESRVCC失败", exceptionMap, lteMroSourceList,cellXdrMap,uuList,lteCellMap,nCellinfoMap,configPara); //02
				}else{
					return NewS1EventMesage(s1Xdr,s1Xdr.getS1mmeXdr().getFailureCause(), 
							InterfaceConstants.S1AP, "ESRVCC失败", exceptionMap, lteMroSourceList,cellXdrMap,uuList,lteCellMap,nCellinfoMap,configPara); //02
				}
			}
		}
		return null; 
	}

	/*public EventMesage stepFive(S1mmeXdrNew s1Xdr,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, String> exceptionMap) {
		List<LteMroSourceNew> ueMrList = new ArrayList<LteMroSourceNew>();
		if (lteMroSourceList != null) {
			for (LteMroSourceNew lteMroSource : lteMroSourceList) {
				if (lteMroSource.getLtemrosource().getVid() == 1
						&& lteMroSource.getLtemrosource().getCellid().equals( 
							ParseUtils.parseLong(s1Xdr.getS1mmeXdr().getCellId()))
						&& lteMroSource.getLtemrosource().getMrtime() >= s1Xdr.getS1mmeXdr()
								.getProcedureStartTime() - 20000
						&& lteMroSource.getLtemrosource().getMrtime() <= s1Xdr.getS1mmeXdr()
								.getProcedureStartTime() + 1000
						&& lteMroSource.getLtemrosource().getKpi1() != -1.0) {
					if (TablesConstants.TABLE_UE_MR_XDR_FLAG
							.equals(lteMroSource.getLtemrosource().getMrname())) {
						ueMrList.add(lteMroSource);
					}
				}
			}
//			for (LteMroSourceNew ueMr : ueMrList) {
				if ( !ueMrList.isEmpty() ueMr != null) {
					LteMroSourceNew miniMr = this.getMinKpi(s1Xdr, ueMrList);
					if (miniMr.getLtemrosource().getKpi1() < 31) {
						return NewS1EventMesage(s1Xdr, "1",
								InterfaceConstants.UU, "ESRVCC失败", exceptionMap, lteMroSourceList);
					} else {
						return NewS1EventMesage(s1Xdr,
								s1Xdr.getS1mmeXdr().getRequestCause(),
								InterfaceConstants.S1AP, "ESRVCC失败", exceptionMap, lteMroSourceList);
					}
				}
//			}
		} 
			
		return NewS1EventMesage(s1Xdr, s1Xdr.getS1mmeXdr().getRequestCause(),
					InterfaceConstants.S1AP, "ESRVCC失败", exceptionMap, lteMroSourceList);
		
	}
*/
	/*public LteMroSourceNew getMinKpi(S1mmeXdrNew s1Xdr,
			List<LteMroSourceNew> lteMroSourceList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, LteMroSourceNew> LteMroSourceMap = new HashMap<Long, LteMroSourceNew>();
		for (LteMroSourceNew lteMroSource : lteMroSourceList) {
			curTime = Math.abs(s1Xdr.getS1mmeXdr().getProcedureStartTime()
					- lteMroSource.getLtemrosource().getMrtime());
			if (minTime > curTime) {
				minTime = curTime;
				LteMroSourceMap.put(minTime, lteMroSource);
			}
		}
		return LteMroSourceMap.get(minTime);
	}*/
	
	public EventMesage NewS1EventMesage(S1mmeXdrNew s1,String failureCause, 
			String exceptionInterface, String eventName,
			Map<String, ExceptionReson>  exceptionMap, List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, List<UuXdrNew> uUXDR,
			Map<String, String> lteCellMap,Map<String,String> nCellinfoMap,Map<String, String> configPara){
		EventMesage eventMesage = new EventMesage();

		eventMesage.setFailureCause(failureCause);
		eventMesage.setProInterface(exceptionInterface);
		
		eventMesage.setCellType(exceptionMap.get(exceptionInterface+ StringUtils.DELIMITER_COMMA +failureCause)==null?null:(exceptionMap.get(exceptionInterface+ StringUtils.DELIMITER_COMMA + failureCause).getCellType()));

		eventMesage.setEventName(eventName);
		eventMesage.setCellRegion(Exception4GUtils.getCellRegion(eventMesage.getCellType()));
		eventMesage.setProcedureStarttime(s1.getS1mmeXdr().getProcedureStartTime());
		eventMesage.setImsi(s1.getS1mmeXdr().getCommXdr().getImsi());
		eventMesage.setImei(s1.getS1mmeXdr().getCommXdr().getImei());
		eventMesage.setProcedureType(ParseUtils.parseInteger(s1.getS1mmeXdr().getProcedureType()));
		eventMesage.setEtype(s1.getEtype());
		eventMesage.setCellid(ParseUtils.parseLong(s1.getS1mmeXdr().getCellId()));
		eventMesage.setCellKeyByCellType_S1MME(s1);
				
		eventMesage.setInteface(s1.getS1mmeXdr().getCommXdr().getInterfaces());
		eventMesage.setLonglat(lteMroSourceList, eventMesage);
//		eventMesage.setRangetime(s1.getS1mmeXdr().getRangeTime());
//		eventMesage.setElong(s1.getElong());
//		eventMesage.setElat(s1.getElat());
		eventMesage.setEupordown(s1.getEupordown());
		eventMesage.getWirelessException().setXdrId(s1.getS1mmeXdr().getCommXdr().getXdrid());
        WirelessAnalyseService wirelessanalyseservice=new WirelessAnalyseService();
        return wirelessanalyseservice.wirelessAnalyse(lteCellMap, cellXdrMap,nCellinfoMap,configPara,uUXDR,lteMroSourceList, eventMesage);

	}
}
