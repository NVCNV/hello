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
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.X2XdrNew;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class TauFailAnalyService {

	/**
	 * TAU\u5931\u8d25\u5206\u6790\u6d41\u7a0b
	 * @param s1mmeXdrList
	 * @param lteMroSourceList
	 * @param uuXdrList
	 * @param cellXdrMap
	 * @param exceptionMap
	 * @return
	 */
	public List<EventMesage> TauFailService(List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,List<X2XdrNew> x2XdrList,List<UuXdrNew> uuXdrList , 
			Map<String, String> ltecellMap,
			Map<String, List<String>> cellXdrMap,
			Map<String, ExceptionReson>  exceptionMap,
			Map<String,String> nCellinfoMap,
			Map<String,String> configPara) {
		List<EventMesage> s1ExceptionMessageList = new ArrayList<EventMesage>();
		if (s1mmeXdrList.size() > 0) {
			for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
				if(ExceptionConstnts.TAU_FAIL == s1mmeXdr.getEtype()&&s1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces()==5){				
					String failcode="";
					EventMesage em= new EventMesage();
					if( "1".equals(s1mmeXdr.getS1mmeXdr().getProcedureStatus())){
						failcode="C01";
					}else {
						int num=0;
						for (X2XdrNew x2 : x2XdrList) {
							if(x2.getX2Xdr().getProcedureStartTime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime() 
									&& x2.getX2Xdr().getProcedureStartTime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()+500
									&& "14".equals(x2.getX2Xdr().getProcedureType())){
								num++;
								break;
							}
						}
						if(num>0){
							failcode="C07";
						}else{
							int flag=0;
							for (UuXdrNew uuXdr : uuXdrList) {
								if(uuXdr.getUuXdr().getProcedureStartTime() >= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()-1000 
										&& uuXdr.getUuXdr().getProcedureStartTime() <= s1mmeXdr.getS1mmeXdr().getProcedureStartTime()+5000
										&& "13".equals(uuXdr.getUuXdr().getProcedureType()) 
										&& uuXdr.getUuXdr().getCellId().equals(s1mmeXdr.getS1mmeXdr().getCellId())){
									flag++;			
								}
							}
							if(flag > 3){
								failcode="C02";
							}else{
								failcode="C01";
							}
						}
					}	
					switch (failcode) {
					case "C01":
						if(s1mmeXdr.getS1mmeXdr().getProcedureStatus().equals("255")){
							em.setFailureCause("接口超时");
						}else{
							em.setFailureCause(s1mmeXdr.getS1mmeXdr().getFailureCause());
						}
						em.setProInterface(InterfaceConstants.NAS);
						break;
					case "C02":
						em.setFailureCause("65535");
						em.setProInterface(InterfaceConstants.S1AP);
						break;
					case "C07":
						em.setFailureCause("2002");
						em.setProInterface(InterfaceConstants.S1AP);
						break;
					default:
						break;
					}
//					em.setCellType(exceptionMap.get(em.getProInterface() +StringUtils.DELIMITER_COMMA+ em.getFailureCause()).getCellType());
					em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

					em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
					em.setEventName("TAU失败");
					em.setProcedureStarttime(s1mmeXdr.getS1mmeXdr().getProcedureStartTime());
					em.setImsi(s1mmeXdr.getS1mmeXdr().getCommXdr().getImsi());
					em.setImei(s1mmeXdr.getS1mmeXdr().getCommXdr().getImei());
					em.setProcedureType(ParseUtils.parseInteger(s1mmeXdr.getS1mmeXdr().getProcedureType()));
					em.setEtype(s1mmeXdr.getEtype());
					em.setCellid(ParseUtils.parseLong(s1mmeXdr.getS1mmeXdr().getCellId()));
					em.setInteface(s1mmeXdr.getS1mmeXdr().getCommXdr().getInterfaces());
					em.setEupordown(s1mmeXdr.getEupordown());
					em.setLonglat(lteMroSourceList, em);
					em.getWirelessException().setXdrId(s1mmeXdr.getS1mmeXdr().getCommXdr().getXdrid());
					if ("UE".equals(em.getCellType())) {
						em.setCellKey(s1mmeXdr.getS1mmeXdr().getCommXdr().getImsi());
					} else if ("CELL".equals(em.getCellType())) {
						em.setCellKey(s1mmeXdr.getS1mmeXdr().getCellId());
					} else if ("MME".equals(em.getCellType())) {
						em.setCellKey(s1mmeXdr.getS1mmeXdr().getMmeGroupId() + "_" + s1mmeXdr.getS1mmeXdr().getMmeCode());
					} 
					WirelessAnalyseService wirelessAnalyseService = new WirelessAnalyseService();
					s1ExceptionMessageList.add(wirelessAnalyseService.wirelessAnalyse(ltecellMap, cellXdrMap, nCellinfoMap, configPara, uuXdrList, lteMroSourceList, em));
				}
			}
		}
		return s1ExceptionMessageList;
	}
}
