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

public class AttachFailAnalyService {

	public List<EventMesage> attachFailAnalyService(List<S1mmeXdrNew> s1XdrList, List<UuXdrNew> uUXDR,
			List<LteMroSourceNew> lteMroSourceList, Map<String, String> ltecellMap, Map<String, List<String>> cellXdrMap,
			Map<String, ExceptionReson>  exceptionMap, Map<String, String> nCellinfoMap, Map<String, String> configPara) {
		List<EventMesage> s1mmeExceptionMessageList = new ArrayList<EventMesage>();
		if (s1XdrList.size() > 0) {
			for (S1mmeXdrNew s1Xdr : s1XdrList) {
					if (ExceptionConstnts.ATTACH_FAIL == s1Xdr.getEtype()) {
						String failCode = this.stepOne(s1Xdr, lteMroSourceList, cellXdrMap, uUXDR, ltecellMap,
								exceptionMap, nCellinfoMap, configPara);
						if (failCode.equals("")) {
							continue;
						}
						EventMesage em = new EventMesage();
						switch (failCode) {
						case "C01":
							if (s1Xdr.getS1mmeXdr().getProcedureStatus().equals("255")) {
								em.setFailureCause("接口超时");
							} else {
								em.setFailureCause(s1Xdr.getS1mmeXdr().getFailureCause());
							}
							em.setProInterface(InterfaceConstants.NAS);
							break;
						case "C02":
							em.setFailureCause(s1Xdr.getS1mmeXdr().getFailureCause());
							em.setProInterface(InterfaceConstants.S1AP);
							break;
						case "C03":
							em.setFailureCause("65535");
							em.setProInterface(InterfaceConstants.S1AP);
							break;
						default:
							break;
						}
						em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));
						em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
						em.setEventName("ATTACH失败");
						em.setProcedureStarttime(s1Xdr.getS1mmeXdr().getProcedureStartTime());
						em.setImsi(s1Xdr.getS1mmeXdr().getCommXdr().getImsi());
						em.setImei(s1Xdr.getS1mmeXdr().getCommXdr().getImei());
						em.setProcedureType(ParseUtils.parseInteger(s1Xdr.getS1mmeXdr().getProcedureType()));
						em.setEtype(s1Xdr.getEtype());
						em.setCellid(ParseUtils.parseLong(s1Xdr.getS1mmeXdr().getCellId()));
						em.setInteface(s1Xdr.getS1mmeXdr().getCommXdr().getInterfaces());
						em.setEupordown(s1Xdr.getEupordown());
						em.setLonglat(lteMroSourceList, em);
						
						if ("UE".equals(em.getCellType())) {
							em.setCellKey(s1Xdr.getS1mmeXdr().getCommXdr().getImsi());
						} else if ("CELL".equals(equals(em.getCellType()))) {
							em.setCellKey(s1Xdr.getS1mmeXdr().getCellId());
						} else if ("MME".equals(em.getCellType())) {
							em.setCellKey(s1Xdr.getS1mmeXdr().getMmeGroupId() + "_" + s1Xdr.getS1mmeXdr().getMmeCode());
						}

						WirelessAnalyseService wirelessAnalyseService = new WirelessAnalyseService();
						em.getWirelessException().setXdrId(s1Xdr.getS1mmeXdr().getCommXdr().getXdrid());
						s1mmeExceptionMessageList.add(wirelessAnalyseService.wirelessAnalyse(ltecellMap, cellXdrMap,
								nCellinfoMap, configPara, uUXDR, lteMroSourceList, em));
						// s1mmeExceptionMessageList.add(em);
					}
			}
		}
		return s1mmeExceptionMessageList;
	}

	public String stepOne(S1mmeXdrNew s1Xdr, List<LteMroSourceNew> lteMroSourceList, Map<String, List<String>> cellXdrMap,
			List<UuXdrNew> uUXDR, Map<String, String> ltecellMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> nCellinfoMap, Map<String, String> configPara) {

		String failCode = "";
		if ("1".equals(s1Xdr.getS1mmeXdr().getProcedureStatus())) {
			failCode = "C01";
		} else {
			int count = 0;
			for (UuXdrNew uuXdr : uUXDR) {
				if (uuXdr.getUuXdr().getProcedureStartTime() >= s1Xdr.getS1mmeXdr().getProcedureStartTime() - 1000
						&& uuXdr.getUuXdr().getProcedureStartTime() <= s1Xdr.getS1mmeXdr().getProcedureStartTime()
								+ 5000
						&& "13".equals(uuXdr.getUuXdr().getProcedureType())
						&& uuXdr.getUuXdr().getCellId().equals(s1Xdr.getS1mmeXdr().getCellId())) {
					count++;
				}
				if (count > 3) {
					break;
				}
			}
			if (count > 3) {
				failCode = "C03";
			} else {
				failCode = "C02";
			}
		}
		return failCode;
	}
}
