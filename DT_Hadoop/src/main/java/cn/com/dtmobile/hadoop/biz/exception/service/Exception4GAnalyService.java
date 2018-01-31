package cn.com.dtmobile.hadoop.biz.exception.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class Exception4GAnalyService {

/*	

*/
	//周期性RSRP（UE_MR）
	private Map<String, String> g_configPara = null ;
	private Map<String,String> g_nCellinfoMap = null;
	
	public List<EventMesage> exception4GAnalyServiceUu(List<UuXdrNew> uuList,
			Map<String, String> ltecellMap, Map<String, List<String>> cellXdrMap,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, ExceptionReson>  exceptionMap, Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		
		g_configPara = configPara;
		g_nCellinfoMap = nCellinfoMap;
		boolean bNeedWirelessCause = false;
		
		List<EventMesage> uuExceptionMessageList = new ArrayList<EventMesage>();
		if (uuList.size() > 0) {
			for (UuXdrNew uuXdr : uuList) {
				//步骤1:UU口 ETYPE  = 10，直接输出CO5：空口处理失败  CO5
				if ( ExceptionConstnts.FG_SWITCH_FAIL == uuXdr.getEtype()) {
					EventMesage em=new EventMesage();
//					bNeedWirelessCause = true;
//					em.setFailureCause(uuXdr.getUuXdr().getKeyword());  //????填写无线侧原因
					if ( "1".equals(uuXdr.getUuXdr().getProcedureStatus()) ) { //空口处理失败
						em.setFailureCause("31");
					}else{ //目标小区未收到重配置完成消息
						em.setFailureCause("32");
					}
					em.setProInterface(InterfaceConstants.UU);  //??????wyq接口是S1ap还是UU
					em.setEventName("4G切换失败");
					em.setProcedureStarttime(uuXdr.getUuXdr().getProcedureStartTime());
					em.setImsi(uuXdr.getUuXdr().getCommXdr().getImsi());
					em.setImei(uuXdr.getUuXdr().getCommXdr().getImei());
					em.setProcedureType(ParseUtils.parseInteger(uuXdr.getUuXdr().getProcedureType()));
					em.setEtype(uuXdr.getEtype());
					em.setTargetCellid(ParseUtils.parseLong(uuXdr.getUuXdr().getTargetCellId()));
					em.setCellid(ParseUtils.parseLong(uuXdr.getUuXdr().getCellId()));
					em.setInteface(uuXdr.getUuXdr().getCommXdr().getInterfaces());
					em.setEupordown(uuXdr.getEupordown());
					em.setLonglat(lteMroSourceList, em);
					if ("UE".equals(em.getCellType())) {
						em.setCellKey(uuXdr.getUuXdr().getCommXdr().getImsi());
					} else if ("CELL".equals(em.getCellType())) {
						em.setCellKey(uuXdr.getUuXdr().getCellId());
					} 
					em.getWirelessException().setXdrId(uuXdr.getUuXdr().getCommXdr().getXdrid());
					Exception4GUtils hoUtils = new Exception4GUtils();
					em = hoUtils.hoWirelessAnalysis(uuXdr.getUuXdr(),lteMroSourceList, ltecellMap, cellXdrMap, g_nCellinfoMap,g_configPara, 
							uuXdr.getUuXdr().getTargetCellId(),uuXdr.getUuXdr().getCellId(),em,bNeedWirelessCause);
//					em.setCellType(exceptionMap.get(em.getProInterface() +StringUtils.DELIMITER_COMMA+ em.getFailureCause()).getCellType());
				
					em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

					em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
					if ("UE".equals(em.getCellType())) {
						em.setCellKey(uuXdr.getUuXdr().getCommXdr().getImsi());
					} else if ("CELL".equals(em.getCellType())) {
						em.setCellKey(uuXdr.getUuXdr().getCellId());
					} 
					uuExceptionMessageList.add(em);
				}
			}
		}
		return uuExceptionMessageList;
	}

	public List<EventMesage> exception4GAnalyServiceX2(List<X2XdrNew> x2List,
			List<S1mmeXdrNew> s1mmeXdrList, 
			List<UuXdrNew> uuList,
			Map<String, String> ltecellMap, 
			Map<String, List<String>> cellXdrMap,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, ExceptionReson>  exceptionMap, 
			Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		
		g_configPara = configPara;
		g_nCellinfoMap = nCellinfoMap;
		boolean bNeedWirelessCause = false;
		
		List<EventMesage> x2ExceptionMessageList = new ArrayList<EventMesage>();
		// enter the interface X2XdrNew
		if (x2List.size() > 0) {
			for (final X2XdrNew x2 : x2List) {
				if ( ExceptionConstnts.FG_SWITCH_FAIL == x2.getEtype()) {
					EventMesage em=new EventMesage();
					int num=0;
					String failcode="";
					String failureCause="";
					String pstatus="";
					if (uuList != null) {
						//步骤2:以interface=2（X2口）的[开始时间，结束时间]为查询时间范围（Uu接口用rangetime查询），查看该用户在该时段内interface=1（Uu接口），
						//procedureType=8且切换源CellID、目标CellID与该XDR相同的XDR， procedure status是否为0。如果是，进行步骤3；否则，接输出CO5：空口处理失败；
						for (UuXdrNew uuXdr : uuList) {
							long rangetimemills=uuXdr.getUuXdr().getProcedureStartTime();
							if ( rangetimemills >= x2.getX2Xdr().getProcedureStartTime()
									&& rangetimemills <= x2.getX2Xdr().getProcedureEndTime()
									&& "8".equals(uuXdr.getUuXdr().getProcedureType()) 
									&& uuXdr.getUuXdr().getCellId().equals(x2.getX2Xdr().getCellId().toString())
									&& uuXdr.getUuXdr().getTargetCellId().equals(x2.getX2Xdr().getTargetCellId())
									&& "0".equals(uuXdr.getUuXdr().getProcedureStatus())){
								num++;
							}
						}
					}
					if(num>0){
						// step 3
						num=0;
						int num1=0;
						if (s1mmeXdrList != null) {
							Collections.sort(s1mmeXdrList, new Comparator<S1mmeXdrNew>() {
								
								@Override
								public int compare(S1mmeXdrNew o1, S1mmeXdrNew o2) {
									int result=(int)(Math.abs(o1.getS1mmeXdr().getProcedureStartTime()-x2.getX2Xdr().getProcedureStartTime())-Math.abs(o2.getS1mmeXdr().getProcedureStartTime()-x2.getX2Xdr().getProcedureStartTime()));
									return result==0?0:(result>0?1:-1);
								}
							});
							// 步骤3:以interface=2（X2口）的[开始时间，结束时间]为查询时间范围，相同IMSI条件下，interface=5（S1接口），procedureType=14，
							//且CellID=X2接口切换XDR的targetCellid的XDR
							for (S1mmeXdrNew s1Xdr : s1mmeXdrList) {
								long rangetimemills=s1Xdr.getS1mmeXdr().getProcedureStartTime();
								//判断其procedure status是否为0。如果不是，则进入步骤4；否则，输出CO2：源小区未接收到目标基站下发的资源释放信令；
								if ( rangetimemills >= x2.getX2Xdr().getProcedureStartTime()
										&& rangetimemills <= x2.getX2Xdr().getProcedureEndTime()
										&& "14".equals(s1Xdr.getS1mmeXdr().getProcedureType())
										&& s1Xdr.getS1mmeXdr().getCellId().equals(x2.getX2Xdr().getTargetCellId())&&"0".equals(s1Xdr.getS1mmeXdr().getProcedureStatus())){
									num++; //CO2
								}
								// 步骤4：	以interface=2（X2口）的[开始时间-1s，开始时间+5s]为查询时间范围，查看该用户在该时段内interface=5（S1接口），procedureType=14时，
								//procedure status是否为1。如果是，则输出CO1：failure cause失败原因，流程结束；否则，输出CO3：MME回复路径转换Ack超时，流程结束；
								if(rangetimemills >= x2.getX2Xdr().getProcedureStartTime()-1000
										&& rangetimemills <= x2.getX2Xdr().getProcedureStartTime()+5000
										&& "14".equals(s1Xdr.getS1mmeXdr().getProcedureType())
										&& s1Xdr.getS1mmeXdr().getCellId().equals(x2.getX2Xdr().getTargetCellId())&&"1".equals(s1Xdr.getS1mmeXdr().getProcedureStatus())){
									num1++;
									if(num1==1){ //C01
										failureCause=s1Xdr.getS1mmeXdr().getFailureCause();
										pstatus=s1Xdr.getS1mmeXdr().getProcedureStatus();
									}
								}
							}
						}
						if(num==0){
							if(num1>0){
								failcode="01";	//C01
							}else{
								failcode="02";	//C03
							}
						}else{
							failcode="09"; //C02
						}
					}else{
						//?????输出CO5：空口处理失败
						//bNeedWirelessCause = true;
						///////????wyq failcode=this.stepSixX2(x2, lteMroSourceList, ltecellMap, cellXdrMap, exceptionMap, t_professMap);
						em.setFailureCause("31");
						em.setProInterface(InterfaceConstants.UU);
					}
					switch (failcode) {
					case "01":
						if(pstatus.equals("255")){
							em.setFailureCause("接口超时");
						}else{
							em.setFailureCause(failureCause);
						}
						em.setProInterface(InterfaceConstants.S1AP);
						break;
					case "02":
						em.setFailureCause("2004");
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
						em.setFailureCause("11");
						em.setProInterface(InterfaceConstants.UU);
						break;
					case "08":
						em.setFailureCause("12");
						em.setProInterface(InterfaceConstants.UU);
						break;
					case "09":
						em.setFailureCause("1001");
						em.setProInterface(InterfaceConstants.X2);
						break;
					case "10":
						em.setFailureCause("15");
						em.setProInterface(InterfaceConstants.UU);
						break;
					default:
						break;
					}
					em.setEventName("4G切换失败");
					em.setProcedureStarttime(x2.getX2Xdr().getProcedureStartTime());
					em.setImsi(x2.getX2Xdr().getCommXdr().getImsi());
					em.setImei(x2.getX2Xdr().getCommXdr().getImei());
					em.setProcedureType(ParseUtils.parseInteger(x2.getX2Xdr().getProcedureType()));
					em.setEtype(x2.getEtype());
					em.setCellid(x2.getX2Xdr().getCellId());
					em.setTargetCellid(ParseUtils.parseLong(x2.getX2Xdr().getTargetCellId()));
					em.setInteface(x2.getX2Xdr().getCommXdr().getInterfaces());
					em.setEupordown(x2.getEupordown());
					em.setLonglat(lteMroSourceList, em);
					
					em.getWirelessException().setXdrId(x2.getX2Xdr().getCommXdr().getXdrid());
					Exception4GUtils hoUtils = new Exception4GUtils();
					em = hoUtils.hoWirelessAnalysis(x2.getX2Xdr(),lteMroSourceList, ltecellMap, cellXdrMap, g_nCellinfoMap,g_configPara, 
							x2.getX2Xdr().getTargetCellId(),String.valueOf(x2.getX2Xdr().getCellId()),em,bNeedWirelessCause);
//					em.setCellType(exceptionMap.get(em.getProInterface() +StringUtils.DELIMITER_COMMA+ em.getFailureCause()).getCellType());
					em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

					em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
					if ("UE".equals(em.getCellType())) {
						em.setCellKey(x2.getX2Xdr().getCommXdr().getImsi());
					} else if ("CELL".equals(em.getCellType())|| "目标CELL".equals(em.getCellType())) {
						em.setCellKey(String.valueOf(x2.getX2Xdr().getCellId()));
					} else if ("MME".equals(em.getCellType())) {
						em.setCellKey(x2.getX2Xdr().getMmeGroupId() + "_" + x2.getX2Xdr().getMmeCode());
					} else if ("源CELL".equals(em.getCellType())) {
						em.setCellKey(x2.getX2Xdr().getTargetCellId());
					}
					x2ExceptionMessageList.add(em);
				} 
			}
		}
		return x2ExceptionMessageList;
	}	

	public List<EventMesage> exception4GAnalyServiceS1(
			List<S1mmeXdrNew> s1mmeXdrList, List<UuXdrNew> uuList,
			Map<String, String> ltecellMap, Map<String, List<String>> cellXdrMap,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, ExceptionReson>  exceptionMap, Map<String,String> nCellinfoMap,Map<String, String> configPara) {
		
		g_configPara = configPara;
		g_nCellinfoMap = nCellinfoMap;
		boolean bNeedWirelessCause = false;
		
		List<EventMesage> s1mmeExceptionMessageList = new ArrayList<EventMesage>();
		// enter the interface S1
		if (s1mmeXdrList.size() > 0) {
			for (S1mmeXdrNew s1Xdr : s1mmeXdrList) {
				if ( ExceptionConstnts.FG_SWITCH_FAIL == s1Xdr.getEtype()) {
					EventMesage em=new EventMesage();
					String cellid="";
					String targetCellid="";
					String failcode="";
					if(s1Xdr.getS1mmeXdr().getProcedureType().equals("15")){ ///???Etype判断的时候限定仅为16
						cellid=s1Xdr.getS1mmeXdr().getOtherEci();
						targetCellid=s1Xdr.getS1mmeXdr().getCellId();
					}else{
						cellid=s1Xdr.getS1mmeXdr().getCellId();
						targetCellid=s1Xdr.getS1mmeXdr().getOtherEci();
					}
					int count=0;
					//步骤5：以interface=5（S1接口）的[开始时间-1s，结束时间+1s]为查询时间范围（Uu接口用rangetime查询），
					//查看该用户在该时段内interface=1（Uu接口），procedureType=8是否存在。如果是，输出CO4：源小区未接收到源MME下发的资源释放信令，流程结束；否则，输出CO1：failure cause失败原因，流程结束
					for (UuXdrNew uuXdr : uuList) {
						if (uuXdr.getUuXdr().getProcedureStartTime() >= s1Xdr.getS1mmeXdr().getProcedureStartTime() - 1000
								&& uuXdr.getUuXdr().getProcedureEndTime() <= s1Xdr.getS1mmeXdr().getProcedureEndTime() + 1000
								&& "8".equals(uuXdr.getUuXdr().getProcedureType())
								&& cellid.equals(uuXdr.getUuXdr().getCellId())&&"0".equals(uuXdr.getUuXdr().getProcedureStatus())) {
							count++;
							break;
						}
					}
					if(count>0){
						failcode="11"; //输出CO4：源小区未接收到源MME下发的资源释放信令
					}else {
						//否则，输出CO1：failure cause失败原因，流程结束
						failcode="01";
					}
					switch (failcode) {
					case "01":
						if(s1Xdr.getS1mmeXdr().getProcedureStatus().equals("255")){
							em.setFailureCause("接口超时");
						}else{
							em.setFailureCause(s1Xdr.getS1mmeXdr().getFailureCause());
						}
						em.setProInterface(InterfaceConstants.S1AP);
						break;
					case "02":
						em.setFailureCause("2004");
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
						em.setFailureCause("11");
						em.setProInterface(InterfaceConstants.UU);
						break;
					case "08":
						em.setFailureCause("12");
						em.setProInterface(InterfaceConstants.UU);
						break;
					case "09":
						em.setFailureCause("1001");
						em.setProInterface(InterfaceConstants.X2);
						break;
					case "10":
						em.setFailureCause("15");
						em.setProInterface(InterfaceConstants.UU);
						break;
					case "11":
						em.setFailureCause("2009");
						em.setProInterface(InterfaceConstants.S1AP);
						break;
					default:
						break;
					}
					em.setEventName("4G切换失败");
					em.setProcedureStarttime(s1Xdr.getS1mmeXdr().getProcedureStartTime());
					em.setImsi(s1Xdr.getS1mmeXdr().getCommXdr().getImsi());
					em.setImei(s1Xdr.getS1mmeXdr().getCommXdr().getImei());
				
					em.setProcedureType(ParseUtils.parseInteger(s1Xdr.getS1mmeXdr().getProcedureType()));
					em.setEtype(s1Xdr.getEtype());
					em.setTargetCellid(ParseUtils.parseLong(targetCellid));
					em.setCellid(ParseUtils.parseLong(cellid));
					em.setInteface(s1Xdr.getS1mmeXdr().getCommXdr().getInterfaces());
					em.setEupordown(s1Xdr.getEupordown());
					em.setLonglat(lteMroSourceList, em);
					
					em.getWirelessException().setXdrId(s1Xdr.getS1mmeXdr().getCommXdr().getXdrid());
					Exception4GUtils hoUtils = new Exception4GUtils();
					em = hoUtils.hoWirelessAnalysis(s1Xdr.getS1mmeXdr(),lteMroSourceList, ltecellMap, cellXdrMap, 
							g_nCellinfoMap,g_configPara, targetCellid,cellid,em,bNeedWirelessCause);
//					em.setCellType(exceptionMap.get(em.getProInterface() +StringUtils.DELIMITER_COMMA+ em.getFailureCause()).getCellType());
					em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));

					em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
					if ("UE".equals(em.getCellType())) {
						em.setCellKey(s1Xdr.getS1mmeXdr().getCommXdr().getImsi());
					} else if ("CELL".equals(em.getCellType())|| "目标CELL".equals(em.getCellType())) {
						em.setCellKey(cellid);
					} else if ("MME".equals(em.getCellType())) {
						em.setCellKey(s1Xdr.getS1mmeXdr().getMmeGroupId() + "_" + s1Xdr.getS1mmeXdr().getMmeCode());
					}
					s1mmeExceptionMessageList.add(em);
				} 
			}
		}
		return s1mmeExceptionMessageList;
	}
}