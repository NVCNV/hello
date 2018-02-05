package cn.com.dtmobile.hadoop.biz.exception.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import cn.com.dtmobile.hadoop.biz.exception.constants.InterfaceConstants;
import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.MathUtil;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class Exception4GUtils {

	//周期性RSRP（UE_MR）
	private List<LteMroSourceNew> cycleUeMroList = null ;	
	//事件性RSRP（UE_MR）
	private List<LteMroSourceNew> eventUeMroList = null ;	
	//所有中含目标小区同频小区（UE_MR）
	private List<LteMroSourceNew> adjSamFreqUeMroList = null ;
	//所有中含目标小区同频模三小区（UE_MR）
	private List<LteMroSourceNew> adjSamFreqMod3UeMroList = null ;
	//所有中含主小区同频模三小区（UE_MR）
	private List<LteMroSourceNew> adjSrcSamFreqMod3UeMroList = null ;
	private boolean bExistW02 = false; //是否存在模3干扰
	private int nWO3Count = 0; //重叠覆盖邻区个数
	//RIP（Cell_MR）
	private Double targetrip = null ; //目标小区RIP
	private Double rip = null ; //主小区RIP
	private Double upSinr = null ; //上行SINR（UE_MR）
	private Double phr = null ; //PHR（UE_MR）
	private Double evtRsrp = null; //事件性最近的主小区RSRP
	private Double targetRsrp = null; //事件性最近的目标小区RSRP
	private Double cylRsrp = null; //周期性最近的主小区RSRP
	private Double cylTargetRsrp = null; //周期性最近的目标小区RSRP
	private boolean bExistCycTargetCell = false; //周期性mr中是否存在事件性MR中的目标小区（即切换目标小区的频点、PCI相同）
	private Double targetCellPCI = null;
	private Double targetCellFreq = null;
	private Double cellPCI = null;
	private Double cellFreq = null;	
	private String cycleXdrid = null ;
	private String eventXdrid = null ;	
	private long procedureStartTime = 0;
	private Double primaryRSRP = null;
	private Double nRSRPAndPRSRPMinus = null;
	private Double nRSRPAndPRSRPMinuNum = null;
	private Double neighborCellNum = null;
	private Double nRSRPAndPRSRPKpi2 = null;
	private Double cycleRSRPKpi1 = null;
	private Map<String,String> g_nCellinfoMap = null;
	private String mrType = null;
	private String gTargetCellid = null;
	private String gCellid = null;
	private Double dbTLon = null;
	private Double dbTLat = null;
	private Double dbTHoriz = null;
	private Double dbSLon = null;
	private Double dbSLat = null;
	private Double dbSHoriz = null;
	private Double dbTgtRsrp = null;
	
	public EventMesage hoWirelessAnalysis(Object xdr, List<LteMroSourceNew> lteMroSourceList,
			Map<String, String> ltecellMap, Map<String, List<String>> cellXdrMap, Map<String,String> nCellinfoMap,
			Map<String, String> configPara, String targetCellid,String cellid,EventMesage em,boolean bSetFailCause){

		g_nCellinfoMap = nCellinfoMap;
		
		//获取配置信息
		getConfigInfo(configPara);
		
		//分析前的数据准备
		preHoWirelessAnalysis(xdr, lteMroSourceList, ltecellMap, cellXdrMap, targetCellid, cellid);
		
		boolean bStep11 = false;
		String failcode = "";
		
		//步骤1：若上述周期性RSRP和事件性RSRP两个UE_MR指标均无数据，则输出WO1（无UE_MR数据）。若存在则进入步骤2。
		if ( cycleUeMroList.isEmpty() && eventUeMroList.isEmpty() ){ //1
			failcode = "16"; //W01
		}
		//步骤2：取事件性RSRP XDR中的最近一条，判断是否满足KPI1（主小区RSRP）<=31（-110dbm，可配）。若是，则输出WO14（切换带弱覆盖），否则进入步骤3。
		else if ( evtRsrp != null && evtRsrp <= primaryRSRP ){ //2
			failcode = "24"; //WO14
		}
		//步骤3：取周期性RSRP XDR，对于每个邻区，判断其与主小区是否同频，若存在某个同频邻小区，其与主小区RSRP之差的绝对值<10db（可配），且二者PCI mod 3相等，
		//则输出WO2（存在模三干扰），否则，即不存在任何同频MOD3冲突的邻小区，则进入步骤4。
		else if ( bExistW02 ){ //3
			failcode = "17"; //WO2
		}
		//步骤4：根据周期性MR中得到所有同频邻小区RSRP XDR，统计与主小区RSRP之差的绝对值<=5db（可配）的同频邻区个数，若个数>=2（可配），则输出WO3（重叠覆盖）。否则，进入步骤5。
		else if ( nWO3Count >= neighborCellNum ){ //4
			failcode = "18"; //WO3
		}
		//步骤5：判断事件性RSRP中的最强邻区的RSRP XDR满足KPI2（邻小区RSRP）<或=36（-105dbm，可配），若是则输出WO21（目标小区下行弱覆盖），若否则进入步骤6。
		else if ( targetRsrp != null && targetRsrp <= nRSRPAndPRSRPKpi2 ){ //5
			failcode = "26"; //WO21
		}
		//步骤6：对比分析最近的周期性MR中是否存在事件性MR中的目标小区，若是则进入步骤7，否则进入步骤11。
		else if ( bExistCycTargetCell ){ //6
			boolean bStep10 = false;
			//步骤7：根据步骤6中目标小区分别在周期性MR和事件性MR中的RSRP值进行差值计算，若该差值的绝对值小于或等于5（可配）则进入步骤8，否则进入步骤10
			if ( targetRsrp != null && Math.abs(cylTargetRsrp-targetRsrp) <= nRSRPAndPRSRPMinuNum ){ //7
				//步骤8：取周期性RSRP XDR，对于每个小区，判断其与目标小区是否同频，若存在某个同频邻小区，其与目标小区RSRP之差的绝对值<10db（可配），且二者PCI mod 3相等，
				//则输出WO22（目标小区存在模三干扰），否则，即不存在任何同频MOD3冲突的小区对，则进入步骤9。
				boolean bExistWO22 = false;
				for(LteMroSourceNew mr3:adjSamFreqMod3UeMroList){
					if (cylTargetRsrp != null && Math.abs(cylTargetRsrp-mr3.getLtemrosource().getKpi2())<nRSRPAndPRSRPMinus ){
						bExistWO22 = true;
						break;
					}
				}
				if ( bExistWO22 ){ //8
					failcode = "27"; //WO22
				}
				//步骤9：根据周期性MR中得到所有同频邻小区RSRP XDR，统计与目标小区RSRP之差的绝对值<=5db（可配）的同频邻区个数，若个数>=2（可配），则输出WO23（目标小区重叠覆盖）。否则，进入步骤10。
				else{
					int nWO23Count = 0;
					for(LteMroSourceNew mr4:adjSamFreqUeMroList){
						if (cylTargetRsrp != null && Math.abs(cylTargetRsrp-mr4.getLtemrosource().getKpi2())<nRSRPAndPRSRPMinuNum ){
							nWO23Count = nWO23Count + 1;
						}
					}
					if ( nWO23Count >= neighborCellNum ){ //9
						failcode = "28"; //WO23
					}else{
						bStep10 = true;
					}
				}
			}else{ //接步骤7的否则步骤10
				bStep10 = true;
			}
			
			//步骤10：对比分析最近的事件性MR和周期性MR中的目标小区是否为最强邻小区，若是则进入步骤11，否则输出WO24（主小区缺失最强邻区）。
			if ( bStep10 ){ //10
				if ( cycleUeMroList.size()>0 && targetCellFreq != null 
					&& targetCellFreq.equals(cycleUeMroList.get(0).getLtemrosource().getKpi11())
					&& targetCellPCI != null && targetCellPCI.equals(cycleUeMroList.get(0).getLtemrosource().getKpi12()) ){
					bStep11 = true;
				}else{
					failcode = "25"; //WO24
				}
			}
		}else{ //11
			bStep11 = true;
		}
		
		//步骤11：分析目标小区RIP，若目标小区RIP>111（-115dbm），则输出WO25（目标小区上行干扰）。否则输出WO26（目标小区下行无线环境正常）
		if ( bStep11 ){ //11
			if ( targetrip != null && targetrip > 111){
				failcode = "15"; //WO25
			}else{
				failcode = "29"; //WO26
			}
		}
		
		em = SetWirelessException(xdr, failcode, em, bSetFailCause, ltecellMap);
		return em;
	}
	
	public boolean getConfigInfo(Map<String, String> configPara){
		//可配置参数
		primaryRSRP = Double.parseDouble(configPara.get("primaryRSRP")) ;
		nRSRPAndPRSRPMinus = Double.parseDouble(configPara.get("nRSRPAndPRSRPMinus")) ;
		nRSRPAndPRSRPMinuNum = Double.parseDouble(configPara.get("nRSRPAndPRSRPMinuNum"));
		neighborCellNum = Double.parseDouble(configPara.get("neighborCellNum")) ;
		nRSRPAndPRSRPKpi2 = Double.parseDouble(configPara.get("nRSRPAndPRSRPKpi2")) ;
		cycleRSRPKpi1 = Double.parseDouble(configPara.get("cycleRSRPKpi1")) ;
			
		//如果没有取到值，设为默认值，防止程序报错
		if(primaryRSRP == null){
			primaryRSRP = 31.0 ;
		}
		if(nRSRPAndPRSRPMinus == null){
			nRSRPAndPRSRPMinus = 10.0 ;
		}
		if(nRSRPAndPRSRPMinuNum == null){
			nRSRPAndPRSRPMinuNum = 5.0 ;
		}
		if(neighborCellNum == null){
			neighborCellNum = 2.0 ;
		}
		if(nRSRPAndPRSRPKpi2 == null){
			nRSRPAndPRSRPKpi2 = 36.0 ;
		}
		if(cycleRSRPKpi1 == null){
			cycleRSRPKpi1 = 31.0 ;
		}
		return true;
	}
	
	//分析前的数据准备
	public boolean preHoWirelessAnalysis(Object xdr, List<LteMroSourceNew> lteMroSourceList,
			Map<String, String> ltecellMap, Map<String, List<String>> cellXdrMap,
			String targetCellid,String cellid){
		procedureStartTime = this.getLongVal(xdr,"getProcedureStartTime"); //获取切换事件的开始事件
		if(lteMroSourceList.size()>0){ //对 Mro List进行排序
			Collections.sort(lteMroSourceList, new Comparator<LteMroSourceNew>() {
	
				@Override
				public int compare(LteMroSourceNew o1, LteMroSourceNew o2) {
					int result=(int)(Math.abs(o1.getLtemrosource().getMrtime()-procedureStartTime)-Math.abs(o2.getLtemrosource().getMrtime()-procedureStartTime));
					return result==0?0:(result>0?1:-1);
				}
			});
		}
		
		dbTLon = null;
		dbTLat = null;
		gTargetCellid = targetCellid;
		gCellid = cellid;
			
		//获取目标小区对应的频点、PCI
		targetCellPCI = null;
		targetCellFreq = null;
		String freq_pci = ltecellMap.get(targetCellid);
		if(StringUtils.isNotBlank(freq_pci)){
			String[] configs = freq_pci.split(StringUtils.DELIMITER_COMMA);
			targetCellFreq = Double.valueOf(configs[0]);
			targetCellPCI = Double.valueOf(configs[1]);
			dbTLat = Double.parseDouble(configs[3]);
			dbTLon = Double.parseDouble(configs[2]);
			dbTHoriz = Double.parseDouble(configs[4]);
		}
		
		//获取主小区对应的频点、PCI
		cellPCI = null;
		cellFreq = null;	
		String freq_pci2 = ltecellMap.get(cellid);
		if(StringUtils.isNotBlank(freq_pci2)){
			String[] configs = freq_pci2.split(StringUtils.DELIMITER_COMMA);
			cellFreq = Double.valueOf(configs[0]);
			cellPCI = Double.valueOf(configs[1]);
			dbSLat = Double.parseDouble(configs[3]);
			dbSLon = Double.parseDouble(configs[2]);
			dbSHoriz = Double.parseDouble(configs[4]);
		}
		
		//初始化成员变量
		cycleUeMroList = new ArrayList<LteMroSourceNew>();
		eventUeMroList = new ArrayList<LteMroSourceNew>();
		adjSamFreqUeMroList = new ArrayList<LteMroSourceNew>();
		adjSamFreqMod3UeMroList = new ArrayList<LteMroSourceNew>();
		adjSrcSamFreqMod3UeMroList = new ArrayList<LteMroSourceNew>();
		cycleXdrid = null ;
		eventXdrid = null ;		
		upSinr = null ; //上行SINR（UE_MR）
		phr = null ; //PHR（UE_MR）
		evtRsrp = null; //事件性最近的主小区RSRP
		targetRsrp = null; //事件性最近的目标小区RSRP
		cylRsrp = null; //周期性最近的主小区RSRP
		cylTargetRsrp = null; //周期性最近的目标小区RSRP
		targetrip = null ;
		rip = null;
		bExistW02 = false; //是否存在模3干扰
		nWO3Count = 0; //重叠覆盖邻区个数
		bExistCycTargetCell = false; //周期性mr中是否存在事件性MR中的目标小区（即切换目标小区的频点、PCI相同）
		mrType = null;
		
//		Long lMrTime = null;
//		Long lUpSinrTime = null;
//		Long lPhrTime = null;
//		Long lEvtTime = null;
//		Long lcycleTime = null;
		
		//从mro_sorce中取出ue_mr
		for(LteMroSourceNew mr:lteMroSourceList){
			boolean haveUemro = false;
			if(null != mr.getLtemrosource().getMrtime()){
				if ( Math.abs(procedureStartTime-mr.getLtemrosource().getMrtime().longValue()) > 30000){ //时间超过预期值，则退出，因为之前进行过排序
					break;
				}
				haveUemro = (procedureStartTime-30000) <= mr.getLtemrosource().getMrtime().longValue()
					&& (procedureStartTime+1000) >= mr.getLtemrosource().getMrtime().longValue()
					&& TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(mr.getLtemrosource().getMrname())
					&& cellid.equals(mr.getLtemrosource().getCellid().toString());
//					&& -1.0!=mr.getLtemrosource().getKpi1();
			}
			
			if(haveUemro){
				
				//Sinr，phr不论EventType
				if ( upSinr == null && -1.0!=mr.getLtemrosource().getKpi8() ){
					upSinr = mr.getLtemrosource().getKpi8();
				}
				if ( phr == null && -1.0!=mr.getLtemrosource().getKpi6() ){
					phr = mr.getLtemrosource().getKpi6();
				}

				//周期性RSRP
				if( "PERIOD".equalsIgnoreCase(mr.getLtemrosource().getEventtype()) && -1.0!=mr.getLtemrosource().getKpi1() ){
					if ( cycleUeMroList.isEmpty() ) { //取最近的一次周期性MR
						cycleXdrid = mr.getLtemrosource().getXdrid();
						cylRsrp = mr.getLtemrosource().getKpi1();
					}
				}
				//事件性RSRP	
				if ( ("A3".equalsIgnoreCase(mr.getLtemrosource().getEventtype()) 
					|| "A4".equalsIgnoreCase(mr.getLtemrosource().getEventtype())
					|| "A5".equalsIgnoreCase(mr.getLtemrosource().getEventtype()))
					&& -1.0!=mr.getLtemrosource().getKpi1() && -1.0!=mr.getLtemrosource().getKpi2() ){
					 //取最近的一次的频点和PCI与目标小区一致的事件性RSRP
					if ( eventUeMroList.isEmpty() 
						&& targetCellFreq != null &&  targetCellFreq.equals(mr.getLtemrosource().getKpi11())
						&& targetCellPCI != null && targetCellPCI.equals(mr.getLtemrosource().getKpi12()) ){
						eventXdrid = mr.getLtemrosource().getXdrid();
						evtRsrp = mr.getLtemrosource().getKpi1();
						targetRsrp = mr.getLtemrosource().getKpi2();
						mrType = mr.getLtemrosource().getEventtype();
						
						//必须在此处取出最近的事件性XDR，因为是根据目标小区进行判断，有可能该条XDR是某次事件上报的第2。。。条，如果在第一循环中取，可能会漏掉前面的。所以这里要第二次循环取出
						for(LteMroSourceNew mr2:lteMroSourceList){
							
							if(null != mr2.getLtemrosource().getXdrid() &&  null != eventXdrid && mr2.getLtemrosource().getXdrid().equals(eventXdrid)){
								eventUeMroList.add(mr2) ;
//								checkSameSrcFreqInfo(mr2); //判断与主小区同频小区的模三等信息
//								checkSameAdjFreqInfo(mr2, targetCellFreq, targetCellPCI); //判断与目标小区同频小区的模三等信息
							}
						}
					}
				}
				
				if(null != mr.getLtemrosource().getXdrid() && null != cycleXdrid && mr.getLtemrosource().getXdrid().equals(cycleXdrid)){
					cycleUeMroList.add(mr) ;			
					checkSameSrcFreqInfo(mr); //判断与主小区同频小区的模三等信息
					checkSameAdjFreqInfo(mr, targetCellFreq, targetCellPCI); //判断与目标小区同频小区的模三等信息
					
					//对比分析最近的周期性MR中是否存在事件性MR中的目标小区
					if ( targetCellFreq != null && targetCellFreq.equals(mr.getLtemrosource().getKpi11())
					&& targetCellPCI != null && targetCellPCI.equals(mr.getLtemrosource().getKpi12()) ){
						bExistCycTargetCell = true;
						cylTargetRsrp = mr.getLtemrosource().getKpi2();
					}
				}
			}
		}

		//  --已完成，计算rip方法有待验证
		if(cellXdrMap.size()>0){
//			targetrip = this.getAvgKpi(targetCellid, procedureStartTime, cellXdrMap) ;
//			rip = this.getAvgKpi(cellid, procedureStartTime, cellXdrMap);
			targetrip =  MathUtil.getAvgKpi(procedureStartTime,Long.parseLong(targetCellid), cellXdrMap) ;
			rip =MathUtil.getAvgKpi(procedureStartTime,Long.parseLong(cellid), cellXdrMap) ;
			
	    }		
		
		//按照kpi2倒序排列，取最强、次强、三强邻区
		if(cycleUeMroList.size()>=2){
			Collections.sort(cycleUeMroList,new Comparator<LteMroSourceNew>(){

				@Override
				public int compare(LteMroSourceNew mro0, LteMroSourceNew mro1) {
					
					if(mro0.getLtemrosource().getKpi2()-mro1.getLtemrosource().getKpi2() >0 ){
						return -1 ;
					}else if(mro0.getLtemrosource().getKpi2()-mro1.getLtemrosource().getKpi2() <0 ){
						return 1 ;
					}else{
						return 0 ;
					}
				}
			});
		}
		
		//如果事件性主小区RSRP不存在，则使用周期性主小区RSRP
		if(evtRsrp == null) evtRsrp = cylRsrp;
		
		return true;
	}
	
	public Double getEffDistance(String strCell, String strNcCell, Map<String, String> ltecellMap){
		Double dbDis = null;
		
		String seriveCell = ltecellMap.get(strCell) ;
		Double serviceAoa = null ;
		Double seriveLongTude = null ;
		Double seriveLatitude = null ;
		if(seriveCell != null){
			String[] splitCell = seriveCell.split(StringUtils.DELIMITER_COMMA) ;
			seriveLongTude = Double.parseDouble(splitCell[2]) ;
			seriveLatitude = Double.parseDouble(splitCell[3]) ;
			serviceAoa = Double.parseDouble(splitCell[4]) ;
		}
		  
		Double neighborAoa = null ;
		Double neighborLongTude = null ;
		Double neighborLatitude = null ;
		String neighborCell = ltecellMap.get(strNcCell) ;
		if(neighborCell != null){
			String[] neighborSplit = neighborCell.split(StringUtils.DELIMITER_COMMA) ;
			neighborLongTude =  Double.parseDouble(neighborSplit[2]) ;		  
			neighborLatitude =  Double.parseDouble(neighborSplit[3]);
			neighborAoa = Double.parseDouble(neighborSplit[4]) ;
		}
		
		if ( serviceAoa != null && neighborAoa != null){
			dbDis =  MathUtil.getEffDistance(serviceAoa,neighborAoa,seriveLongTude,seriveLatitude,neighborLongTude,neighborLatitude);
		}		
		return dbDis;
	}
	
	public EventMesage SetTargetCellInfo(EventMesage em, int nNIndex, String strNCellid, Double dbRsrp, Map<String, String> ltecellMap){
		
		//获取邻小区信息
		Double dbLat = null;
		Double dbLon = null;
		Double dbHoriz = null;
		if ( strNCellid != null ){
			String freq_pci = ltecellMap.get(strNCellid);
			if(StringUtils.isNotBlank(freq_pci)){
				String[] configs = freq_pci.split(StringUtils.DELIMITER_COMMA);
				dbLat = Double.parseDouble(configs[3]);
				dbLon = Double.parseDouble(configs[2]);
				dbHoriz = Double.parseDouble(configs[4]);
			}
		}
		
		
		
		
		if ( 0 == nNIndex ){
			em.getWirelessException().setTargetHighestCellRSRP(dbRsrp);
			if ( strNCellid != null ){
				em.getWirelessException().setTargetHighestCellId(Long.parseLong(strNCellid));
				if ( dbTLat != null && dbLat != null ){ //距离是计算与目标小区的
					em.getWirelessException().setTargetActualRange(MathUtil.cell_distance(dbLat, dbLon, dbTLat, dbTLon));
					em.getWirelessException().setTargetEquivalentDistance(MathUtil.getEffDistance(dbTHoriz, dbHoriz, dbTLon, dbTLat, dbLon, dbLat));
				}
			}
		}else if ( 1 == nNIndex ){
			if ( strNCellid != null ){
				em.getWirelessException().setTargetSecondCellId(Long.parseLong(strNCellid));
			}
			em.getWirelessException().setTargetSecondCellRSRP(dbRsrp);							
		}else if ( 2 == nNIndex ){
			if ( strNCellid != null ){
				em.getWirelessException().setTargetThirdCellId(Long.parseLong(strNCellid));
			}
			em.getWirelessException().setTargetThirdCellRSRP(dbRsrp);	
		}	
		return em;
	}
	
	public EventMesage SetWirelessException(Object xdr, String failcode, EventMesage em, boolean bSetFailCause,
			Map<String, String> ltecellMap){
		
		em.getWirelessException().setExceptionCode(failcode);
//		em.getWirelessException().setXdrId(cycleXdrid);
		em.getWirelessException().setCellRSRP(evtRsrp);
		em.getWirelessException().setRip(rip);
		em.getWirelessException().setPhr(phr);
		em.getWirelessException().setUpSinr(upSinr);
		em.getWirelessException().setMRType(mrType);
		if (cellPCI != null) em.getWirelessException().setPci(cellPCI.longValue());
		
		int nIndex = 0;
		int nNIndex = 0;
		boolean bCheckSrc = false; //判断是否比较了源小区
		if ( cycleUeMroList != null ){
			for(LteMroSourceNew mr:cycleUeMroList){
				if ( "0".equals(mr.getLtemrosource().getNeighborcellnumber()) ){ //表明没有邻区
					break;
				}else{
					String strNcInfo = MathUtil.getActualDistince(ltecellMap, mr, g_nCellinfoMap);
					String[] higestSplit = strNcInfo.split(StringUtils.DELIMITER_COMMA);
					if ( 0 == nIndex ){
						em.getWirelessException().setHighestCellRSRP(mr.getLtemrosource().getKpi2());						
						if (higestSplit.length > 1) {
							em.getWirelessException().setHighestCellId(Long.parseLong(higestSplit[0]));
							em.getWirelessException().setActualRange(Double.valueOf(higestSplit[1]));
	
							//set 等效距离
							Double equleDistice = getEffDistance(mr.getLtemrosource().getCellid().toString(), higestSplit[0], ltecellMap);
							if ( equleDistice != null){
								em.getWirelessException().setEquivalentDistance(equleDistice);
							}
						}
					}else if ( 1 == nIndex ){
						if (higestSplit.length > 1) {
							em.getWirelessException().setSecondCellid(Long.parseLong(higestSplit[0]));
						}
						em.getWirelessException().setSecondCellRSRP(mr.getLtemrosource().getKpi2());							
					}else if ( 2 == nIndex ){
						if (higestSplit.length > 1) {
							em.getWirelessException().setThirdCellId(Long.parseLong(higestSplit[0]));
						}
						em.getWirelessException().setThirdCellRSRP(mr.getLtemrosource().getKpi2());	
						//break; //只取前面三个邻区
					}	
					
					//目标小区前三强信息
					if ( !bCheckSrc && cylRsrp >= mr.getLtemrosource().getKpi2() ){ //说明主小区比较强，先排主小区
						bCheckSrc = true;
						em = SetTargetCellInfo(em, nNIndex, gCellid, cylRsrp, ltecellMap);
						nNIndex = nNIndex + 1;
					}
					boolean bAdd = false;
					if ((higestSplit.length > 1 && !gTargetCellid.equals(higestSplit[0])) 
						|| (targetCellPCI != null && targetCellPCI.equals(mr.getLtemrosource().getKpi12()) 
						&& targetCellFreq != null && targetCellFreq.equals(mr.getLtemrosource().getKpi11()) ) ) { //保证不是目标小区
						if (higestSplit.length > 1) {
							em = SetTargetCellInfo(em, nNIndex, higestSplit[0], mr.getLtemrosource().getKpi2(), ltecellMap);
						}else{
							em = SetTargetCellInfo(em, nNIndex, null, mr.getLtemrosource().getKpi2(), ltecellMap);
						}
						bAdd = true;
					}
					
					nIndex = nIndex+1;
					if ( bAdd ) nNIndex = nNIndex + 1; //保证不是目标小区
					if ( nIndex >= 3 && nNIndex >= 3 ) break; //只取前面三个邻区
				}
			}
		}
		
		if ( adjSrcSamFreqMod3UeMroList.size() >= 2 ){
			//按照数据与主小区或目标小区RSRP差的绝对值小->大排序，取小的
			Collections.sort(adjSrcSamFreqMod3UeMroList, new Comparator<LteMroSourceNew>() {				
				@Override
				public int compare(LteMroSourceNew o1, LteMroSourceNew o2) {
					int result=(int)(Math.abs(o1.getLtemrosource().getKpi1()-o1.getLtemrosource().getKpi2())-
							Math.abs(o2.getLtemrosource().getKpi1()-o2.getLtemrosource().getKpi2()));
					return result==0?0:(result>0?1:-1);
				}
			});
		}
		for(LteMroSourceNew mr2:adjSrcSamFreqMod3UeMroList){
			em.getWirelessException().setModelThreeNPci(mr2.getLtemrosource().getKpi12().longValue());
			em.getWirelessException().setModelThreeNRSRP(mr2.getLtemrosource().getKpi2());
			
			String strNcInfo = MathUtil.getActualDistince(ltecellMap, mr2, g_nCellinfoMap);
			String[] higestSplit = strNcInfo.split(StringUtils.DELIMITER_COMMA);
			if (higestSplit.length > 1) {
				em.getWirelessException().setModelThreeNCellId(Long.parseLong(higestSplit[0]));
				em.getWirelessException().setModelThreeActualRange(Double.valueOf(higestSplit[1]));

				//set 等效距离
				Double equleDistice = getEffDistance(mr2.getLtemrosource().getCellid().toString(), higestSplit[0], ltecellMap);
				if ( equleDistice != null){
					em.getWirelessException().setModelThreeEquivDistance(equleDistice);
				}	
			}
			break; //取差的绝对值最小的那个
		}
		
		if (targetCellPCI != null) em.getWirelessException().setTargetPci(targetCellPCI.longValue());
		em.getWirelessException().setTargetCellRip(targetrip);
		em.getWirelessException().setTargetCellRSRP(targetRsrp);

		dbTgtRsrp = cylTargetRsrp;
		if ( dbTgtRsrp != null ) {
			//目标小区模3干扰信息
			LteMroSourceNew mod3Mr = null;
			if ( adjSamFreqMod3UeMroList.size() >= 2 ){
				Collections.sort(adjSamFreqMod3UeMroList, new Comparator<LteMroSourceNew>() {				
					@Override
					public int compare(LteMroSourceNew o1, LteMroSourceNew o2) {
						int result=(int)(Math.abs(dbTgtRsrp-o1.getLtemrosource().getKpi2())-
								Math.abs(dbTgtRsrp-o2.getLtemrosource().getKpi2()));
						return result==0?0:(result>0?1:-1);
					}
				});			
			}
			if ( adjSamFreqMod3UeMroList.size() >= 1 ) mod3Mr = adjSamFreqMod3UeMroList.get(0);
			
			boolean bSource = false; //标识主小区是否是模三
			if ( cellFreq != null && targetCellFreq != null && targetCellFreq.equals(cellFreq) 
				&& targetCellPCI % 3 == (cellPCI % 3) ){//主小区与目标小区同频, 且模三
				if ( mod3Mr == null || ( mod3Mr != null && Math.abs(dbTgtRsrp-cylRsrp)<nRSRPAndPRSRPMinus &&
					Math.abs(dbTgtRsrp-cylRsrp) <= Math.abs(dbTgtRsrp-mod3Mr.getLtemrosource().getKpi2()) ) ){ //说明主小区干扰
					bSource = true;
					
					em.getWirelessException().setTargetModelThreeNCellId(Long.parseLong(gCellid));
					em.getWirelessException().setTargetModelThreeNPci(cellPCI.longValue());
					em.getWirelessException().setTargetModelThreeNRSRP(cylRsrp);
					
					if ( dbTLat != null && dbSLat != null ){ //距离是计算与目标小区的
						em.getWirelessException().setTargetModelThreeActualRange(MathUtil.cell_distance(dbSLat, dbSLon, dbTLat, dbTLon));
						em.getWirelessException().setTargetModelThreeEquivDistance(MathUtil.getEffDistance(dbTHoriz, dbSHoriz, dbTLon, dbTLat, dbSLon, dbSLat));
					}
				}
			}
			if ( !bSource && mod3Mr != null && Math.abs(dbTgtRsrp-mod3Mr.getLtemrosource().getKpi2())<nRSRPAndPRSRPMinus){
				
				em.getWirelessException().setTargetModelThreeNPci(mod3Mr.getLtemrosource().getKpi12().longValue());
				em.getWirelessException().setTargetModelThreeNRSRP(mod3Mr.getLtemrosource().getKpi2());

				String strNcInfo = MathUtil.getActualDistince(ltecellMap, mod3Mr, g_nCellinfoMap);
				String[] higestSplit = strNcInfo.split(StringUtils.DELIMITER_COMMA);
				if (higestSplit.length > 1) {
					em.getWirelessException().setTargetModelThreeNCellId(Long.parseLong(higestSplit[0]));
					
					//获取邻小区信息
					Double dbLat = null;
					Double dbLon = null;
					Double dbHoriz = null;
					String freq_pci = ltecellMap.get(higestSplit[0]);
					if(StringUtils.isNotBlank(freq_pci)){
						String[] configs = freq_pci.split(StringUtils.DELIMITER_COMMA);
						dbLat = Double.parseDouble(configs[3]);
						dbLon = Double.parseDouble(configs[2]);
						dbHoriz = Double.parseDouble(configs[4]);
					}
					if ( dbTLat != null && dbSLat != null ){ //距离是计算与目标小区的
						em.getWirelessException().setTargetModelThreeActualRange(MathUtil.cell_distance(dbLat, dbLon, dbTLat, dbTLon));
						em.getWirelessException().setTargetModelThreeEquivDistance(MathUtil.getEffDistance(dbTHoriz, dbHoriz, dbTLon, dbTLat, dbLon, dbLat));
					}				
				}			
			}
		}
	
		if ( bSetFailCause ){
			em.setFailureCause(failcode);
			em.setProInterface(InterfaceConstants.UU);
		}
		return em;
	}
	
	//判断与主小区同频小区的模三等信息
	public boolean checkSameSrcFreqInfo(LteMroSourceNew mr){
		
		if ( -1.0!=mr.getLtemrosource().getKpi2() && mr.getLtemrosource().getKpi9().equals(mr.getLtemrosource().getKpi11()) ){//同频小区
			//判断其与主小区是否同频，若存在某个同频邻小区，其与主小区RSRP之差的绝对值<10db（可配），且二者PCI mod 3相等
			if (Math.abs(mr.getLtemrosource().getKpi1()-mr.getLtemrosource().getKpi2())<nRSRPAndPRSRPMinus
			   && (mr.getLtemrosource().getKpi10() % 3) == (mr.getLtemrosource().getKpi12() % 3) ){
				adjSrcSamFreqMod3UeMroList.add(mr);
				bExistW02 = true;
			}
			//所有同频邻小区RSRP XDR，统计与主小区RSRP之差的绝对值<=5db（可配）的同频邻区个数，若个数>=2（可配）
			if (Math.abs(mr.getLtemrosource().getKpi1()-mr.getLtemrosource().getKpi2())<nRSRPAndPRSRPMinuNum){
				nWO3Count = nWO3Count + 1;
			}
		}
		return true;
	}
	
	//判断与目标小区同频小区的模三等信息
	public boolean checkSameAdjFreqInfo(LteMroSourceNew mr, Double targetFreq, Double targetPCI){
		
		if ( -1.0!=mr.getLtemrosource().getKpi2() &&
			targetFreq != null && targetFreq.equals(mr.getLtemrosource().getKpi11())
			&& targetPCI != null && !targetPCI.equals(mr.getLtemrosource().getKpi12() )){//与目标小区同频，不同的PCI,排除掉目标小区
			//判断其与目标小区是否同频，若存在某个同频邻小区，其与目标小区RSRP之差的绝对值<10db（可配），且二者PCI mod 3相等
			if ( targetPCI != null && targetPCI % 3 == (mr.getLtemrosource().getKpi12() % 3) ){
				adjSamFreqMod3UeMroList.add(mr);
			}
			//所有同频邻小区RSRP XDR，统计与目标小区RSRP之差的绝对值<=5db（可配）的同频邻区个数
			adjSamFreqUeMroList.add(mr);
		}
		return true;
	}	

	public long getLongVal(Object xdr, String methodName) {
		Method method;
		try {
			method = xdr.getClass().getMethod(methodName);
			return (Long) method.invoke(xdr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public String getStringVal(Object xdr, String methodName) {
		Method method;
		try {
			method = xdr.getClass().getMethod(methodName);
			return (String) method.invoke(xdr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}	
	
	public Double getAvgKpi(String targetCellid,long procedureStartTime, Map<String, String> cellXdrMap) {
		
		if(targetCellid == null || cellXdrMap == null){
			return null ;
		}
		
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Iterator<Entry<String,String>> iter = cellXdrMap.entrySet().iterator();
		Double kpiAvg = (double) 0;
		while (iter.hasNext()) {
			Entry<String,String> entry =iter.next();
			String[] value = entry.getValue().split(",");
			if((procedureStartTime- Long.valueOf(value[1]))<-20000 && (procedureStartTime- Long.valueOf(value[1])>1000)){
					continue;
				}
			if (entry.getKey().equals(targetCellid.toString())) {
				curTime = Math.abs(procedureStartTime - Long.valueOf(value[1]));
				if (minTime > curTime) {
					minTime = curTime;
					kpiAvg = Double.valueOf(value[1]);
				}
			}
		}
		return kpiAvg;
	}
	
	public static String getCellRegion(String cellType){
		String[] key1s=new String[]{"CELL","源CELL","目标CELL","邻区关系","ENB"};
		Set<String> set1s=new HashSet<String>();
		set1s.addAll(Arrays.asList(key1s));
		String[] key2s=new String[]{"MME","MSC"};
		Set<String> set2s=new HashSet<String>();
		set2s.addAll(Arrays.asList(key2s));
		
		if("UE".equals(cellType)){
			return "UE";
		}else if(set1s.contains(cellType)){
			return "EUTRAN域";
		}else if(set2s.contains(cellType)){
			return "EPC域";
		}else if("IMS".equals(cellType)){
			return "IMS域";
		}
		return "";
	}

}