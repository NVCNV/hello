package cn.com.dtmobile.hadoop.biz.exception.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.com.dtmobile.hadoop.util.StringUtils;
import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.util.MathUtil;

/**
 * @完成功能:
 * @创建时间:2017年7月17日 下午11:02
 * @param
 */
public class WirelessAnalyseService {
	/**
	 * 
	 * @param uu
	 * @param lteMroSourceList ue_mrosource
	 * @param cellXdrMap cell_mersource
	 * @param em 需要传入celldId ProcedureStarttime TargetCellid
	 * @param object
	 * @return
	 */
	// 周期性RSRP（UE_MR）
	private List<LteMroSourceNew> cycleUeMroList = null;
	// 事件性RSRP（UE_MR）
	private List<LteMroSourceNew> eventUeMroList = null;
	// 上行SINR（UE_MR）
	private Double upSinr = null;
	// PHR（UE_MR）
	private Double phr = null;
	// RIP（Cell_MR）
	private Double rip = null;
	// 小区表map
	private Map<String, String> ltecellMapNew;
	// 是否有事件性RSRP
	private boolean isHaveEvent = false;
	// UU接口切换XDR
	private List<UuXdrNew> uuXdrList = null;

	private Long eventMrTime = null;

	
	private String WT1 = null;
	private String WT2 = null;
	private String WT3 = null;
	private String WT4 = null;
	private String WT5 = null;
	private String WT6 = null;
	private String WT7 = null;
	private Double cellRSRP = null ;
	private Double higestCellRsrp = null ;
	private Long highestCellId = null ;
	private Long proceducerTime = null ; 
	private Integer interfaces= null ;
	public EventMesage wirelessAnalyse(Map<String, String> ltecellMap,
			Map<String, List<String>> cellXdrMap,
			Map<String, String> nCellinfoMap, Map<String, String> configPara,
			List<UuXdrNew> uUXDR, List<LteMroSourceNew> lteMroSourceList,
			EventMesage em) {
		cycleUeMroList = new ArrayList<LteMroSourceNew>();
		eventUeMroList = new ArrayList<LteMroSourceNew>();
		uuXdrList = new ArrayList<UuXdrNew>();
		ltecellMapNew = ltecellMap;
		Double primaryRSRP = null;
		Double nRSRPAndPRSRPMinus = null;
		Double nRSRPAndPRSRPMinuNum = null;
		Double neighborCellNum = null;
		Double nRSRPAndPRSRPKpi2 = null;
		Double cycleRSRPKpi1 = null;

		
		
		if (configPara != null) {
			// 可配置参数
			primaryRSRP = Double.parseDouble(configPara.get("primaryRSRP"));
			nRSRPAndPRSRPMinus = Double.parseDouble(configPara.get("nRSRPAndPRSRPMinus"));
			nRSRPAndPRSRPMinuNum = Double.parseDouble(configPara.get("nRSRPAndPRSRPMinuNum"));
			neighborCellNum = Double.parseDouble(configPara.get("neighborCellNum"));
			nRSRPAndPRSRPKpi2 = Double.parseDouble(configPara.get("nRSRPAndPRSRPKpi2"));
			cycleRSRPKpi1 = Double.parseDouble(configPara.get("cycleRSRPKpi1"));
		}
		// 如果没有取到值，设为默认值，防止程序报错
		if (primaryRSRP == null) {
			primaryRSRP = 31.0;
		}
		if (nRSRPAndPRSRPMinus == null) {
			nRSRPAndPRSRPMinus = 10.0;
		}
		if (nRSRPAndPRSRPMinuNum == null) {
			nRSRPAndPRSRPMinuNum = 5.0;
		}
		if (neighborCellNum == null) {
			neighborCellNum = 2.0;
		}
		if (nRSRPAndPRSRPKpi2 == null) {
			nRSRPAndPRSRPKpi2 = 36.0;
		}
		if (cycleRSRPKpi1 == null) {
			cycleRSRPKpi1 = 31.0;
		}
		
		if(em.getProcedureStarttime() != null){
			proceducerTime = em.getProcedureStarttime() ;
		}
	
		// 根据异常事件进行排序
		final Long procedureStartTime = proceducerTime;
		
		if (lteMroSourceList.size() > 0) { // 对 Mro List进行排序
			Collections.sort(lteMroSourceList,new Comparator<LteMroSourceNew>() {
		   @Override
			public int compare(LteMroSourceNew o1,LteMroSourceNew o2) {
				int result = (int) (Math.abs(o1.getLtemrosource().getMrtime() - procedureStartTime) - Math.abs(o2.getLtemrosource().getMrtime()- procedureStartTime));
				return result == 0 ? 0 : (result > 0 ? 1 : -1);
				}
					});
		}

		
		String eventXdrid = null;
		String cycleXdrid = null;
		boolean findevent = true;
		boolean findcycle = true;
		
		LteMroSourceNew oneNcellMr = null;
		
		String cellid = null ;
		// 从mro_sorce中取出ue_mr
		if(em.getCellid() == null){
			em.getWirelessException().setExceptionCode("16");
			return em ;
		}else{
			cellid = em.getCellid().toString() ;
		}
		
		
		
		if(em.getInteface() != null){
			interfaces = em.getInteface() ;
		}
		
		
		
	//无邻区标志
		boolean noNcell = true ;
		for (LteMroSourceNew mr : lteMroSourceList) {
			boolean haveUemro = false;
			if (null != proceducerTime && null != mr.getLtemrosource().getMrtime()) {
				
				if((interfaces !=null) && (interfaces == 14 ||  interfaces==26)){
				haveUemro = proceducerTime.longValue() - 30000 <= mr.getLtemrosource().getMrtime().longValue()
						&&  proceducerTime.longValue() + 10000 >= mr.getLtemrosource().getMrtime().longValue();
			    }else{
				haveUemro = proceducerTime.longValue() - 30000 <= mr.getLtemrosource().getMrtime().longValue()
						&&  proceducerTime.longValue() + 1000  >= mr.getLtemrosource().getMrtime().longValue()
						&&  cellid.equals(mr.getLtemrosource().getCellid().toString());
			    }
		    }
			

			if (haveUemro) {

				if (upSinr == null && -1.0 != mr.getLtemrosource().getKpi8()) {
					upSinr = mr.getLtemrosource().getKpi8();
				}
				if (phr == null && -1.0 != mr.getLtemrosource().getKpi6() ) {
					phr = mr.getLtemrosource().getKpi6();
				}

				// 周期性RSRP
				if (null != mr.getLtemrosource().getMrname() && mr.getLtemrosource().getMrname().equalsIgnoreCase("MR.LteScRSRP")) {
					//找到事件性Ue_Mr
					if (findevent) {
						if (("PERIOD".equalsIgnoreCase(mr.getLtemrosource().getEventtype()) || "NIL".equalsIgnoreCase(mr.getLtemrosource().getEventtype()) )
								&& mr.getLtemrosource().getKpi1() != -1.0
								&& mr.getLtemrosource().getCellid() != null
								&& mr.getLtemrosource().getCellid().equals(em.getCellid())) {
							if(cellRSRP == null || cellRSRP == -1.0){
							cellRSRP = mr.getLtemrosource().getKpi1() ;
						}
							if ("1".equals(mr.getLtemrosource().getNeighborcellnumber())) {
								// 设置标记，下方设置
								oneNcellMr = mr;
								cycleXdrid = mr.getLtemrosource().getXdrid() ;
							} else if (Long.parseLong(mr.getLtemrosource().getNeighborcellnumber()) > 1) {
								cycleXdrid = mr.getLtemrosource().getXdrid() ;
							}else{
								noNcell = false ;
								cycleXdrid = mr.getLtemrosource().getXdrid() ;
							}

						}

					}
			

				// 找到最近的一条，就不找了（已排序）
				if (cycleXdrid != null) {
					findevent = false;
				}
				
				//找到事件性Ue_Mr
				if (findcycle) {
					// 事件性RSRP
					boolean isHaveEventMr = ("A3".equals(mr.getLtemrosource().getEventtype())
							|| "A4".equals(mr.getLtemrosource().getEventtype())
							|| "A5".equals(mr.getLtemrosource().getEventtype()) 
							|| "B2".equals(mr.getLtemrosource().getEventtype()))
							&& mr.getLtemrosource().getKpi1() != -1.0
							&& cellid.equals(mr.getLtemrosource().getCellid().toString())
							&& Long.parseLong(mr.getLtemrosource().getNeighborcellnumber()) >= 1;

					if (isHaveEventMr) {
						eventXdrid = mr.getLtemrosource().getXdrid();
					}
				}
				
				// 找到最近的一条，就不找了（已排序）
				if (eventXdrid != null) {
					findcycle = false;
				 }
				
			}
		}
	}


		// 取出周期性和事件性RSRP
		for (LteMroSourceNew mr2 : lteMroSourceList) {

			if (null != mr2.getLtemrosource().getXdrid() && null != cycleXdrid && mr2.getLtemrosource().getXdrid().equals(cycleXdrid)) {
				cycleUeMroList.add(mr2);
			} else if (null != mr2.getLtemrosource().getXdrid() && null != eventXdrid && mr2.getLtemrosource().getXdrid().equals(eventXdrid)) {
				eventUeMroList.add(mr2);
			}
		}

	
		// 判断5秒内有没有 事件性rsrp isHaveEvent
		for (LteMroSourceNew event : eventUeMroList) {
			if (event.getLtemrosource().getMrtime() > proceducerTime - 5000 /*&& 	event.getLtemrosource().getMrtime() < event.getLtemrosource().getMrtime()*/ ) {
				isHaveEvent = true;
				eventMrTime = event.getLtemrosource().getMrtime();
				break;
			}
		}

		// UU接口uUXDR
		for (UuXdrNew uu : uUXDR) {
			boolean isUUXDR = uu.getUuXdr().getProcedureType() != null
					&& ("7".equals(uu.getUuXdr().getProcedureType()) || "8".equals(uu.getUuXdr().getProcedureType()) || "10".equals(uu.getUuXdr().getProcedureType()))
							&& null != uu.getUuXdr().getCellId()
							&& null != em.getCellid()
							&& uu.getUuXdr().getCellId().equals(em.getCellid().toString());

			if (isUUXDR) {
				uuXdrList.add(uu);
			}
		}

		// --已完成
		if (cellXdrMap.size() > 0) {
			rip = MathUtil.getAvgKpi(proceducerTime,em.getCellid(), cellXdrMap);
		}
		// 设置主小区pci
		Long cellPCI = null;
		String freq_pci2 = ltecellMap.get(cellid);
		if(StringUtils.isNotBlank(freq_pci2)){
			String[] configs = freq_pci2.split(StringUtils.DELIMITER_COMMA);
			cellPCI = Long.valueOf(configs[1]);
		}
		em.getWirelessException().setPci(cellPCI);
		
		// 子过程P1
		// 步骤1
		if (cycleUeMroList.isEmpty() && upSinr == null && phr == null) {
			em.getWirelessException().setExceptionCode("16");
			return em;
		} else {
			
			// 上行sinr
			em.getWirelessException().setUpSinr(upSinr);
			// phr
			em.getWirelessException().setPhr(phr);
			//rip
			em.getWirelessException().setRip(rip);
			
			// 步骤2
			if (cycleUeMroList.size() > 0) {

				// set主服务小区RSRP
//				cellRSRP = cycleUeMroList.get(0).getLtemrosource().getKpi1() ;
				em.getWirelessException().setCellRSRP(cellRSRP);
				
			if(noNcell){
				// 设置邻区为1的cellid和rsrp
				if (oneNcellMr != null) {
					String highestCell = MathUtil.getActualDistince(ltecellMapNew, oneNcellMr, nCellinfoMap);
					String[] higestSplit = highestCell.split(StringUtils.DELIMITER_COMMA);
					if (higestSplit.length > 1) {
						highestCellId = Long.parseLong(higestSplit[0]) ;
						em.getWirelessException().setHighestCellId(highestCellId);
					}
					higestCellRsrp = oneNcellMr.getLtemrosource().getKpi2() ;
					em.getWirelessException().setHighestCellRSRP(higestCellRsrp);
					
					
					String noNeighborcellDis = MathUtil.getActualDistince(ltecellMapNew, oneNcellMr, nCellinfoMap);
					Double cellIdAndDis = null;
					if (noNeighborcellDis != null) {
						String[] tmp = noNeighborcellDis.split(StringUtils.DELIMITER_COMMA);
						if (tmp.length > 1) {
							cellIdAndDis = Double.parseDouble(tmp[1]);
						}
					}

					// 设置最强邻区与主服务小区实际距离
					em.getWirelessException().setActualRange(cellIdAndDis);

					String strNcInfo = MathUtil.getActualDistince(ltecellMap, oneNcellMr, nCellinfoMap);
					String[] higestSplitin = strNcInfo.split(StringUtils.DELIMITER_COMMA);
					if(higestSplitin.length > 1){
						Double equleDistice = MathUtil.getEquivalentDistance(oneNcellMr.getLtemrosource().getCellid().toString(),higestSplit[0],ltecellMap) ;
						
						// set 等效距离
						em.getWirelessException().setEquivalentDistance(equleDistice);
					}	
					
					
				}

			
				// 按照kpi2倒序排列，取最强、次强、三强邻区
				if (cycleUeMroList.size() >= 2) {
					Collections.sort(cycleUeMroList,new Comparator<LteMroSourceNew>() {

								@Override
								public int compare(LteMroSourceNew mro0,LteMroSourceNew mro1) {

									if (mro0.getLtemrosource().getKpi2()- mro1.getLtemrosource().getKpi2() > 0) {
										return -1;
									} else if (mro0.getLtemrosource().getKpi2()- mro1.getLtemrosource().getKpi2() < 0) {
										return 1;
									} else {
										return 0;
									}
								}
							});

					LteMroSourceNew noNeighborcell = null;
					String highestCell = MathUtil.getActualDistince(ltecellMapNew, cycleUeMroList.get(0), nCellinfoMap);
					String[] higestSplit = highestCell.split(StringUtils.DELIMITER_COMMA);
					if (higestSplit.length > 1) {
						highestCellId = Long.parseLong(higestSplit[0]) ;
						em.getWirelessException().setHighestCellId(highestCellId);
					}

					noNeighborcell = cycleUeMroList.get(0);
					// 求主小区与最强小区的实际距离
					String noNeighborcellDis = MathUtil.getActualDistince(ltecellMapNew, noNeighborcell, nCellinfoMap);
					Double cellIdAndDis = null;
					if (noNeighborcellDis != null) {
						String[] tmp = noNeighborcellDis.split(StringUtils.DELIMITER_COMMA);
						if (tmp.length > 1) {
							cellIdAndDis = Double.parseDouble(tmp[1]);
						}
					}

					// 设置最强邻区与主服务小区实际距离
					em.getWirelessException().setActualRange(cellIdAndDis);

					String strNcInfo = MathUtil.getActualDistince(ltecellMap, noNeighborcell, nCellinfoMap);
					String[] higestSplitin = strNcInfo.split(StringUtils.DELIMITER_COMMA);
					if(higestSplitin.length > 1){
						Double equleDistice = MathUtil.getEquivalentDistance(noNeighborcell.getLtemrosource().getCellid().toString(),higestSplit[0],ltecellMap) ;
						
						// set 等效距离
						em.getWirelessException().setEquivalentDistance(equleDistice);
					}
					
					

					String secondCell = MathUtil.getActualDistince(ltecellMapNew, cycleUeMroList.get(1), nCellinfoMap);
					String[] secondSplit = secondCell.split(StringUtils.DELIMITER_COMMA);
					if (secondSplit.length > 1) {
						em.getWirelessException().setSecondCellid(Long.parseLong(secondSplit[0]));
					}
					higestCellRsrp = cycleUeMroList.get(0).getLtemrosource().getKpi2() ;
					em.getWirelessException().setHighestCellRSRP(higestCellRsrp);
					em.getWirelessException().setSecondCellRSRP(cycleUeMroList.get(1).getLtemrosource().getKpi2());

					// 如果邻区个数超过2个，才设置三强邻区
					if (cycleUeMroList.size() > 2) {
						String thirdCell = MathUtil.getActualDistince(ltecellMapNew, cycleUeMroList.get(2),nCellinfoMap);
						String[] thirdSplit = thirdCell.split(StringUtils.DELIMITER_COMMA);
						if (thirdSplit.length > 1) {
							em.getWirelessException().setThirdCellId(Long.parseLong(thirdSplit[0]));
						}

						em.getWirelessException().setThirdCellRSRP(cycleUeMroList.get(2).getLtemrosource().getKpi2()==-1.0?null:cycleUeMroList.get(2).getLtemrosource().getKpi2());
					}
				}
			}
		
				// 步骤3
				if ( cycleUeMroList.get(0).getLtemrosource().getKpi1() != -1.0  && cycleUeMroList.get(0).getLtemrosource().getKpi1() <= primaryRSRP) {
					// 步骤6
					boolean isLess36 = false;
					
				//判断是否具有邻小区，false为无邻区
				if(noNcell){
					  for (LteMroSourceNew lte : cycleUeMroList) {
						 if (lte.getLtemrosource().getKpi2() !=null && lte.getLtemrosource().getKpi2() != -1.0) {
							// 步骤7
							if (higestCellRsrp !=-1.0 &&  higestCellRsrp < nRSRPAndPRSRPKpi2) {
								em.getWirelessException().setExceptionCode("1");
								return em;

							} else {
								// 步骤8
								isLess36 = true;
								break;
							}
						} else {
							em.getWirelessException().setExceptionCode("1");
							return em;
						}
					}
					
				}else{
					em.getWirelessException().setExceptionCode("1");
					return em;
				}
					
					// 步骤8
					boolean isConfNCell = false;
					if (isLess36) {
						Long cellId = cycleUeMroList.get(0).getLtemrosource().getCellid();

						if (cellId == null || highestCellId == null) {
							em.getWirelessException().setExceptionCode("11");
							return em;
						}

						
						Iterator<Entry<String, String>> iter = nCellinfoMap.entrySet().iterator();
						while (iter.hasNext()) {
							Entry<String, String> entry = iter.next();
							if (Long.parseLong(entry.getKey()) == cellId && Long.parseLong(entry.getValue()) == highestCellId) {
								isConfNCell = true;
								break;
							}
						}

						
						if (isConfNCell) {
							// 步骤9
							if (isHaveEvent) {
								// 步骤10
								boolean isIn = false ;
								UuXdrNew uuIn = null ;
							
								
								if (uuXdrList.size() > 0) {
									for (UuXdrNew uu : uuXdrList) {
										 isIn = (eventMrTime < uu.getUuXdr().getProcedureStartTime() && uu.getUuXdr().getProcedureStartTime() < proceducerTime)
												|| (proceducerTime < uu.getUuXdr().getProcedureStartTime() && uu.getUuXdr().getProcedureStartTime() < eventMrTime);
										if(isIn){
											uuIn = uu ;
											break  ;
										}
									}
									
									if (uuIn != null) {
										if ("0".equals(uuIn.getUuXdr().getProcedureStatus())) {
											em.getWirelessException().setExceptionCode("100");
											return em;
										} else {
											em.getWirelessException().setExceptionCode("24");
											return em;
										}

									 } else {
										em.getWirelessException().setExceptionCode("23");
										return em;
									}
									
								}else{
									em.getWirelessException().setExceptionCode("23");
									return em;
								}
									
								
							} else {
								// 步骤12
								if (cellRSRP !=-1.0 && cellRSRP <= cycleRSRPKpi1) {
									WT3 = "下行弱覆盖";
									this.stepTwo(em);
								} else {
									WT2 = "下行覆盖正常";
									this.stepTwo(em);
								}
							}

						} else {
							em.getWirelessException().setExceptionCode("11");
							return em;
						}

					}

				} else {
					// 步骤4
					Double kpi2 = Double.MIN_VALUE;
					int sameFreqCellNum = 0;
					boolean notHasMaxRsrp = false;
					LteMroSourceNew notHasMaxRsrpLte = null;
					for (LteMroSourceNew lte : cycleUeMroList) {
						boolean isHaveNeibor =  lte.getLtemrosource().getKpi2() != -1.0 &&
												lte.getLtemrosource().getKpi9().equals(lte.getLtemrosource().getKpi11()) &&
								                Math.abs(lte.getLtemrosource().getKpi1()- lte.getLtemrosource().getKpi2()) < nRSRPAndPRSRPMinus
								             && (lte.getLtemrosource().getKpi10() % 3) == (lte.getLtemrosource().getKpi12() % 3);
						if (isHaveNeibor) {
//							// 可能存在多条，取RSRP最大的一条
							if (lte.getLtemrosource().getKpi2() != null && lte.getLtemrosource().getKpi2() > kpi2) {
								// 找到最大RSRP
								kpi2 = lte.getLtemrosource().getKpi2();
							} else {
//								// #123
								notHasMaxRsrp = true;
								notHasMaxRsrpLte = lte;
								break;
							}

						} else {
							// 步骤5
							// 步骤6
							boolean isHave = lte.getLtemrosource().getKpi2()  != -1.0 &&
											 lte.getLtemrosource().getKpi9().equals(lte.getLtemrosource().getKpi11())
									&& Math.abs(lte.getLtemrosource().getKpi1()- lte.getLtemrosource().getKpi2()) < nRSRPAndPRSRPMinuNum;
							if (isHave) {
								sameFreqCellNum = sameFreqCellNum +1 ;
							}

						}

					}

					// #123
					if (notHasMaxRsrp) {
						em.getWirelessException().setExceptionCode("17");

						String mod3 = MathUtil.getActualDistince(ltecellMapNew,notHasMaxRsrpLte, nCellinfoMap);
						Double mod3Dis = null;
						if (mod3 != null) {
							String[] tmp = mod3.split(StringUtils.DELIMITER_COMMA);
							if (tmp.length > 1) {
								mod3Dis = Double.parseDouble(tmp[1]);
							}
						}

						em.getWirelessException().setModelThreeActualRange(mod3Dis);

						String strNcInfo = MathUtil.getActualDistince(ltecellMap, notHasMaxRsrpLte, nCellinfoMap);
						String[] higestSplitin = strNcInfo.split(StringUtils.DELIMITER_COMMA);
						if(higestSplitin.length > 1){
							Double equleDistice = MathUtil.getEquivalentDistance(notHasMaxRsrpLte.getLtemrosource().getCellid().toString(),higestSplitin[0],ltecellMap) ;
							
							// set 等效距离
							em.getWirelessException().setModelThreeEquivDistance(equleDistice);
						}	
						
								
						String neighborCellId = this.getCellId(notHasMaxRsrpLte.getLtemrosource().getCellid(),
								notHasMaxRsrpLte.getLtemrosource().getKpi12().intValue(), notHasMaxRsrpLte.getLtemrosource()
								.getKpi11().intValue(), nCellinfoMap);
						
						
						// 主小区模3干扰邻小区CellID
						if (StringUtils.isNotBlank(neighborCellId)) {
							em.getWirelessException().setModelThreeNCellId(Long.parseLong(neighborCellId));
						}//
							// 主小区模3干扰邻小区PCI
						em.getWirelessException().setModelThreeNPci(notHasMaxRsrpLte.getLtemrosource().getKpi10() == -1.0 ? null: notHasMaxRsrpLte.getLtemrosource().getKpi10().longValue());
						// 主小区模3干扰邻小区RSRP
						em.getWirelessException().setModelThreeNRSRP(notHasMaxRsrpLte.getLtemrosource().getKpi2() == -1.0 ? null:notHasMaxRsrpLte.getLtemrosource().getKpi2() );

						
						return em;

					}
					// 步骤5-2
					if (sameFreqCellNum >= neighborCellNum) {
						em.getWirelessException().setExceptionCode("18");
						return em;
					} else {
						// 步骤12
						if (cycleUeMroList.get(0).getLtemrosource().getKpi1() != -1.0 && cycleUeMroList.get(0).getLtemrosource().getKpi1() <= cycleRSRPKpi1) {
							WT3 = "下行弱覆盖";
							this.stepTwo(em);
						} else {
							WT2 = "下行覆盖正常";
							this.stepTwo(em);
						}
					}
				}

			} else {
				// 步骤1，否则
				WT1 = "缺失RSRP";
				this.stepTwo(em) ;
			}
		}

		if(WT3 != null && WT4 != null && WT5 !=null){
			em.getWirelessException().setExceptionCode("19");
		}
		if(WT1 != null && WT4 != null && WT7 != null){
			em.getWirelessException().setExceptionCode("20");
		}
		if(WT2 != null && WT4 != null && WT6 != null){
			em.getWirelessException().setExceptionCode("21");
		}
		if(WT2 != null && WT4 != null && WT7 != null){
			em.getWirelessException().setExceptionCode("21");
		}
		if(WT3 != null && WT4 != null && WT6 != null){
			em.getWirelessException().setExceptionCode("22");
		}
		if(WT3 != null && WT4 != null && WT7 != null){
			em.getWirelessException().setExceptionCode("22");
		}
		if(WT1 != null && WT4 != null && WT5 != null){
			em.getWirelessException().setExceptionCode("30");
		}
		if(WT1 != null && WT4 != null && WT6 != null){
			em.getWirelessException().setExceptionCode("16");
		}
		if(WT2 != null && WT4 != null && WT5 != null){
			em.getWirelessException().setExceptionCode("100");
		}
		
		return em;

	}

	

	public EventMesage stepTwo(EventMesage em) {
		
		// 步骤20
		if (rip != null) {
			// 步骤21
			if (rip > 111) {
				em.getWirelessException().setExceptionCode("2");
				return em;
			} else {
				WT4 = "RIP正常";
				this.stepThree(em);
			}

		} else {
			WT4 = "RIP正常";
			this.stepThree(em) ;
		}

		return em;
	}

	// 子过程P3


	public EventMesage stepThree(EventMesage em) {
		// 步骤30
		if (null == phr && null == upSinr) {
			WT6 = "缺失上行干扰数据";
		} else {
			// 步骤31
			if (phr != null) {
				// 步骤32
				if (phr < 25) {
					// 步骤33
					if (upSinr != null) {
						// 步骤34
						if (upSinr >= 22) {
							em.getWirelessException().setExceptionCode("3");
							return em;
						} else {
							em.getWirelessException().setExceptionCode("4");
							return em;
						}

					} else {
						em.getWirelessException().setExceptionCode("3");
						return em;
					}
				} else {
					WT5 = "上行覆盖正常";
				}

			} else {
				// 步骤35
				if (upSinr != null && upSinr >= 22) {
					WT7 = "上行SINR正常，缺失PHR数据";
				
				} else {
					em.getWirelessException().setExceptionCode("4");
					return em;
				}
			}
		}

		return em;
	}

	// //根据pci和频点得到小区cellid ltecellMapNew
	public String getCellId(Long cellid, int pci, int freq,Map<String, String> nCellinfoMap) {

		List<String> nerberCell = new ArrayList<String>();
		Iterator<Entry<String, String>> iter = nCellinfoMap.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			if (entry.getKey().equals(cellid.toString())) {
				nerberCell.add(entry.getValue());
			}

		}

		String ncellCellId = null;
		for (String cell : nerberCell) {
			String ncell = ltecellMapNew.get(cell);
			if (ncell != null) {
				String[] tmp = ncell.split(StringUtils.DELIMITER_COMMA);
				if (pci == Integer.parseInt(tmp[1])
						&& freq == Integer.parseInt(tmp[0])) {
					ncellCellId = cell;
				}
			}

		}

		return ncellCellId == null ? " " : ncellCellId;
	}

}
