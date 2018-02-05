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
import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class VolteExceptionCalledAnalyService {

	/**
	 * VOLTE未接通分析流程（被叫�?zhangchao
	 * 
	 * @return
	 * @version 2016-11-11
	 */
	public EventMesage getEventMesage(MwNew mwXdr, S1mmeXdrNew s1mme,
			String failcode, Map<String, String> lteGMMap,
			Map<String, ExceptionReson> exceptionMap,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, List<UuXdrNew> uUXDR,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {
		EventMesage em = new EventMesage();
		switch (failcode) {
		case "01":
			em.setFailureCause("6");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "02":
			em.setFailureCause("7");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "03":
			em.setFailureCause("8");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "04":
			em.setFailureCause("9");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "05":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "06":
			em.setFailureCause(mwXdr.getMwXdr().getResponse_code());
			em.setProInterface(InterfaceConstants.SIP);
			break;
		case "07":
			if (s1mme != null) {
				if (s1mme.getS1mmeXdr().getProcedureStatus().equals("255")) {
					em.setFailureCause("接口超时");
				} else {
					em.setFailureCause(s1mme.getS1mmeXdr().getFailureCause());
				}
			}

			em.setProInterface(InterfaceConstants.NAS);
			break;
		case "08":
			if (s1mme != null) {
			if (s1mme.getS1mmeXdr().getProcedureStatus().equals("255")) {
				em.setFailureCause("接口超时");
			} else {
				em.setFailureCause(s1mme.getS1mmeXdr().getFailureCause());
			}
			}
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		case "09":
			em.setFailureCause("1");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "10":
			em.setFailureCause("2");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "11":
			em.setFailureCause("3");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "12":
			em.setFailureCause("4");
			em.setProInterface(InterfaceConstants.UU);
			break;
		case "13":
			em.setFailureCause("2006");
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		case "14":
			em.setFailureCause("2005");
			em.setProInterface(InterfaceConstants.S1AP);
			break;
		default:
			break;
		}
		em.setEtype(mwXdr.getEtype());
		em.setCellType(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause())==null?null:(exceptionMap.get(em.getProInterface()+ StringUtils.DELIMITER_COMMA + em.getFailureCause()).getCellType()));
		em.setCellRegion(Exception4GUtils.getCellRegion(em.getCellType()));
		if (em.getEtype() == 4) {
			em.setEventName("VOLTE语音未接通(被叫)");
		} else {
			em.setEventName("VOLTE视频未接通(被叫)");
		}

		em.setProcedureStarttime(mwXdr.getMwXdr()
				.getProcedurestarttime());
		em.setImsi((mwXdr).getMwXdr().getImsi());
		em.setImei((mwXdr).getMwXdr().getImei());
		em.setProcedureType(ParseUtils.parseInteger(mwXdr.getMwXdr().getProceduretype()));
		
		em.setInteface(mwXdr.getMwXdr().getInterfaces());
		em.setEupordown(mwXdr.getEupordown());
		em.getWirelessException().setXdrId(mwXdr.getMwXdr().getXdrid());

		if (failcode.equals("07") || failcode.equals("08")) {

//			if ("1".equals(s1mme.getS1mmeXdr().getCellId())) {
//				em.setCellid(ParseUtils.parseLong(s1mme.getS1mmeXdr()
//						.getCellId()));
//			} else if ("2".equals(s1mme.getS1mmeXdr().getCellId())) {
//				em.setCellid(ParseUtils.parseLong(s1mme.getS1mmeXdr()
//						.getCellId()));
//			}
			em.setCellid(ParseUtils.parseLong(s1mme.getS1mmeXdr().getCellId()));
			if ("UE".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(em.getImsi());
			} else if ("CELL".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(s1mme.getS1mmeXdr().getCellId());
				/*
				 * } else if ("IMS".toUpperCase().equals(em.getCellType())) {
				 * em.setCellKey((mwXdr).getMwXdr().getSource_ne_ip());
				 */
			} else if ("MME".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(lteGMMap.get(failcode));
			}

		} else {

			if ("1".equals(mwXdr.getMwXdr().getCall_side())) {
				em.setCellid(ParseUtils.parseLong(mwXdr.getMwXdr()
						.getSource_eci()));
			} else if ("2".equals(mwXdr.getMwXdr().getCall_side())) {
				em.setCellid(ParseUtils.parseLong(mwXdr.getMwXdr()
						.getDest_eci()));
			}

			if ("UE".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(em.getImsi());
			} else if ("CELL".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(mwXdr.getMwXdr().getDest_eci());
			} else if ("IMS".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(mwXdr.getMwXdr().getDest_ne_ip());
			} else if ("MME".toUpperCase().equals(em.getCellType())) {
				em.setCellKey(lteGMMap.get(failcode));
			}

		}

		// em.setCellid(ParseUtils.parseLong(((MwNew)
		// mwXdr).getMwXdr().getSource_eci()));
		// em.setRangetime((mwXdr).getMwXdr().getRangeTime());
		// em.setElong(mwXdr.getElong());
		// em.setElat(mwXdr.getElat());

		em.setLonglat(lteMroSourceList, em);
		WirelessAnalyseService wirelessanalyse = new WirelessAnalyseService();
		return wirelessanalyse.wirelessAnalyse(lteCellMap, cellXdrMap,
				nCellinfoMap, configPara, uUXDR, lteMroSourceList, em);

	}

	Map<String, String> lteGMMap = new HashMap<String, String>();

	public List<EventMesage> volteNoConnCalledService(List<MwNew> mwXdrList,
			List<S1mmeXdrNew> s1mmeXdrList, List<UuXdrNew> uuXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {
		List<EventMesage> mwExceptionMessageList = new ArrayList<EventMesage>();
		// MwExceptionMessage mwExceptionMessage = null;

		EventMesage eventMesage = new EventMesage();

		List<MwNew> iscMgMwXdrList = new ArrayList<MwNew>();
		if (mwXdrList.size() > 0) {
			for (MwNew mwXdr : mwXdrList) {
				if (mwXdr.getMwXdr().getInterfaces() == 14
						|| 15 == mwXdr.getMwXdr().getInterfaces()
						|| 18 == mwXdr.getMwXdr().getInterfaces()) {
					iscMgMwXdrList.add(mwXdr);
				}

			}
			for (MwNew mwXdr : mwXdrList) {
				if (mwXdr.getMwXdr().getInterfaces() == 14
						&& AttributeConstnts.MW_CALLSIDE_DEST.equals(mwXdr
								.getMwXdr().getCall_side())
						&& (ExceptionConstnts.VOLET_VOICE_CALL_FAIL == mwXdr
								.getEtype() || ExceptionConstnts.VOLET_VIEDO_CALL_FAIL == mwXdr
								.getEtype())) {
					eventMesage = this._volteNoConnCalledAnalyOne(mwXdr,
							s1mmeXdrList, uuXdrList, iscMgMwXdrList,
							lteMroSourceList, cellXdrMap, exceptionMap,
							lteCellMap, nCellinfoMap, configPara);
					if (eventMesage != null) {
						mwExceptionMessageList.add(eventMesage);
						continue;
					}
				}
			}
		}
		return mwExceptionMessageList;
	}

	/**
	 * \u5931\u8d25\u89e3\u6790
	 * 
	 * @param mwXdr
	 *            mw\u63a5\u53e3
	 * @param s1mmeXdrList
	 *            s1mme\u63a5\u53e3
	 * @param uuXdrList
	 *            uu\u63a5\u53e3
	 * @param exceptionMap
	 * @author zhangchao15
	 * @version 2016-11-11
	 * @return
	 */
	private EventMesage _volteNoConnCalledAnalyOne(MwNew mwXdr,
			List<S1mmeXdrNew> s1mmeXdrList, List<UuXdrNew> uuXdrList,
			List<MwNew> iscMgXdrList, List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {
		// \u6b65\u9aa41
		if ("487".equals(mwXdr.getMwXdr().getResponse_code())) {
			boolean isExist13 = false;
			boolean isExist5 = false;
			// S1mmeXdrNew s1mme13Xdr = null;
			for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
				// \u6b65\u9aa42
				if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr
						.getMwXdr().getProcedurestarttime() - 2000
						&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr
								.getMwXdr().getProcedureendtime() + 2000) {
					if ("13".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())) {
						isExist13 = true;
					} else if ("5".equals(s1mmeXdr.getS1mmeXdr()
							.getProcedureType())) {
						isExist5 = true;
					}

				}
			}

			if (isExist13 == true) { // 步驟8
				return this._volteNoConnCalledAnalyEight(mwXdr, uuXdrList,
						iscMgXdrList, s1mmeXdrList, lteMroSourceList,
						cellXdrMap, exceptionMap, lteCellMap, nCellinfoMap,
						configPara);
			} else {
				if (isExist5 == true) { // Outupt O1
					return this.getEventMesage(mwXdr,null,"01", lteGMMap,
							exceptionMap, lteMroSourceList, cellXdrMap,
							uuXdrList, lteCellMap, nCellinfoMap, configPara);
					// new EventMesage(mwXdr, "6",InterfaceConstants.UU,
					// exceptionMap, lteCellMap);

				} else { // 步驟4

					boolean isExistUu = false;
					for (UuXdrNew uuXdr : uuXdrList) {
						// \u6b65\u9aa42
						if (uuXdr.getUuXdr().getProcedureStartTime() >= mwXdr
								.getMwXdr().getProcedurestarttime() - 10000
								&& uuXdr.getUuXdr().getProcedureStartTime() <= mwXdr
										.getMwXdr().getProcedureendtime() + 10000) {
							isExistUu = true;
							break;
						}
					}

					if (isExistUu == true) {
						return this._volteNoConnCalledAnalyFifth(mwXdr,
								uuXdrList, lteMroSourceList, cellXdrMap,
								exceptionMap, lteCellMap, nCellinfoMap,
								configPara);
					} else {
						return this._volteNoConnCalledAnalySeven(mwXdr,
								uuXdrList, iscMgXdrList, s1mmeXdrList,
								lteMroSourceList, cellXdrMap, exceptionMap,
								lteCellMap, nCellinfoMap, configPara);
					}
				}
			}

		} else {
			return this._volteNoConnCalledAnalySeven(mwXdr, uuXdrList,
					iscMgXdrList, s1mmeXdrList, lteMroSourceList, cellXdrMap,
					exceptionMap, lteCellMap, nCellinfoMap, configPara);
		}

	}

	private EventMesage _volteNoConnCalledAnalyFifth(MwNew mwXdr,
			List<UuXdrNew> uuXdrList, List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {

		List<UuXdrNew> beforeUuList = new ArrayList<UuXdrNew>();
		List<UuXdrNew> afterUuList = new ArrayList<UuXdrNew>();
		UuXdrNew firstUuXdr = null;
		UuXdrNew lastUuXdr = null;

		// Mw開始之前最後一個，與結束之後第一個進行比較
		for (UuXdrNew uuXdr : uuXdrList) {
			if (uuXdr.getUuXdr().getProcedureStartTime() >= mwXdr.getMwXdr()
					.getProcedurestarttime() - 1000
					&& uuXdr.getUuXdr().getProcedureEndTime() < mwXdr
							.getMwXdr().getProcedurestarttime()) {
				beforeUuList.add(uuXdr);
			}
			if (uuXdr.getUuXdr().getProcedureStartTime() >= mwXdr.getMwXdr()
					.getProcedureendtime()
					&& uuXdr.getUuXdr().getProcedureEndTime() <= mwXdr
							.getMwXdr().getProcedureendtime() + 10000) {
				afterUuList.add(uuXdr);
			}
		}

		lastUuXdr = getMinUu(mwXdr.getMwXdr().getProcedureendtime(),
				beforeUuList);
		firstUuXdr = getMinUu(mwXdr.getMwXdr().getProcedureendtime(),
				afterUuList);

		if ((uuXdrList.size() == 1)
				|| (firstUuXdr != null && lastUuXdr != null && firstUuXdr
						.getUuXdr().getCellId()
						.equals(lastUuXdr.getUuXdr().getCellId()))) {
			return this.getEventMesage(mwXdr,null,"02", lteGMMap, exceptionMap,
					lteMroSourceList, cellXdrMap, uuXdrList, lteCellMap,
					nCellinfoMap, configPara);
			// new EventMesage(mwXdr, "7", InterfaceConstants.UU, exceptionMap,
			// lteCellMap);
		} else {
			List<LteMroSourceNew> ueMrList = new ArrayList<LteMroSourceNew>();
			for (LteMroSourceNew lteMroSource : lteMroSourceList) {
				if (TablesConstants.TABLE_UE_MR_XDR_FLAG.equals(lteMroSource
						.getLtemrosource().getMrname())
						&& lteMroSource.getLtemrosource().getMrtime() >= mwXdr
								.getMwXdr().getProcedurestarttime() - 10000
						&& lteMroSource.getLtemrosource().getMrtime() <= mwXdr
								.getMwXdr().getProcedureendtime() + 10000
						&& lteMroSource
								.getLtemrosource()
								.getCellid()
								.equals(ParseUtils.parseLong(mwXdr.getMwXdr()
										.getDest_eci()))
						&& lteMroSource.getLtemrosource().getKpi1() != -1.0
						&& 1 == lteMroSource.getLtemrosource().getVid()) {
					ueMrList.add(lteMroSource);
				}
			}
			LteMroSourceNew miniKpi = this.getMinKpi(mwXdr, ueMrList);
			if (miniKpi != null && miniKpi.getLtemrosource().getKpi1() < 21) {
				// eventMesage = new EventMesage(mwXdr, "8",
				// exceptionMap, lteCellMap);
				// lteGMMap.put("02",
				// s1mmeXdr.getS1mmeXdr().getMmeGroupId()+StringUtils.DELIMITER_INNER_ITEM1+s1mmeXdr.getS1mmeXdr().getMmeCode())
				// ;
				return this.getEventMesage(mwXdr,null, "03", lteGMMap, exceptionMap,
						lteMroSourceList, cellXdrMap, uuXdrList, lteCellMap,
						nCellinfoMap, configPara);
				// eventMesage;
			} else {
				// eventMesage = new EventMesage(mwXdr, "9",
				// InterfaceConstants.UU, exceptionMap, lteCellMap);
				return this.getEventMesage(mwXdr,null,"04", lteGMMap, exceptionMap,
						lteMroSourceList, cellXdrMap, uuXdrList, lteCellMap,
						nCellinfoMap, configPara);
			}
		}
	}

	private EventMesage _volteNoConnCalledAnalySeven(MwNew mwXdr,
			List<UuXdrNew> uuXdrList, List<MwNew> iscMgXdrList,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson>  exceptionMap,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {
		if ("408".equals(mwXdr.getMwXdr().getResponse_code())
				|| "480".equals(mwXdr.getMwXdr().getResponse_code())
				|| "500".equals(mwXdr.getMwXdr().getResponse_code())
				|| "503".equals(mwXdr.getMwXdr().getResponse_code())
				|| "504".equals(mwXdr.getMwXdr().getResponse_code())
				|| "580".equals(mwXdr.getMwXdr().getResponse_code())) {
			// \u6b65\u9aa48
			return this._volteNoConnCalledAnalyEight(mwXdr, uuXdrList,
					iscMgXdrList, s1mmeXdrList, lteMroSourceList, cellXdrMap,
					exceptionMap, lteCellMap, nCellinfoMap, configPara);
		} else {
			return this.getEventMesage(mwXdr,null, "05", lteGMMap, exceptionMap,
					lteMroSourceList, cellXdrMap, uuXdrList, lteCellMap,
					nCellinfoMap, configPara);
			// new EventMesage(mwXdr,
			// mwXdr.getMwXdr().getResponse_code(), InterfaceConstants.SIP,
			// exceptionMap, lteCellMap);
		}
	}

	private EventMesage _volteNoConnCalledAnalyEight(MwNew mwXdr,
			List<UuXdrNew> uuXdrList, List<MwNew> iscMgMwXdrList,
			List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson> exceptionMap,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {

		for (MwNew iscMgMwXdr : iscMgMwXdrList) {
			if (iscMgMwXdr.getMwXdr().getProcedurestarttime() >= mwXdr
					.getMwXdr().getProcedurestarttime()
					&& iscMgMwXdr.getMwXdr().getProcedurestarttime() <= mwXdr
							.getMwXdr().getProcedureendtime()
					&& !iscMgMwXdr.getMwXdr().toString()
							.equals(mwXdr.getMwXdr().toString())
					&& !"0".equals(iscMgMwXdr.getMwXdr().getProcedurestatus())) {
				// 步骤8，输出O5
				return this.getEventMesage(mwXdr,null, "05", lteGMMap, exceptionMap,
						lteMroSourceList, cellXdrMap, uuXdrList, lteCellMap,
						nCellinfoMap, configPara);
				// new EventMesage(mwXdr,
				// iscMgMwXdr.getMwXdr().getResponse_code(),
				// InterfaceConstants.SIP,
				// exceptionMap, lteCellMap);
			}
		}
		// List<S1mmeXdrNew> S1mme = new ArrayList<S1mmeXdrNew>();
		// 步骤9
		// boolean isExist13 = false;
		for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
			if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr
					.getMwXdr().getProcedurestarttime()
					&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr
							.getMwXdr().getProcedureendtime()
					&& "13".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())) {
				// isExist13 = true;
				if (!"0".equals(s1mmeXdr.getS1mmeXdr().getProcedureStatus())) {

					lteGMMap.put("07", s1mmeXdr.getS1mmeXdr().getMmeGroupId()
							+ StringUtils.DELIMITER_INNER_ITEM1
							+ s1mmeXdr.getS1mmeXdr().getMmeCode());
					return this.getEventMesage(mwXdr,s1mmeXdr,"07", lteGMMap,
							exceptionMap, lteMroSourceList, cellXdrMap,
							uuXdrList, lteCellMap, nCellinfoMap, configPara);
					// new EventMesage(mwXdr,
					// s1mmeXdr.getS1mmeXdr().getFailureCause(),
					// InterfaceConstants.NAS, exceptionMap, lteCellMap);
				} else {
					if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr
							.getMwXdr().getProcedurestarttime()
							&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr
									.getMwXdr().getProcedureendtime()
							&& "21".equals(s1mmeXdr.getS1mmeXdr()
									.getProcedureType())) {
						lteGMMap.put("08", s1mmeXdr.getS1mmeXdr()
								.getMmeGroupId()
								+ StringUtils.DELIMITER_INNER_ITEM1
								+ s1mmeXdr.getS1mmeXdr().getMmeCode());
						return this
								.getEventMesage(mwXdr,s1mmeXdr, "08", lteGMMap,
										exceptionMap, lteMroSourceList,
										cellXdrMap, uuXdrList, lteCellMap,
										nCellinfoMap, configPara);
						// new EventMesage(mwXdr, bearReleaseCause,
						// InterfaceConstants.S1AP, exceptionMap, lteCellMap);

					}
				}
			} else {
				if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr
						.getMwXdr().getProcedurestarttime()
						&& s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr
								.getMwXdr().getProcedureendtime()
						&& "21".equals(s1mmeXdr.getS1mmeXdr()
								.getProcedureType())) {
					lteGMMap.put("08", s1mmeXdr.getS1mmeXdr().getMmeGroupId()
							+ StringUtils.DELIMITER_INNER_ITEM1
							+ s1mmeXdr.getS1mmeXdr().getMmeCode());
					return this.getEventMesage(mwXdr,s1mmeXdr, "08", lteGMMap,
							exceptionMap, lteMroSourceList, cellXdrMap,
							uuXdrList, lteCellMap, nCellinfoMap, configPara);
					// new EventMesage(mwXdr, bearReleaseCause,
					// InterfaceConstants.S1AP, exceptionMap, lteCellMap);

				}

			}
		}
		return setup12(mwXdr, uuXdrList, iscMgMwXdrList, s1mmeXdrList,
				lteMroSourceList, cellXdrMap, exceptionMap, lteCellMap,
				nCellinfoMap, configPara);
		// setup 11

		// for (S1mmeXdrNew s1mmeXdr : s1mmeXdrList) {
		// if (s1mmeXdr.getS1mmeXdr().getProcedureStartTime() >= mwXdr
		// .getMwXdr().getProcedurestarttime()
		// && s1mmeXdr.getS1mmeXdr().getProcedureStartTime() <= mwXdr
		// .getMwXdr().getProcedureendtime()
		// && "21".equals(s1mmeXdr.getS1mmeXdr().getProcedureType())) {
		// String[] sb = s1mmeXdr.getS1mmeXdr().getBearerArr();
		// String bearReleaseCause = null;
		// for (Integer i = 0; i < sb.length; i++) {
		// if (sb[i] == null)
		// break;
		// bearReleaseCause = sb[i].split(StringUtils.DELIMITER_COMMA)[4];
		// if ("3".equals(bearReleaseCause)
		// || "6".equals(bearReleaseCause)
		// || "21".equals(bearReleaseCause)
		// || "26".equals(bearReleaseCause)) {
		// return this._volteNoConnCalledAnalyThirteen(mwXdr,
		// bearReleaseCause, lteMroSourceList, cellXdrMap,
		// exceptionMap, lteCellMap, s1mmeXdr,lteMroSourceList);
		// }
		// }
		//
		// S1mme.add(s1mmeXdr);
		//
		// lteGMMap.put("08", s1mmeXdr.getS1mmeXdr().getMmeGroupId()
		// + StringUtils.DELIMITER_INNER_ITEM1
		// + s1mmeXdr.getS1mmeXdr().getMmeCode());
		// return this.getEventMesage(s1mmeXdr, "08", lteGMMap,
		// exceptionMap,lteMroSourceList);
		// // new EventMesage(mwXdr, bearReleaseCause,
		// // InterfaceConstants.S1AP, exceptionMap, lteCellMap);
		//
		// }
		// }

		// if (S1mme.size() != 1) {
		// Collections.sort(S1mme, new Comparator<S1mmeXdrNew>() {
		// @Override
		// public int compare(S1mmeXdrNew a, S1mmeXdrNew b) {
		// long timeOne = Long.valueOf(a.getS1mmeXdr()
		// .getProcedureStartTime());
		// long timeTwo = Long.valueOf(a.getS1mmeXdr()
		// .getProcedureStartTime());
		// return (int) -(timeOne - timeTwo);
		// }
		// });
		// }
		//
		// // isExist21 = false
		// if (isExist13 == false) {
		// if (S1mme.size() > 0) {
		// lteGMMap.put("13", S1mme.get(0).getS1mmeXdr().getMmeGroupId()
		// + StringUtils.DELIMITER_INNER_ITEM1
		// + S1mme.get(0).getS1mmeXdr().getMmeCode());
		// }
		// return this.getEventMesage(mwXdr, "13", lteGMMap,
		// exceptionMap,lteMroSourceList,cellXdrMap,uuXdrList,
		// lteCellMap,nCellinfoMap,configPara);
		// // new EventMesage(mwXdr, "2006",
		// // InterfaceConstants.S1AP, exceptionMap, lteCellMap);
		// } else {
		// if (S1mme.size() > 0) {ÓïÒô
		// lteGMMap.put("14", S1mme.get(0).getS1mmeXdr().getMmeGroupId()
		// + StringUtils.DELIMITER_INNER_ITEM1
		// + S1mme.get(0).getS1mmeXdr().getMmeCode());
		// }
		// return this.getEventMesage(mwXdr, "14", lteGMMap,
		// exceptionMap,lteMroSourceList,cellXdrMap,uuXdrList,
		// lteCellMap,nCellinfoMap,configPara);
		// // new EventMesage(mwXdr, "2005",
		// // InterfaceConstants.S1AP, exceptionMap, lteCellMap);
		// }

	}

	private EventMesage setup12(MwNew mwXdr, List<UuXdrNew> uuXdrList,
			List<MwNew> iscMgMwXdrList, List<S1mmeXdrNew> s1mmeXdrList,
			List<LteMroSourceNew> lteMroSourceList,
			Map<String, List<String>> cellXdrMap, Map<String, ExceptionReson> exceptionMap,
			Map<String, String> lteCellMap, Map<String, String> nCellinfoMap,
			Map<String, String> configPara) {
		if (s1mmeXdrList.size() > 0) {
			for (S1mmeXdrNew s1mme : s1mmeXdrList) {
				if (s1mme.getS1mmeXdr().getProcedureStartTime() >= mwXdr
						.getMwXdr().getProcedurestarttime()
						&& s1mme.getS1mmeXdr().getProcedureStartTime() <= mwXdr
								.getMwXdr().getProcedureendtime()) {
					lteGMMap.put("14", s1mme.getS1mmeXdr().getMmeGroupId()
							+ StringUtils.DELIMITER_INNER_ITEM1
							+ s1mme.getS1mmeXdr().getMmeCode());
					return this.getEventMesage(mwXdr,null, "14", lteGMMap,
							exceptionMap, lteMroSourceList, cellXdrMap,
							uuXdrList, lteCellMap, nCellinfoMap, configPara);
				}

			}

		}
		return this.getEventMesage(mwXdr,null,"13", lteGMMap, exceptionMap,
				lteMroSourceList, cellXdrMap, uuXdrList, lteCellMap,
				nCellinfoMap, configPara);
	}

	// public EventMesage _volteNoConnCalledAnalyThirteen(MwNew mwXdr,ÓïÒô
	// String eRABReleaseCause, List<LteMroSourceNew> ueMroSourceList,
	// Map<String, List<String>> cellXdrMap, Map<String, String> exceptionMap,
	// Map<String, String> lteCellMap, S1mmeXdrNew s1mmeXdr,
	// List<LteMroSourceNew> lteMroSourceList) {
	// List<LteMroSourceNew> ueMrKpi1List = new ArrayList<LteMroSourceNew>();
	// List<LteMroSourceNew> ueMrKpi6List = new ArrayList<LteMroSourceNew>();
	// if (ueMroSourceList.size() > 0) {
	// for (LteMroSourceNew ueMroSource : ueMroSourceList) {
	//
	// String cellid = null;
	// if (AttributeConstnts.MW_CALLSIDE_DEST.equals(mwXdr.getMwXdr()
	// .getCall_side())) {
	// cellid = mwXdr.getMwXdr().getDest_eci();
	// } else {
	// cellid = mwXdr.getMwXdr().getSource_eci();
	// }
	//
	// if (ueMroSource.getLtemrosource().getMrtime() >= mwXdr
	// .getMwXdr().getProcedurestarttime() - 20000
	// && ueMroSource.getLtemrosource().getMrtime() <= mwXdr
	// .getMwXdr().getProcedurestarttime() + 1000
	// && ueMroSource.getLtemrosource().getCellid()
	// .equals(ParseUtils.parseLong(cellid))
	// && 1 == ueMroSource.getLtemrosource().getVid()) {
	// // kpi1\u4e0d\u4e3a\u7a7a
	// if (ueMroSource.getLtemrosource().getKpi1() != -1.0) {
	// ueMrKpi1List.add(ueMroSource);
	// // kpi6\u4e0d\u4e3a\u7a7a
	// }
	// if (ueMroSource.getLtemrosource().getKpi6() != -1.0) {
	// ueMrKpi6List.add(ueMroSource);
	// }
	// }
	// }
	// //
	// KPI1\u4e0d\u4e3a\u7a7a\u7684UE_MR,\u6700\u5c0fKPI1\uff08\u4e3b\u5c0f\u533aRSRP\uff09<31\uff0c\u8fd4\u56deO5
	// if (ueMrKpi1List.size() > 0) {
	// if (getMinKpi(mwXdr, ueMrKpi1List).getLtemrosource().getKpi1() < 31) {
	// return this.getEventMesage(mwXdr, "09", lteGMMap,
	// exceptionMap,lteMroSourceList);
	// // new EventMesage(mwXdr, "1", InterfaceConstants.UU,
	// // exceptionMap, lteCellMap);
	// }
	// }
	// Long rip = getAvgKpi(mwXdr, cellXdrMap);
	// if (rip > 111) {
	// return this.getEventMesage(mwXdr, "10", lteGMMap,
	// exceptionMap,lteMroSourceList);
	// // new EventMesage(mwXdr, "2", InterfaceConstants.UU,
	// // exceptionMap, lteCellMap);
	// }
	// // UE_MR,kpi6kpi6\u4e0d\u4e3a\u7a7a
	// if (ueMrKpi6List.size() > 0) {
	// // \u53d6\u6700\u8fd1UE_MR
	// LteMroSourceNew ueMroSource = getMinKpi(mwXdr, ueMrKpi6List);
	// if (ueMroSource.getLtemrosource().getKpi6() < 25) {
	// // kpi8\uff08UL SINR\uff09>=22,\u8fd4\u56deO7
	// if (ueMroSource.getLtemrosource().getKpi8() >= 22) {
	// return this.getEventMesage(mwXdr, "11", lteGMMap,
	// exceptionMap,lteMroSourceList);
	// // new EventMesage(mwXdr, "3", InterfaceConstants.UU,
	// // exceptionMap, lteCellMap);
	//
	// } else {
	// // kpi8\uff08UL SINR\uff09<22,\u8fd4\u56deO8
	// return this.getEventMesage(mwXdr, "12", lteGMMap,
	// exceptionMap,lteMroSourceList);
	// // new EventMesage(mwXdr, "4", InterfaceConstants.UU,
	// // exceptionMap, lteCellMap);
	// }
	// } else {
	// // kpi6>=25,\u8fd4\u56deO3
	// return this.getEventMesÓïÒôage(mwXdr, "08", lteGMMap,
	// exceptionMap,lteMroSourceList);
	// // new EventMesage(mwXdr, eRABReleaseCause,
	// // InterfaceConstants.S1AP, exceptionMap,
	// // lteCellMap);
	// }
	// }
	//
	// } else {
	// lteGMMap.put("14", s1mmeXdr.getS1mmeXdr().getMmeGroupId()
	// + StringUtils.DELIMITER_INNER_ITEM1
	// + s1mmeXdr.getS1mmeXdr().getMmeCode());
	// return this.getEventMesage(mwXdr, "14", lteGMMap,
	// exceptionMap,lteMroSourceList); // 未确�?
	// // new EventMesage(mwXdr, eRABReleaseCause, InterfaceConstants.S1AP,
	// // exceptionMap, lteCellMap);
	// }
	// return null;
	// }
//			Map<String, List<String>> cellXdrMap, Map<String, String> exceptionMap,

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

	public UuXdrNew getMinUu(Long mwtime, List<UuXdrNew> UuXdrNewList) {
		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Map<Long, UuXdrNew> UuXdrMap = new HashMap<Long, UuXdrNew>();
		for (UuXdrNew UuXdr : UuXdrNewList) {
			curTime = Math.abs(mwtime
					- UuXdr.getUuXdr().getProcedureStartTime());
			if (minTime > curTime) {
				minTime = curTime;
				UuXdrMap.put(minTime, UuXdr);
			}
		}
		return UuXdrMap.get(minTime);
	}

	@SuppressWarnings("rawtypes")
	public Long getAvgKpi(MwNew mwXdr, Map<String, List<String>> cellXdrMap) {

		Long minTime = Long.MAX_VALUE;
		Long curTime = 0L;
		Iterator iter = cellXdrMap.entrySet().iterator();
		Long kpiAvg = 0l;

		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			if (entry.getKey().equals(mwXdr.getMwXdr().getSource_eci())) {
				String[] valueCell = entry.getValue().toString()
						.split(StringUtils.DELIMITER_COMMA);
				if (mwXdr.getMwXdr().getProcedurestarttime()
						- Long.valueOf(valueCell[1]) < -20000
						&& mwXdr.getMwXdr().getProcedurestarttime()
								- Long.valueOf(valueCell[1]) > 1000) {
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