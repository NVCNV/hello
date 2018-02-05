package cn.com.dtmobile.hadoop.biz.exception.mr;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.com.dtmobile.hadoop.biz.exception.model.EventMesage;
import cn.com.dtmobile.hadoop.biz.exception.model.ExceptionReson;
import cn.com.dtmobile.hadoop.biz.exception.service.AttachFailAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.EsrvccFailAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.Exception4GAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.IMSRegistFailAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.Lose4GAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.OffLine4GAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.ServiceRequestAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.TauFailAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.VolteExceptionAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.VolteExceptionCalledAnalyService;
import cn.com.dtmobile.hadoop.biz.exception.service.VolteExceptionLoseAnalyService;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.GxRxNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.MwNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.S1mmeXdrNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.SvNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.UuXdrNew;
import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.X2XdrNew;
import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.ReadConfig;
import cn.com.dtmobile.hadoop.util.StringUtils;

@SuppressWarnings("deprecation")
public class ExceptionCommonReduce extends Reducer<Text, Text, NullWritable, Text> {
	List<S1mmeXdrNew> s1mmeXdrList = null;
	List<LteMroSourceNew> lteMroSourceList = null;
	List<X2XdrNew> x2List = null;
	List<MwNew> mwXdrList = null;
	List<UuXdrNew> uuList = null;
	List<SvNew> svList = null;
	Map<String, String> configPara = null ;
	Map<String,String> nCellinfoMap = null;
	
	List<GxRxNew> gxXdrList = null;
	Map<String, ExceptionReson> exceptionResonMap = new HashMap<String, ExceptionReson>();
//	Map<String, ArrayList<String>> cellXdrMap = new HashMap<String, ArrayList<String>>();
	Map<String, List<String>> cellXdrMap = new HashMap<String, List<String>>();
	Map<String, String> t_professMap = new HashMap<String, String>();
	Map<String, String> ltecellMap = new HashMap<String, String>();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		Path[] paths;
		try {
			paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			BufferedReader data_exception = new BufferedReader(new FileReader(
					paths[0].toString()));
//			BufferedReader data_exception = new BufferedReader(new FileReader(
//					"C:/Users/xiaopan/Desktop/liaoling/t_fail_case.txt"));
			String lines = null;
			while (StringUtils.isNotEmpty((lines = data_exception.readLine()))) {
				String[] values = lines.split(StringUtils.DELIMITER_INNER_ITEM,-1);
				//表头 ：接口	Status Code	meaning	问题网元类型	归类	描述 呈现详情 
				//values[0] 接口 values[1] Status Code values[3]问题网元类型
				ExceptionReson exceptionReson =new ExceptionReson(values) ;
				exceptionResonMap.put(exceptionReson.getProInterface() +StringUtils.DELIMITER_COMMA+exceptionReson.getStatusCode(), exceptionReson);
			}
			data_exception.close();
			
		
			//读取cell——mr信息
			BufferedReader data_cellMR = new BufferedReader(new FileReader(paths[1].toString()));
//			BufferedReader data_cellMR = new BufferedReader(new FileReader("C:/Users/xiaopan/Desktop/liaoling/cell_mr.csv"));
			String lines_CELLMR = null;
			
			while (StringUtils.isNotEmpty(lines_CELLMR = data_cellMR.readLine())){
				String[] values = lines_CELLMR.split(StringUtils.DELIMITER_COMMA);
				double a = 0;
				double count = 0;
				
				for(int i=20;i<30;i++){    //values[19]-values[28] KPI1~KPI10
					if ("".equals(values[i]) || "\\N".equals(values[i])) {
						continue;
					}else{
						a += ParseUtils.parseDouble(values[i]);
						count++;
					}
				}
				if (count > 0) {
					Double avg = a / count;
					List<String> tmp = cellXdrMap.get(values[9]);
					if (tmp != null && !tmp.isEmpty()) {
						tmp.add(avg.intValue() + "," + values[105]);
						cellXdrMap.put(values[9], tmp);
					} else {
						tmp = new ArrayList<String>();
						tmp.add(avg.intValue() + "," + values[105]);
						cellXdrMap.put(values[9], tmp);
					}

									
				}
			}
			
			data_cellMR.close();
			
		
			/**
			 * ltecell 表头
			 * MCC	MNC	MMEGROUPID	MMEID	eNodeB_ID	SiteName	CellName	CID	ECI	TAC	PCI	EARFCN	EARFCN	BeamWidth	BeamWidth	FREQCOUNT	Longitude	Latitude	SECTORTYPE	Type	tilt_total	TILTM	tilt_total	Azimuth	BeamWidth	VBEAMWIDTH	天线挂高	CITY	COMPANY	区域	ENBTYPE	ENBVERSION
			 */
			BufferedReader data_ltcell = new BufferedReader(new FileReader(paths[2].toString()));
//			BufferedReader data_ltcell = new BufferedReader(new FileReader("C:/Users/xiaopan/Desktop/liaoling/ltecell.csv"));
			String lines_ltcell = null;
			while (StringUtils.isNotEmpty(lines_ltcell = data_ltcell.readLine())){
				String[] values = lines_ltcell.split(StringUtils.DELIMITER_COMMA);
				/**
				 * values[8] cellid
				 * values[11] freq
				 * values[10] pci
				 * values[16] 经度
				 * values[17] 纬度
				 * values[23] 方位角
				 * values[6]  小区名称
				 */
				ltecellMap.put(values[8], values[11]+StringUtils.DELIMITER_COMMA+values[10]+StringUtils.DELIMITER_COMMA+values[16]+StringUtils.DELIMITER_COMMA+values[17]+StringUtils.DELIMITER_COMMA+values[23]+StringUtils.DELIMITER_COMMA+values[6]);
			}
			data_ltcell.close();
			
			//读取邻区配置表信息
			nCellinfoMap = new IdentityHashMap<String, String>() ;
			BufferedReader nCellInfo = new BufferedReader(new FileReader(paths[3].toString()));
			String lineCellInfo = null ;
			while(StringUtils.isNotEmpty(lineCellInfo = nCellInfo.readLine())){
				String[] cellInfo = lineCellInfo.split(StringUtils.DELIMITER_COMMA);
				nCellinfoMap.put(new String(cellInfo[0]), cellInfo[1]) ;
			}
			nCellInfo.close(); 
			
		
			//读取参数配置文件信息
			configPara = new HashMap<String, String>() ;
			configPara = ReadConfig.readConfig(paths[4].toString()) ;
			
			/*BufferedReader data_t_pro = new BufferedReader(new FileReader(paths[3].toString()));
			String lines_t_pro = null;
			while (StringUtils.isNotEmpty(lines_t_pro = data_t_pro.readLine())){
				String[] values = lines_t_pro.split(StringUtils.DELIMITER_COMMA);
				t_professMap.put(values[15]+values[17], values[22]+StringUtils.DELIMITER_COMMA+values[23]);
			}
			data_t_pro.close();*/
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
//		MapTableUtils maptables = new MapTableUtils();
//		exceptionResonMap = maptables.map_exceptions(context);
//		cellXdrMap = maptables.map_cellXdr(context);
//		t_professMap = maptables.t_profess(context);
//		ltecellMap = maptables.map_lte(context);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		x2List = new ArrayList<X2XdrNew>();
		uuList = new ArrayList<UuXdrNew>();
		s1mmeXdrList = new ArrayList<S1mmeXdrNew>();
		lteMroSourceList = new ArrayList<LteMroSourceNew>();
		svList = new ArrayList<SvNew>();
		mwXdrList = new ArrayList<MwNew>();
		gxXdrList = new ArrayList<GxRxNew>();
        int len;
        String[] arr;
		for (Text value : values) {
			arr = value.toString().split(StringUtils.DELIMITER_INNER_ITEM);
			len = arr.length;
			String tableTag = arr[len - 1];
			if (TablesConstants.HIVE_UU.equals(tableTag)) {
				uuList.add(new UuXdrNew(arr));
			} else if (TablesConstants.HIVE_S1MME.equals(tableTag)) {
				s1mmeXdrList.add(new S1mmeXdrNew(arr));
			} else if (TablesConstants.HIVE_LTE_MRO_SOURCE.equals(tableTag)) {
				lteMroSourceList.add(new LteMroSourceNew(arr));
			} else if (TablesConstants.HIVE_X2.equals(tableTag)) {
				x2List.add(new X2XdrNew(arr));
			} else if (TablesConstants.HIVE_SV.equals(tableTag)) {
				svList.add(new SvNew(arr));
			} else if (TablesConstants.HIVE_MW.equals(tableTag)) {
				mwXdrList.add(new MwNew(arr));
			} else if (TablesConstants.HIVE_GXRX.equals(tableTag)) {
				gxXdrList.add(new GxRxNew(arr));
			} else { 
				return;
			}

		}
		
		@SuppressWarnings("unused")
		EventMesage em = new EventMesage(exceptionResonMap,ltecellMap);
		
		Exception4GAnalyService exception4GAnalyService = new Exception4GAnalyService();
		List<EventMesage> x2ExceptionMessageList = exception4GAnalyService
				.exception4GAnalyServiceX2(x2List, s1mmeXdrList, uuList,
						ltecellMap, cellXdrMap, lteMroSourceList,
						exceptionResonMap, nCellinfoMap,configPara);
		List<EventMesage> s1mmeExceptionMessageList = exception4GAnalyService
				.exception4GAnalyServiceS1(s1mmeXdrList, uuList, ltecellMap,
						cellXdrMap, lteMroSourceList, exceptionResonMap,
						nCellinfoMap,configPara);
		List<EventMesage> uuExceptionMessageList = exception4GAnalyService
				.exception4GAnalyServiceUu(uuList,ltecellMap, cellXdrMap, lteMroSourceList,
						exceptionResonMap, nCellinfoMap,configPara);
		AttachFailAnalyService attachFailAnalyService = new AttachFailAnalyService();
		List<EventMesage> s1mmeExceptionMessageList_att = attachFailAnalyService
				.attachFailAnalyService(s1mmeXdrList, uuList, lteMroSourceList,
						ltecellMap,cellXdrMap, exceptionResonMap,nCellinfoMap,configPara);
		EsrvccFailAnalyService esrvccFailAnalyService = new EsrvccFailAnalyService();
		List<EventMesage> s1mmeExceptionMessageList_Esrv = esrvccFailAnalyService
				.esrvccFailAnalyService(svList, uuList, s1mmeXdrList,
						lteMroSourceList, exceptionResonMap,cellXdrMap,ltecellMap,nCellinfoMap,configPara);
		IMSRegistFailAnalyService iMSRegistFailAnalyService = new IMSRegistFailAnalyService();
		List<EventMesage> mwExceptionMessageList_IMS = iMSRegistFailAnalyService
				.imsRegistFailAnalyService(mwXdrList, lteMroSourceList,
						cellXdrMap, exceptionResonMap, ltecellMap,uuList,nCellinfoMap,configPara);
		//4G掉线 
		Lose4GAnalyService lose4GAnalyService = new Lose4GAnalyService();
		List<EventMesage> s1mmeExceptionMessageList_lose4g = lose4GAnalyService
				.Lose4GService(s1mmeXdrList, lteMroSourceList, uuList,
						ltecellMap, cellXdrMap, exceptionResonMap,nCellinfoMap,configPara, x2ExceptionMessageList, s1mmeExceptionMessageList);
		//4G脱网
		OffLine4GAnalyService offLine4GAnalyService = new OffLine4GAnalyService();
		List<EventMesage> s1mmeExceptionMessageList_tuowang4g = offLine4GAnalyService
				.OffLine4GService(s1mmeXdrList, lteMroSourceList, uuList,
						ltecellMap, cellXdrMap, exceptionResonMap,nCellinfoMap,configPara);
		ServiceRequestAnalyService serviceRequestAnalyService = new ServiceRequestAnalyService();
		List<EventMesage> s1mmeExceptionMessageList_servicerequest = serviceRequestAnalyService
				.volteServiceRequestService(s1mmeXdrList, lteMroSourceList,
						cellXdrMap, exceptionResonMap,ltecellMap,uuList,nCellinfoMap,configPara);
		TauFailAnalyService tauFailAnalyService = new TauFailAnalyService();
		List<EventMesage> s1mmeExceptionMessageList_tau = tauFailAnalyService
				.TauFailService(s1mmeXdrList, lteMroSourceList, x2List,uuList,ltecellMap,
						cellXdrMap, exceptionResonMap,nCellinfoMap,configPara);
		VolteExceptionLoseAnalyService volteExceptionLoseAnalyService = new VolteExceptionLoseAnalyService();
		List<EventMesage> gxExceptionMessageList = volteExceptionLoseAnalyService
				.volteNoConnService(gxXdrList, s1mmeXdrList, lteMroSourceList,
						cellXdrMap, exceptionResonMap,ltecellMap,uuList,nCellinfoMap,configPara);
		VolteExceptionCalledAnalyService volteExceptionCalledAnalyService = new VolteExceptionCalledAnalyService();
		List<EventMesage> mwExceptionMessageList = volteExceptionCalledAnalyService
				.volteNoConnCalledService(mwXdrList, s1mmeXdrList, uuList,
						lteMroSourceList, cellXdrMap, exceptionResonMap,
						ltecellMap,nCellinfoMap,configPara);
		VolteExceptionAnalyService volteExceptionAnalyService = new VolteExceptionAnalyService();
		List<EventMesage> mwExceptionMessageList_volte = volteExceptionAnalyService
				.volteNoConnService(mwXdrList, s1mmeXdrList, lteMroSourceList,
						cellXdrMap, exceptionResonMap, ltecellMap,uuList,nCellinfoMap,configPara);
		x2ExceptionMessageList.addAll(mwExceptionMessageList_volte);
		x2ExceptionMessageList.addAll(mwExceptionMessageList);
		x2ExceptionMessageList.addAll(gxExceptionMessageList);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList_tau);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList_servicerequest);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList_tuowang4g);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList_lose4g);
		x2ExceptionMessageList.addAll(mwExceptionMessageList_IMS);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList_Esrv);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList_att);
		x2ExceptionMessageList.addAll(uuExceptionMessageList);
		x2ExceptionMessageList.addAll(s1mmeExceptionMessageList);
		if (x2ExceptionMessageList.size() > 0) {
			for (EventMesage x2ExceptionMessage : x2ExceptionMessageList) {
				context.write(NullWritable.get(), new Text(x2ExceptionMessage.toString()));
			}
		}
	}

}
