package cn.com.dtmobile.hadoop.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.com.dtmobile.hadoop.biz.train.model.highspeeduser.LteMroSourceNew;

public class MathUtil {

	static final float EARTH_RADIUS = 6378.137f; // 地球半径

	static final double PI = 3.14159265; // π 取值

	static final double RADIANPERDEGREE = 0.0174532925;

	static final double INVALIDVALUE = -255D;

	static final double FDISLEVER = 0.3; // 加权因子

	public static double cell_distance(double lat1, double lon1, double lat2, double lon2) {
		double R = 6378.137; // 地球半径
		double a = (lat1 - lat2) * Math.PI / 180.0;
		double b = (lon1 - lon2) * Math.PI / 180.0;
		double sa2 = Math.sin(a / 2.0);
		double sb2 = Math.sin(b / 2.0);
		return 2 * R * Math.asin(
				Math.sqrt(sa2 * sa2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * sb2 * sb2));

	}

	public static Double getEquivalentDistance(String strCell, String strNcCell, Map<String, String> ltecellMap){
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
	
	//计算rip
		public static Double getAvgKpi(Long procedureStarttime,Long cellID ,Map<String, List<String>> cellXdrMap) {
			
			if(cellID == null || cellXdrMap == null){
				return null ;
			}
			
			Long minTime = Long.MAX_VALUE;
			Long curTime = 0L;
			Double kpiAvg = null;
			
			List<String> cell_mr = cellXdrMap.get(cellID.toString()) ;
			if(cell_mr != null && !cell_mr.isEmpty()){
				
				for(String avgAndTime : cell_mr){
				
					String[] value = avgAndTime.split(",");
				
					if((procedureStarttime- Long.valueOf(value[1]))<-30000 && (procedureStarttime- Long.valueOf(value[1])>1000)){
					continue;
					}	
				
					curTime = Math.abs(procedureStarttime - Long.valueOf(value[1]));
					if (minTime > curTime) {
						minTime = curTime;
						kpiAvg = Double.valueOf(value[0]);
					}
				
				}
			}
			
			
			return kpiAvg;
		}
	
	
	
	
	
	
	//计算主小区与最强小区的实际距离和最强小区的cellid	@SuppressWarnings("unused")
		public static String getActualDistinceAll(Map<String, String> ltecellMap,LteMroSourceNew lteMro){
			
			//利用工参获取目标小区PCI和频�?
			int targetCellPCI = lteMro.getLtemrosource().getKpi12().intValue();
			int targetCellFreq = lteMro.getLtemrosource().getKpi11().intValue();
			String cellID = lteMro.getLtemrosource().getCellid().toString() ;
									
			Iterator<Entry<String, String>> iter = ltecellMap.entrySet().iterator();
			String srcCell = ltecellMap.get(cellID)  ;
			String[] srcCelllatlons =null ;
			double lat2 = 0.0 ;
			double lon2 = 0.0 ;
			
			if(srcCell != null){
				srcCelllatlons = srcCell.split(StringUtils.DELIMITER_COMMA);
				 lat2 = Double.parseDouble(srcCelllatlons[3]);
				 lon2 = Double.parseDouble(srcCelllatlons[2]);
			}else{
				return " ";
			}
			
			
			
			Double minDis = 6.0 ;
			String neighborCellid = null ;
			String nCellid = null;
			while (iter.hasNext()) {
				Entry<String, String> entry = iter.next();
				neighborCellid = entry.getKey();
				String value=entry.getValue();
				if(neighborCellid.equals(cellID)){
					continue;
				}
				String[] words = value.split(StringUtils.DELIMITER_COMMA);
				double lat1 = Double.parseDouble(words[3]);
				double lon1 = Double.parseDouble(words[2]);
					
				if(targetCellFreq == Integer.parseInt(words[0]) &&  targetCellPCI == Integer.parseInt(words[1]) ){
					double dist = MathUtil.cell_distance(lat1, lon1, lat2,lon2);
					if(dist<minDis){
						minDis = dist ;
						nCellid = neighborCellid;
					}
				}
			}
						
						
			return (nCellid==null) ? " " : nCellid+StringUtils.DELIMITER_COMMA+minDis ;
		}	
		

	
		public static String getActualDistince(Map<String, String> ltecellMap,LteMroSourceNew lteMro,Map<String,String> nCellinfoMap){
			Long cellid = lteMro.getLtemrosource().getCellid() ;
			
			String word = ltecellMap.get(cellid.toString()) ;
			double lat2 = 0.0 ;
			double lon2 = 0.0 ;
			if(word != null){
				String[] tmp = word.split(StringUtils.DELIMITER_COMMA) ;
				lat2 = Double.parseDouble(tmp[3]); ;
				lon2 = Double.parseDouble(tmp[2]);
			}
			
			int pci = lteMro.getLtemrosource().getKpi12().intValue() ;
			int freq = lteMro.getLtemrosource().getKpi11().intValue() ;
			List<String> nerberCell = new ArrayList<String>() ;
			Iterator<Entry<String, String>> iter = nCellinfoMap.entrySet().iterator();
			while(iter.hasNext()){
				Entry<String, String> entry = iter.next();
				if( entry.getKey().equals(cellid.toString())){
					nerberCell.add(entry.getValue()) ;
				}
				
			}
			
			
			Double minDis = 6.0 ;
			String result = null ;
			String ncellCellId = null ;
			for(String cell:nerberCell){
			String ncellInfo =	ltecellMap.get(cell) ;
				if(ncellInfo != null){
					String[] tmp = ncellInfo.split(StringUtils.DELIMITER_COMMA) ;
					if(pci == Integer.parseInt(tmp[1]) && freq == Integer.parseInt(tmp[0])){
						
						double lat1 = Double.parseDouble(tmp[3]);
						double lon1 = Double.parseDouble(tmp[2]);
						double dist = MathUtil.cell_distance(lat1, lon1, lat2,lon2);
						if(dist<minDis){
							minDis = dist ;
							ncellCellId = cell ;
						}
					}
				}
				
			}
			
			
			if(ncellCellId != null){
				result = ncellCellId == null ?" ":ncellCellId+StringUtils.DELIMITER_COMMA+minDis ;
			}else{
				//如果在邻区表里面没有找到小区，全表扫描ltecellMap
				result = MathUtil.getActualDistinceAll(ltecellMap,lteMro) ;
			}
			
			return result;
		}

	
	
	
	
	/**
	 * 邻区距离 单位（米）
	 * 
	 * @param rSLon
	 *            源小区经度
	 * @param rSLat
	 *            源小区纬度
	 * @param rNLon
	 *            目标小区经度
	 * @param rNLat
	 *            目标小区纬度
	 * @return
	 */
	public static Double getDistance(Double rSLon, Double rSLat, Double rNLon, Double rNLat) {
		Double s = 0D;
		try {
			Double a = rSLat * PI / 180 - rNLat * PI / 180;
			Double b = rSLon * PI / 180 - rNLon * PI / 180;
			s = (Double) (2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
					+ Math.cos(rSLat * PI / 180) * Math.cos(rNLat * PI / 180) * Math.pow(Math.sin(b / 2), 2))));
			s = s * EARTH_RADIUS;
			s = (s * 10000000) / 10000;
		} catch (Exception e) {
			return 0D;
		}
		return s;
	}

	/**
	 * NodeB的方位角
	 * 
	 * @param sLon
	 * @param sLat
	 * @param nLon
	 * @param nLat
	 * @return
	 */
	public static Double calNodeBsAngle(Double sLon, Double sLat, Double nLon, Double nLat) {
		Double angle = 0D;
		try {
			// 是否转成米的形式，根据准确性
			int dX = ((nLon * PI / 180 - sLon * PI / 180) > 0) ? 1 : -1; // 表示方向性
			int dY = ((nLat * PI / 180 - sLat * PI / 180) > 0) ? 1 : -1;
			Double dbXOff = getDistance(sLon, nLat, nLon, nLat) * dX;
			Double dbYOff = getDistance(sLon, sLat, sLon, nLat) * dY;

			angle = Math.atan2(dbXOff, dbYOff) * 180 / PI;
			angle = (angle > 360) ? (angle - 360) : angle;
			angle = (angle < 0) ? (angle + 360) : angle;
		} catch (Exception e) {
			return angle;
		}
		// 两小区连线与正北方向的夹角，顺时针0～360度,再转换成弧度
		return angle;
	}

	/**
	 * 功能描述：计算等效距离 单位（千米）
	 * 
	 * @param nSHoriz
	 *            服务小区方位角
	 * @param nNHoriz
	 *            目标小区方位角
	 * @param rSLon
	 *            源小区经度
	 * @param rSLat
	 *            源小区纬度
	 * @param rNLon
	 *            目标小区经度
	 * @param rNLat
	 *            目标小区纬度
	 * @return
	 */
	public static Double getEffDistance(Double nSHoriz, Double nNHoriz, Double rSLon, Double rSLat, Double rNLon,
			Double rNLat) {
		
		if(nSHoriz == null || nNHoriz == null || rSLon == null || rSLat == null || rNLon ==null || rNLat == null){
			return null ;
		}
		
		
		
		Integer shSType = 0; // 服务小区扇区类型（1：全向；2：定向; 3: 室分）
		Integer shNType = 0; // 目标小区扇区类型
		// 判断服务小区扇区类型
		try {
			if (nSHoriz == 360) {
				shSType = 1;
			} else if (nSHoriz == -1) {
				shSType = 3;
			} else {
				shSType = 2;
			}
			// 判断目标小区扇区类型
			if (nNHoriz == 360) {
				shNType = 1;
			} else if (nNHoriz == -1) {
				shNType = 3;
			} else {
				shNType = 2;
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		Double dbDis = getDistance(rSLon, rSLat, rNLon, rNLat); // 单位：米
		Double dbAngle = calNodeBsAngle(rSLon, rSLat, rNLon, rNLat); // 单位：度
		Double getEffDistance = null;
		try {
			if ((shSType == 1) && (shNType == 1)) { // 主,目标小区都是全向
				getEffDistance = (dbDis * (1 - FDISLEVER * 2)) / 1000;
				return getEffDistance;
			} else if ((shSType == 1) && (shNType == 2)) { // 主小区全向站,目标小区定向站
				// getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos((nNHoriz
				// - dbAngle) * PI / 180.0) - FDISLEVER * Math.cos(0)));
				getEffDistance = (dbDis * (1 + FDISLEVER * (Math.cos((nNHoriz - dbAngle) * PI / 180.0) - 1))) / 1000;
				return getEffDistance;
			} else if ((shSType == 1) && (shNType == 3)) {// 主小区全向站,目标小区室分站
				// getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos(0) -
				// FDISLEVER * Math.cos(0)));
				getEffDistance = dbDis / 1000;
				return getEffDistance;
			} else if ((shSType == 2) && (shNType == 1)) {// 主小区定向站,目标小区全向站
				// getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos(180) -
				// FDISLEVER * Math.cos((nSHoriz - dbAngle) * PI / 180.0)));
				getEffDistance = (dbDis * (1 - FDISLEVER * (1 + Math.cos((nSHoriz - dbAngle) * PI / 180.0)))) / 1000;
				return getEffDistance;
			} else if ((shSType == 2) && (shNType == 2)) { // 主小区定向站,目标小区定向站
				getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos((nNHoriz - dbAngle) * PI / 180.0) - FDISLEVER * Math.cos((nSHoriz - dbAngle) * PI / 180.0)))  / 1000;
				return getEffDistance;
			} else if ((shSType == 2) && (shNType == 3)) { // 主小区定向站,目标小区室分站
				// getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos(0) -
				// FDISLEVER * Math.cos((nSHoriz - dbAngle) * PI / 180.0)));
				getEffDistance = (dbDis * (1 + FDISLEVER * (1 - Math.cos((nSHoriz - dbAngle) * PI / 180.0)))) / 1000;
				return getEffDistance;
			} else if ((shSType == 3) && (shNType == 1)) { // 主小区室分站,目标小区全向站
				// getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos(0) -
				// FDISLEVER * Math.cos(180)));
				getEffDistance = (dbDis * (1 + FDISLEVER * 2))  / 1000 ;
				return getEffDistance;
			} else if ((shSType == 3) && (shNType == 2)) { // 主小区室分站,目标小区定向站
				getEffDistance = (dbDis
						* (1 + FDISLEVER * Math.cos((nNHoriz - dbAngle) * PI / 180.0) - FDISLEVER * Math.cos(0)))  / 1000;
				return getEffDistance;
			} else { // 主小区室分站,目标小区室分站
				// getEffDistance = (dbDis * (1 + FDISLEVER * Math.cos(0) -
				// FDISLEVER * Math.cos(0)));
				getEffDistance = dbDis  / 1000;
			}
		} catch (Exception e) {
			return getEffDistance;
		}
		return getEffDistance;
	}
}
