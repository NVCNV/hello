package cn.com.dtmobile.hadoop.biz.trainfilter;

import java.util.HashMap;
import java.util.Map;


public class XdrMap {

	private static String [] s1mmeArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn","procedureType","procedureStartTime","procedureEndTime","procedureStatus","requestCause","failureCause","keyword1","keyword2","keyword3","keyword4","mmeUeS1apId","oldMmeGroupId","oldMmeCode","oldMtmsi","mmeGroupId","mmeCode","mTmsi","tmsi","userIpv4","userIpv6","mmeIpAdd","enbIpAdd","mmePort","enbPort","tac","cellId","otherTac","otherEci","apn","epsBearerNumber"}; ;
	private static String [] uuArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn","procedureType","procedureStartTime","procedureEndTime","keyword1","keyword2","procedureStatus","plmnId","enbId","cellId","crnti","targetEnbId","targetCellId","targetCrnti","mmeUeS1apId","mmeGroupId","mmeCode","mTmsi","csfbIndication","redirectednetwork","epsBearerNumber"};
	private static String [] x2Arr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn","procedureType","procedureStartTime","procedureEndTime","procedureStatus","cellId","targetCellId","enbid","targetEnbId","mmeUes1apId","mmeGroupId","mmeCode","requestCause","failureCause"};
	private static String [] mrosourceArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn"};
	private static String [] mwArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn"};
	private static String [] svArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn"};
	private static String [] gxrxArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn"};
	private static String [] httpArr = {"length","city","interfaces","xdrid","rat","imsi","imei","msisdn"};
	public static Map<String,Integer> s1mmeXdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> uuXdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> x2XdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> mrosourceXdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> mwXdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> svXdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> gxrxXdrMap = new HashMap<String,Integer>();
	public static Map<String,Integer> httpXdrMap = new HashMap<String,Integer>();
	static{
		for (int i=0 ; i < s1mmeArr.length ; i++) {
			s1mmeXdrMap.put(s1mmeArr[i], i);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		String str = "229,0415,5,000004b399bf40b8000000013cf3866c,6,460079974857915,862731038830250,8615935988032,5,1500874666369,1500874666418,0,65535,65535,3,1,3,255,54542472,515,192,3616163025,515,192,3607912319,4294967295,,,100.74.251.129,100.74.123.13,36412,36412,16700,69966848,65535,4294967295,,0,";
		String [] arr = str.split(",");
//		S1mmeXdr s1mme = new S1mmeXdr(arr,123,"");
//		System.out.println(s1mme);
	}
	
}
