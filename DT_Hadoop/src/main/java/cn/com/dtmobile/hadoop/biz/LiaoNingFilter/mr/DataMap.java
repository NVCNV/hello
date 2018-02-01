package cn.com.dtmobile.hadoop.biz.LiaoNingFilter.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import cn.com.dtmobile.hadoop.model.HttpXdr;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.com.dtmobile.hadoop.biz.exception.constants.LtecellConstants;
import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.DateUtils;
import cn.com.dtmobile.hadoop.util.ParseUtils;
import cn.com.dtmobile.hadoop.util.StringUtils;

@SuppressWarnings("deprecation")
public class DataMap extends Mapper<LongWritable, Text, Text, Text> {
	Set<String> lteCellMap = new HashSet<String>();
	// private Text key = new Text();
	@SuppressWarnings("rawtypes")
	public MultipleOutputs mos;
	private final Text key = new Text();
	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	    this.mos = new MultipleOutputs(context);
	    Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	    BufferedReader volte_data = new BufferedReader(new FileReader(
	      paths[0].toString()));
	    String lines_volte = null;
	    volte_data.readLine();
	    while (StringUtils.isNotEmpty(lines_volte = volte_data.readLine()))
	    {
	      int cellid = 
	        ParseUtils.parseInteger(lines_volte.split(",")[9]).intValue();
	      this.lteCellMap.add(String.valueOf(cellid));
	    }
	}

	@SuppressWarnings("unchecked")
	@Override
	public void map(LongWritable keys, Text value, Context context)
			throws IOException, InterruptedException {
		
		  if (value.getLength() > 0) {
		      String filePath = context.getInputSplit().toString().toUpperCase();
		      String[] current_hour = context.getConfiguration().getStrings("sys_hour");
		      String[] values = value.toString().split(StringUtils.DELIMITER_INNER_ITEM,-1);
//		      String[] values = value.toString().split(StringUtils.DELIMITER_COMMA,-1);
		      if (filePath.contains("VOLTE_SV")) {
		        if ((values.length > 36) && (values[9].length() == 13) && (StringUtils.isNumeric(values[9]))){
		          if (DateUtils.stampToDate_HH(Long.valueOf(values[9]).longValue()).equals(current_hour[0])){
		        	  if(values[1].equals(LtecellConstants.CITY_DANDONG)){
//		        		  this.mos.write("voltesv", NullWritable.get(), value);
		        		  key.set(values[5]);
		        		  context.write(key, new Text(value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_SV));
		        	  }
		          }
		        }
		      }else if (filePath.contains("VOLTE_RX")) {
		        if ((values.length > 36) && (values[9].length() == 13) && (StringUtils.isNumeric(values[9]))){
		          if (DateUtils.stampToDate_HH(Long.valueOf(values[9]).longValue()).equals(current_hour[0]))
		        	  if(values[1].equals(LtecellConstants.CITY_DANDONG)){
//		        		  this.mos.write("volterx", NullWritable.get(), value);
//		        		  GxXdr gxXdr = new GxXdr(values,1);
		        		  key.set(values[5]);
//		        		  context.write(key, new Text( gxXdr.toString() + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_RX));
		        		  context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_RX)); 
		        	  }
		        }
		      }else if (filePath.contains("S1MME_ORGN")) {
		        if ((values.length > 33) && (values[13].length() == 13) && (StringUtils.isNumeric(values[13]))){
		          if ((DateUtils.stampToDate_HH(Long.valueOf(values[13]).longValue()).equals(current_hour[0])) && (this.lteCellMap.contains(values[44]))) {
//		            String line = value.toString().replaceAll(",", "|");
//		            S1mmeXdr s1mme = new S1mmeXdr(line.split(StringUtils.DELIMITER_INNER_ITEM),1);
//		            this.mos.write("s1mmeorgn", NullWritable.get(),new Text(s1mme.toStringShare()));
		            key.set(values[9]);
//		            context.write(key, new Text( s1mme.toStringShare() + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_S1MME));
		            context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_S1MME));
		          
		          }
		        }
		      }else if (filePath.contains("SGS_ORGN")) {
		        if ((values.length > 28) && (values[9].length() == 13) && (StringUtils.isNumeric(values[9]))){
		          if ((DateUtils.stampToDate_HH(Long.valueOf(values[9]).longValue()).equals(current_hour[0])) && (this.lteCellMap.contains(values[28]))) {
//		            this.mos.write("sgsorgn", NullWritable.get(), value);
		        	  key.set(values[5]);
		        	  context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_SGS));
		          }
		        }
		      }else if (filePath.contains("VOLTE_SIP")) {
		        if ((values.length > 81) && (values[9].length() == 13) && (StringUtils.isNumeric(values[9]))){
		          if (DateUtils.stampToDate_HH(ParseUtils.parseLong(values[9])).equals(current_hour[0])){
		        	  if(values[1].trim().equals(LtecellConstants.CITY_DANDONG)){
//		        		  MwXdr mwXdr = new MwXdr(values,1);
//		        		  this.mos.write("volteorgn", NullWritable.get(), new Text(mwXdr.toString()));
		        		  key.set(values[5]);
//		        		  context.write(key, new Text( mwXdr.toString() + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_VOLTE));
		        	 
		        		  context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_VOLTE));
		        	  }
		          }
		        }
		      }else if (filePath.contains("S1U_HTTP_ORGN") && (values.length == 96 ) && !(value.toString().contains("gis-rss-cx.sf")) && (values[25].length() == 16) && (StringUtils.isNumeric(values[25]))){
		        if ((DateUtils.stampToDate_HH(Long.valueOf(values[25]).longValue()/1000).equals(current_hour[0])) && (this.lteCellMap.contains(values[22]))){
		        	key.set(values[9]);
//		        	context.write(key, new Text( http.toString() + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP));
//		        	 context.write(new Text(TablesConstants.FILTER_HTTP), value);
		        	context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP));
		        }
		      }
		    }
		  
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
	
	
	public static void main(String[] args) throws IOException {
		/*File file = new File("C:\\Users\\zhangchao15\\Desktop\\山西\\123.csv");
		 BufferedReader br = null;
		 br = new BufferedReader(new FileReader(file));
		 String line = null;
		 StringBuffer sb = new StringBuffer();
		 List<String> list = new ArrayList<String>();
		 List<String> error = new ArrayList<String>();
		 while((line = br.readLine())!=null){
			 list.add(line);
         }
		 int i =0;
		 int j=0;
		 for (String s : list) {
			 String[] values = s.split(",",-1);
			 j++;
			 if ((values.length > 81) && (values[9].length() == 13) && (StringUtils.isNumeric(values[9]))){
		          if (DateUtils.stampToDate_HH(Long.valueOf(values[9]).longValue()).equals("18")){
		        	  if(values[1].equals(LtecellConstants.CITY_DANDONG)){
		        		  i++;
		        	  }else{
		        		  error.add(s);
		        	  }
		          }else{
		        	  error.add(s);
		          }
			 }else{
				 error.add(s);
			 }
			
		 }
		 System.out.println(i);
		 System.out.println(j);
		 int k = 0;
		 for (String s : error) {
			String [] arr =s.split(",");
			System.out.println(arr[1]);
			if(arr[1].equals(LtecellConstants.CITY_DANDONG)){
				System.out.println(s);
			}else{
				k++;
				
			}
		}
		 System.out.println("过滤出的数据 city不是0415的总数= "+k);
		 */
		
		System.out.println(DateUtils.stampToDate_HH(Long.valueOf("1514429920672761")/1000).equals("10"));
		String value = "1803|240|0427|0|000|2|11|01000000000000060010533df52c57d3|6|460002794459353|867376022332999|8613942761427|1|100.74.246.209|100.74.185.212|221.177.30.101|2152|2152|2152|462359552|1727468620|16851|12261764|cmnet.mnc000.mcc460.gprs|103|1505969712053813|1505969712483939|122.05673000|40.74387000|1|2|1|4|0|0|10.39.106.27||39656|0|211.140.17.81||80|3533|102944|47|97|430126|315050|0|0|0|0|47437|52546|0|0|90|47605|65535|1360|1|0|1|1|1|0|0|0|3|6|200|48332|362474|430126|wap.cmread.com|83|http://wap.cmread.com/rbc/p/content/repository/ues/image/s109/20170913113531653.jpg||74|CMREADBC_Android_720*1208_V7.11(720*1208;HUAWEI;ALE-TL00;Android 6.0;cn;);|image/jpeg;charset=UTF-8|53|http://wap.cmread.com/rbc/p/test_ycpd.jsp?cm=M8010001|644|uid=cb9d05373a83478c27457340c93a7f9d; JSESSIONID=A39E8463DCEC71F3FD2883D785A9D2CC.8ngG2pC5W.2.0; cookies_user_wap_version=3; userVistorId=90048405657; isShowUserInfoMore=1; _ca_tk=nvaa1z7m8wqtajmrntrs2nqza39xsc39; _ca_is_new_user=1; _ca_starttm=1505457672127; bc_autologin_user=\"9Ficx/ul4k0gt/KWSsrxCA==\"; bc_autologin_token=bb6580ad29f1466d2fa24abaae643f472450eaf0; cmread_version_no=2.0; _gscu_1111308731=052733413q7t7e11; _gscbrs_1111308731=1; Hm_lvt_f4d78a8865b7556077738f2b1dcebf0b=1505273343; Hm_lpvt_f4d78a8865b7556077738f2b1dcebf0b=1505963607; a4526_times=10; WT_FPC=id=2f11b488ae80c454e9d1499066079658:lv=1505963879979:ss=1505963441047|97543||1|3|48332|0|5|0||0|";
		String[] values = value.split("\\|",-1);
	
		if ( (values.length >= 96) && !(value.toString().contains("gis-rss-cx.sf")) && (values[25].length() == 16) && (StringUtils.isNumeric(values[25]))){
        if ((DateUtils.stampToDate_HH(Long.valueOf(values[25]).longValue()/1000).equals("12")) ){
        	HttpXdr http = new HttpXdr(values);
        	System.out.println(http.toString());
        }
	}
	}
		  
}
