package cn.com.dtmobile.hadoop.biz.hangzhoufilter.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.StringUtils;

@SuppressWarnings("deprecation")
public class DataMap extends Mapper<LongWritable, Text, Text, Text> {
	Set<String> lteCellMap = new HashSet<String>();
	@SuppressWarnings("rawtypes")
	public MultipleOutputs mos;
	private final Text key = new Text();
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	    this.mos = new MultipleOutputs(context);
	}

	@Override
	public void map(LongWritable keys, Text value, Context context)
			throws IOException, InterruptedException {
		
		  if (value.getLength() > 0) {
		      String filePath = context.getInputSplit().toString().toUpperCase();
		      String[] values = value.toString().split(StringUtils.DELIMITER_INNER_ITEM,-1);
		      if (filePath.contains("S1MME")) {
        		  key.set(values[2]);
        		  context.write(key, new Text(value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_S1MME));
		      }else if (filePath.contains("SV")) {
	        		key.set(values[2]);
		        	context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP)); 
		      }else if (filePath.contains("MW")) {
	        		key.set(values[2]);
		        	context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP)); 
		      }else if (filePath.contains("HTTP")) {
	        		key.set(values[2]);
		        	context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP)); 
		      }else if (filePath.contains("GXRX")) {
	        		key.set(values[2]);
		        	context.write(key, new Text( value + StringUtils.DELIMITER_BETWEEN_ITEMS + TablesConstants.FILTER_HTTP)); 
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
		 String filePath = "/DATANG/VOLTE_ORGN/MWDATA1.CSV";
		  if (filePath.contains("VOLTE_SV")) {
			  System.out.println(1);
		  }else if (filePath.contains("RX")) {
			  System.out.println(2);
		  }else if (filePath.contains("S1MME_ORGN")) {
			  System.out.println(3);
		  }else if (filePath.contains("SGS_ORGN")) {
			  System.out.println(4);
		  }else if (filePath.contains("VOLTE_ORGN")) {
			  System.out.println(5);
		  }else if (filePath.contains("S1U_HTTP_ORGN")){
			  System.out.println(6);
		  }
		  
	}
		  
}
