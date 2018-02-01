package cn.com.dtmobile.hadoop.biz.exception.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.com.dtmobile.hadoop.biz.exception.Pservice.MwService;
import cn.com.dtmobile.hadoop.biz.exception.Pservice.RxgxService;
import cn.com.dtmobile.hadoop.biz.exception.Pservice.S1mmeService;
import cn.com.dtmobile.hadoop.biz.exception.Pservice.UuService;
import cn.com.dtmobile.hadoop.biz.exception.Pservice.X2Service;
import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.StringUtils;

/** 
 * @完成功能:将回填Etype功能Reduce合并
 * @创建时间:2017年3月27日 上午10:01:28 
 * @param    
 */
public class CommnoProcessReduce extends Reducer<Text, Text, Text, Text>{
	  private MultipleOutputs<NullWritable,Text> outputs;
	  private RxgxService rxgxService = null;
	  private S1mmeService s1mmeService ;
	  private MwService mwService ;
	  private UuService uuService ;
	  private X2Service x2Service ;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		outputs = new MultipleOutputs(context);  
		rxgxService = new RxgxService() ;
		s1mmeService = new S1mmeService() ;
		mwService = new MwService() ;
		uuService = new UuService() ;
		x2Service = new X2Service() ;
	}

	@Override
	protected void reduce(Text arg0, Iterable<Text> v,
			Context context) throws IOException,InterruptedException {
		String tableName = null ;
	
		ArrayList<Text> valueList  = new ArrayList<Text>() ;
		ArrayList<Text> S1mmeList = new ArrayList<Text>() ;
		
		
		for(Text t:v){
			valueList.add(new Text(t)) ;
			String[] values = t.toString().split(StringUtils.DELIMITER_BETWEEN_ELEMENT,-1) ;
			tableName = values[1] ;
			if(tableName.equals(TablesConstants.HIVE_S1MME)){
				S1mmeList.add(new Text(values[0])) ;
			}
		}

			
		
		
	for(Text vl:valueList){
			String[] values = vl.toString().split(StringUtils.DELIMITER_BETWEEN_ELEMENT) ;
			tableName = values[1] ;
			Text value = new Text(values[0]) ;
			if(tableName.equals(TablesConstants.HIVE_GXRX)){
				//输出GxRx
				//outputs.write("rx", NullWritable.get(), rxgxService.rxgxService(value),conf.get("output")+"/"+TablesConstants.HIVE_GXRX+"/");	
				outputs.write(NullWritable.get(), rxgxService.rxgxService(value), "rx");
			}else if(tableName.equals(TablesConstants.HIVE_S1MME)){
				//输出S1mme
//				outputs.write("S1mme", NullWritable.get(), s1mmeService.s1mmeService(value),conf.get("output")+"/"+TablesConstants.HIVE_S1MME+"/");
				outputs.write(NullWritable.get(), s1mmeService.s1mmeService(value,S1mmeList), "S1MME_ORGN");
			}else if(tableName.equals(TablesConstants.HIVE_MW)){
				//输出MW
//				outputs.write("mw", NullWritable.get(),mwService.MwService(value),conf.get("output")+"/"+TablesConstants.HIVE_MW+"/");
				outputs.write(NullWritable.get(), mwService.MwService(value), "VOLTE_ORGN");
			}else if(tableName.equals(TablesConstants.HIVE_UU)){
				//输出UU
//				outputs.write("uu", NullWritable.get(),uuService.uuService(value),conf.get("output")+"/"+TablesConstants.HIVE_UU+"/");
				outputs.write(NullWritable.get(), uuService.uuService(value), "TB_XDR_IFC_UU");
			}else if(tableName.equals(TablesConstants.HIVE_X2)){
				//输出X2
//				outputs.write("x2", NullWritable.get(),x2Service.x2Service(value),conf.get("output")+"/"+TablesConstants.HIVE_X2+"/");
				outputs.write(NullWritable.get(), x2Service.x2Service(value), "TB_XDR_IFC_X2");
			}else{
				return ;
			}
		}
		
		
		
		
	
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		outputs.close();
	}

	
}
