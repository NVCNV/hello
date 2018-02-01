package cn.com.dtmobile.hadoop.biz.LiaoNingFilter.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.model.GxXdr;
import cn.com.dtmobile.hadoop.model.HttpXdr;
import cn.com.dtmobile.hadoop.model.MwXdr;
import cn.com.dtmobile.hadoop.model.S1mmeXdr;
import cn.com.dtmobile.hadoop.util.StringUtils;

@SuppressWarnings("rawtypes")
public class DataReduce extends Reducer<Text, Text, NullWritable, Text> {
	public MultipleOutputs mos;
	public final Text key = new Text();
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs(context);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void reduce(Text inputKey, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
//		String tableName = values.toString().split(StringUtils.DELIMITER_INNER_ITEM5)[1];
		for (Text text : values) {
			String arr[] = text.toString().split(StringUtils.DELIMITER_BETWEEN_ITEMS);
			String tableName = arr[1];
			String data = arr[0];
			if (tableName.contains(TablesConstants.FILTER_RX)) {
				  GxXdr gxXdr = new GxXdr(data.split(StringUtils.DELIMITER_INNER_ITEM,-1),1);
				mos.write("volterx", NullWritable.get(), new Text(gxXdr.toString()));
				mos.write("sharevolterx", NullWritable.get(),new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_S1MME)) {
				
				  String line = data.replaceAll(",", "|");
		          S1mmeXdr s1mme = new S1mmeXdr(line.split(StringUtils.DELIMITER_INNER_ITEM,-1),1);
				  String newText = s1mme.toStringShare().replaceAll(StringUtils.DELIMITER_INNER_ITEM,StringUtils.DELIMITER_VERTICAL) ;
					
				mos.write("s1mmeorgn",  NullWritable.get(), new Text(newText));
				mos.write("shares1mme",  NullWritable.get(), new Text(data.replaceAll(StringUtils.DELIMITER_COMMA, StringUtils.DELIMITER_VERTICAL).replaceAll(StringUtils.DELIMITER_INNER_ITEM4, StringUtils.DELIMITER_VERTICAL)));
					
			
			} else if (tableName.contains(TablesConstants.FILTER_SV)) {
					mos.write("voltesv",  NullWritable.get(), new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_SGS)) {
					mos.write("sgsorgn",  NullWritable.get(), new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_HTTP)){
				
				HttpXdr http = new HttpXdr(data.split(StringUtils.DELIMITER_INNER_ITEM,-1));
					mos.write("s1uhttporgn",  NullWritable.get(), new Text(http.toString()));
//					mos.write("sharehttp",  NullWritable.get(), new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_VOLTE)){
				
				    MwXdr mwXdr = new MwXdr(data.split(StringUtils.DELIMITER_INNER_ITEM,-1),1);
					mos.write("volteorgn", NullWritable.get(), new Text(mwXdr.toString()));
					mos.write("sharevolteorgn", NullWritable.get(), new Text(data));
			}
		}
		
		
		/*if (tableName.contains(TablesConstants.FILTER_RX)) {
			for (Text text : values) {
				mos.write("volterx", NullWritable.get(), text);
			}
		} else if (tableName.contains(TablesConstants.FILTER_S1MME)) {
			for (Text text : values) {
				String newText = text.toString().replaceAll(StringUtils.DELIMITER_INNER_ITEM,
						StringUtils.DELIMITER_VERTICAL) ;
				mos.write("s1mmeorgn",  NullWritable.get(), new Text(newText));
			}
		} else if (tableName.contains(TablesConstants.FILTER_SV)) {
			for (Text text : values) {
				mos.write("voltesv",  NullWritable.get(), text);
			}
		} else if (tableName.contains(TablesConstants.FILTER_SGS)) {
			for (Text text : values) {
				mos.write("sgsorgn",  NullWritable.get(), text);
			}
		} else if (tableName.contains(TablesConstants.FILTER_HTTP)){
			for (Text text : values) {
				mos.write("s1uhttporgn",  NullWritable.get(), text);
			}
		} else if (tableName.contains(TablesConstants.FILTER_VOLTE)){
			for (Text text : values) {
				mos.write("volteorgn", NullWritable.get(), text);
			}
		}else {
			return;
		}*/
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
