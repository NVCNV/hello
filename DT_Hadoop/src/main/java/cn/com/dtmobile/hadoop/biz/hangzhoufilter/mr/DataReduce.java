package cn.com.dtmobile.hadoop.biz.hangzhoufilter.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cn.com.dtmobile.hadoop.constants.TablesConstants;
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
		for (Text text : values) {
			String arr[] = text.toString().split(StringUtils.DELIMITER_BETWEEN_ITEMS);
			String tableName = arr[1];
			String data = arr[0];
			if (tableName.contains(TablesConstants.FILTER_RX)) {
				mos.write("volterx", NullWritable.get(), new Text(data),"");
			} else if (tableName.contains(TablesConstants.FILTER_S1MME)) {
				mos.write("s1mmeorgn",  NullWritable.get(), new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_SV)) {
					mos.write("voltesv",  NullWritable.get(), new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_HTTP)){
					mos.write("s1uhttporgn",  NullWritable.get(), new Text(data));
			} else if (tableName.contains(TablesConstants.FILTER_VOLTE)){
					mos.write("volteorgn", NullWritable.get(), new Text(data));
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
