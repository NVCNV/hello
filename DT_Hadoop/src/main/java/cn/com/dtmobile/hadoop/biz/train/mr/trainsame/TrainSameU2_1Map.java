package cn.com.dtmobile.hadoop.biz.train.mr.trainsame;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.com.dtmobile.hadoop.util.StringUtils;

public class TrainSameU2_1Map extends Mapper<LongWritable, Text, Text, Text> {
	public final Text key = new Text();
	@Override
	protected void map(LongWritable inKey, Text value,Context context)throws IOException, InterruptedException {
		if(value.getLength() > 0){
			String [] values = value.toString().split(StringUtils.DELIMITER_INNER_ITEM);
			//cellid+targetcellid+imsi+dir
//			System.out.println("cellid: " +values[2] + " \t targetcellid : " +values[3] +" \t imsi : " +values[0] +"\t dir : " +values[4] +"\t groupName : "+ values[8] );
			key.set(values[2] + StringUtils.DELIMITER_INNER_ITEM + values[3]  + StringUtils.DELIMITER_INNER_ITEM + values[0] + StringUtils.DELIMITER_INNER_ITEM + values[4]);
			context.write(key,new Text(values[8]));
		}
	}
}
