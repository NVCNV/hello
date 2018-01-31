package cn.com.dtmobile.hadoop.biz.train.mr.votleuser;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VotleUserRecduce extends Reducer<Text, Text, NullWritable, Text>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2) throws IOException,
			InterruptedException {
		for(Text volte:arg1){
			if(volte!=null){
				arg2.write(NullWritable.get(), volte);
				break ;
			}
		}
	}
}
