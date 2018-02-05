package cn.com.dtmobile.hadoop.biz.train.job.volteuserjob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dtmobile.hadoop.biz.train.mr.votleuser.VotleUserMap;
import cn.com.dtmobile.hadoop.biz.train.mr.votleuser.VotleUserRecduce;

public class VolteJob  extends Configured implements Tool  {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration() ;
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length!=3){
			System.err.println("Usage: VolteUserJob <in> <out>");
			System.exit(2);
		}
		conf.set("hour", otherArgs[2]);
		
		Job job = Job.getInstance(conf) ;
		
		job.setJarByClass(VolteJob.class);
		job.setMapperClass(VotleUserMap.class);
		job.setReducerClass(VotleUserRecduce.class);
		job.setNumReduceTasks(4);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		try {
			int returnCode = ToolRunner.run(new VolteJob(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
