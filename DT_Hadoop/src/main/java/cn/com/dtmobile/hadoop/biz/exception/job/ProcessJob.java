package cn.com.dtmobile.hadoop.biz.exception.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dtmobile.hadoop.biz.exception.mr.CommnoProcessMap;
import cn.com.dtmobile.hadoop.biz.exception.mr.CommnoProcessReduce;

/** 
 * @完成功能:
 * @创建时间:2017年3月27日 上午11:06:32 
 * @param    
 */
public class ProcessJob extends Configured implements Tool  {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, arg0).getRemainingArgs();
		
		int reduceNum = 15 ;
		if (otherArgs.length < 6) {
			System.err.println("Usage: ProcessJob <in> <out>");
			System.exit(2);
		}
		
		if(otherArgs.length==7){
			reduceNum = Integer.valueOf(otherArgs[6]) ;
		}
			
		FileSystem hdfs = FileSystem.get(conf) ;
		
		hdfs.delete(new Path(otherArgs[5]), true) ;
		
		conf.set("output", otherArgs[5]);
/*		conf.set("day", otherArgs[6]);
		conf.set("hour", otherArgs[7]);*/
		Job job = Job.getInstance(conf) ;
		job.setJobName("EtypeFillJob");
		job.setJarByClass(ProcessJob.class);
		job.setMapperClass(CommnoProcessMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(CommnoProcessReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(reduceNum) ;
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
		
//		MultipleOutputs.addNamedOutput(job,"rx", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job,"S1mme", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job,"mw", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job,"uu", TextOutputFormat.class, NullWritable.class, Text.class);
//		MultipleOutputs.addNamedOutput(job,"x2", TextOutputFormat.class, NullWritable.class, Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));
		
		return job.waitForCompletion(true)?0:1;
	}

	
	public static void main(String[] args) {
		try {
			int returnCode = ToolRunner.run(new ProcessJob(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
}
