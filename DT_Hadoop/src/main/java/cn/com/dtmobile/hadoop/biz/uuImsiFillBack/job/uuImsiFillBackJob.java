package cn.com.dtmobile.hadoop.biz.uuImsiFillBack.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.com.dtmobile.hadoop.biz.uuImsiFillBack.mr.UuFillBackMap;
import cn.com.dtmobile.hadoop.biz.uuImsiFillBack.mr.UuFillBackReduce;

public class uuImsiFillBackJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration() ;
		
		Job job = Job.getInstance(conf) ;
		job.setJobName("uuImsiFillBack");
		
		job.setJarByClass(uuImsiFillBackJob.class);
		job.setMapperClass(UuFillBackMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(UuFillBackReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
//		
		job.setNumReduceTasks(5);
		
		
		
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
