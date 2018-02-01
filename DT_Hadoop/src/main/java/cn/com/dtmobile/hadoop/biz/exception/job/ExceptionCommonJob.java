package cn.com.dtmobile.hadoop.biz.exception.job;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dtmobile.hadoop.biz.exception.mr.CommonMaps;
import cn.com.dtmobile.hadoop.biz.exception.mr.ExceptionCommonReduce;

@SuppressWarnings("deprecation")
public class ExceptionCommonJob extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 8) {
			System.err.println("Usage: ExceptionCommonJob <in> <out>");
			System.exit(2);
		}
		
		//use DistributedCache
		int reduceNum = 30  ;
		/*if(otherArgs.length==11){
			reduceNum = Integer.valueOf(otherArgs[11]) ;
		}*/
		
		
		DistributedCache.addCacheFile(new URI(otherArgs[8]), conf);  //exception
		DistributedCache.addCacheFile(new URI(otherArgs[9]), conf);  //cellMR
		DistributedCache.addCacheFile(new URI(otherArgs[10]), conf);  //lteCell
		//小区与邻区关系表
		DistributedCache.addCacheFile(new URI(otherArgs[11]), conf);  //nCellInfo
		//配置文件路径
		DistributedCache.addCacheFile(new URI(otherArgs[12]), conf); //配置文件
		
		 SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss") ;
		String appName = "ExceptionAnaJob-"+df.format(new Date()) ;
		Job job = new Job(conf, appName);
		job.setJarByClass(ExceptionCommonJob.class);
		job.setMapperClass(CommonMaps.class);
		job.setReducerClass(ExceptionCommonReduce.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[5]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[6]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[7]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		try {
			int returnCode = ToolRunner.run(new ExceptionCommonJob(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}