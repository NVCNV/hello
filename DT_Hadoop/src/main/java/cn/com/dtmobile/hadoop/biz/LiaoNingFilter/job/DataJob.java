package cn.com.dtmobile.hadoop.biz.LiaoNingFilter.job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dtmobile.hadoop.biz.LiaoNingFilter.mr.DataMap;
import cn.com.dtmobile.hadoop.biz.LiaoNingFilter.mr.DataReduce;
import cn.com.dtmobile.hadoop.util.StringUtils;

@SuppressWarnings("deprecation")
public class DataJob extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class,
				CompressionCodec.class);
		//TODO 本地测试需要注释
		conf.set("fs.defaultFS", "hdfs://nameservice1:8020");
		// conf.set("dfs.nameservices", "nameservice1");
		// conf.set("dfs.ha.namenodes.nameservice1", "namenode119,namenode149");
		// conf.set("dfs.namenode.rpc-address.nameservice1.namenode119",
		// "192.168.32.50:8020");
		// conf.set("dfs.namenode.rpc-address.nameservice1.namenode149",
		// "192.168.32.51:8020");
		// conf.set("dfs.client.failover.proxy.provider.nameservice1",
		// "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// if (otherArgs.length < 2) {
		// System.err.println("Usage: DataJob <in> <out>");
		// System.exit(2);
		// }

		String sysHour = otherArgs[8];

		// use DistributedCache
		DistributedCache.addCacheFile(new URI(otherArgs[7]), conf); // cellid
		Job job = new Job(conf, "LiaoNingFilter");
		job.getConfiguration().setStrings("sys_hour", sysHour);
		job.setJarByClass(DataJob.class);
		job.setMapperClass(DataMap.class);
		job.setReducerClass(DataReduce.class);
		job.setNumReduceTasks(100);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, "volterx", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "s1mmeorgn",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "voltesv", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sgsorgn", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "s1uhttporgn",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "volteorgn",
				TextOutputFormat.class, NullWritable.class, Text.class);
		
		MultipleOutputs.addNamedOutput(job, "sharevolterx", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "shares1mme",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sharehttp",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sharevolteorgn",
				TextOutputFormat.class, NullWritable.class, Text.class);

//		FileSystem hdfs = FileSystem.get(conf);
//
//		Path path0 = new Path(otherArgs[0]);
//		Path path1 = new Path(otherArgs[1]);
//		Path path2 = new Path(otherArgs[2]);
//		Path path3 = new Path(otherArgs[3]);
//		Path path4 = new Path(otherArgs[4]);
//		Path path5 = new Path(otherArgs[5]);

		// if (hdfs.exists(path0)) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// }
		// if (hdfs.exists(path1)) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		// }
		// if (hdfs.exists(path2)) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		// }
		// if (hdfs.exists(path3)) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		// }
		// if (hdfs.exists(path4)) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
		// }
		// if (hdfs.exists(path5)) {
		FileInputFormat.addInputPath(job, new Path(otherArgs[5]));
		// }

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[6]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		try {
			int returnCode = ToolRunner.run(new DataJob(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
