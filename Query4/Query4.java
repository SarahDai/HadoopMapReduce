package Query4;

import java.net.URI;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.*;

public class Query4 extends Configured implements Tool {
	public int run(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf("Two parameters are required- <input dir><output dir>n");
			return -1;
		}
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("Query4");
		DistributedCache.addCacheFile(new URI("/user/hadoop/cs561/dataset/Customer.txt"), conf);
		job.setJarByClass(Query4.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		boolean success = job.waitForCompletion(true);
		return success? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(),new Query4(), args);
	System.exit(exitCode);	
	}//main
	
}
