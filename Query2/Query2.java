package Query2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class Query2 {
	public static class Map extends MapReduceBase implements 
	              Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable ID = new IntWritable(0);
		private Text information = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{
			String line = value.toString();
			String[] transaction = line.split(",");
			
			String info = "1," + transaction[2];
			ID.set(Integer.parseInt(transaction[1]));
			information.set(info);
			output.collect(ID, information);
		}
	}
	
//	public static class Combiner extends MapReduceBase implements 
//	              Reducer<IntWritable, Text, IntWritable, Text>{
//		private Text outword = new Text();
//		
//		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
//			double sum = 0;
//			int count = 0;
//			while (values.hasNext()){
//				String transtotal = values.next().toString();
//				String[] subresult = transtotal.split(",");
//				double trans = Double.parseDouble(subresult[1]);
//				int number = Integer.parseInt(subresult[0]);
//				sum += trans;
//				count += number;
//			}
//			String out =Integer.toString(count) + "," +  Double.toString(sum);
//			outword.set(out);
//			output.collect(key, outword);
//		}
//	}
	
	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
		private Text outword = new Text();
		
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException{			
			double sum = 0;
			int count = 0;
			while (values.hasNext()){
				String transtotal = values.next().toString();
				String[] subresult = transtotal.split(",");
				double trans = Double.parseDouble(subresult[1]);
				int number = Integer.parseInt(subresult[0]);
				sum += trans;
				count += number;				
			}
			String out =Integer.toString(count) + ", " +  Double.toString(sum);
			outword.set(out);
			output.collect(key, outword);
		}
	}
	
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(Query2.class);
		conf.setJobName("Query2");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Combiner.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
		
}

