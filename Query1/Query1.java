package Query1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

//Read in Customer File
public class Query1 {
	public static class Map extends MapReduceBase implements 
	              Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable ID = new IntWritable(0);
		private Text information = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)throws IOException{
			String line = value.toString();
			String[] customer = line.split(",");
			
			int CountryCode = Integer.parseInt(customer[3]);

			if (CountryCode>=2 && CountryCode<=6){
				ID.set(Integer.parseInt(customer[0]));
				String info= customer[3];
				information.set(info);
				output.collect(ID, information);
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(Query1.class);
		conf.setJobName("Query1");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
				
	}
	   
	

}
