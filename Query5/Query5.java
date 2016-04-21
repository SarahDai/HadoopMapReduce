import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;

public class TwoFiles extends Configured implements Tool {

	public static class TransMapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
			String line = value.toString();
			String[] column = line.split(",");
			
			String custID = column[1];
			
			output.write(new Text(custID+"T"), new Text("1"));
		
	}
	}
	
	public static class CustMapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
			String line = value.toString();
			String[] column = line.split(",");
			
			String custID = column[0];
			String custName = column[1];
			
			output.write(new Text(custID+"C"), new Text(custName));
		
	}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		
		private static final SortedSet<Map.Entry<Integer, Integer>> sortedTransSet = new TreeSet<Map.Entry<Integer, Integer>>(
	            new Comparator<Map.Entry<Integer, Integer>>() {
	                @Override
	                public int compare(Map.Entry<Integer, Integer> e1,
	                        Map.Entry<Integer, Integer> e2) {
	                    return e1.getValue().compareTo(e2.getValue());
	                }
	            });
		
		private static final SortedSet<Map.Entry<Integer, String>> sortedCustSet = new TreeSet<Map.Entry<Integer, String>>(
	            new Comparator<Map.Entry<Integer, String>>() {
	                @Override
	                public int compare(Map.Entry<Integer, String> e1,
	                        Map.Entry<Integer, String> e2) {
	                    return e1.getKey().compareTo(e2.getKey());
	                }
	            });

	   private static final SortedMap<Integer, Integer> transCustIDCount = new TreeMap<Integer, Integer>();
	   private static final SortedMap<Integer, String> custIDName = new TreeMap<Integer, String>();
		
		//@Override
	     public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

	    	 int sum = 0;
	    	 
	    	 String custID_src = key.toString();
	    	 
	    	 if(custID_src.substring(custID_src.length()-1).equals("T"))
	    	 {
	    		 for (Text value: values)
	    		 {
	    			 sum += Integer.parseInt(value.toString());
	    		 }
			
	    		 //output.write(key, new Text(Integer.toString(sum)));
	    	 
	    		 //output.write(key, new Text(custID_src.length()+"|"+custID_src.substring(custID_src.length()-1)));
	    	 
	    		 transCustIDCount.put(Integer.parseInt(custID_src.substring(0, custID_src.length()-1)), sum);
	    	 }
	    	 
	    	 if(custID_src.substring(custID_src.length()-1).equals("C"))
	    	 {
	    		
	    		 //String value = values.toString();
	    		 
	    		 String nameStr = "";
	    		 
	    		 for(Text name: values)
	    		 {
	    			 nameStr = nameStr + name;
	    		 }
	    		 
	    		 custIDName.put(Integer.parseInt(custID_src.substring(0, custID_src.length()-1)), nameStr);
	    	 }
	    	 
		}
	     
	     @Override
	     protected void cleanup(Context context) throws IOException, InterruptedException{
	    	 //IntWritable k = new IntWritable();
	    	 sortedTransSet.addAll(transCustIDCount.entrySet());
	    	 
	    	 SortedMap<Integer, Integer> leastCustIDCount = new TreeMap<Integer, Integer>();
	     
	    	 Entry<Integer,Integer> firstRec = sortedTransSet.first();
	    	 
	    	 Iterator<Entry<Integer,Integer>> i = sortedTransSet.iterator();
	    	 
	    	 Integer custID;
	    	 Integer count;
	    	 Entry<Integer, Integer> rec;
	    	 
	    	 while(i.hasNext())
	    	 {
	    		 rec = i.next();
	    		 custID = rec.getKey();
	    		 count = rec.getValue();
	    				 
	    		 if (count == firstRec.getValue()) {
					//context.write(new Text(custID.toString()), new Text(count.toString()));
					leastCustIDCount.put(custID, count);
				}
	    		 else
	    			 break;
	    		 
	    	 }
	    	 	 
	    	 sortedCustSet.addAll(custIDName.entrySet());
	    	 
	    	 Iterator<Entry<Integer,Integer>>leastCustIterator = leastCustIDCount.entrySet().iterator();
	    	 
	    	 Entry<Integer,Integer> leastRec;
	    	 Integer leastCustID;
	    	 Integer leastCustCount;
	    	 
	    	 Entry<Integer,String> allCust;
	    	 Integer allCustID;
	    	 String allCustName;
	    	 
	    	 //context.write(new Text(""), new Text(""));
	    	 
	    	 while(leastCustIterator.hasNext())
	    	 {
	    		 Iterator<Entry<Integer,String>> custIDNameIterator = sortedCustSet.iterator();
	    		 
	    		 leastRec = leastCustIterator.next();
	    		 leastCustID = leastRec.getKey();
	    		 leastCustCount = leastRec.getValue();
	    		 
	    		 //context.write(new Text(leastCustID.toString()), new Text(leastCustCount.toString()));
	    		 
	    		 allCustID = 0;
	    		 
	    		 //context.write(new Text("Now From Cust"), new Text(""));
	    		 
	    		 while(custIDNameIterator.hasNext() && allCustID < leastCustID)
	    		 {
	    			allCust = custIDNameIterator.next();
	    			allCustID = allCust.getKey();
	    			allCustName = allCust.getValue();
	    			
	    			//context.write(new Text(allCustID.toString()), new Text(allCustName));
	    			
	    			//context.write(new Text(allCustID.toString()), new Text(leastCustID.toString()));
	    			
	    			if(leastCustID.equals(allCustID))
	    			{
	    				//context.write(new Text("If succeeded"),new Text(""));
	    				context.write(new Text(allCustName), new Text(leastCustCount.toString()));
	    			}
	    			
	    		 }
	    	 }
	    	 
	    	 
	     }
	    
	}
	
	     public int run(String[] args) throws Exception {
			    Configuration conf = getConf();
			    Job job = new Job(conf, "TwoFiles");
			    job.setJarByClass(TwoFiles.class);

			    Path custIn = new Path(args[0]);
			    Path transIn = new Path(args[1]);
			    Path out = new Path(args[2]);
			    
			    MultipleInputs.addInputPath(job, custIn, TextInputFormat.class, CustMapClass.class);
			    MultipleInputs.addInputPath(job, transIn, TextInputFormat.class, TransMapClass.class);
			    
			    //FileInputFormat.setInputPaths(job, in);
			    FileOutputFormat.setOutputPath(job, out);

			    //job.setMapperClass(TransMapClass.class);
			    job.setReducerClass(ReduceClass.class);
			    job.setNumReduceTasks(1);

			    //job.setInputFormatClass(TextInputFormat.class);
			    job.setOutputFormatClass(TextOutputFormat.class);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(Text.class);

			    System.exit(job.waitForCompletion(true)?0:1);

			    return 0;

			  } //~run

			  public static void main(String[] args) throws Exception {
			     int res = ToolRunner.run(new Configuration(), new TwoFiles(), args);

			     System.exit(res);
			  } //~main
}
