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

public class JoinMappersOnly33 extends Configured implements Tool {

	public static class TransMapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
			String line = value.toString();
			String[] column = line.split(",");
			
			String custID = column[1];
			String transTotal = column[2];
			String transNumItems = column[3];
			
			output.write(new Text(custID+"T"), new Text(transTotal+","+transNumItems));
		
	}
	}
	
	public static class CustMapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
			String line = value.toString();
			String[] column = line.split(",");
			
			String custID = column[0];
			String custName = column[1];
			String salary = column[4];
			
			output.write(new Text(custID+"C"), new Text(custName+","+salary));
		
	}
	}
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		
		private static final SortedSet<Map.Entry<Integer, String>> sortedTransSet = new TreeSet<Map.Entry<Integer, String>>(
	            new Comparator<Map.Entry<Integer, String>>() {
	                @Override
	                public int compare(Map.Entry<Integer, String> e1,
	                        Map.Entry<Integer, String> e2) {
	                    return e1.getKey().compareTo(e2.getKey());
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

	   private static final SortedMap<Integer, String> transCustIDCount = new TreeMap<Integer, String>();
	   private static final SortedMap<Integer, String> custIDName = new TreeMap<Integer, String>();
		
		//@Override
	     public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

	    	 int sum = 0;
	    	 Double totalSum = 0.0;
	    	 String transTotal = "";
	    	 String transNumItems = "";
	    	 int minItems = 0;
	    	 //int recMinItems;
	    	 //String column[] = new String[2];
	    	 //String line;
	    	 String salary = "";
	    	 
	    	 String custID_src = key.toString();
	    	 
	    	 if(custID_src.substring(custID_src.length()-1).equals("T"))
	    	 {
	    		 for (Text value: values)
	    		 {
	    			 String line = value.toString();
	    			 String column[] = line.split(",");
	    			 
	    			 transTotal = column[0];
	    			 transNumItems = column[1];
	    			 
	    			 sum += 1;
	    			 
	    			 totalSum += Double.parseDouble(transTotal);
	    			 
	    			 
	    			 // IMP: transNumItems is between 1 and 10 and can never be 0
	    			 if(minItems == 0 || Integer.parseInt(transNumItems) < minItems)
	    			 {
	    				 minItems = Integer.parseInt(transNumItems);
	    			 }
	    		 }
			
	    		 
	    	 
	    		 transCustIDCount.put(Integer.parseInt(custID_src.substring(0, custID_src.length()-1)), sum+","+totalSum+","+minItems);
	    	 }
	    	 
	    	 if(custID_src.substring(custID_src.length()-1).equals("C"))
	    	 {
	    		 String nameStr = "";
	    		 
	    		 for(Text custValue: values) // for loop is used as generic approach. but expecting only 1 line per customer ID
	    		 {
	    			 String line = custValue.toString();
	    			 String column[] = line.split(",");
	    			 
	    			 nameStr = column[0];
	    			 salary = column[1];
	    			 
	    		 }
	    		 
	    		 custIDName.put(Integer.parseInt(custID_src.substring(0, custID_src.length()-1)), nameStr+","+salary);
	    	 }
	    	 
		}
	     
	     @Override
	     protected void cleanup(Context context) throws IOException, InterruptedException{
	    	 
	    	 sortedTransSet.addAll(transCustIDCount.entrySet());
	    	 sortedCustSet.addAll(custIDName.entrySet());
	    	 
	    	 Iterator<Entry<Integer,String>> transIter = sortedTransSet.iterator();
	    	 Iterator<Entry<Integer,String>> custIter = sortedCustSet.iterator();
	    	 
	    	 /*Integer transKey;
	    	 Integer custKey;
	    	 
	    	 String transVal;
	    	 String custVal;*/
	    	 
	    	 Entry<Integer,String> transRec;
	    	 Entry<Integer,String> custRec;
	    	 
	    	 if(sortedTransSet.size() == 50000)
	    	 {
	    		 if(sortedCustSet.size() == 50000)
	    		 {
	    			 for(int i = 1; i<=50000; i++)
	    	    	 {
	    				 transRec = transIter.next();
	    				 custRec = custIter.next();
	    				 
	    				 //ensured the correctness of the this method by pasting the keys from both the transaction and customer dataset 
	    				 //context.write(new Text(transRec.getKey()+","+custRec.getKey()), new Text(custRec.getValue()+","+transRec.getValue()));
	    				 context.write(new Text(Integer.toString(transRec.getKey())), new Text(custRec.getValue()+","+transRec.getValue()));
	    	    	 }
	    		 }
	    		 else
	    		 {
	    			 context.write(new Text("sortedCustSet is not equal to 50000:"), new Text(Integer.toString(sortedCustSet.size())));
	    		 }
	    	 }
	    	 else
	    	 {
	    		 context.write(new Text("sortedTransSet is not equal to 50000:"), new Text(Integer.toString(sortedTransSet.size())));
	    	 }
	    	 
	     }
	    
	}
	
	     public int run(String[] args) throws Exception {
			    Configuration conf = getConf();
			    Job job = new Job(conf, "JoinMappersOnly33");
			    job.setJarByClass(JoinMappersOnly33.class);

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
			     int res = ToolRunner.run(new Configuration(), new JoinMappersOnly33(), args);

			     System.exit(res);
			  } //~main
}
