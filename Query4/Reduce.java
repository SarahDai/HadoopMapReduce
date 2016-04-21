package Query4;

import java.io.*;
import java.util.*;

import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class Reduce extends Reducer<Text, Text, Text, Text>{
	private static HashMap<String, Double> CustTransMap = new HashMap<String, Double>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		String[] result = {"-1", "-1", "-1", "-1"};
		
		for (Text info:values){
		StringTokenizer sub = new StringTokenizer(info.toString(),",");
		String[] subresult = {"0", "0", "0", "0", "0"};
		int i = 0;
		while(sub.hasMoreTokens()){
			subresult[i++] = sub.nextToken().toString();
		}
		
//		while (values.hasNext()){
//			String mapjoin = values.next().toString();			
//			String[] subresult = mapjoin.split(",");
			if(CustTransMap.containsKey(subresult[0])){
				double newAmount = Double.parseDouble(subresult[1]) + CustTransMap.get(subresult[0]);
				CustTransMap.put(subresult[0].trim(), newAmount);
			}
			else{
				CustTransMap.put(subresult[0].trim(), Double.parseDouble(subresult[1]));
				count++;
			}
		}
		
		Collection c = CustTransMap.values();
		result[1] = Integer.toString(count);
		result[2] = Collections.min(c).toString();
		result[3] = Collections.max(c).toString();
		CustTransMap.clear();
		context.write(key, new Text(result[1]+","+result[2]+","+result[3]));
	}
}
