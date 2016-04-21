package Query4;

import java.io.*;
import java.util.*;

import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends 
       Mapper<LongWritable, Text, Text, Text>{
	
	private static HashMap<String, String> CustomerMap =  new HashMap<String, String>();
	private BufferedReader brReader;
	private String strCountryCode = "";
	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");
	
	enum MYCOUNTER{
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}
	
	protected void setup(Context context) throws IOException, InterruptedException{
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path eachPath: cacheFilesLocal){
			if (eachPath.getName().toString().trim().equals("Customer.txt")){
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadCustomersHashMap(eachPath, context);
			}
		}
	}
	
	private void loadCustomersHashMap(Path filePath, Context context) throws IOException{
		String strLineRead = "";
		try{
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			 while ((strLineRead = brReader.readLine()) != null) {
				 String custFieldArray[] = strLineRead.split(",");
				 CustomerMap.put(custFieldArray[0].trim(), custFieldArray[3].trim());
			 }			 
		} catch (FileNotFoundException e){
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e){
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		} finally{
			if (brReader != null){
				brReader.close();
			}
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
		if (value.toString().length() > 0){
			String arrEmpAttributes[] = value.toString().split(",");
			try{
				strCountryCode = CustomerMap.get(arrEmpAttributes[1].toString());
			} finally{
				strCountryCode = ((strCountryCode.equals(null) || strCountryCode.equals("")) ? "NOT_FOUND": strCountryCode);
			}
			txtMapOutputKey.set(strCountryCode);
			txtMapOutputValue.set(arrEmpAttributes[1] + "," + arrEmpAttributes[2].toString());
		}
		context.write(txtMapOutputKey, txtMapOutputValue);
		}
	
	
	}


