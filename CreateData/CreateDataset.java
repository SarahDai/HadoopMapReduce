import java.io.*;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang.RandomStringUtils;

public class CreateDataset {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		// Creating Customer file
		
		PrintWriter writer = new PrintWriter("Customer.txt", "UTF-8");
		String custName;
		int age;
		int randomNameLen;
		int countryCode;
		double salary;
		
		for(int i = 1; i<= 50000; i++)
		{
			randomNameLen = ThreadLocalRandom.current().nextInt(10,21);
			
			custName = RandomStringUtils.randomAlphabetic(randomNameLen);
			
			age = ThreadLocalRandom.current().nextInt(10,71);
			
			countryCode = ThreadLocalRandom.current().nextInt(1,11); 
			
			salary = ThreadLocalRandom.current().nextDouble(100, 10001);
			
			writer.println(i + "," + custName + "," + age + "," + countryCode + "," + salary);
		}
		
		writer.close();

		// Creating Transactions Dataset
		
		PrintWriter transactionWriter = new PrintWriter("Transaction.txt", "UTF-8");
		int custID;
		double transTotal;
		int transNumItems;
		int transDescLen;
		String transDesc;
		
		for(int transID = 1; transID <= 5000000; transID++)
		{
			custID = ThreadLocalRandom.current().nextInt(1,50001);
			transTotal = ThreadLocalRandom.current().nextDouble(10, 1001);
			transNumItems = ThreadLocalRandom.current().nextInt(1, 11);
			
			transDescLen = ThreadLocalRandom.current().nextInt(20, 51);
			
			transDesc = RandomStringUtils.randomAlphabetic(transDescLen);
			
			transactionWriter.println(transID + "," + custID + "," + transTotal + "," + transNumItems + "," + transDesc );
		}
		
		transactionWriter.close();
	}

}
