import java.net.*;
import java.text.*;
import java.util.*;
import java.io.*;

public class NetCat {

private static List<String> issuers = Arrays.asList("Tesco", "Sainsbury", "Asda Wal-Mart Stores", "Morrisons",
			"Marks & Spencer", "Boots", "John Lewis", "Waitrose", "Argos", "Co-op", "Currys", "PC World", "B&Q",
			"Somerfield", "Next", "Spar", "Amazon", "Costa", "Starbucks", "BestBuy", "Wickes", "TFL", "National Rail",
			"Pizza Hut", "Local Pub");
  
  public static void main(String args[]){
	try {
			ServerSocket serverSocket = new ServerSocket(9999);
		    Socket clientSocket = serverSocket.accept();
			PrintWriter out =
		        new PrintWriter(clientSocket.getOutputStream(), true);
	        	
        	while(true){
				
				String issuer = issuers.get(new Double(Math.random() * issuers.size()).intValue());
				
				String text = issuer + ";1\n";
				System.out.print(text);
				out.write(text);
				out.flush();
				Thread.sleep(100);				
			}
    	} catch (Throwable e) {
    		e.printStackTrace();
    	}

  }
}
