/**
 * Author: Vikram Khatri vikram.khatri@us.ibm.com
 * Copyright: IBM Corporation
 */

package ibm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PingWorker extends Thread
{
	private final static String osType = (System.getProperty("os.name").toUpperCase().startsWith("WIN")) ? "WIN" : 
    	(System.getProperty("os.name").startsWith("z/OS")) ? "z/OS" : "OTHER";		
	String ipAddress, command;
	int interval;
	boolean stopMonitor = false;
	Pattern pattern;
	Matcher matcher;
	double avg, ageavergae, average = 0;
	boolean first = true;
	
	public PingWorker(String ipAddress, int interval)
	{
		this.ipAddress = ipAddress;
		this.interval = interval;
		
		if (osType.equals("WIN"))
		{
			command = "ping -n 3 " + ipAddress;
			pattern = Pattern.compile("Average = (.*)ms");
		} else if (osType.equals("z/OS"))
		{
			stopMonitor = true;
			command = "";
			pattern = null;
		} else
		{
			command = "ping " + ipAddress + " -c 3";
			pattern = Pattern.compile(" = (.*?)/(.*?)/");
		}		
	}
	
	private void getPing()
	{
		String output = IBMExtractUtilities.execCommand(command);
		if (output == null || output.length() == 0)
		{
			stopMonitor = true;
		} else
		{
			if (output.contains("Request timed out"))
				stopMonitor = true;
		}
		
		
		if (pattern != null)
		{
			matcher = pattern.matcher(output);
			if (matcher.find())
			{
	            avg = (osType.equals("WIN")) ? Double.parseDouble(matcher.group(1)) : Double.parseDouble(matcher.group(2));
	            if (first) 
	            {
	            	average = avg;
	            	first = false;
	            }
	            ageavergae = (avg + average) / 2;
	            average = avg;
			} 
		}
	}
	
	public void run()
	{		
		while (!stopMonitor)
		{
			getPing();
            long sleepTime = (1000 * interval);
            if (sleepTime > 0) 
            {
                try
				{
					Thread.sleep(sleepTime);
				} catch (InterruptedException e)
				{
					;
				}
            }
		}
	}
	
	public void shutdown()
	{
		stopMonitor = true;
	}
	
	public static void main(String[] args) 
	{
		PingWorker p = new PingWorker("192.168.142.101", 10);
        p.run();
    }
	
	public String toString()
	{
		return (stopMonitor) ? "" : Double.toString(ageavergae);
	}
}
