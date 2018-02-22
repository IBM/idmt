/**
 * Author: Vikram Khatri vikram.khatri@us.ibm.com
 * Copyright: IBM Corporation
 */

package ibm;

public class StopWatch
{

	private long startTime = -1;
	private long stopTime = -1;
	private boolean running = false;

	public StopWatch start() 
	{
		startTime = System.currentTimeMillis();
		running = true;
		return this;
	}

	public StopWatch stop() 
	{
		stopTime = System.currentTimeMillis(); 
		running = false;
		return this;
	}

	/**
	 * returns elapsed time in milliseconds if the watch has never been started
	 * then return zero
	 */
	public long getElapsedTime() 
	{
		if (startTime == -1)
		{
			return 0;
		}
		if (running)
		{
			return System.currentTimeMillis() - startTime;
		} else
		{
			return stopTime - startTime;
		}
	}
	
	public String toString()
	{
		return String.format("%20.3f", (float)getElapsedTime()/1000);
	}

	public StopWatch reset() 
	{
		startTime = -1;
		stopTime = -1;
		running = false;
		return this;
	}
}
