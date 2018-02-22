/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

class InputStreamHandler extends Thread
{
	private BufferedReader reader = null;

	public InputStreamHandler(InputStream stream)
	{
		reader = new BufferedReader(new InputStreamReader(stream));
		start();
	}

	public void run()
	{
		String line;
		try
		{
			while ((line = reader.readLine()) != null)
			{
				if (!(line.equals("")))
				{
					IBMExtractUtilities.log(line);
				}
			}
		} catch (IOException e)
		{
		}
	}
}
