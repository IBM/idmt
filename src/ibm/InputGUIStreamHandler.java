/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.swing.JTextArea;

public class InputGUIStreamHandler extends Thread
{
	private boolean skipPrint, pipeOk = true;	
	private BufferedInputStream buffer = null;
	private static final int MIN_WAIT_TIME = 50;
	private static final int MAX_WAIT_TIME = 5000;
	private static final int INITIAL_WAIT_TIME = 500;
	private byte[] bytes = null;
	private int dynamicWaitTime = INITIAL_WAIT_TIME;
	private int dataLoadInt = 0;
	private JTextArea output = null;

	
	public InputGUIStreamHandler(JTextArea output, InputStream stream, boolean skipPrint)
	{
		this.output = output;
		this.skipPrint = skipPrint;
		buffer = new BufferedInputStream(stream);
		start();
	}

	private void readUntilEnd()
	{
		int n = 0;
		try
		{
			if (pipeOk)
			{
			   n = buffer.available();
			   bytes = new byte[n];
			   buffer.read(bytes);
			   writeData();
			}
		} catch (IOException ex)
		{
			pipeOk = false;
			bytes = null;
			//ex.printStackTrace();
			writeData("Output truncated... Please see IBMDataMovementTool.log file for the remaining output");
			System.out.println("Pipe broken. Bytes available " + n);
		}
	}

	private void writeData(String str)
	{
		if (output == null)
		{
			System.out.print(str);
		} else
		{
			output.append(str);
			try { output.setCaretPosition(output.getDocument().getLength()); } catch (Exception ex) {}
		}
	}
	
	private void writeData()
	{
		if (bytes != null && bytes.length > 0 && !skipPrint)
		{
			char[] result = new char[bytes.length];
			for (int i = 0; i < bytes.length; i++)
			{
				result[i] = (char) bytes[i];
			}
			if (output == null)
			{
				System.out.print(result);
			} else
			{
				output.append(String.copyValueOf(result));
				try { output.setCaretPosition(output.getDocument().getLength()); } catch (Exception ex) {}
			}
			bytes = null;
		}
	}

	private void decreaseWaitTime()
	{
		if (dataLoadInt == 1 && dynamicWaitTime > MIN_WAIT_TIME)
		{
			dynamicWaitTime /= 2;
			if (dynamicWaitTime < MIN_WAIT_TIME)
			{
				dynamicWaitTime = MIN_WAIT_TIME;
			}
		} else if (dataLoadInt < 1)
		{
			dataLoadInt++;
		}
	}

	private void increaseWaitTime()
	{
		if (dataLoadInt == -1 && dynamicWaitTime < MAX_WAIT_TIME)
		{
			dynamicWaitTime *= 2;
			if (dynamicWaitTime > MAX_WAIT_TIME)
			{
				dynamicWaitTime = MAX_WAIT_TIME;
			}
		} else if (dataLoadInt > -1)
		{
			dataLoadInt--; 
		}
	}

	@Override
	public final void run()
	{
		synchronized (this)
		{
			try
			{
				while (this.isAlive())
				{
					if (pipeOk)
					{
						if (buffer.available() > 0)
						{
							while (buffer.available() > 0)
							{
								readUntilEnd();
							}
							decreaseWaitTime();
						} else
						{
							increaseWaitTime();
						}
						//wait(dynamicWaitTime);
						wait(MIN_WAIT_TIME);
					} else
					{
						break;
					}
				}
			} catch (InterruptedException ex)
			{
				;
			} catch (IOException ex)
			{
				;
			}
		}
	}	
}
