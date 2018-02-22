package ibm;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

public class ProcessLOBFile
{
	private String lobsDirectory;
	private String lobFileName;
	private boolean isOpen;
	private RandomAccessFile raf = null;
	private PrintStream ps;
	
	public ProcessLOBFile(PrintStream ps, String lobsDirectory, String lobFileName)
	{
		this.ps = ps;
		this.lobsDirectory = lobsDirectory;
		this.lobFileName = lobFileName;
	}
	
	public void open(String lobsDirectory, String lobFileName)
	{
		if (lobsDirectory.equals(this.lobsDirectory) && lobFileName.equals(this.lobFileName) && !isOpen)
		{
			try 
		    {
				close();
				String file = lobsDirectory + Constants.filesep + lobFileName;
				File f = new File(file);				
		        raf = new RandomAccessFile(f, "r");
		        isOpen = true;
		    } catch (IOException e) 
		    {
		        e.printStackTrace();
		        isOpen = false;
		    }
		}
	}
	
	public byte[] getBytes(long offset, int length)
	{
		byte[] buffer = null;

		if (!isOpen)
			return buffer;
		
		try
		{
			buffer = new byte[length];
			raf.seek(offset);
			raf.read(buffer, 0, length);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return buffer;
	}
	
	public void queueOutputStream(long offset, int length, OutputStream out) throws IOException
	{
		if (!isOpen)
			throw new IOException("Cannot read from a closed InputStream.");
		
		int bytesRead, remainingBytes = length;
		byte[] buffer;
		try
		{
			buffer = new byte[Constants.readLobBuffer];
			raf.seek(offset);
			while ((bytesRead = raf.read(buffer, 0, Constants.readLobBuffer)) != -1)
			{
				if (bytesRead > remainingBytes)
				{
					if (remainingBytes > 0)
					   out.write(buffer, 0, remainingBytes);
					break;
				} else
				{
				    out.write(buffer, 0, bytesRead);
				    remainingBytes -= bytesRead;
				}
			}
			out.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public Reader getClob(long offset, int length) throws IOException
	{
		byte[] buffer = null;

		if (!isOpen)
			throw new IOException("Cannot read from a closed InputStream.");
		
		try
		{
			buffer = new byte[length];
			raf.seek(offset);
			raf.read(buffer, 0, length);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return (buffer == null) ? null : new StringReader(new String(buffer, 0, length));
	}

	public void queueOutputStream(long offset, int length, Writer out) throws IOException
	{
		if (!isOpen)
			throw new IOException("Cannot read from a closed InputStream.");
		
		int bytesRead, remainingBytes = length;
		byte[] buffer;
		try
		{
			buffer = new byte[Constants.readLobBuffer];
			raf.seek(offset);
			while ((bytesRead = raf.read(buffer, 0, Constants.readLobBuffer)) != -1)
			{
				if (bytesRead > remainingBytes)
				{
					if (remainingBytes > 0)
					   out.write(new String(buffer, 0, remainingBytes));
					break;
				} else
				{
				    out.write(new String(buffer, 0, bytesRead));
				    remainingBytes -= bytesRead;
				}
			}
			out.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try 
	    {
			if (raf != null)
			   raf.close();
	    } catch (IOException e) 
	    {
	       e.printStackTrace();
	    }
	}	
	
	public static void main(String args[])
	{
		int bytesRead, totalBytesRead;
		char[] buf = new char[1000];
		String lobsDirectory = "C:\\temp\\xe\\data\\testcase_blobclobtest";
		String lobFileName = "C0.lob";
		final ProcessLOBFile plfBlob = new ProcessLOBFile(null, lobsDirectory, lobFileName);
		final IOBuffer ioBlob = new IOBuffer(false);
		plfBlob.open(lobsDirectory, lobFileName);
		new Thread
		(
			new Runnable() 
			{
				public void run()
				{
					try
					{
						plfBlob.queueOutputStream(789516, 87724, ioBlob.rout);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			}
		).start();
		try
		{
			BufferedReader bis = new BufferedReader(ioBlob.rin);
			totalBytesRead = 0;
			while ((bytesRead = bis.read(buf)) != -1)
			{
				totalBytesRead += bytesRead;
				System.out.print(new String(buf, 0, bytesRead));
			}
			System.out.println("Bytes read = " + totalBytesRead);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
