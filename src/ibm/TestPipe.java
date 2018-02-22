package ibm;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestPipe
{
	static final int ERROR_PIPE_CONNECTED = 535;
	static final int ERROR_BROKEN_PIPE = 109;
	private int namedPipeHandle;
	private String pipeName, srcFile;
	private int pipeBuffer = 131072, fileBuffer = 8192;
	
	public TestPipe(String pipeName, String srcFile)
	{
		this.pipeName = pipeName;
		this.srcFile = srcFile;		
	}
	
	private void log(String message)
	{
		System.out.println(message);		
	}	

	private boolean createPipe()
	{
		boolean ok = false;
		namedPipeHandle = Pipes.CreateNamedPipe(pipeName, 0x00000003, 0x00000000, 2, pipeBuffer, pipeBuffer, 0xffffffff, 0);
		if (namedPipeHandle == -1)
		{
			log("CreateNamedPipe failed for " + pipeName + 
					" for error " + " Message " + Pipes.FormatMessage(Pipes.GetLastError()));
			ok = false;
		} else
		{
			log("Named Pipe " + pipeName + " created successfully Handle=" + namedPipeHandle);
			ok = true;
		}
		return ok;
	}
	
	private boolean connectToPipe()
	{
		log("Waiting for a client to connect to pipe " + pipeName);
		boolean connected = Pipes.ConnectNamedPipe(namedPipeHandle, 0);
		if (!connected)
		{
			int lastError = Pipes.GetLastError();
			if (lastError == ERROR_PIPE_CONNECTED)
				connected = true;
		}
		if (connected)
		{
			log("Connected to the pipe " + pipeName);
		} else
		{
			log("Falied to connect to the pipe " + pipeName);
		}
		return connected;
	}

	public void runPipe()
	{
		if (createPipe())
		{
			if (!connectToPipe())
			{
				log("Connect ConnectNamedPipe failed : " + Pipes.FormatMessage(Pipes.GetLastError()));
				return;
			} else
			{
				log("Client connected.");
			}
			
			try
			{
				File f1 = new File(this.srcFile);
				InputStream in = new FileInputStream(f1);
				log("Sending data to the pipe");
				byte[] buf = new byte[fileBuffer];
			    int len, bytesWritten;
			    while ((len = in.read(buf)) > 0)
			    {
					bytesWritten = Pipes.WriteFile(namedPipeHandle, buf, len);
					log("Sent " + len + "/" + bytesWritten + " bytes to the pipe");
					if (bytesWritten == -1)
					{
						int errorNumber = Pipes.GetLastError();
						log("Error Writing to pipe " + Pipes.FormatMessage(errorNumber));
					}					    
			    }
			    in.close();
			    Pipes.FlushFileBuffers(namedPipeHandle);
			    Pipes.CloseHandle(namedPipeHandle);
			    Pipes.DisconnectNamedPipe(namedPipeHandle);
				log("Writing to the pipe completed.");
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException 
	{
		String pipeName = "\\\\.\\pipe\\mynamedpipe";
		String fileName = "C:\\db2tabledata.txt";;
		TestPipe testPipe = new TestPipe(pipeName, fileName);
		testPipe.runPipe();
	}
}
