package ibm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.List;

import com.ibm.zos.net.ftp.FTPClient;
import com.ibm.zos.net.ftp.FTPClientErrorException;
import com.ibm.zos.net.ftp.FTPClientProcessKillException;
import com.ibm.zos.net.ftp.FTPException;
import com.ibm.zos.net.ftp.FTPInterfaceErrorException;
import com.ibm.zos.net.ftp.FTPInternalErrorException;

public class ZOSFtp
{
	FTPClient ftp;
	int port = 21;
	private String userName, password, server;
	private String linesep = Constants.linesep;
	
	public ZOSFtp(String server, String userName, String password)
	{
		this.server = server;
		this.userName = userName;
		this.password = password;
		ftp = new FTPClient();		
	}
	
	private void log(String str)
	{
		System.out.println(str);
	}
	
	private void showResponse()
	{
		String line;
		List list;
		log("Return code = " + ftp.getReplyCode());
		try
		{
			list = ftp.getlCopy();
			
			Iterator itr = list.iterator();
			while (itr.hasNext())
			{
				line = itr.next().toString();
				log(line);
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private void connect()
	{
		int status, code;
		String message;
		try
		{
			ftp.init();			
			status = ftp.scmd("OPEN " + server + " " + port);
			showResponse();
			status = ftp.scmd("USER " + userName);
			showResponse();
			status = ftp.scmd("PASS " + password);
			showResponse();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void disConnect()
	{
		int status;
		try
		{
			status = ftp.term();
			showResponse();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void runJCL()
	{
		int status;
		String tmpFile, jcl = "//MYJOB   JOB  CLASS=A,MSGCLASS=A, " + linesep +
			"//      MSGLEVEL=(1,1),REGION=256K  " + linesep +
			"//S1      EXEC PGM=IEBGENER " + linesep +
			"//SYSUT1   DD  DISP=SHR,DSN=SYS1.PROCLIB(INIT) " + linesep +
			"//SYSUT2   DD  DISP=OLD,DSN=VOLKER.SEQ.FILE " + linesep +
			"//SYSPRINT DD  SYSOUT=A " + linesep +
			"//SYSIN    DD  DUMMY " + linesep  +
			"//";	
		File tmp;
		try
		{
			tmp = File.createTempFile("cp", "tmp");
			tmp.deleteOnExit();
			tmpFile = tmp.getCanonicalPath();
			log("The temp file is " + tmpFile);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmp.getCanonicalPath(), false)));
			bw.write(jcl);
			bw.close();			
			status = ftp.scmd("quote site filetype=jes");
			showResponse();
			status = ftp.scmd("PUT " + tmpFile);
			showResponse();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		ZOSFtp f = new ZOSFtp("demomvs.demopkg.ibm.com","DDS0226","cxzzxc00");
		f.connect();
		f.disConnect();
	}
}
