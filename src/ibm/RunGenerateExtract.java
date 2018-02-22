/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.InputStream;
import java.text.SimpleDateFormat;

import javax.swing.JTextArea;

public class RunGenerateExtract implements Runnable
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
	private static String osType = (System.getProperty("os.name").toUpperCase().startsWith("WIN")) ? "WIN" : (System.getProperty("os.name").toUpperCase().startsWith("Z/OS")) ? "z/OS" : "OTHER";
	private String shellCommandFileName = "", dirName = "", windowsDrive = "C:";
	private JTextArea output;
	
	public RunGenerateExtract (JTextArea output, String dirName, String shellCommandFileName)
	{
		this.output = output;
		this.dirName = dirName;
		this.shellCommandFileName = shellCommandFileName;
		if (osType.equalsIgnoreCase("win") && dirName != null)
		{
		   dirName = dirName.trim();
		   if ((dirName.charAt(0) >= 'C' && dirName.charAt(0) <= 'Z') ||
			   (dirName.charAt(0) >= 'c' && dirName.charAt(0) <= 'z'))
			   windowsDrive = dirName.charAt(0) + ":";
		}
	}
	
	private void consoleRun()
	{		
		Process p = null;
		InputStream stdInput = null, stdError = null;

		try
		{
			if (osType.equalsIgnoreCase("win"))
			{
				p = Runtime.getRuntime().exec("cmd" + " /c " + windowsDrive + " && cd \"" + dirName + "\"" + " && " + shellCommandFileName);
			} else
			{
				if (!shellCommandFileName.startsWith("./"))
					shellCommandFileName ="./" + shellCommandFileName;
				String cmd[] = {IBMExtractUtilities.getShell(),"-c", "cd \"" + dirName + "\" ; " + shellCommandFileName};
				//log("DirName= " + "cd \"" + dirName + "\" ; " + shellCommandFileName);
				p = Runtime.getRuntime().exec(cmd);
			}
			stdInput = p.getInputStream();				
			stdError = p.getErrorStream();
			new InputStreamHandler(stdInput);
			new InputStreamHandler(stdError);
			p.waitFor();
			stdInput.close();
			stdError.close();
			p.getInputStream().close();
			p.getErrorStream().close();
			p.destroy();
		} catch (Exception e) 
		{	
			e.printStackTrace();
		}
		IBMExtractUtilities.DataExtracted = true; 		
	}
	
	private void guiRun()
	{
		Process p = null;
		InputStream stdInput = null, stdError = null;

		try
		{
			if (osType.equalsIgnoreCase("win"))
			{
				p = Runtime.getRuntime().exec("cmd" + " /c " + windowsDrive + " && cd \"" + dirName + "\"" + " && " + shellCommandFileName);
			} else
			{
				if (!shellCommandFileName.startsWith("./"))
					shellCommandFileName ="./" + shellCommandFileName;
				String cmd[] = {IBMExtractUtilities.getShell(),"-c", "cd \"" + dirName + "\" ; " + shellCommandFileName};
				p = Runtime.getRuntime().exec(cmd);
			}
			stdInput = p.getInputStream();				
			stdError = p.getErrorStream();
			new InputGUIStreamHandler(output, stdInput, false);
			new InputGUIStreamHandler(output, stdError, false);
			p.waitFor();
			Thread.sleep(1000);
			stdInput.close();
			stdError.close();
			p.getInputStream().close();
			p.getErrorStream().close();
			p.destroy();
		} catch (Exception e) 
		{	
			e.printStackTrace();
		}
		IBMExtractUtilities.DataExtracted = true;		
	}
	
	public void run()
	{
    	if (output == null)
    	{
    		consoleRun();
    	} else
    	{
	    	guiRun();    		
    	}
	}
	
    public static void main(String[] args)
    {
    	RunGenerateExtract gen = new RunGenerateExtract(null, "D:\\IBMDataMovementTool\\FA", "unload.cmd");
    	gen.run();
    }
}
