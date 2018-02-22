/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.File;
import java.io.InputStream;

import javax.swing.JTextArea;

public class RunDB2Script implements Runnable
{
	private static String osType = (System.getProperty("os.name").toUpperCase()
			.startsWith("WIN")) ? "WIN" : (System.getProperty("os.name")
			.toUpperCase().startsWith("Z/OS")) ? "z/OS" : "OTHER";
	private static String filesep = System.getProperty("file.separator");
	private String db2ScriptName = "", cmdPath = "", output = "";
	private JTextArea buffer;
	private boolean skipPrint;

	/**
	 * @return the db2ScriptName
	 */
	public String getDb2ScriptName()
	{
		return db2ScriptName;
	}

	/**
	 * @param db2ScriptName the db2ScriptName to set
	 */
	public void setDb2ScriptName(String db2ScriptName)
	{
		this.db2ScriptName = db2ScriptName;
	}

	public RunDB2Script(JTextArea buffer,
			String cmdPath,
			String db2ScriptName, boolean skipPrint)
	{
		this.cmdPath = cmdPath;
		this.db2ScriptName = db2ScriptName;
		this.buffer = buffer;
		this.skipPrint = skipPrint;
	}

	/*
	 * Execute db2ScriptName in db2 handling spaces in directories
	 * 
	 * @see java.lang.Runnable#run()
	 */
	public void run()
	{
		String scriptName;
		Process p = null;
		InputStream stdInput = null, stdError = null;
		String cmd[] = null;
  
		try
		{
			File db2ScriptFile = new File(db2ScriptName);
			scriptName = db2ScriptFile.getName();
			String dirName = db2ScriptFile.getParent();
			if (this.buffer == null)
			{
				output = " -z " + scriptName.substring(0, scriptName.lastIndexOf('.')) + ".loadlog ";
			}
			if (osType.equalsIgnoreCase("win"))
			{
				if (!cmdPath.equals(""))
				{
					cmdPath = cmdPath + filesep + "BIN" + filesep + "db2cmd";
				}
				cmd = new String[] {
						cmdPath, "/c", "/i", "/w", "cd \"" + dirName + "\" && db2 " + output + "-tvf " + scriptName};
			} else
			{
				//String shellName = IBMExtractUtilities.getBashShellName();
				String shellName = IBMExtractUtilities.getShell();
				cmd = new String[] {
						shellName, "-c", "cd " + dirName + " ; db2 " + output + "-tvf " + scriptName};
			}
			p = Runtime.getRuntime().exec(cmd);
			stdInput = p.getInputStream();
			stdError = p.getErrorStream();
			new InputGUIStreamHandler(buffer, stdInput, skipPrint);
			new InputGUIStreamHandler(buffer, stdError, skipPrint);
			p.waitFor();
			stdInput.close();
			stdError.close();
			p.getInputStream().close();
			p.getErrorStream().close();
			p.destroy();
			if (buffer != null)
			  IBMExtractUtilities.db2ScriptCompleted = true;
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
