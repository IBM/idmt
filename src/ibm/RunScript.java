/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.File;
import java.io.InputStream;

import javax.swing.JTextArea;

public class RunScript implements Runnable
{
   private static String osType = (System.getProperty("os.name").toUpperCase().startsWith("WIN")) ? "WIN" : (System.getProperty("os.name").toUpperCase().startsWith("Z/OS")) ? "z/OS" : "OTHER";
   private static String filesep = System.getProperty("file.separator");
   private String shellCommandFileName = "", cmdPath = "", db2Instance = "";
   private JTextArea buffer;
   private String windowsDrive;
   private int taskNum = 0;
   
   public RunScript(JTextArea buffer, String db2Instance, String cmdPath, String shellCommandFileName, int taskNum)
   {
      this.db2Instance = db2Instance;
      this.buffer = buffer;
      this.cmdPath = cmdPath;
	  this.shellCommandFileName = shellCommandFileName;
	  this.taskNum = taskNum;
      
	  if (osType.equalsIgnoreCase("win") && shellCommandFileName != null)
	  {
		  windowsDrive = shellCommandFileName.trim();
	      if ((windowsDrive.charAt(0) >= 'C' && windowsDrive.charAt(0) <= 'Z') ||
		     (windowsDrive.charAt(0) >= 'c' && windowsDrive.charAt(0) <= 'z'))
		     windowsDrive = windowsDrive.charAt(0) + ":";
	  }
	  
      if (db2Instance == null || db2Instance.equals(""))
      {
         this.db2Instance = "";
      } else
      {
         this.db2Instance = " && SET DB2INSTANCE=" + db2Instance;
      }
   }
   
   /*
    *  Execute shellCommandFileName in db2cmd
    * @see java.lang.Runnable#run()
    */
   public void run()
   {
	  InputStream stdInput = null, stdError = null;
      Process p = null;
      String cmd[] = null;
      
      // Use exec(String[]) to handle spaces in directory
      try
      {
         File shellScriptFile = new File(shellCommandFileName);
         String script = shellScriptFile.getName() + " " + taskNum;
         String dirName = shellScriptFile.getParent();
         
         if (osType.equalsIgnoreCase("win"))
         {
            String cmdLine = windowsDrive + " && cd \"" + dirName + "\" " + db2Instance + " && " + script; 
            if (!cmdPath.equals(""))
            {
               cmdPath = cmdPath + filesep + "BIN" + filesep + "db2cmd";
               cmd = new String[]{cmdPath, "/c","/i","/w", cmdLine};
            } else
               cmd = new String[]{"cmd", "/c", cmdLine};
         } 
         else
         {        	 
        	 if (!script.startsWith("./"))
        		 script = "./" + script;
             cmd = new String[]{IBMExtractUtilities.getShell(),"-c", "cd \"" + dirName + "\" ; " + script};             
         } 
         //log("CMD values " + Arrays.toString(cmd));
         p = Runtime.getRuntime().exec(cmd);
		 stdInput = p.getInputStream();				
		 stdError = p.getErrorStream();
		 new InputGUIStreamHandler(buffer, stdInput, false);
		 new InputGUIStreamHandler(buffer, stdError, false);
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
      IBMExtractUtilities.ScriptExecutionCompleted = true; 
   }   
}
