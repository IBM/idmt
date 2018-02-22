/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.awt.GraphicsEnvironment;

public class RunTool
{
	
    private static void checkGUI(String laf)
    {
    	try
        {  
            GraphicsEnvironment.getLocalGraphicsEnvironment();
            String[] args = {laf};
            IBMExtractGUI2.main(args);

        }         
        catch (Throwable t)
        {     
    		System.out.println("Switching to Console Mode of operation");
        	IBMExtractConfig.main(null);
        }
    }


	public static void main(String[] args)
	{
		String version = System.getProperty("java.version");
		if (version != null)
		{
			try
			{
				int endPos, pos1 = version.indexOf('.');
				if (pos1 > 0) 
				{
					endPos = pos1 + 2;
				} else
					endPos = version.length(); 
				String ver = version.substring(0,endPos);
				float fver = Float.valueOf(ver);
				if (fver < 1.5F)
				{
					System.out.println("Your Java Version is " + version);
					System.out.println("You need minimum Java 1.5 to run IBM Data Movement Tool");
				    System.exit(-1);					
				}
			} catch (Exception e)
			{
				System.out.println("Java version could not be determined. Proceeding with the tool launch ....");
			}
	    }
        if (args.length > 0) 
        {
            String args_0 = args[0];
            if (args_0.equalsIgnoreCase("-version"))
            {
            	Version.main(null);
            } 
            else if (args_0.equalsIgnoreCase("-shortVersion"))
            {
            	Version.shortVersion();
            } 
            else if (args_0.equalsIgnoreCase("-console"))
            {
            	IBMExtractConfig.main(null);
            } 
            else if (args_0.equalsIgnoreCase("-meet") || args_0.equalsIgnoreCase("-mtk"))
            {
            	GenerateMeet.runMeet();
            } 
            else if (args_0.equalsIgnoreCase("-check"))
            	System.out.println(new RunIDMTVersion().GetIDMTVersion());
            else
            {
            	checkGUI(args_0);
            }
        } else
        {
        	checkGUI(null);
        }
	}
}
