package ibm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class FixCheck
{
   private static String OUTPUT_DIR = System.getProperty("OUTPUT_DIR") == null ? "logs" : System.getProperty("OUTPUT_DIR");
   public static PrintStream ps = null;

   private static void log(String msg)
   {
      IBMExtractUtilities.log(msg);
   }
	
   public static void main(String[] args)
   {
	   IBMExtractConfig cfg;
	   String caseSensitive, TABLES_PROP_FILE, exceptSchemaSuffix, exceptTableSuffix;
	   String dstVendor, dstServer = "";
	   String dstDBName, dstUid, dstPwd;
	   int dstPort;
	    
 	   String appHome = IBMExtractUtilities.getAppHome();
       if (appHome == null || appHome.equals(""))
       {
    		System.out.println("Specify location of application home using -DAppHome=<Location of the Tool Jar file directory>");
            System.exit(-1);
       }
	   
       File tmpfile = new File(OUTPUT_DIR);
       tmpfile.mkdirs();

	   try
	   {
			ps = new PrintStream(new FileOutputStream(OUTPUT_DIR + IBMExtractUtilities.filesep + "fixcheck.log"));
	   } catch (FileNotFoundException e)
	   {
			ps = null;
			e.printStackTrace();
	   }

	   cfg = new IBMExtractConfig();    	
	   cfg.loadConfigFile();
	   cfg.getParamValues();    
	   if (!cfg.paramPropFound)
	   {
			log("Error in loading "+IBMExtractUtilities.getConfigFile()+" file.");
			System.exit(-1);
	   }
       if (args.length < 1)
       {
           System.out.println("usage: java -Xmx600m -DAppHome=<Location of Tool Jar file directory> ibm.FixCheck TableFileName");
           System.exit(-1);
       }
       TABLES_PROP_FILE = args[0];
       caseSensitive = cfg.getCaseSensitiveTabColName();
       exceptSchemaSuffix = cfg.getExceptSchemaSuffix(); 
       exceptTableSuffix = cfg.getExceptTableSuffix();   
       dstVendor = cfg.getDstVendor();
       dstServer = cfg.getDstServer();
       dstDBName = cfg.getDstDBName();
       dstPort = cfg.getDstPort();
       dstUid = cfg.getDstUid();
       dstPwd = IBMExtractUtilities.Decrypt(cfg.getDstPwd());	      	      
       IBMExtractUtilities.checkTargetTablesStatus(ps, Boolean.valueOf(cfg.getLoadException()), caseSensitive, exceptSchemaSuffix, exceptTableSuffix, TABLES_PROP_FILE, dstVendor, dstServer, dstPort, dstDBName, dstUid, dstPwd);
   }
}
