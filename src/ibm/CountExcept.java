/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CountExcept
{
   private static String OUTPUT_DIR = System.getProperty("OUTPUT_DIR") == null ? "logs" : System.getProperty("OUTPUT_DIR");
   private static String linesep = System.getProperty("line.separator");

   private String caseSensitiveTabColName = "true", srcVendor, dstVendor, CountFilename;
   private TableList t;
   private IBMExtractConfig cfg;
   private PrintStream ps = null;    
   private Connection dstConn = null;
   private boolean debug;
   private DBData dstData;

   private static void log(String msg)
   {
      IBMExtractUtilities.log(msg);
   }

   private void log(PrintStream ps, String msg)
   {
   	IBMExtractUtilities.log(ps, msg);
   }
   
   public CountExcept(String tableFile)
   {
	   String dstServer = "";
	   String dstDBName, dstUid, dstPwd;
	   int dstPort;
 	   String exceptSchemaSuffix, exceptTableSuffix;
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
		   CountFilename = new File(OUTPUT_DIR + IBMExtractUtilities.filesep + "CountExcept.log").getAbsolutePath();
 		   ps = new PrintStream(new FileOutputStream(CountFilename));
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
       srcVendor = cfg.getSrcVendor();
       dstVendor = cfg.getDstVendor();
       caseSensitiveTabColName = cfg.getCaseSensitiveTabColName();
       exceptSchemaSuffix = cfg.getExceptSchemaSuffix();
       exceptTableSuffix = cfg.getExceptTableSuffix();
	   t = new TableList(caseSensitiveTabColName, srcVendor, tableFile, exceptSchemaSuffix, exceptTableSuffix);
	   debug = Boolean.valueOf(cfg.getDebug());
       dstServer = cfg.getDstServer();
       dstDBName = cfg.getDstDBName();
       dstPort = cfg.getDstPort();
       dstUid = cfg.getDstUid();
       dstPwd = cfg.getDstPwd();
       dstPwd = IBMExtractUtilities.Decrypt(dstPwd);
       
       dstData = new DBData(dstVendor, dstServer, dstPort, dstDBName, dstUid, dstPwd, "", 0);
       dstData.setAutoCommit(true);
       dstConn = dstData.getConnection();        	  
   }

   private String pad(Object str, int padlen, String pad)
   {
      String padding = new String();
      int len = Math.abs(padlen) - str.toString().length();
      if (len < 1)
         return str.toString();
      for (int i = 0; i < len; ++i)
         padding = padding + pad;

      return (padlen < 0 ? padding + str : str + padding);
   }

   private void countRows()
   {
      int i;
      PreparedStatement dstStatement;
      ResultSet dstReader = null;
      String dstStr;
      long dstNumber;
      StringBuffer buffer = new StringBuffer();

      buffer.setLength(0);
      try
      {
    	 if (dstVendor.length() > 0)
            buffer.append(pad(dstVendor, 85, " ") + linesep);
         for (i = 0; i < t.totalTables; ++i)
         {
            if (dstVendor.length() > 0)
            {
                dstStr = t.exceptSchemaName[i] + "." + t.exceptTableName[i];
            	dstNumber = -1;
            	try
            	{
	                dstStatement = dstConn.prepareStatement(t.countExceptSQL[i]);
	                dstReader = dstStatement.executeQuery();
	                dstNumber = 0;
	                while (dstReader.next())
	                {
	                   dstNumber = dstReader.getLong(1);
	                }
	                if (dstReader != null)
	                    dstReader.close();
	                if (dstStatement != null)
	                	dstStatement.close();
            	} catch (SQLException e)
            	{
            		if (e.getErrorCode() == -204)
            		{
            			log(dstStr + " table not found in " + dstVendor);
            		} else
            		{
                		log(t.countExceptSQL[i]);
                		e.printStackTrace(ps);            			
            		}
            	}
            	catch (Exception ex)
            	{
            		log(t.countExceptSQL[i]);
            		ex.printStackTrace(ps);
            	}
                buffer.append(pad(dstStr, 60, " ") + " : " + dstNumber + linesep);
                if (debug) log(pad(dstStr, 60, " ") + " : " + dstNumber);
            }
         }
         log(ps,linesep+buffer.toString());
         log("Output saved in " + CountFilename);
      } catch (Exception e)
      {
         e.printStackTrace(ps);
      }
   }

   private void close()
   {
	   dstData.close();
   }
   
   public static void main(String[] args)
   {
      if (args.length < 1)
      {
         System.out.println("usage: java -Xmx500m -DAppHome=<Location of Tool Jar file directory> -DOUTPUT_DIR=logs ibm.CountExcept table_prop_file");
         System.exit(-1);
      }
      CountExcept pg = new CountExcept(args[0]);
      pg.countRows();      
      pg.close();
   }
}
