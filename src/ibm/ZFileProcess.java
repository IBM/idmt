package ibm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.jzos.CatalogSearch;
import com.ibm.jzos.CatalogSearchField;
import com.ibm.jzos.RcException;
import com.ibm.jzos.ZFile;
import com.ibm.jzos.ZFileException;
import com.ibm.jzos.ZUtil;

public class ZFileProcess
{
	private long rowNum = 0;
	private String linesep = Constants.linesep, zEncoding, ddName;
	private boolean debug = false, allocated;
	private ZFile[] zfp;
	private  TableList t;
	private int suffix = 1, secondary = 0, spaceMult = 1;
	private String zHLQName, zdb2tableseries = "Q", nocopypend, overAllocStr = "1.3636", storclas = "NONE", secondaryStr = "0", generateJCLScriptsStr;
	private double overAlloc = 1.3636;

	private String jobCard1 = "//DB2IDMT JOB (DB2IDMT),'DB2 IDMT',CLASS=A,";
	private String jobCard2 = "//     MSGCLASS=A,NOTIFY=&SYSUID";
	public String db2SubSystemID = "DB2P";
	public String SDSNLOAD = "DB2.SDSNLOAD";
	public String RUNLIBLOAD = "DB2.RUNLIB.LOAD";
	public String DSNTEPXX = "DSNTEP91"; 	
	public String[] zOSDDName, zOSDataSets;
	public boolean znocopypend, generateJCLScripts;
	
	public ZFileProcess()
	{
		zEncoding = ZUtil.getDefaultPlatformEncoding();
		zHLQName = System.getProperty("user.name").toUpperCase();
	}
	
	public ZFileProcess(Properties propParams,  TableList t)
	{
		this.t = t;
        zfp = new ZFile[t.totalTables];
        zOSDataSets = new String[t.totalTables];
        zOSDDName = new String[t.totalTables];
        for (int i = 0; i < t.totalTables; ++i)
        {
        	zfp[i] = null;
	        zOSDataSets[i] = null;
        }        
        zEncoding = ZUtil.getDefaultPlatformEncoding();
		debug = Boolean.valueOf((String) propParams.getProperty("debug"));
		zHLQName = (String) propParams.getProperty("zHLQName");	
        zdb2tableseries = (String) propParams.getProperty("zdb2tableseries");
        zdb2tableseries = zdb2tableseries.toUpperCase();
        nocopypend = (String) propParams.getProperty("znocopypend");
        overAllocStr = (String) propParams.getProperty("zoveralloc");
        secondaryStr = (String) propParams.getProperty("zsecondary");
        jobCard1 = (String) propParams.getProperty("jobCard1");
        jobCard2 = (String) propParams.getProperty("jobCard2");
        db2SubSystemID = (String) propParams.getProperty("db2SubSystemID");
        SDSNLOAD = (String) propParams.getProperty("SDSNLOAD");
        RUNLIBLOAD = (String) propParams.getProperty("RUNLIBLOAD");
        DSNTEPXX = (String) propParams.getProperty("DSNTEPXX");
        generateJCLScriptsStr = (String) propParams.getProperty("generateJCLScripts");

        if (zdb2tableseries == null || nocopypend == null || overAllocStr == null || 
                secondaryStr == null || storclas == null || jobCard1 == null || jobCard2 == null ||
                db2SubSystemID == null || SDSNLOAD == null || RUNLIBLOAD == null || DSNTEPXX == null || generateJCLScriptsStr == null)
            {
               log("zdb2tableseries=Q # Use this as a series name for zDB2 datasets to store LOAD data."  + linesep + 
                 "znocopypend=[true|false] #If true, use NOCOPYPEND option in LOAD for zDB2"  + linesep +
                 "zoveralloc=1.3636 #The overAlloc variable specifies by how much we want to oversize our file allocation"  + linesep +
                 "zsecondary=0 #The secondary extents. Start with a default value of 0"  + linesep +
                 "storclas=none #Specify none if do not want to use storclas. Length can be from 1 to 8 only" +
                 "jobcard1=<Value> #Specify your definition of the job card 1 and it will be used as the first line of the JCL. Only first word will be changed for different jobs." + linesep +					
				 "jobcard2=<Value> #Specify your definition of the job card 2 and it will be used as the first line of the JCL." + linesep +					
				 "db2SubSystemID=<Value> #Specify ID of the DB2 subsystem." + linesep +
				 "SDSNLOAD=<Value> #Specify your name of the SDSN LOAD" + linesep +
				 "RUNLIBLOAD=<Value> #Specify your name of the RUNLIB LOAD" + linesep +
				 "DSNTEPXX=<Value> #Specify your name of the DSNTEPXX" + linesep +
				 "#generateJCLScripts=[true|false]. #If true, the JCL scripts will be generated so that they can be run from z/OS and not USS");

         	   System.exit(-1);
            }
        secondary = Integer.parseInt(secondaryStr);
        overAlloc = Double.parseDouble(overAllocStr);
        storclas = (String) propParams.getProperty("storclas");
        znocopypend = Boolean.valueOf(nocopypend);
        generateJCLScripts = Boolean.valueOf(generateJCLScriptsStr);

		if (zHLQName == null || zHLQName.length() == 0)
		   log("zHLQName              : (Not Set). User ID HLQ will be used");
		else
		   log("zHLQName              : " + zHLQName);	
		log("zdb2tableseries       : " + zdb2tableseries);
		log("zoveralloc            : " + overAlloc);
		log("zsecondary            : " + secondary);
		log("storclas              : " + storclas);
		log("jobCard1              : " + jobCard1);
		log("jobCard2              : " + jobCard2);
		log("db2SubSystemID        : " + db2SubSystemID);
		log("SDSNLOAD              : " + SDSNLOAD);
		log("RUNLIBLOAD            : " + RUNLIBLOAD);
		log("DSNTEPXX              : " + DSNTEPXX);
		log("znocopypend           : " + ((znocopypend) ? "true" : "false"));
		log("generateJCLScripts    : " + ((generateJCLScripts) ? "true" : "false"));

		if (storclas.length() > 8) 
		{
			log("Length of storclas can  not be greater than 8.");
			System.exit(-1);
		}
        log("ZUtil.getDefaultPlatformEncoding(): " + zEncoding);
        log("Default file.encoding:" + System.getProperty("file.encoding"));
        try
        {
           cleanupPSFiles(zHLQName);
        } catch (Exception e)
        {
           log("Problem with deleting the PS datasets ");
           e.printStackTrace();
        }
	}

	private void log(String msg)
	{
		IBMExtractUtilities.log(msg);
	}
		
    private static boolean delDataset(String dsnName)
    {
          try
	  	  {
	  		 if (ZFile.dsExists("//'" + dsnName + "'"))
	  		 {
	  		     ZFile.remove("//'" + dsnName + "'");
	  		 }
	  		 return true;
	  	  } catch (ZFileException e)
	  	  {	  		  
	  		 e.printStackTrace();
	  	  }       
	  	  return false;
    }
    
    private static String getDSName(String hlq, String dsPrefix)
    {
    	if (hlq == null || hlq.length() == 0)
    	   return ZFile.getFullyQualifiedDSN(dsPrefix);
    	else
    	   return hlq + "." + dsPrefix;	
    }

    private static void deleteFile(String hlq, String fileSuffix) throws Exception
    {
       String dsn;
       String tql = getDSName(hlq, "TBLDATA");
       CatalogSearch catSearch;

       catSearch = new CatalogSearch(tql + ".**."+fileSuffix);
       catSearch.addFieldName("ENTNAME");
       catSearch.search();
       while (catSearch.hasNext()) 
       {
          CatalogSearch.Entry entry = (CatalogSearch.Entry)catSearch.next();
          if (entry.isDatasetEntry()) 
          {
             CatalogSearchField field = entry.getField("ENTNAME");
             dsn = field.getFString().trim();
             if (delDataset(dsn))
                IBMExtractUtilities.log("Deleted="+dsn);
          }
       }       
    }

    public static void cleanupPSFiles(String hlq) throws Exception 
    {
        deleteFile(hlq, "CERR");
        deleteFile(hlq, "LERR");
        deleteFile(hlq, "DISC");
        deleteFile(hlq, "UT1");
        deleteFile(hlq, "OUT");
    }

    private String getPDSName(String dsName, String dsPrefix)
    {
       if (dsName.equals(""))
       {
    	   int n, j = dsPrefix.length(), pos = dsPrefix.indexOf('.');
           dsName = getDSName(zHLQName, dsPrefix); 
           String pdsName = "" + suffix;
           int i = pdsName.length();
           n = (pos == -1) ? 8 - j : 8 - j + pos;
           while (i < n)
           {
              pdsName = "0" + pdsName;
              i++;
           }
           suffix++;
           return "'" + dsName + pdsName + "'";
       } else
           return dsName;
    }
    
    private void close(ZFile z, String ddName)
    {
		 try
		 {
			 if (z != null)
			 {
			    z.close();
			    ZFile.bpxwdyn("free fi(" + ddName + ") msg(wtp)");
			    z = null;			    
			 }
		 } catch (Exception e)
		 {
			 log("Closing Error for " + ddName);    	   	 
			 e.printStackTrace();
		 } finally
		 {
			 if (z != null)
			 {
			    ZFile.bpxwdyn("free fi(" + ddName + ") msg(wtp)");
			 }
		 }
    }
    
    public void closeZDataFile(int id)
    {
    	 try
    	 {
    		 if (zfp[id] != null)
    		 {
    		    zfp[id].close();
    		    ZFile.bpxwdyn("free fi(" + zOSDDName[id] + ") msg(wtp)");
    		    zfp[id] = null;       		    
    		 }
    	 } catch (Exception e)
    	 {
    		 log("Closing Error for " + zOSDDName[id]);    	   	 
    		 e.printStackTrace();
    	 } finally
    	 {
    		 if (zfp[id] != null)
    		    ZFile.bpxwdyn("free fi(" + zOSDDName[id] + ") msg(wtp)");
    	 }
    }
    
    public void close()
    {
        for (int i = 0; i < t.totalTables; ++i)
        {
           closeZDataFile(i);
        }
    }
    
    private boolean isDD(String inputName)
	{
		return (inputName.length() <= 8);
	}
    
    private boolean deleteDataSet(String zFileName)
    {
		try
		{
	    	if (isDD(zFileName))
	    	{
	    		ZFile.remove("//DD:" + zFileName);
	    	} else
	    	{
	    		ZFile.remove("//" + zFileName);
	    	}
    		if (debug) log("Deleted " + zFileName);
	    	return true;
		} catch (ZFileException e)
		{
			e.printStackTrace();
		}    		
		return false;
    }

    private void allocateDataSet(String ddName, String pdsName) throws ZFileException, RcException
    {
        if (!ZFile.dsExists(pdsName))
        {
            if (debug) log("ALLOC FI(" + ddName + ") DA(" + pdsName + 
                    ") LRECL(80) DSORG(PS) RECFM(F,B) " +
                    " REUSE NEW CATALOG MSG(WTP) BLKSIZE(3200) " + (storclas.equalsIgnoreCase("none") ? "" : " STORCLAS (" + storclas + ")") +
                    " CYL SPACE("+(int)(spaceMult * overAlloc)+"," + (int) (secondary * overAlloc) +")");
            ZFile.bpxwdyn("ALLOC FI(" + ddName + ") DA(" + pdsName + 
                  ") LRECL(80) DSORG(PS) RECFM(F,B) " +
                  " REUSE NEW CATALOG MSG(WTP) BLKSIZE(3200) " + (storclas.equalsIgnoreCase("none") ? "" : " STORCLAS (" + storclas + ")") +
                  " CYL SPACE("+(int)(spaceMult * overAlloc)+"," + (int) (secondary * overAlloc) +")");
 	        log("Allocated new PS dataset " + pdsName + " for " + ddName);
        }
    }
    
    private void createDataSet(String ddName, String pdsName)
    {
        try 
        {        	 
             if (ZFile.dsExists(pdsName))
             {
            	 if (!deleteDataSet(pdsName))
            	 {
                 	ZFile.bpxwdyn("free fi(" + ddName + ") msg(wtp)");   
                 	if (!deleteDataSet(pdsName))
                 	{
                 		log("Close the dataset " + pdsName + " and try again.");
                        System.exit(-1);
                 	}
            	 }            	 
             }
             allocateDataSet(ddName, pdsName);
        } catch (Exception e)
        {
            log("Error occured allocating PS Dataset = " + pdsName);              
            e.printStackTrace();
    	    ZFile.bpxwdyn("free fi(" + ddName + ") msg(wtp)");
            System.exit(-1);
        }
    }
    
    private String getDSName(String operation)
    {
    	return "'" + zHLQName + "." + operation + "'";
    }
    
    private void unAllocate(String ddName)
    {
    	if (allocated) 
    	{
 		   try 
 		   {
 			   ZFile.bpxwdyn("free fi(" + ddName + ") msg(wtp)");
 		   } catch(Exception rce) 
 		   {
 			   rce.printStackTrace();
 		   }
 	   }
    }
    
    private ZFile openZFile(String ddName, String pdsName)
    {
    	ZFile zfile = null;
        try
        {
    		ZFile.bpxwdyn("alloc fi(" + ddName + ") da(" + pdsName	+ ") reuse shr msg(wtp)");
    		//allocateDataSet(ddName, pdsName);
    		allocated = true;
    		zfile = new ZFile("//DD:" + ddName, "ab");
        } catch (ZFileException e)
        {
            log("Error occured opening PS Dataset = " + pdsName + " for " + ddName);              
            e.printStackTrace();
            unAllocate(ddName);
            System.exit(-1);
        }
        return zfile;
    }
    
    private String padLine(StringBuffer text, int len, int recLen)
    {
		for (int i = len; i < recLen; i++) 
			text.append(' ');  
		return text.toString();
    }

    private String[] wrap(StringBuffer text, int len, int recLen)
    {
    	if (text == null)
    	   return new String [] {};
    	if (len <= 0)
    	   return new String [] {""};
    	if (len <= recLen)    	
     	   return new String [] {padLine(text, len, recLen)};    	
    	char [] chars = text.toString().toCharArray();
    	Vector lines = new Vector();
    	StringBuffer line = new StringBuffer();
    	StringBuffer word = new StringBuffer();

    	for (int i = 0; i < chars.length; i++) 
    	{
    	    word.append(chars[i]);
    	    if (chars[i] == ' ') 
    	    {
    	       if ((line.length() + word.length()) > len) 
    	       {
    	          lines.add(line);
    	          line.delete(0, line.length());
    	       }
    	       line.append(word);
    	       word.delete(0, word.length());
    	    }
       }
       if (word.length() > 0) 
       {
    	    if ((line.length() + word.length()) > len) 
    	    {
    	        lines.add(line);
    	        line.delete(0, line.length());
    	    }
    	    line.append(word);
       }
       if (line.length() > 0) 
       {
    	    lines.add(line);
       }
       String [] ret = new String[lines.size()];
       int c = 0; // counter
       for (Enumeration e = lines.elements(); e.hasMoreElements(); c++) 
       {
    	   StringBuffer b =  (StringBuffer) e.nextElement();
    	   int slen = b.toString().length();
    	   ret[c] = padLine(b, slen, recLen);
       }
       return ret;
    }
    
    private void writeZRecord(BufferedWriter writer, ZFile zfile, String buffer) throws ZFileException, UnsupportedEncodingException
    {
    	int recLen = zfile.getLrecl();
    	String[] lines;
    	int len;
    	StringBuffer newBuffer = new StringBuffer(2 * buffer.length());
    	String regex = "^(.*)";
		Pattern p = Pattern.compile(regex,Pattern.MULTILINE);
		Matcher m = p.matcher(buffer.toString());
		while (m.find())
		{
			newBuffer.setLength(0);
			len = m.end()-m.start();
			newBuffer.append(m.group());
			lines = wrap(newBuffer, len, recLen);
			for (int i = 0; i < lines.length; ++i)
			{
				zfile.write(lines[i].getBytes(zEncoding));
				if (writer != null)
				{
					try
					{
						writer.write(lines[i]+linesep);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
    }
    
    private String changeJobCard(String job)
    {
    	if (job == null || job.length() == 0)
    		return jobCard1;
    	
    	StringBuffer buffer = new StringBuffer();
		String token;
			  
		StringTokenizer stoke = new StringTokenizer(jobCard1);
		int i = 0;
	    while (stoke.hasMoreElements())
	    {
	    	token = stoke.nextToken();
	    	if (i == 0 && token.startsWith("//"))
	    		buffer.append("//" + job.substring(0,8) + " ");
	    	else
	    		buffer.append(token);
	    }
	    return buffer.toString();
    }
    
    public void zLoadWriter(BufferedWriter writer)
    {
    	StringBuffer buffer = new StringBuffer();
    	String dsName = getDSName(Constants.zOSLoadPDSName);
    	ddName = ZFile.allocDummyDDName();
    	createDataSet(ddName, dsName);
    	buffer.append(Constants.zCommentLine + linesep);
	    buffer.append(changeJobCard(Constants.zOSLoadPDSName) + linesep);
	    buffer.append(jobCard2 + linesep);
    	buffer.append(Constants.zCommentLine + linesep);
    	writezLoad(writer, buffer.toString());
    }
    
    public synchronized void writezLoad(BufferedWriter writer, String script)
    {
    	ZFile zfile = null;
    	try
		{
    		zfile = openZFile(ddName, getDSName(Constants.zOSLoadPDSName));
    		writeZRecord(writer, zfile, script);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			close(zfile, ddName);
		}
    }
    
    private String allocTBLPDS(int id, String tblDSN, int spaceMult)
    {
        tblDSN = getPDSName(tblDSN, "TBLDATA." + zdb2tableseries);
        try 
        {
             closeZDataFile(id);
             if (ZFile.dsExists(tblDSN))
             {
            	 if (!deleteDataSet(tblDSN))
            	 {
                 	ZFile.bpxwdyn("free fi(" + zOSDDName[id] + ") msg(wtp)");   
                 	if (!deleteDataSet(tblDSN))
                 	{
                 		log("Close the dataset " + tblDSN + " and try again.");
                        System.exit(-1);
                 	}
            	 }
             }
             if (!ZFile.dsExists(tblDSN))
             {
                 if (debug) log("Calling ZFile.bpxwdyn(ALLOC FI(" + zOSDDName[id] + ") DA(" + tblDSN + 
                       ") LRECL(32756) DSORG(PS) RECFM(V,B) MAXVOL(60) " +
                       " REUSE NEW CATALOG MSG(WTP) BLKSIZE(32760) " + (storclas.equalsIgnoreCase("none") ? "" : " STORCLAS (" + storclas + ")") +
                       " CYL SPACE("+(int)(spaceMult * overAlloc)+"," + (int) (secondary * overAlloc) +")");
                 ZFile.bpxwdyn("ALLOC FI(" + zOSDDName[id] + ") DA(" + tblDSN + 
                       ") LRECL(32756) DSORG(PS) RECFM(V,B) MAXVOL(60) " +
                       " REUSE NEW CATALOG MSG(WTP) BLKSIZE(32760) " + (storclas.equalsIgnoreCase("none") ? "" : " STORCLAS (" + storclas + ")") +
                       " CYL SPACE("+(int)(spaceMult * overAlloc)+"," + (int) (secondary * overAlloc) +")");	                 	                 
             }
        } catch (Exception e)
        {
             e.printStackTrace();
             spaceMult = Math.max((int) (0.95 * spaceMult),1);
             log("Alloc failed ("+id+"). Trying with new value  " + spaceMult);
             if (spaceMult > 10)
                allocTBLPDS(id, tblDSN, spaceMult);
             else
             {
                log("We are out of space. Aborting ... ");
                System.exit(-1);
             }
        } 
        return tblDSN;
    }

    private String openTBLPSDataset(int id, int spaceMult)
    {
    	zOSDDName[id] = ZFile.allocDummyDDName();
        String schema = IBMExtractUtilities.removeQuote(t.dstSchemaName[id].toLowerCase());
        String table = IBMExtractUtilities.removeQuote(t.dstTableName[id].toLowerCase());
        String dataPS = allocTBLPDS(id, "", spaceMult);
        try
        {
           zfp[id] = new ZFile("//DD:" + zOSDDName[id], "wb,type=record,noseek");
	       log("Allocating new PS dataset ("+id+")" + dataPS + " for " + t.dstSchemaName[id] + "." + t.dstTableName[id]);
        } catch (ZFileException e)
        {
            log("Error occured opening PS Dataset ("+id+")= " + dataPS + " for Schema = " + schema + " Table = " + table);              
            e.printStackTrace();
     	    closeZDataFile(id);
            System.exit(-1);
        }
        return dataPS;
    }
    
    public void writeZData(int id, long recordCount, byte[] bytes) throws Exception
    {
        int rowSize = 0;
        long maxRows;
        double mult;
        String dsName;

        if (recordCount == 0)
            return;
         //bytes = data.getBytes();
         rowSize = Math.max(rowSize, bytes.length);
         try
		 {
            if (zfp[id] == null)
            {
               mult = Math.floor(32756 / (rowSize + 4)) * 15;
               spaceMult = (int) Math.ceil(recordCount / mult);
               spaceMult = Math.max(Math.min(spaceMult, (int)(4367.0/overAlloc)),1);
               dsName = openTBLPSDataset(id, spaceMult);
               rowNum = 0; 
               zOSDataSets[id] = dsName;
            }
            maxRows = 32756 / (rowSize + 4);
            maxRows = (long) Math.floor(maxRows);
            maxRows = 15 * (spaceMult + 15 * secondary) * maxRows;
            if (rowNum > maxRows)
            {
                dsName = openTBLPSDataset(id, spaceMult);
                log("rowNum > maxRows Rows = " + rowNum + " Allocating a new PS dataset " + dsName + " table is " + t.dstSchemaName[id] + "." + t.dstTableName[id]);
                rowNum = 0;
                zOSDataSets[id] = zOSDataSets[id] + "," + dsName;
            }                 
			   zfp[id].write(bytes);
			   if (debug) 
			   {
				  //log(encoding + ":" + printBytes(bytes));
                  //log("Default:" + printBytes(data.getBytes()));
			   }
		 } catch (ZFileException e)
		 {
             if (debug) 
             {
             	e.printStackTrace();
             }
             dsName = openTBLPSDataset(id, spaceMult);
             log(" rowSize=" + rowSize + " We should never be in this exception. Rows = " + rowNum + " Allocating a new PS dataset " + dsName + " table is " + t.dstSchemaName[id] + "." + t.dstTableName[id]);
             rowNum = 0;
             zOSDataSets[id] = zOSDataSets[id] + "," + dsName;                  
             zfp[id].write(bytes);
		 }
         ++rowNum;    	
    }
    
    public static void main(String[] args) throws IOException
    {
    	String script = IBMExtractUtilities.FileContents("/u/dds0226/migr/IDMTConfig.properties");
    	ZFileProcess zp = new ZFileProcess();
    	zp.zLoadWriter(null);
    	zp.writezLoad(null,script);
    }
}
