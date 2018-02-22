package ibm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DataCheck
{
	private static String OUTPUT_DIR = System.getProperty("OUTPUT_DIR") == null ? "logs" : System.getProperty("OUTPUT_DIR");
    private static String filesep = System.getProperty("file.separator");	
	private String dataCheckFilename, srcVendor, dstVendor;
    private PrintStream ps = null;    
    private TableList t;
    private IBMExtractConfig cfg;
    private Connection srcConn = null, dstConn = null;
    private boolean debug;
    private DatabaseMetaData srcMetadata;
    private DBData srcData, dstData;
    private ArrayList colList = new ArrayList();
    private ArrayList pkList = new ArrayList();
    private ArrayList pkValues = new ArrayList();
    private long numRecords;
        
    private String RemoveQuote(String s)
    {
    	return IBMExtractUtilities.removeQuote(s);
    }
    
    private String PutQuote(String s)
    {
    	return IBMExtractUtilities.putQuote(s);
    }
    
    private void log(String msg)
    {
       IBMExtractUtilities.log(msg);
    }
		
    private void log(PrintStream ps, String msg)
    {
    	IBMExtractUtilities.log(ps, msg);
    }
    
	public DataCheck(String tableFile, String numRecordsToCheck)
	{
	    String srcServer = "", dstServer = "";
	    String srcDBName, dstDBName, srcUid, dstUid, srcPwd, dstPwd;
	    int srcPort, dstPort;
 	    String exceptSchemaSuffix, exceptTableSuffix;
 	    if (numRecordsToCheck == null || numRecordsToCheck.length() == 0)
 	    {
 	    	log("Number of records to check have not been specified. Exiting ...");
 	    	System.exit(-1);
 	    } else
 	    {
 	    	if (numRecordsToCheck.equalsIgnoreCase("all"))
 	    	{
 	    		numRecords = -1L;
 	    	} else
 	    	{
 	    		try
 	    		{
 	    			numRecords = Long.valueOf(numRecordsToCheck);
 	    			if (numRecords < 1L)
 	    			{
 	 	    	    	log("Invalid value specified for number of records to check. Exiting ...");
 	 	    	    	System.exit(-1); 	    				
 	    			}
 	    		} catch (Exception e)
 	    		{
 	    	    	log("Invalid value specified for number of records to check. Exiting ...");
 	    	    	System.exit(-1); 	    			
 	    		}
 	    	}
 	    }
  	    String appHome = IBMExtractUtilities.getAppHome();
    	if (appHome == null || appHome.equals(""))
    	{
    		System.out.println("Specify location of application home using -DAppHome=<Location of the Tool Jar file directory>");
            System.exit(-1);
    	}
        File tmpfile = new File(OUTPUT_DIR);
        tmpfile.mkdirs();
        
        IBMExtractUtilities.replaceStandardOutput(OUTPUT_DIR + filesep + "DataCheck.log");
        IBMExtractUtilities.replaceStandardError(OUTPUT_DIR + filesep + "DataCheckError.log");

 	    try
 	    {
 		   dataCheckFilename = new File(OUTPUT_DIR + IBMExtractUtilities.filesep + "DataCheck.log").getAbsolutePath();
 		   ps = new PrintStream(new FileOutputStream(dataCheckFilename));
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
        exceptSchemaSuffix = cfg.getExceptSchemaSuffix();
        exceptTableSuffix = cfg.getExceptTableSuffix();
		t = new TableList(cfg.getCaseSensitiveTabColName(), srcVendor, tableFile, exceptSchemaSuffix, exceptTableSuffix);
		debug = Boolean.valueOf(cfg.getDebug());
        srcServer = cfg.getSrcServer();
        srcDBName = cfg.getSrcDBName();
        srcPort = cfg.getSrcPort();
        srcUid = cfg.getSrcUid();
    	srcPwd = IBMExtractUtilities.Decrypt(cfg.getSrcPwd());
        dstServer = cfg.getDstServer();
        dstDBName = cfg.getDstDBName();
        dstPort = cfg.getDstPort();
        dstUid = cfg.getDstUid();
    	dstPwd = IBMExtractUtilities.Decrypt(cfg.getDstPwd());
   	    srcData = new DBData(srcVendor, srcServer, srcPort, srcDBName, srcUid, srcPwd, "", 0);
	    srcConn = srcData.getConnection();
	    try
		{
			srcMetadata = srcConn.getMetaData();
		} catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
        dstData = new DBData(dstVendor, dstServer, dstPort, dstDBName, dstUid, dstPwd, "", 0);
        dstConn = dstData.getConnection();        	  
	}
	
	private boolean isBlobTable(String schema, String table)
	{
		boolean blobFound = false;
		String dataType;
		colList.clear();
		try
		{
			ResultSet rs = srcMetadata.getColumns(null, schema, table, "%");
			while (rs.next())
			{
				dataType = rs.getString(6);
				if (dataType != null && dataType.equalsIgnoreCase("BLOB"))
				{
					blobFound = true;
					colList.add(rs.getString(4));
				}
			}
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
			return false;
		}
		return blobFound;
	}
	
	private boolean isPrimaryKeyAvailable(String schema, String table)
	{
    	Hashtable pkCols = new Hashtable();
		ResultSet pkeys;
		boolean pkFound = false;
		String colName;
		pkList.clear();
		try
		{
            if (srcVendor.equalsIgnoreCase("oracle"))
                pkeys = srcConn.getMetaData().getPrimaryKeys(null, schema, table);
            else if (srcVendor.equalsIgnoreCase("hxtt"))
                pkeys = srcConn.getMetaData().getPrimaryKeys(null, null, table);
            else if (srcVendor.equalsIgnoreCase("zdb2"))
                pkeys = srcConn.getMetaData().getPrimaryKeys(null, schema, table);
            else if (srcVendor.equalsIgnoreCase("idb2"))
                pkeys = srcConn.getMetaData().getPrimaryKeys(null, schema, table);
            else if (srcVendor.equalsIgnoreCase("db2"))
                pkeys = srcConn.getMetaData().getPrimaryKeys(null, schema, table);
            else if (srcVendor.equalsIgnoreCase("mysql"))
                pkeys = srcConn.getMetaData().getPrimaryKeys(schema, null, table);
            else
                pkeys = srcConn.getMetaData().getPrimaryKeys(cfg.getSrcDBName(), null, table);
			while (pkeys.next())
			{
				int keySeq = pkeys.getShort(5);
                String columnName = pkeys.getString(4);
                pkCols.put(new Integer(keySeq), columnName);
                pkFound = true;
			}
			pkeys.close();
            Vector v = new Vector(pkCols.keySet());
            Collections.sort(v);	            
            for (Enumeration e = v.elements(); e.hasMoreElements();) {
                String val = (String)pkCols.get((Integer)e.nextElement());
                pkList.add(val);
            }
		} catch (SQLException e)
		{
			e.printStackTrace();
			return false;
		}
		return pkFound;		
	}
	
	private String cols()
	{
		String colName = "";
		boolean found = false;
		Iterator iter = pkList.iterator();
	    while (iter.hasNext()) 
	    {
	    	if (found)
	           colName += ",";
	    	found = true;
	        colName += (String) iter.next();
	    }
		Iterator iter2 = colList.iterator();
	    while (iter2.hasNext()) 
	    {
	        colName += "," + (String) iter2.next();
	    }
	    return colName;
	}
	
	private String getColName(String colNames, int idx)
	{
		String[] names = colNames.split(",");
		return names[idx-1];
	}
	
	private String buildDB2SQL(int id)
	{
		int i = 0;
		String colNames = "", dataType;
		String schema = t.dstSchemaName[id], table = t.dstTableName[id];
		Object[] pkVal = pkValues.toArray();
		String whereClause = "";
		Iterator iter = colList.iterator();
	    while (iter.hasNext()) 
	    {
	    	if (i > 0)
	    		colNames += ",";
	        colNames += (String) iter.next();
	        i = 1;
	    }
		Iterator iter2 = pkList.iterator();
		i = 0;
	    while (iter2.hasNext()) 
	    {
	    	if (i > 0)
	    		whereClause += " AND ";
	    	whereClause += (String) iter2.next() + " = " + (String) pkVal[i];
	    	++i;
	    }
	    return "SELECT " + colNames + " FROM " + schema + "." + table + " WHERE " + whereClause;
	}
	
	private String getDstBlobLength(int id)
	{
		String methodName = "getDstBlobLength";
        Blob blob = null;
        long dstBlobLength;
		String sql = buildDB2SQL(id), blobLength = "";
    	SQL s = new SQL(dstConn);
        try
        {
			int colIndex = 1;
			s.PrepareExecuteQuery(methodName, sql);
			while (s.next())
			{
				if (colIndex > 1)
					blobLength += ",";
                Object obj = s.rs.getObject(colIndex);
                dstBlobLength = 0L;
                if (obj != null)
                {
                    blob = (Blob) obj;
                    InputStream input = blob.getBinaryStream();
                    byte[] buffer = new byte[1024*1000];
                    int bytesRead = 0;
                    while ((bytesRead = input.read(buffer)) != -1)
                    {
                    	dstBlobLength += bytesRead;
                    }
                    ++colIndex;
                }
                blobLength += dstBlobLength;
			}
			s.close(methodName);
        } catch (SQLException e1)
        {
    		for (int i = 0; i < colList.size(); ++i)
    		{
    			if (i > 0)
    				blobLength += ",";
    		   blobLength += "-1";
    		}
        	if (e1.getErrorCode() != -204)
        	{
            	e1.printStackTrace();        		
        	}
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        }          
        return blobLength;
	}
	
    private String massageTS(String str)
    {
    	int idx;
    	if ((idx = str.indexOf('-')) < 5) 
    	{
    		return IBMExtractUtilities.padRight(str, str.length()+4-idx, "0");
    	}
    	return str;
    }              
	
	private void compareBlobLength(int id, String schema, String table)
	{
		String colNames = cols(), javaType, colType, tmpStr;
    	String methodName = "compareBlobLength";
		String srcSQL = "SELECT " + colNames + " FROM " + PutQuote(schema) + "." + PutQuote(table);
        Blob blob = null;
        java.sql.ResultSetMetaData md;
        long srcBlobLength;
        String srcBlob = "", dstBlob = "";
        long curRec = 0L;
		
    	SQL s = new SQL(srcConn);
        try
        {
			s.PrepareExecuteQuery(methodName, srcSQL);
			md = s.rs.getMetaData();
			int totalCols = pkList.size() + colList.size();
			while (s.next())
			{
			    ++curRec;
				int colIndex = 1;
				pkValues.clear();
				colType = md.getColumnTypeName(colIndex).toUpperCase();
                javaType = md.getColumnClassName(colIndex);
				while (colIndex <= pkList.size())
				{
                    if (javaType.equalsIgnoreCase("java.lang.String"))
                    {
                    	tmpStr = "'" + s.rs.getString(colIndex) + "'";
                    } else
                    {
	                    if (colType.startsWith("TIMESTAMP"))
	                    {
	                 	  Timestamp tst = s.rs.getTimestamp(colIndex); 
	                 	  if (tst != null)
	                 	  {
	             	         tmpStr = tst.toString();
	             	         if (tmpStr != null)
	             	         {
	             		        tmpStr = massageTS(tmpStr);
	             	         }
	                 	  } else
	                 		  tmpStr = null;
	                    }                   
	                    else if (colType.equalsIgnoreCase("DATE"))
	                    {
	                 	   tmpStr = s.rs.getString(colIndex);
	                 	   if (tmpStr != null)
	                 	   {
	                 		   tmpStr = massageTS(tmpStr);
	                 	   }
	                    } else
	                    {
	                    	tmpStr = s.rs.getString(colIndex);                    	
	                    }
                    }
					pkValues.add(tmpStr);
                    colIndex++;
				} 
				boolean found = false;
				srcBlob = "";
				while (colIndex <= totalCols)
				{
					if (found)
						srcBlob += ",";
	                Object obj = s.rs.getObject(colIndex);
                    srcBlobLength = 0L;
	                if (obj != null)
	                {
	                    blob = (Blob) obj;
	                    InputStream input = blob.getBinaryStream();
	                    byte[] buffer = new byte[1024*1000];
	                    int bytesRead = 0;
	                    while ((bytesRead = input.read(buffer)) != -1)
	                    {
	                    	srcBlobLength += bytesRead;
	                    }
	                }
	                srcBlob += srcBlobLength;
	                dstBlob = getDstBlobLength(id);
	                ++colIndex;
				}	
				String[] srcStr = srcBlob.split(",");
				String[] dstStr = dstBlob.split(",");
				colIndex = 0;
				Iterator iter = colList.iterator();
			    while (iter.hasNext()) 
			    {
			        colNames = (String) iter.next();
	                log(schema + "." + table + "." + colNames + " Source Length = " + srcStr[colIndex] + " Destination Length = " + dstStr[colIndex]);
			        colIndex++;
			    }
			    if (numRecords != -1 && curRec >= numRecords)
			    	break;
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	e.printStackTrace();
        }            	
	}
	

	public void CheckBlobs()
	{
		String schema, table;
		log("*** Initiating Blob length check only on tables having primary key");
		for (int i = 0; i < t.srcTableName.length; ++i)
		{
			schema = RemoveQuote(t.srcSchName[i]);
			table = RemoveQuote(t.srcTableName[i]);
			if (isBlobTable(schema, table))
			{
				Iterator iter = colList.iterator();
			    while (iter.hasNext()) 
			    {
			       String colName = (String) iter.next();
				   //log("Column " + colName);
			    }
			    if (isPrimaryKeyAvailable(schema, table))
			    {
				    //log("Table " + t.srcSchName[i] + "." + t.srcTableName[i] + " has PK / BLOB");			    	
				    Iterator iter2 = pkList.iterator();
				    while (iter2.hasNext()) 
				    {
				       String colName = (String) iter2.next();
					   //log("Column " + colName);
				    }
				    compareBlobLength(i, schema, table);
			    } else
			    {
					log("Table " + t.srcSchName[i] + "." + t.srcTableName[i] + " has BLOB but no PK so will not be checked for BLOB data compare");			    	
			    }
			}
		}		
	}
	
	public void Close()
	{
		dstData.commit();
		srcData.commit();
		dstData.close();
		srcData.close();
	}
	
	public static void main(String[] args)
    {		
	    long start = System.currentTimeMillis();	    		
	    if (args.length < 2)
	    {
	       System.out.println("usage: java -Xmx500m -DAppHome=<Location of Tool's JAR file directory name> -DOUTPUT_DIR=logs ibm.DataCheck table_prop_file NUM_RECS_TO_CHECK");
	       System.out.println("Valid value for NUM_RECS_TO_CHECK is ALL or a number > 0");
	       System.exit(-1);
	    }
	    DataCheck dc = new DataCheck(args[0], args[1]);
	    dc.CheckBlobs();
	    dc.Close();
        IBMExtractUtilities.log("==== Total time: " + IBMExtractUtilities.getElapsedTime(start));
    }
}
