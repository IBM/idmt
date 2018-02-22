package ibm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RunDB2Load  implements Runnable
{

	private DBData data;
	StringBuffer buffer;
	String schema, table;
	ArrayList loadWarningList;
	private HashMap messageMap, fileMap;
	long rowsInWarnings = 0L;
	
	public RunDB2Load(DBData data, ArrayList loadWarningList, HashMap messageMap, HashMap fileMap, StringBuffer buffer)
	{
		this.data = data;
		this.buffer = buffer;		
		this.loadWarningList = loadWarningList;
		this.messageMap = messageMap;
		this.fileMap = fileMap;
	}
	
    private String massageLoad(String sql)
    {
        sql = sql.replaceAll("(?im)^--.*", "");
        sql = sql.replaceAll("(?im)^MESSAGES.*", "MESSAGES ON SERVER");
        sql = sql.replaceAll("(?sm)^\\s*[\\n|\\r]", "");
        sql = "CALL SYSPROC.ADMIN_CMD('"+sql+"')";
        return sql;
    }

	private String parseBuffer()
	{
		StringBuffer sql = new StringBuffer();
		String line, key;
		String[] keys;
		BufferedReader reader = new BufferedReader(new StringReader(buffer.toString()));
    	try
		{
			while ((line = reader.readLine()) != null)
			{
				if (line.startsWith("--#SET :"))
				{
					key = line.substring(line.indexOf(":")+1);	
					keys = key.split(":");	
					schema = keys[1];
					table = keys[2];
				} else if (line.equals(";"))
				{
					;
				} 
				else
				{
					sql.append(line + " ");
				}
			}
	    	reader.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return sql.toString();
	}

	private String GetColumnValue(ResultSet rs, int colIndex) throws Exception
    {
        String tmp = "";        
        try
        {
            tmp = rs.getString(colIndex);                         
        } catch (SQLException e)
        {
        	IBMExtractUtilities.log("Col[" + colIndex + "] Error:" + e.getMessage());
           return "SkipThisRow";               
        }
        return tmp;
    }
    
	
	private void CallMessages(String schemaTableKey, String sql)
	{
		try
		{
			String colsep = "  ";
			PreparedStatement statement = data.connection.prepareStatement(sql);
	        statement.setFetchSize(100);
	        ResultSet Reader = statement.executeQuery();
	        
            ResultSetMetaData rsmeta = null;
            StringBuffer buffer = new StringBuffer();  
            int colCount, colLength;
            boolean skip = false;
            String colValue = "";
	           
            if (Reader == null) return;
            rsmeta = statement.getMetaData(); 
            if (rsmeta == null)
              rsmeta = Reader.getMetaData();
            colCount = rsmeta.getColumnCount();
       	    int[] len = new int[colCount];
            while (Reader.next()) 
            { 
               buffer.setLength(0);
               skip = false;
               for( int j = 1; j <= colCount; j++ ) 
               { 
                  colValue = GetColumnValue(Reader, j);
                  if (colValue != null && colValue.equals("SkipThisRow"))
                  {
                      skip = true;
                      continue;
                  }
                  if (j == 1 && colValue != null && colValue.startsWith("SQL3107W"))
                  {
                	  loadWarningList.add(schemaTableKey);
                  }
                  if (j == 1 && colValue != null && (colValue.startsWith("SQL3121W") || colValue.startsWith("SQL3125W") 
                		  || (colValue.startsWith("SQL3114W"))))
                  {
                	  rowsInWarnings++;
                  }                  
            	  colValue = pad(colValue, len[j-1], " ");
                  buffer.append(colValue);
                  if (j != colCount)
                      buffer.append(colsep);                           
               }
               if (!skip)
               {
    			  IBMExtractUtilities.log(buffer.toString(), 1);			
               }
            }
            if (Reader != null)
        	   Reader.close();
            if (statement != null)
        	   statement.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private String pad(Object str, int padlen, String pad)
    {
    	if (str == null)
    		str = "";
        String padding = new String();
        int len = Math.abs(padlen) - str.toString().length();
        if (len < 1)
          return str.toString();
        for (int i = 0; i < len; ++i)
           padding = padding + pad;

        return (padlen < 0 ? padding + str : str + padding);
    }

    public void ProcessLoadmessages()
    {
    	if (messageMap.size() == 0) return;
    	PrintStream ps;
    	String key, messageSQL;
		String file = Constants.OUTPUT_DIR + "db2pipeloadmessages.log", fileName;
		try
		{
		   File f = new File(file);
		   fileName = f.getCanonicalPath();
		} catch (Exception e)
		{
			fileName = file;
		}
    	try
		{
			ps = new PrintStream(new FileOutputStream(fileName, false));
			Iterator iter = messageMap.entrySet().iterator();
			while (iter.hasNext()) 
			{
				Map.Entry pairs = (Map.Entry) iter.next();
		        key = (String) pairs.getKey();
		        messageSQL = (String) pairs.getValue();
				IBMExtractUtilities.log(ps, "=================================================================", 1);
				IBMExtractUtilities.log(ps, "Messages start for " + key, 1);
				IBMExtractUtilities.log(ps, "=================================================================", 1);
				CallMessages(key, messageSQL);
				IBMExtractUtilities.log(ps, "=================================================================", 1);
				IBMExtractUtilities.log(ps, "", 1);
			}    	
			int loadWarnings = loadWarningList.size();
			if (loadWarnings > 0)
			{
				IBMExtractUtilities.log(" *************** ATTENTION REQUIRED *********************************", 1);
				IBMExtractUtilities.log(" " + loadWarnings + " LOAD WARNINGS were found.", 1);
				IBMExtractUtilities.log(" This typically means that either truncation occured or invalid data was made NULL by the DB2 LOAD",1);
				IBMExtractUtilities.log(" The following tables threw the warnings.",1);
				Iterator iter2 = loadWarningList.iterator();
				IBMExtractUtilities.log(" ********************************************************************", 1);
				while (iter2.hasNext()) 
				{
					IBMExtractUtilities.log(" " + (String) iter2.next(), 1);					
				}				
				IBMExtractUtilities.log(" ********************************************************************", 1);
				IBMExtractUtilities.log(" A total of " + rowsInWarnings + " warnings were generated for above tables.", 1);
				IBMExtractUtilities.log(" Please review file " + fileName + " and look for SQL3121W messages", 1);
				IBMExtractUtilities.log(" ********************************************************************", 1);
			} else
			{
				IBMExtractUtilities.log("", 1);
				IBMExtractUtilities.log(" Please do not forget to check file " + fileName + " for DB2 LOAD messages", 1);
				IBMExtractUtilities.log("", 1);
			}
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		
		Iterator iter = fileMap.entrySet().iterator();
		while (iter.hasNext()) 
		{
			Map.Entry pairs = (Map.Entry) iter.next();
	        key = (String) pairs.getKey();
	        messageSQL = (String) pairs.getValue();
			IBMExtractUtilities.executeUpdate(data.connection, messageSQL);
		}		
    }

    private void runLoad(String sql)
    {
    	long start = System.currentTimeMillis();
    	long rowsRead, rowsLoaded, rowsRejected, rowsDeleted, rowsCommitted;
    	String messageStr, fileStr;
    	String s = "";
    	try
    	{
	        CallableStatement cstmt = data.connection.prepareCall(sql);
	        cstmt.execute();
	        ResultSet rs = cstmt.getResultSet();
			while (rs.next())
			{
				rowsRead      = rs.getLong(1);
				rowsLoaded    = rs.getLong(3);
				rowsRejected  = rs.getLong(4);
				rowsDeleted   = rs.getLong(5);
				rowsCommitted = rs.getLong(6);
				messageStr    = rs.getString(9);
				fileStr       = rs.getString(10);
				IBMExtractUtilities.log("Schema            " + schema);
				IBMExtractUtilities.log("Table             " + table);
				IBMExtractUtilities.log("Rows read         " + rowsRead);
				IBMExtractUtilities.log("Rows loaded       " + rowsLoaded);
				IBMExtractUtilities.log("Rows rejected     " + rowsRejected);
				IBMExtractUtilities.log("Rows deleted      " + rowsDeleted);
				IBMExtractUtilities.log("Rows committed    " + rowsCommitted);
				IBMExtractUtilities.log("Elapsed Time      " + IBMExtractUtilities.getElapsedTime(start), 1);
				messageMap.put(schema+"."+table,messageStr);
				fileMap.put(schema+"."+table,fileStr);
		    	start = System.currentTimeMillis();		    	
			}
			if (rs != null)
				rs.close();
			if (cstmt != null)
	           cstmt.close();
			IBMExtractUtilities.log(s, 1);
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }
	
	public void run()
	{
		String sql = massageLoad(parseBuffer());
	    Statement statement;
		try
		{
			statement = data.connection.createStatement();
			statement.execute("SET CURRENT SCHEMA = '" + schema + "'");
			statement.execute("SET PATH = SYSTEM PATH,'" + schema +"'");			    				
			statement.close();
			runLoad(sql);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}	
	}
}
