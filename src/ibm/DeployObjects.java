package ibm;

import ibm.lexer.GPLexer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeployObjects 
{
	private static String OUTPUT_DIR = Constants.OUTPUT_DIR;
	public static PrintStream ps = null, oldps;
	private IBMExtractConfig cfg = null;
	private String sqlFileName;
	private String  sqlCode = "0", failedLineNumber, useSkinFeature = "", sqlMessage; 
	private ArrayList sqlCommands = null, loadWarningList = new ArrayList();
	private HashMap messageMap = new HashMap(), fileMap = new HashMap();
	private DBData data, ants;
	long rowsInWarnings = 0L;
	
	public DeployObjects(String sqlFileName)
	{
		this.sqlFileName = sqlFileName;		
	}
	
	public void Open()
	{
    	this.cfg = new IBMExtractConfig();    	
    	cfg.loadConfigFile();
    	cfg.getParamValues();    
    	if (!cfg.paramPropFound)
    	{
    		log("Error in loading "+IBMExtractUtilities.getConfigFile()+" file.");
    		System.exit(-1);
    	}

        String userid = cfg.getDstUid();
        String pwd = IBMExtractUtilities.Decrypt(cfg.getDstPwd());
    	data = new DBData(cfg.getDstVendor(), cfg.getDstServer(), cfg.getDstPort(), cfg.getDstDBName(), 
    			userid, pwd, "", 0);
    	data.setAutoCommit(true);
    	data.setPrintStream(ps);
    	data.getConnection();
        if (useSkinFeature.length() > 0)
        {
        	ants = new DBData(Constants.DB2SKIN, cfg.getDstServer(), cfg.getDstPort(), cfg.getDstDBName(),
        			userid, pwd, "", 0);
        	ants.setAutoCommit(true);
        	ants.setPrintStream(ps);
        	ants.getConnection();
        }
	}
	
	public void Close()
	{
		data.commit();
		data.close();
        if (useSkinFeature.length() > 0)
        {
        	ants.commit();
        	ants.close();
        }
	}
	
	private void deployObject(String type, String schema, String objectName, String sql)
	{		
		String sqlerrmc = "";
    	int lineNumber = -1;
		
		Statement statement = null;
		try
		{
			sqlCode = "0";
			if (useSkinFeature != null && useSkinFeature.length() > 0)
			{
			    statement = ants.connection.createStatement();
		    	try
		    	{
				   statement.execute(useSkinFeature);
		    	} catch (Exception e)
		    	{
		    		statement.execute("use master");
		    		statement.execute(useSkinFeature.replace("use", "create database"));
					statement.execute(useSkinFeature);
		    	}
			}
			else
			{
			    if (Constants.netezza())
			    {
			    	if (schema.equalsIgnoreCase(data.getDBName()))
			    	{
			    		statement = data.connection.createStatement();	
			    	} else
			    	{
			    		statement = data.changeDatabase(schema).createStatement();
			    		data.setDBName(schema);
			    	}
			    } else
			    {
				    statement = data.connection.createStatement();	
					statement.execute("SET CURRENT SCHEMA = '" + schema + "'");
					statement.execute("SET PATH = SYSTEM PATH,'" + schema +"'");			    	
			    }
			}
			statement.execute(sql);
			data.commit();
			if (statement != null)
				statement.close();
		} catch (SQLException e)
		{
			if (Constants.netezza())
			{
				sqlMessage = e.getMessage();
				if (sqlMessage.matches("(?sim).*already\\s+exists.*"))
				{
				   sqlCode = "0";
				   sqlMessage = "";
				} 
				else
				{
				   sqlCode = "" + e.getErrorCode();					   
				   failedLineNumber = "-1";
				}				
			} else
			{
				if (e instanceof com.ibm.db2.jcc.DB2Diagnosable)
				{
					com.ibm.db2.jcc.DB2Sqlca sqlca = ((com.ibm.db2.jcc.DB2Diagnosable)e).getSqlca();
					if (sqlca != null)
					{
						lineNumber = sqlca.getSqlErrd()[2];
						sqlerrmc = sqlca.getSqlErrmc();					
					}
				}				
				// These error codes should match with DepolyObjects from RunDeployObjects and IBMExtract.DeployObject 
				// For consistency in messages
				if (IBMExtractUtilities.CheckExistsSQLErrorCode(e.getErrorCode()))
				{
					sqlCode = "1";
				} 
				else if (IBMExtractUtilities.CheckNotExistsSQLErrorCode(e.getErrorCode()))
				{
					sqlCode = "2";
				} 
				else
				{
				    sqlCode = "" + e.getErrorCode();
				    failedLineNumber = "" + lineNumber;
				    sqlMessage = e.getMessage();
				}
			}			
		}
	}

	public void ReadSQLCommands()
	{
		try
		{
		    sqlCommands = GPLexer.ParseSQLFile(sqlFileName);
		} catch (Exception e)
		{
			log("Error with the file operation " + sqlFileName + " Error Message : " + e.getMessage());
			return;
		}
		
		if (sqlCommands == null)
			return;
		Iterator iter = sqlCommands.iterator();
		if (iter.hasNext()) 
		{
			String element = (String) iter.next();
			int pos = element.indexOf("~#@");
			String key = element.substring(0, pos);
			String value = element.substring(pos+3);
			if (key.equalsIgnoreCase("useDatabase"))
			{
				useSkinFeature = value;
			}
		}
	}
	
    private String GetColumnValue(ResultSet rs, int colIndex) throws Exception
    {
        String tmp = "";        
        try
        {
            tmp = rs.getString(colIndex);                         
        } catch (SQLException e)
        {
           log("Col[" + colIndex + "] Error:" + e.getMessage());
           return "SkipThisRow";               
        }
        return tmp;
    }
    
    private void CallUpdate(String schema, String table, String sql)
    {
    	try
    	{
    		PreparedStatement statement;
    		if (Constants.netezza())
		    {
		    	if (schema.equalsIgnoreCase(data.getDBName()))
		    	{
		    		statement = data.connection.prepareStatement(sql);	
		    	} else
		    	{
		    		statement = data.changeDatabase(schema).prepareStatement(sql);
		    		data.setDBName(schema);
		    	}
		    } else
		    {
			    statement = data.connection.prepareStatement("SET CURRENT SCHEMA = '" + schema + "'");	
				statement.execute();
			    statement = data.connection.prepareStatement("SET PATH = SYSTEM PATH,'" + schema +"'");	
				statement.execute();			    	
		    }
    		if (Constants.netezza())
    		{
    			IBMExtractUtilities.log(pad("Starting load for " + schema + "." + table, 55, "."), false);
    			log(pad("Starting load for " + schema + "." + table, 55, "."), false);
    		}
            int rows = statement.executeUpdate();
    		if (Constants.netezza())
    		{
    			IBMExtractUtilities.log(" .... completed.", 1);
    			log(" .... completed.", 1);
    		} else
			    IBMExtractUtilities.log(ps, "Rows affected " + rows, 1);
            if (statement != null)
         	   statement.close();
    	} catch (Exception e)
    	{
    		e.printStackTrace(ps);
    	}
    }

	private void CallSelect(String schema, String objectName, String sql)
	{
		try
		{
			boolean header = true;
			String colsep = "  ";
			PreparedStatement statement;
			if (Constants.netezza())
			{
		    	if (schema.equalsIgnoreCase(data.getDBName()))
		    	{
		    		statement = data.connection.prepareStatement(sql);	
		    	} else
		    	{
		    		statement = data.changeDatabase(schema).prepareStatement(sql);
		    		data.setDBName(schema);
		    	}
			} else
			{
				statement = data.connection.prepareStatement(sql);				
			}
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
               if (header)
               {
            	  header = false; 
                  for( int j = 1; j <= colCount; j++ ) 
                  { 
                	  colLength = rsmeta.getColumnDisplaySize(j);
                	  colValue = IBMExtractUtilities.trim(rsmeta.getColumnLabel(j));
                	  len[j-1] = colLength < colValue.length() ? colValue.length() : colLength;
                	  colValue = pad(colValue, colLength, " ");
                      buffer.append(colValue);
                      if (j != colCount)
                          buffer.append(colsep);                	  
                  }
                  IBMExtractUtilities.log(buffer.toString(), 1);
    			  IBMExtractUtilities.log(ps, buffer.toString(), 1);			
                  buffer.setLength(0);
               }
               for( int j = 1; j <= colCount; j++ ) 
               { 
                  colValue = GetColumnValue(Reader, j);
                  if (colValue != null && colValue.equals("SkipThisRow"))
                  {
                      skip = true;
                      continue;
                  }
            	  colValue = pad(colValue, len[j-1], " ");
                  buffer.append(colValue);
                  if (j != colCount)
                      buffer.append(colsep);                           
               }
               if (!skip)
               {
       			  IBMExtractUtilities.log(buffer.toString(), 1);
    			  IBMExtractUtilities.log(ps, buffer.toString(), 1);			
               }
            }
            if (Reader != null)
        	   Reader.close();
            if (statement != null)
        	   statement.close();
		} catch (Exception e)
		{
			e.printStackTrace(ps);
		}
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
    			  IBMExtractUtilities.log(ps, buffer.toString(), 1);			
               }
            }
            if (Reader != null)
        	   Reader.close();
            if (statement != null)
        	   statement.close();
		} catch (Exception e)
		{
			e.printStackTrace(ps);
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

    private void CallDSNUTIL(String sql)
    {    	
        ResultSetMetaData rsmeta = null;
		boolean header;
		String colsep = "  ";
    	String regex = "(?sim)('LOAD\\s+DATA.*IDENTITYOVERRIDE')";
    	StringBuffer buffer = new StringBuffer();
    	String loadStatement = "";
        int colCount, colLength;
        boolean skip = false;
        String colValue = "";
    	
    	Pattern pattern = Pattern.compile(regex);
    	Matcher matcher = pattern.matcher(sql);
    	boolean matchFound = matcher.find();
    	if (matchFound)
    	{
    		matcher.appendReplacement(buffer, "?");
    		loadStatement = matcher.group(0);
    	}
    	matcher.appendTail(buffer);  

    	try
    	{
	        CallableStatement cstmt = data.connection.prepareCall(buffer.toString());
	        cstmt.setString(1, loadStatement);
	        cstmt.registerOutParameter(2, java.sql.Types.SMALLINT);
	        cstmt.execute();
	        ResultSet Reader = cstmt.getResultSet();
            if (Reader == null) return;
            rsmeta = cstmt.getMetaData(); 
            if (rsmeta == null)
              rsmeta = Reader.getMetaData();
            colCount = rsmeta.getColumnCount();
            header = true;
       	    int[] len = new int[colCount];
			while (Reader.next())
			{
               buffer.setLength(0);
               skip = false;
               if (header)
               {
            	  header = false; 
                  for( int j = 1; j <= colCount; j++ ) 
                  { 
                	  colLength = rsmeta.getColumnDisplaySize(j);
                	  colValue = IBMExtractUtilities.trim(rsmeta.getColumnLabel(j));
                	  len[j-1] = colLength < colValue.length() ? colValue.length() : colLength;
                	  colValue = pad(colValue, colLength, " ");
                      buffer.append(colValue);
                      if (j != colCount)
                          buffer.append(colsep);                	  
                  }
       			  IBMExtractUtilities.log(buffer.toString(), 1);
    			  IBMExtractUtilities.log(ps, buffer.toString(), 1);			
                  buffer.setLength(0);
               }
               for( int j = 1; j <= colCount; j++ ) 
               { 
                  colValue = GetColumnValue(Reader, j);
                  if (colValue != null && colValue.equals("SkipThisRow"))
                  {
                      skip = true;
                      continue;
                  }
            	  colValue = pad(colValue, len[j-1], " ");
                  buffer.append(colValue);
                  if (j != colCount)
                      buffer.append(colsep);                           
               }
               if (!skip)
               {
       			  IBMExtractUtilities.log(buffer.toString(), 1);
    			  IBMExtractUtilities.log(ps, buffer.toString(), 1);			
               }
            }
            if (Reader != null)
        	   Reader.close();
            if (cstmt != null)
            	cstmt.close();
    	} catch (Exception e)
    	{
    		e.printStackTrace(ps);
    	}    	
    }
    
    private void CallSP(String schema, String tableName, String sql)
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
				s = String.format("%12s %30s %10d %10d %10d %10d %11d %10s", 
						schema, tableName, rowsRead, rowsLoaded, rowsRejected, rowsDeleted, rowsCommitted,
			            IBMExtractUtilities.getElapsedTime(start), 1);
				messageMap.put(schema+"."+tableName,messageStr);
				fileMap.put(schema+"."+tableName,fileStr);
		    	start = System.currentTimeMillis();		    	
			}
			if (rs != null)
				rs.close();
			if (cstmt != null)
	           cstmt.close();
			IBMExtractUtilities.log(s, 1);
			IBMExtractUtilities.log(ps, s, 1);			
    	} catch (Exception e)
    	{
    		e.printStackTrace(ps);
    	}
    }
    
    private String massageLoad(String sql)
    {
        sql = sql.replaceAll("(?im)^--.*", "");
        sql = sql.replaceAll("(?im)^MESSAGES.*", "MESSAGES ON SERVER");
        //sql = sql.replaceAll("(?im)^DUMPFILE=.*", "");        
        sql = sql.replaceAll("(?sm)^\\s*[\\n|\\r]", "");
        //sql = sql.replaceAll("\\\\", "\\\\\\\\");
        sql = "CALL SYSPROC.ADMIN_CMD('"+sql+"')";
        return sql;
    }
    
    private String fold(String s, int len)
    {
    	int l = s.length();
    	if (len < l)
    	{
    		s = s.substring(0,len-3) + "...";
    	}
    	return s;
    }
    
    public void ProcessLoadmessages()
    {
    	if (messageMap.size() == 0) return;
    	String key, messageSQL;
    	oldps = ps;
		String file = OUTPUT_DIR + "db2loadmessages.log", fileName;
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
			ps = oldps;
			e.printStackTrace();
		}
		ps = oldps;
		
		Iterator iter = fileMap.entrySet().iterator();
		while (iter.hasNext()) 
		{
			Map.Entry pairs = (Map.Entry) iter.next();
	        key = (String) pairs.getKey();
	        messageSQL = (String) pairs.getValue();
			IBMExtractUtilities.executeUpdate(data.connection, messageSQL);
		}		
    }
    
	public void DeployAllObjectsInDB2()
	{
		String method, sqlBody, type, schema, objectName;
		boolean loadHeader = true;
		String[] toks;
		
		Iterator iter = sqlCommands.iterator();
		while (iter.hasNext()) 
		{
			String element = (String) iter.next();
			int pos = element.indexOf("~#@");
			method = element.substring(0, pos);
			sqlBody = element.substring(pos+3);
			if (method.equalsIgnoreCase("useDatabase"))
			{
				continue;
			} else
			{
				toks = method.split(":");
				type = toks[0];
				schema = toks[1];
				objectName = toks[2];
			    if (sqlBody.matches("(?sim)^\\s*(select|values).*"))
			    {
			    	CallSelect(schema, objectName, sqlBody);
			    } else if (sqlBody.matches("(?sim)^\\s*(insert|update|delete).*"))
			    {
			    	CallUpdate(schema, objectName, sqlBody);
			    }			    
			    else if (sqlBody.matches("(?sim)^\\s*load\\s+from.*")) 
                {
			    	if (loadHeader)
			    	{
			    		loadHeader = false;
						String s = String.format("%12s %30s %10s %10s %10s %10s %11s %10s", 
								fold("Schema Name",12), fold("Table Name",30), "# Read", "# Loaded", "# Rejected", "# Deleted", "# Committed", "Time");
						IBMExtractUtilities.log(s, 1);
						IBMExtractUtilities.log(ps, s, 1);
			    	}
			    	CallSP(schema, objectName, massageLoad(sqlBody));
                } 
			    else if (sqlBody.matches("(?sim)CALL\\s+SYSPROC.DSNUTILS.*"))
			    {
			    	CallDSNUTIL(sqlBody);
			    }
			    else if (sqlBody.matches("(?sim)^\\s*load\\s+client\\s+from.*")) 
                {
			    	log("Client Load " + schema + "." + objectName + " should be run from db2 command window");
                } 
			    else
			    {
				    log(pad("Executing " + method, 55, "."),false);
			        deployObject(type, schema, objectName, sqlBody);
				    if (sqlCode.equals("0"))
				    {
				    	log(" .... success.", 1);
				    } else if (sqlCode.equals("1")) 
				    {
				    	log(" .... exists already.", 1);
				    }
				    else if (sqlCode.equals("2")) 
				    {
				    	log(" .... does not exist.", 1);
				    }
				    else
				    {
				    	log(" .... failed. SQL Code = " + sqlCode + " Line # = " + failedLineNumber + " " + sqlMessage, 1);		    	
				    }
			    }
			}
		}
	}
	
	public static void log(String msg, boolean line)
	{
		IBMExtractUtilities.log(ps, msg, line);
	}
	
	public static void log(String msg, int flag)
	{
		IBMExtractUtilities.log(ps, msg, flag);
	}
	
	public static void log(String msg)
	{
		IBMExtractUtilities.log(ps, msg);
	}
	
    public static void main(String[] args)
    {
    	String logFile = "";
    	long start = System.currentTimeMillis();
  	    String appHome = IBMExtractUtilities.getAppHome();
    	if (appHome == null || appHome.equals(""))
    	{
    		System.out.println("Specify location of application home using -DAppHome=<Location of the Tool Jar file directory>");
            System.exit(-1);
    	}
    	
        if (args.length < 1)
        {
            System.out.println("usage: java -Xmx600m -DAppHome=<Location of the Tool Jar file directory> ibm.DeployObjects sqlFileName");
            System.exit(-1);
        }

        OUTPUT_DIR = OUTPUT_DIR + "logs" + Constants.filesep;
        File tmpfile = new File(OUTPUT_DIR);
        tmpfile.mkdirs();

        File f = new File(args[0]);
    	int pos = f.getName().lastIndexOf(".");
    	logFile = f.getName().substring(0, pos) + ".log";
    	try
		{
			ps = new PrintStream(new FileOutputStream(OUTPUT_DIR + logFile, false));
			log("Log file = " + logFile);
		} catch (FileNotFoundException e)
		{
			ps = null;
			e.printStackTrace();
		}
        DeployObjects d = new DeployObjects(args[0]);
        d.ReadSQLCommands();
        d.Open();
        d.DeployAllObjectsInDB2();
        d.ProcessLoadmessages();
        d.Close();
        log("==== Total time: " + IBMExtractUtilities.getElapsedTime(start));

        if (ps != null)
            ps.close();
    }
}
