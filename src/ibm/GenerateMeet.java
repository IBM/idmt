/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class GenerateMeet
{
	private static String OUTPUT_DIR = System.getProperty("OUTPUT_DIR") == null ? "." : System.getProperty("OUTPUT_DIR");
    private static String linesep = System.getProperty("line.separator");
    private static String filesep = System.getProperty("file.separator");
    private static String sqlTerminator;
    private static boolean validObjects = true;
    private static Connection mainConn;
    private static int port = 1521, fetchSize = 1000;
    private static String dbSourceName = "oracle", server = "localhost", dbName = "XE", schemaList = "";
    private static String uid = "", pwd = "", jdbcHome = ".";
    private static int majorSourceDBVersion = -1, minorSourceDBVersion;
    private static BufferedWriter db2MEETPublicWriter, runMEETCommandWrite;
    private static BufferedWriter[] db2MEETWriter;
    private static String meetPublicFileName = "meet_public_input.sql";
    private static String mtkHome = (Constants.win()) ? "C:\\MTK" : "$HOME/MTK";
    private static String runMeetCommand;
    private static String[] meetFileName;
	private static ArrayList mtkFileList = new ArrayList();

    public GenerateMeet()
    {
    }

    private String executeSQL(String sql)
    {
    	return executeSQL(sql, true);
    }
    
    private String executeSQL(String sql, boolean terminator)
    {
    	int count = 0;
    	StringBuffer sb = new StringBuffer();

        PreparedStatement queryStatement;
        ResultSet Reader = null;
        String tok = (terminator) ? linesep + sqlTerminator + linesep : "~";

        try
        {
        queryStatement = mainConn.prepareStatement(sql);
        Reader = queryStatement.executeQuery();
        while (Reader.next()) 
        {
        	if (count > 0)
        		sb.append(tok);
        	sb.append(Reader.getString(1));
        	count++;
        }
        if (count > 0)
        	if (terminator)
        	   sb.append(linesep + sqlTerminator + linesep);
        if (Reader != null)
            Reader.close(); 
        if (queryStatement != null)
        	queryStatement.close();
        } catch (Exception e)
        {
        	e.printStackTrace();
        }
        return sb.toString();
    }
    
    private void initDBSources()
    {
        File tmpfile = new File(OUTPUT_DIR);
        tmpfile.mkdirs();
        try
	    {
		   log("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());
	    } catch (IOException e1)
	    {
		   log(e1.getMessage());
	    }
	    
	    DBData data = new DBData(dbSourceName, server, port, dbName, uid, pwd, "", 0);
	    mainConn = data.getConnection();
	    if (mainConn == null)
	    	return;
	    
	    majorSourceDBVersion = data.majorSourceDBVersion;
	    minorSourceDBVersion = data.minorSourceDBVersion;
    }

    private void GenerateRunMeetCommand() throws IOException
    {
    	String tmpFileName;
    	StringBuffer buffer = new StringBuffer();
    	
    	new File(OUTPUT_DIR).mkdirs();
	    if (Constants.win()) 
        {
	    	runMeetCommand = "runmeet.cmd";
        } else
        {
        	runMeetCommand = "runmeet";
        } 
		tmpFileName = OUTPUT_DIR + filesep + runMeetCommand;    	  
        buffer.setLength(0);
        
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep);
            buffer.append(":: This is a command script to run sql files through MEET in a batch mode." + linesep);
            buffer.append(":: Copy this file in the directory where MEET tool is installed." + linesep);
            buffer.append(":: Make sure that meetclp command is available in the directory." + linesep);
            buffer.append(":: Modify the SQL_HOME where you unzipped all *.sql files from the meet.zip." + linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Running MEET Analysis"  + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET OUTFILE=runmeet_OUTPUT.txt" + linesep);
            buffer.append("SET SQL_HOME=<Specify here the full path to the dir where *.sql were unzipped>"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            if (dbSourceName.equalsIgnoreCase("oracle"))
            {
	            buffer.append("ECHO Running "+meetPublicFileName+" through meet" + linesep);
	            buffer.append("CALL meetclp %SQL_HOME%" + filesep + meetPublicFileName + " >> %OUTFILE% 2>&1 " + linesep);
            }
            for (int i = 0; i < meetFileName.length; ++i)
            {
                buffer.append("ECHO Running "+meetFileName[i]+" through meet" + linesep);
            	buffer.append("CALL meetclp %SQL_HOME%" + filesep + meetFileName[i] + " >> %OUTFILE% 2>&1 " + linesep);
            }

            runMEETCommandWrite = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+IBMExtractUtilities.getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# This is a command script to run sql files through MEET in a batch mode."+linesep);
            buffer.append("# Copy this file in the directory where MEET tool is installed."+linesep);
            buffer.append("# Make sure that meetclp command is available in the directory."+linesep);
            buffer.append("# Modify the SQL_HOME name where you unzipped all *.sql files from the meet.zip."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Running MEET Analysis" + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("OUTFILE=runmeet_OUTPUT.txt" + linesep);
            buffer.append("SQL_HOME=<Specify here the full path to the dir where *.sql were unzipped"+linesep);
            buffer.append("#"+linesep+linesep);

            if (dbSourceName.equalsIgnoreCase("oracle"))
            {
	            buffer.append("echo Running "+meetPublicFileName+" through meet" + linesep);
	            buffer.append(". ./meetclp $SQL_HOME" + filesep + meetPublicFileName + " >> $OUTFILE 2>&1 " + linesep);
            }
            for (int i = 0; i < meetFileName.length; ++i)
            {
                buffer.append("echo Running "+meetFileName[i]+" through meet" + linesep);
            	buffer.append(". ./meetclp $SQL_HOME" + filesep + meetFileName[i] + " >> $OUTFILE 2>&1 " + linesep);
            }

            Runtime rt= Runtime.getRuntime();
            runMEETCommandWrite = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        runMEETCommandWrite.write(buffer.toString());
        runMEETCommandWrite.close();
        log("Command File " + tmpFileName + " created.");
	}
    
    private void genMEETOutputForPublicSchema(String schemaList)
    {
    	String sql = "";
    	String sql2 = (validObjects) ? " AND STATUS = 'VALID'" : "";
    	try
    	{
	    	db2MEETPublicWriter = new BufferedWriter(new FileWriter(OUTPUT_DIR + filesep + meetPublicFileName, false));
        	String version = GenerateMeet.class.getPackage().getImplementationVersion();
	        if (version != null)
	        	db2MEETPublicWriter.write("-- IBM Data Movement Tool Version    : " + version + linesep);
	        db2MEETPublicWriter.write("-- This MEET output was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
    	    log("Starting generation of Meet output");
    	    if (majorSourceDBVersion > 8)
    	    {
        	    // Public DB Links
	    	    sql = "SELECT TO_CHAR(DBMS_METADATA.GET_DDL('DB_LINK', object_name,'PUBLIC')) ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = 'PUBLIC' " +
			    		"AND OBJECT_TYPE = 'DATABASE LINK'" + sql2;
	    	    db2MEETPublicWriter.write(executeSQL(sql));
	    	    log("Meet output for public db links done");
	    	    // Public Directory
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('DIRECTORY', object_name) ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OBJECT_TYPE = 'DIRECTORY' " +
			    		"AND OBJECT_NAME NOT IN ('DATA_PUMP_DIR','ORACLECLRDIR')" + sql2;
	    	    db2MEETPublicWriter.write(executeSQL(sql));
	    	    log("Meet output for public directory done");
	    	    // Public Synonym
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('SYNONYM', SYNONYM_NAME, 'PUBLIC') ddl_string " +
			    		"FROM DBA_SYNONYMS WHERE OWNER = 'PUBLIC' " +
			    		"AND DB_LINK IS NULL " +
			    		"AND TABLE_OWNER IN ("+schemaList+")";    	    
	    	    db2MEETPublicWriter.write(executeSQL(sql));
    	    }
    		db2MEETPublicWriter.write("-- End of MEET output for public schema objects"  + linesep);
			db2MEETPublicWriter.close();
    	    log("Meet output for public schema objects done");
    	}
    	catch (Exception e)
    	{
    		e.printStackTrace();
    	}    	
    }
    
    private boolean checkWrapped(String data)
    {
    	String tokStr;
    	boolean wrapped = false, begin = false;
    	StreamTokenizer st = null;
    	
    	StringReader reader = new StringReader(data);
    	
    	try
    	{
    		st = new StreamTokenizer(reader);
    		while (st.nextToken() != StreamTokenizer.TT_EOF) 
    		{
    			tokStr = st.sval;
    			if(st.ttype == StreamTokenizer.TT_WORD && tokStr.equalsIgnoreCase("create"))
    				begin = true;
    			if (begin && st.ttype == StreamTokenizer.TT_WORD && tokStr.equalsIgnoreCase("wrapped"))
    				wrapped = true;
                if (begin && st.ttype == StreamTokenizer.TT_EOL)
                    break;
            }
    	} catch (Exception e) {	}    	
    	return wrapped;
    }
    
    private void getPLSQLSource(String schema, String type, String objectName, StringBuffer buffer)
    {

        PreparedStatement queryStatement;
        ResultSet Reader = null;
        String dstSchema = schema;
    	String plSQL = "select text from dba_source where owner = '"+dstSchema+"' " +
    			"and name = '"+objectName+"' and type = '"+type+"' order by line asc";
        try
        {
	        queryStatement = mainConn.prepareStatement(plSQL);
	        queryStatement.setFetchSize(fetchSize);
	        Reader = queryStatement.executeQuery();
	        int objCount = 0;
	        while (Reader.next()) 
	        {
	        	try
	        	{
		            if (objCount ==  0)
		            {
		            	buffer.append("CREATE OR REPLACE " + Reader.getString(1));
		            	objCount++;
		            } else
		               buffer.append(Reader.getString(1));
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting PL/SQL " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (Reader != null)
	            Reader.close();
	        if (queryStatement != null)
	        	queryStatement.close();
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }   
    }
    
    private void genMEETPLSQL(int idx, String schema)
    {
    	String plSQL = "", plsqlTemplate = "select dbms_metadata.get_ddl('&type&','&name&','&schema&') from dual";
    	String type, origType, name, ddlSQL;
        PreparedStatement queryStatement, plsqlStatement;
        ResultSet Reader = null, plsqlReader = null;
        StringBuffer chunks = new StringBuffer();
        String newType;

        if (validObjects)
        	plSQL = "select type, name from dba_source s, dba_objects o " +
					"where o.owner = s.owner " +
					"and o.owner = '"+schema+"' " +
					"and o.object_name = s.name " +
					"and o.status = 'VALID' " +
					"group by type, name";
        else
            plSQL = "select type, name from dba_source where owner = '"+schema+"' group by type, name";
        
        if (plSQL.equals("")) return;
        
        try
        {
	        queryStatement = mainConn.prepareStatement(plSQL);
	        queryStatement.setFetchSize(fetchSize);
	        Reader = queryStatement.executeQuery();
	        int objCount = 0;
	        while (Reader.next()) 
	        {
	        	try
	        	{
		            origType = Reader.getString(1);
		            name = Reader.getString(2);
		            type = origType.replace(" ", "_");
		            newType = type;
		            if (type.equals("PACKAGE"))
		            	newType = "PACKAGE_SPEC";		        	
		        	ddlSQL = plsqlTemplate.replace("&schema&", schema);
		            ddlSQL = ddlSQL.replace("&name&", name);
		            ddlSQL = ddlSQL.replace("&type&", newType);
		        	try
		        	{
		            	chunks.setLength(0);
			            plsqlStatement = mainConn.prepareStatement(ddlSQL);
			            plsqlReader = plsqlStatement.executeQuery();          
			            if (plsqlReader.next())
			            {
			            	IBMExtractUtilities.getStringChunks(plsqlReader, 1, chunks);
			            	if (chunks.length() == 0)
			            		getPLSQLSource(schema, origType, name, chunks);
			            }
			            if (plsqlReader != null)
				           plsqlReader.close();
				        if (plsqlStatement != null)
				           plsqlStatement.close();
		        	} catch (Exception ex)
		        	{
		        		if (chunks.length() == 0)
		        		    getPLSQLSource(schema, origType, name, chunks);
		        	}
		            if (chunks.length() > 0)
		            {
		            	chunks.append(linesep + sqlTerminator + linesep + linesep);   
		        	    try
						{
		        	    	String str = chunks.toString();
		        	    	if (origType.equalsIgnoreCase("TRIGGER"))
		        	    	{
		        	    		str = str.replaceAll("ALTER TRIGGER.*ENABLE", "");
		        	    	}
		        	    	boolean wrapped = checkWrapped(str);
		        	    	if (wrapped)
		        	    	{
		        	    		db2MEETWriter[idx].write("-- " + linesep);
		    					db2MEETWriter[idx].write("-- " + origType + ":" + schema + "." + name + " source is encrypted. Contact owner for the source." + linesep);
		        	    		db2MEETWriter[idx].write("-- " + linesep);
		        	    	} else
							    db2MEETWriter[idx].write(str);
							db2MEETWriter[idx].flush();
						} catch (IOException e)
						{
							log("Error writing to the meet file.");
							e.printStackTrace();
						}
		            }
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " numbers of PL/SQL objects extracted for schema " + schema);
		            objCount++;
	        	} 
	        	catch (SQLException ex)
	        	{
	        		log("Error getting PL/SQL " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " Total numbers of PL/SQL objects extracted for schema " + schema);
	        if (Reader != null)
	            Reader.close();
	        if (queryStatement != null)
	        	queryStatement.close();
        }
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }

        plSQL = "select object_type, object_name from dba_objects " +
			"where owner = '"+schema+"' " +
			"and status = 'INVALID' " +
			"group by object_type, object_name";
        try
        {
        	try
        	{
				db2MEETWriter[idx].write("-- List of Invalid Objects " + linesep);
	            queryStatement = mainConn.prepareStatement(plSQL);
	            queryStatement.setFetchSize(fetchSize);
	            Reader = queryStatement.executeQuery();
	            int objCount = 0;
	            Formatter fmt;
		        while (Reader.next()) 
		        {
	        		++objCount;
	        		fmt = new Formatter();
		            origType = Reader.getString(1);
		            name = Reader.getString(2);
		            fmt.format("-- %-30s %-30s", name, origType); 
					db2MEETWriter[idx].write(fmt.toString() + linesep);
		        }
		        db2MEETWriter[idx].write("-- " + objCount + " Total numbers of invalid objects found in schema " + schema + linesep);
		        if (Reader != null)
		            Reader.close();
		        if (queryStatement != null)
		        	queryStatement.close();
        	}
		    catch (SQLException e)
	        {
	        	e.printStackTrace();
	        }
        }
        catch (IOException e1)
	    {
			log("Error writing to the meet file.");
			e1.printStackTrace();
	    }
    }

    private String getColumnList(String schema, String mviewName)
    {
    	String sql = "";
    	String columnList = "";
    	
    	PreparedStatement queryStatement;
        ResultSet Reader = null;

        if (dbSourceName.equalsIgnoreCase("oracle"))
        	sql = "SELECT COLUMN_NAME FROM DBA_TAB_COLUMNS WHERE OWNER = '" + schema + 
        	      "' AND TABLE_NAME = '" + mviewName + "' ORDER BY COLUMN_ID ASC";
        
        if (sql.equals("")) return "";
        
        try
        {
          queryStatement = mainConn.prepareStatement(sql);
          queryStatement.setFetchSize(fetchSize);
          Reader = queryStatement.executeQuery();
          int objCount = 0;
          while (Reader.next()) 
          {
        	  if (objCount == 0)
        	  {
        		 columnList = IBMExtractUtilities.putQuote(Reader.getString(1));
         	  } else
        	  {
        		 columnList = columnList + "," + IBMExtractUtilities.putQuote(Reader.getString(1));
        	  }
        	  objCount++;
          }
          if (Reader != null)
             Reader.close(); 
          if (queryStatement != null)
        	 queryStatement.close();
        } catch (Exception e)
        {
        	log("Error in getting materialized query columns for materialized view " + mviewName);
        	e.printStackTrace();
        }    
        return columnList;
    }
    
    private String getViewSource(String schema, String viewName)
    {

        PreparedStatement queryStatement;
        ResultSet Reader = null;
        StringBuffer buffer = new StringBuffer();
        StringBuffer sb = new StringBuffer();
        String dstSchema = schema, columnList = "";
    	String plSQL = "", headView = ""; 
    	
    	if (dbSourceName.equalsIgnoreCase("oracle"))
    	{
    		headView = "CREATE OR REPLACE VIEW " + IBMExtractUtilities.putQuote(dstSchema) + "." + IBMExtractUtilities.putQuote(viewName) + linesep;
    		plSQL = "select text from dba_views where owner = '"+schema+"' and view_name = '"+viewName+"'";
    	}
    	
    	if (plSQL.equals(""))
    		return "";	

        try
        {
        	columnList = "(" + getColumnList(schema, viewName) + ")" + linesep + " AS " + linesep;
	        queryStatement = mainConn.prepareStatement(plSQL);
	        queryStatement.setFetchSize(fetchSize);
	        Reader = queryStatement.executeQuery();
	        int objCount = 0;
	        while (Reader.next()) 
	        {
	        	try
	        	{
	        		sb.setLength(0);
	        		IBMExtractUtilities.getStringChunks(Reader, 1, sb);
		            if (objCount ==  0)
		            {
		            	buffer.append(headView + columnList + sb.toString());
		            	objCount++;
		            } else
		               buffer.append(sb.toString());
	        	} catch (Exception ex)
	        	{
	        		log("Error getting PL/SQL " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (Reader != null)
	            Reader.close();
	        if (queryStatement != null)
	        	queryStatement.close();
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }   
	    return buffer.toString();
    }
    
    private String genMEETViews(String schema)
    {
    	String viewSQL = "", viewTemplate = "", viewName, ddlSQL, viewDDL;
        PreparedStatement queryStatement, viewStatement;
        ResultSet Reader = null, ViewDDLReader = null;
        StringBuffer buffer = new StringBuffer();

        if (dbSourceName.equalsIgnoreCase("oracle"))
        {
        	viewTemplate = "select dbms_metadata.get_ddl('VIEW','&viewName&','&schemaName&') from dual";
        	viewSQL = "select view_name from dba_views where owner = '"+schema+"' and view_name not like 'AQ$%'";
        }
        
        if (viewSQL.equals("")) return "";
        
        try
        {
	        queryStatement = mainConn.prepareStatement(viewSQL);
	        queryStatement.setFetchSize(fetchSize);
	        Reader = queryStatement.executeQuery();
	        int objCount = 0;
	        while (Reader.next()) 
	        {
	            viewName = Reader.getString(1);
	            ddlSQL = viewTemplate.replace("&schemaName&", schema);
	            ddlSQL = ddlSQL.replace("&viewName&", viewName);            
	            viewStatement = mainConn.prepareStatement(ddlSQL);
	            try 
	            {    
	                viewDDL = null;
	            	try
	            	{
	                    ViewDDLReader = viewStatement.executeQuery();
	                    if (ViewDDLReader.next())
	                    {
	                    	viewDDL = ViewDDLReader.getString(1);
	                    	if (viewDDL == null || viewDDL.length() == 0)
	                    		viewDDL = getViewSource(schema, viewName);
	                    }
	            	} catch (Exception ex)
	            	{
	            		viewDDL = getViewSource(schema, viewName);
	            	}
	                if (viewDDL != null && !viewDDL.equals(""))
	                {
	                    buffer.append(viewDDL);
	                	buffer.append(linesep + sqlTerminator +  linesep);            	
	                }
	                if (objCount > 0 && objCount % 20 == 0)
	                	log(objCount + " numbers of views extracted for MEET schema " + schema);
	                objCount++;
	            } catch (Exception e)
	            {
	            	log("genViews SQL=" + ddlSQL);
	            	e.printStackTrace();
	            }
	            if (ViewDDLReader != null)
	               ViewDDLReader.close();
	            if (viewStatement != null)
	            	viewStatement.close();
	        }
	        if (objCount > 0)
	    	   log(objCount + " Total numbers of views extracted for MEET schema " + schema);
	        if (Reader != null)
	            Reader.close();
	        if (queryStatement != null)
	        	queryStatement.close();        
        } catch (Exception e)
        {
        	e.printStackTrace();
        }
        return buffer.toString();
    }
    
    private String getMSSQLTSQLcript(String objName, String type)
    {
    	String sql, baseName = objName;
    	
    	if (type.equalsIgnoreCase("synonym"))
    	{
    		String[] strs = objName.split("\\.");
    		baseName = getBaseTableName(strs[0], strs[1]);
    		if (baseName.length() > 0)
    			type = "user table";
    		
    	}
    	if (type.equalsIgnoreCase("user table"))
    	{
	    	sql = "declare @table varchar(100),@tblScript varchar(max) " + 
			"select @table = '"+baseName+"',@tblScript='' " + 
			"select @tblScript = @tblScript + tbl " + 
			"from  " +
			"(  " +
			"        select 'create table ' + @table + '(' tbl, 0 ORDINAL_POSITION " + 
			"         union all  " +
			"         select CHAR(10)  " +
			"          +case when ORDINAL_POSITION = 1 then '' else  ',' end " + 
			"          + c.COLUMN_NAME " + 
			"          + ' ' " + 
			"          +DATA_TYPE " + 
			"          + ' '  " +
			"          + case when CHARACTER_MAXIMUM_LENGTH is not null then '(' + case when CHARACTER_MAXIMUM_LENGTH = -1 then 'max' else CAST(CHARACTER_MAXIMUM_LENGTH as varchar) end+ ')' else '' end " + 
			"          + ' '  " +
			"          + case when  ic.object_id is not null then ' IDENTITY(' + CAST(seed_value AS VARCHAR(5))+ ',' + CAST(increment_value AS VARCHAR(5)) + ')' ELSE '' END " +   
			"          + case when c.IS_NULLABLE = 'no' then 'not null' else 'null' end " + 
			"                ,ORDINAL_POSITION  " +
			"          from INFORMATION_SCHEMA.COLUMNS c  " +  
			"          LEFT JOIN sys.identity_columns ic ON c.TABLE_NAME = OBJECT_NAME(ic.object_id) AND c.COLUMN_NAME = ic.Name  " +   
			"        where TABLE_SCHEMA + '.' + TABLE_NAME = @table  " + 
			"        union all select CHAR(10) + ')',999999 " + 
			")d  " +
			"order by ORDINAL_POSITION " + 
			"select @tblScript";
    	} else if (type.equalsIgnoreCase("scalar function") || type.equalsIgnoreCase("table function") || type.equalsIgnoreCase("stored procedure"))
    	{
    		sql = "exec sp_helptext '"+objName+"'";
    	} else
    	{
    		sql = "";
    	}
    	return sql;
    }
    
    private String getBaseTableName(String schema, String table)
    {
    	if (schema == null || schema.length() == 0) return "";
    	if (table == null || table.length() == 0) return "";
    	String sql = "select replace(replace(s.base_object_name,'[',''),']','') " +
    			"from sys.synonyms s, sys.schemas c " +
    			"where s.schema_id = c.schema_id " +
    			"and c.name = '"+schema+"' " +
    			"and s.name = '"+table+"'";
    	return IBMExtractUtilities.executeSQL(mainConn, sql, "", false);    	
    }
    
    private String getSQLResults(String sql, String type)
    {
    	String objSource;
    	StringBuffer buf = new StringBuffer();
		if (sql != null && sql.length() > 0)
		{
			try
			{
			Statement stat3 = mainConn.createStatement();
			boolean results3 = stat3.execute(sql);
			ResultSet Reader3 = null; 
			int count3 = 0;
			do 
			{
				if (results3)
				{
					Reader3 = stat3.getResultSet();
					while (Reader3.next())
					{
						objSource = Reader3.getString(1);
						if (!(type.equalsIgnoreCase("user table") || type.equalsIgnoreCase("synonym")))
						{
							objSource = objSource.replaceAll("\\r","");
							objSource = objSource.replaceAll("\\n","");
						}
						buf.append(objSource + linesep);
					}
					buf.append("go"+linesep+linesep);
				} else
				{
					count3 = stat3.getUpdateCount();
				}
				results3 = stat3.getMoreResults();
			} while (results3 || count3 != -1); 
        	if (Reader3 != null)
	            Reader3.close();
	        if (stat3 != null)
	        	stat3.close();
			} catch (Exception e)
			{
				String msg = e.getMessage();
				if (!(msg.startsWith("There is no text") || msg.matches("The object.*does not exist.*")))
				   e.printStackTrace();
			}			
		}
		return buf.toString();
    }
    
    private void genMTKConfigFile(String outputdir)
    {
    	StringBuffer buf = new StringBuffer();
    	
    	buf.append("<?xml version=\"1.0\"?>" + linesep);
    	buf.append("<!DOCTYPE MTK SYSTEM \"" + mtkHome+filesep+"mtk.dtd\">" + linesep);
    	buf.append("<MTK>" + linesep);
    	buf.append("<PROJECT"  + linesep);
    	buf.append("NAME=\"MTKMIGR\" DIRECTORY=\""+outputdir+"\""  + linesep);
    	buf.append("SRCDBTYPE=\"SQL_SERVER\" TRGTDBTYPE=\"DB2LUW_V9.5\">" + linesep);
    	buf.append("<SPECIFY_SOURCE>" + linesep);
    	for (int i = 0; i < meetFileName.length; ++i)
    	{
    	    buf.append("<IMPORT>"+outputdir + filesep + meetFileName[i]+"</IMPORT>" + linesep);
    	}
    	buf.append("</SPECIFY_SOURCE>" + linesep);
    	buf.append("<CONVERSIONS>" + linesep);
    	for (int i = 0; i < meetFileName.length; ++i)
    	{
        	buf.append("<CONVERSION>" + linesep);
        	buf.append("<CONVERT SRCSQLFILES=\""+meetFileName[i]+"\"></CONVERT>" + linesep);
        	buf.append("<GENERATE_DATA_TRANSFER_SCRIPTS></GENERATE_DATA_TRANSFER_SCRIPTS>" + linesep);
        	buf.append("<DEPLOY_TO_TARGET></DEPLOY_TO_TARGET>" + linesep);
        	buf.append("</CONVERSION>" + linesep);
    	}
    	buf.append("</CONVERSIONS>" + linesep);
    	buf.append("</PROJECT>" + linesep);
    	buf.append("</MTK>" + linesep);
    	
		String tmpFileName = outputdir + filesep + "mtkconfig.xml"; 
		try
		{
			BufferedWriter mtkConfigWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
			mtkConfigWriter.write(buf.toString());
			mtkConfigWriter.close();
	        log("MTK Config File " + tmpFileName + " created.");
		} catch (Exception e)
		{
			e.printStackTrace();
		}
    }
    
    private void genMTKRunScript(String outputdir) throws IOException
    {
    	String tmpFileName, mtkRunScriptName;
    	StringBuffer buffer = new StringBuffer();
    	BufferedWriter mtkScriptWriter;
    	
    	if (Constants.win()) 
        {
    		mtkRunScriptName = "runmtk.cmd";
        } else
        {
        	mtkRunScriptName = "runmtk.sh";
        } 
		tmpFileName = OUTPUT_DIR + filesep + mtkRunScriptName;   
    	
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep);
            buffer.append(":: This is a command script to run SQL Server Stored Procedure through MTK in a batch mode." + linesep + linesep);
            buffer.append(":: ******************** PLEASE READ ********************************************" + linesep + linesep);
            buffer.append(":: Check JAVA_HOME used in this script. Make sure that it points to IBM Java 1.5 or later." + linesep);
            buffer.append(":: This is necessary as MTK will not run using Sun Java." + linesep);
            buffer.append(":: Open a command line window and run \"java -version\" command and check if it says Sun Java or IBM Java." + linesep);
            buffer.append(":: The Java Home used here is the same Java used for IDMT which may not be IBM Java." + linesep);
            buffer.append(":: If you have db2 installed on this machine, you will have IBM Java in sqllib/java/jdk/jre/bin directory." + linesep);
            buffer.append(":: If you do not have IBM Java, download from IBM Site and modify JAVA_HOME " + linesep);
            buffer.append(":: in this script manually before running it." + linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Running MTK in batch mode"  + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
                        
            buffer.append("SET OLDCD=%CD%" + linesep);
            buffer.append("SET OUTFILE=%OLDCD%\\runmtk_OUTPUT.txt" + linesep);
            buffer.append("SET OLDPATH=%PATH%" + linesep);
            buffer.append("SET JAVA_HOME="+IBMExtractUtilities.db2JavaPath()+linesep);
            buffer.append("SET PATH=\"%JAVA_HOME%\\bin\";%PATH%" + linesep);
            buffer.append("ECHO."+linesep+linesep);
            buffer.append("ECHO START TIME=%DATE% %TIME%"+linesep);
            
            buffer.append("ECHO Running "+mtkRunScriptName + linesep);
            buffer.append("CD " + mtkHome + linesep);
            buffer.append("CALL MTKMain.bat -config " + outputdir + filesep + "mtkconfig.xml" + " -import -convert 2>&1 > %OUTFILE%" +  linesep);
            buffer.append("CD %OLDCD%" + linesep);
            buffer.append("SET PATH=%OLDPATH%" + linesep);
            buffer.append("ECHO END TIME=%DATE% %TIME%"+linesep);
            mtkScriptWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+IBMExtractUtilities.getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# This is a command script to run SQL Server Stored Procedure through MTK in a batch mode." + linesep + linesep);
            buffer.append("# ******************** PLEASE READ ********************************************" + linesep + linesep);
            buffer.append("# Check JAVA_HOME used in this script. Make sure that it points to IBM Java 1.5 or later." + linesep);
            buffer.append("# This is necessary as MTK will not run using Sun Java." + linesep);
            buffer.append("# Open a command line window and run \"java -version\" command and check if it says Sun Java or IBM Java." + linesep);
            buffer.append("# The Java Home used here is the same Java used for IDMT which may not be IBM Java." + linesep);
            buffer.append("# If you have db2 installed on this machine, you will have IBM Java in sqllib/java/jdk/jre/bin directory." + linesep);
            buffer.append("# If you do not have IBM Java, download from IBM Site and modify JAVA_HOME " + linesep);
            buffer.append("# in this script manually before running it." + linesep + linesep);
            buffer.append("# run command \"chmod +x MTKMain.sh\" in " + mtkHome +linesep+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo $(date)" + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Running MTK in batch mode" + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("OLDCD=$PWD" + linesep);
            buffer.append("OUTFILE=$OLDCD/runmtk.log" + linesep);
            buffer.append("SET OLDPATH=$PATH" + linesep);
            buffer.append("JAVA_HOME="+IBMExtractUtilities.db2JavaPath()+linesep);
            buffer.append("PATH=$JAVA_HOME/bin:$PATH" + linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            buffer.append("ECHO Running "+mtkRunScriptName + linesep);
            buffer.append("cd " + mtkHome + linesep);
            buffer.append("MTKMain.sh -config " + outputdir + filesep + "mtkconfig.xml" + " -import -convert 2>&1 > $OUTFILE" +  linesep);
            buffer.append("cd $OLDCD" + linesep);
            buffer.append("PATH=$OLDPATH" + linesep);
            buffer.append("echo $(date)" + linesep);
            Runtime rt= Runtime.getRuntime();
            mtkScriptWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        mtkScriptWriter.write(buffer.toString());
        mtkScriptWriter.close();
        log("Command File " + tmpFileName + " created.");
    }
    
    private void genMEETInputForMSSQL(String schema)
    {
    	BufferedWriter mtkInputWriter = null;
    	PreparedStatement stat = null;
        ResultSet Reader = null;
    	Statement stat2 = null;
        ResultSet Reader2 = null;
    	String sql = "", sql2 = "", sql3, spName, objectName, type;
    	ArrayList al = new ArrayList();
    	
    	sql = "select routine_name " +
    			"from INFORMATION_SCHEMA.ROUTINES where routine_schema = '" + schema + "'";
    	
        try
        {
        	stat = mainConn.prepareStatement(sql);
        	stat.setFetchSize(fetchSize);
        	Reader = stat.executeQuery();
	        int objCount = 0;
	        while (Reader.next()) 
	        {
	        	spName = Reader.getString(1);
				String fileName = "mtk_"+ schema.toLowerCase() + "_" + spName + ".sql";
				mtkFileList.add(fileName);
	        	sql2 = "exec sp_depends '"+schema+"."+spName+"'";
	        	try
	        	{
	        		mtkInputWriter =  new BufferedWriter(new FileWriter(OUTPUT_DIR + filesep + fileName, false));
	    			writeHelp(mtkInputWriter, schema, spName);
	            	stat2 = mainConn.createStatement();
	            	boolean results = stat2.execute(sql2);
	            	int count = 0;
	            	do 
	            	{
	            		if (results)
	            		{
	            			Reader2 = stat2.getResultSet();
	            			while (Reader2.next())
	            			{
	            				objectName = Reader2.getString(1);
	            				type = Reader2.getString(2);
	            				if (!al.contains(objectName+"."+type) && !type.equalsIgnoreCase("stored procedure"))
	            				{
	            				    al.add(objectName+"."+type);
		            				//log(Reader2.getString(1) + ":" + Reader2.getString(2));
	            					sql3 = getMSSQLTSQLcript(objectName, type);
	            					mtkInputWriter.write(getSQLResults(sql3, type));
	            				}
	            			}
	            		} else
	            		{
	            			count = stat2.getUpdateCount();
	            		}
	            		results = stat2.getMoreResults();
	            		al.clear();
	            	} while (results || count != -1);	            	
	            	if (Reader2 != null)
	    	            Reader2.close();
	    	        if (stat2 != null)
	    	        	stat2.close();
	    	        sql3 = getMSSQLTSQLcript(spName, "stored procedure");
					mtkInputWriter.write(getSQLResults(sql3, "stored procedure"));
	    	        mtkInputWriter.close();
	        	} catch (Exception ex)
	        	{
	        		log(sql2);
	        		ex.printStackTrace();
	        	}
	            if (objCount > 0 && objCount % 20 == 0)
	            	log(objCount + " numbers of stored procedures extracted for schema " + schema);
		        objCount++;	        	
	        }
	        if (stat != null)
	        	stat.close();
	        if (Reader != null)
	        	Reader.close();
	        if (objCount > 0)
	        	log("Total " + objCount + " numbers of stored procedures extracted for schema " + schema);
	        meetFileName = new String[mtkFileList.size()];
	        mtkFileList.toArray(meetFileName);	        
        }
        catch (SQLException e)
        {
        	e.printStackTrace();
        } 
    }
    
    private void genMEETInputForSybase(int idx, String schema, String objTypeCode, String objType)
    {
    	PreparedStatement queryStatement = null;
        ResultSet Reader = null;
    	PreparedStatement queryStatement2 = null;
        ResultSet Reader2 = null;
    	String sql = "", sql2 = "", id, objectName, schemaName, line;
    	
    	/*
    	 * For procs we want to extract the independent ones first followed by
           the dependent ones.  Will only work if the dependency is local.
        
         NOTE: What to do about those procs that rely on temp tables ie, a
               parent proc creates a temp table, calls a subordinate that
               fills it. When you build these by hand the temp table creation
               has to be there for the subordinate proc to build successfully.
               However, it is next to impossible to determine if this is the
               case when reverse engineering.
    	 */ 
    	
    	sql = "" +
		    "	SELECT DISTINCT " +
		    "    o1.name, " +
		    "    u1.name, " +
		    "    o1.id " +
		    " FROM dbo.sysobjects  o1, " +
		    "    dbo.sysusers      u1, " +
		    "    dbo.sysprocedures p1 " +
		    " WHERE o1.type         = '"+objTypeCode+"' " +
            " AND u1.name           = '"+schema+"' " +
		    " AND u1.uid            = o1.uid " +
		    " AND o1.id             = p1.id " +
		    " AND p1.status & 4096 != 4096 " +
		    " AND NOT EXISTS (SELECT 1 " +
		    "                  FROM dbo.sysdepends d1, " +
		    "                       dbo.sysobjects o2 " +
		    "                 WHERE o1.id    = d1.id " +
		    "                   AND d1.depid = o2.id " +
		    "                   AND o2.type  = 'P') " +
		 " UNION " +
		 " SELECT DISTINCT " +
		 "       o3.name, " +
		 "       u3.name, " +
		 "       o3.id " +
		 "  FROM dbo.sysobjects    o3, " +
		 "       dbo.sysusers      u3, " +
		 "       dbo.sysprocedures p3 " +
		 " WHERE o3.type           = '"+objTypeCode+"' " +
         "   AND u3.name           = '"+schema+"' " +
		 "   AND u3.uid            = o3.uid " +
		 "   AND o3.id             = p3.id " +
		 "   AND p3.status & 4096 != 4096 " +
		 "   AND EXISTS (SELECT 1 " +
		 "                 FROM dbo.sysdepends d3, " +
		 "                      dbo.sysobjects o4 " +
		 "                WHERE o3.id    = d3.id " +
		 "                  AND d3.depid = o4.id " +
		 "                  AND o4.type  = 'P') ";
    	
    	sql2 = "SELECT text FROM dbo.syscomments WHERE id = &ID& ORDER BY colid ASC";
    	
        try
        {
        	queryStatement = mainConn.prepareStatement(sql);
        	queryStatement.setFetchSize(fetchSize);
        	Reader = queryStatement.executeQuery();
	        int objCount = 0;
	        while (Reader.next()) 
	        {
	        	try
	        	{
		            objectName = Reader.getString(1).trim();
		            schemaName = Reader.getString(2).trim();
		            id = Reader.getString(3);
		            try
		            {
		                String tmpsql = sql2.replace("&ID&", id);
		            	queryStatement2 = mainConn.prepareStatement(tmpsql);
		            	queryStatement2.setFetchSize(fetchSize);
		            	Reader2 = queryStatement2.executeQuery();
			            db2MEETWriter[idx].write("--#SET :" + objType.toUpperCase() + ":" + schemaName.toUpperCase() + ":" + objectName.toUpperCase() + linesep); 
		    	        while (Reader2.next()) 
		    	        {
		    	        	line = Reader2.getString(1);
		    	        	if (line != null)
		    	        	    db2MEETWriter[idx].write(line);
		    	        	else
		    	        		db2MEETWriter[idx].write("Source not retrieved. Null Error.");
		    	        }
			            db2MEETWriter[idx].write(linesep + "go" + linesep + linesep); 
		            	if (Reader2 != null)
		    	            Reader2.close();
		    	        if (queryStatement2 != null)
		    	        	queryStatement2.close();
		            } catch (Exception es)
		            {
		            	es.printStackTrace();		            	
		            }
		            
	        	} catch (SQLException ex)
	        	{
	        		ex.printStackTrace();
	        	}
	            if (objCount > 0 && objCount % 20 == 0)
	            	log(objCount + " numbers of "+objType+" extracted for schema " + schema);
	            objCount++;
	        }        	
	        if (Reader != null)
	            Reader.close();
	        if (queryStatement != null)
	        	queryStatement.close();
        }
        catch (SQLException e)
        {
        	e.printStackTrace();
        }    
    }

    private void genMEETInputForOracle(int idx, String schema)
    {
    	String sql = "";
    	String sql2 = (validObjects) ? " AND STATUS = 'VALID'" : "";
    	try
    	{
    		if (majorSourceDBVersion > 8)
    		{
	    		// Schema Tables
	    		sql = "SELECT     DBMS_METADATA.GET_DDL('TABLE', table_name,'"+schema+"') ddl_string " +
			    		"FROM DBA_TABLES WHERE OWNER = '"+schema+"' " + 
			    		"AND NVL(IOT_TYPE,'X') != 'IOT_OVERFLOW'";
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for tables done");
	    	    // Schema RI Constraints
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('REF_CONSTRAINT', CONSTRAINT_NAME, '"+schema+"') ddl_string " +
			    		"FROM DBA_CONSTRAINTS WHERE OWNER = '"+schema+"' " +
			    		"AND CONSTRAINT_TYPE = 'R'" + ((validObjects) ? " AND INVALID IS NULL" : "");
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for RI done");
	    	    // Schema indexes
	    	    sql = "SELECT dbms_lob.substr(DBMS_METADATA.GET_DDL('INDEX', object_name,'"+schema+"'),3990) ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
			    		"AND OBJECT_TYPE = 'INDEX'" + sql2;
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for indexes done");
	    	    // Schema Sequences
	    	    sql = "SELECT dbms_lob.substr(DBMS_METADATA.GET_DDL('SEQUENCE', object_name,'"+schema+"'),3990) ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
			    		"AND OBJECT_TYPE = 'SEQUENCE'" + sql2;
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for sequences done");
	    	    // DB Links
	    	    sql = "SELECT dbms_lob.substr(DBMS_METADATA.GET_DDL('DB_LINK', object_name,'"+schema+"'),3990) ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
			    		"AND OBJECT_TYPE = 'DATABASE LINK'" + sql2;
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for DB Links done");
	    	    // Schema Directory
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('DIRECTORY', object_name, '"+schema+"') ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
			    		"AND OBJECT_TYPE = 'DIRECTORY'" + sql2;
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for Directory done");
	    	    // Materialized Views
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW', object_name, '"+schema+"') ddl_string " +
	    	    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
	    	    		"AND OBJECT_TYPE = 'MATERIALIZED VIEW'" + sql2;
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for Materialized Views done");
	    	    // Materialized Views Log
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW_LOG', object_name, '"+schema+"') ddl_string " +
	    	    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
	    	    		"AND OBJECT_TYPE = 'MATERIALIZED VIEW LOG'" + sql2;
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for Materialized View Logs done");
	    	    // Java Source
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('JAVA_SOURCE', object_name, '"+schema+"') ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
			    		"AND OBJECT_TYPE = 'JAVA SOURCE'" + sql2;    	    
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for Java Source done");
	    	    // Synonym
	    	    sql = "SELECT DBMS_METADATA.GET_DDL('SYNONYM', object_name, '"+schema+"') ddl_string " +
			    		"FROM DBA_OBJECTS WHERE OWNER = '"+schema+"' " +
			    		"AND OBJECT_TYPE = 'SYNONYM'" + sql2;    	    
	    	    db2MEETWriter[idx].write(executeSQL(sql));
	    	    log("Meet output for Synonym done");
    		}
    	    // Views
    	    db2MEETWriter[idx].write(genMEETViews(schema));
    	    log("Meet output for Views done");
    	    // PL/SQL Objects
    	    genMEETPLSQL(idx, schema);
    		db2MEETWriter[idx].write("-- End of MEET output for schema = " + schema  + linesep);
    	    log("End of MEET output for schema = " + schema);
    	}
    	catch (Exception e)
    	{
    		log("SQL statement in error is :" + sql);
    		e.printStackTrace();
    	}
    }    

    private void writeHelp(BufferedWriter writer, String schema, String spName)
    {
    	String version = GenerateMeet.class.getPackage().getImplementationVersion();
		try
		{
	        if (version != null)
	        	writer.write("-- IBM Data Movement Tool Version    : " + version + linesep);
	        writer.write("-- This MTK Input file for schema '"+schema+"."+spName+"' was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
		} catch (IOException e)
		{
			e.printStackTrace();
		}    	
    }
    
    private void writeHelp(int idx, String schema)
    {
    	String version = GenerateMeet.class.getPackage().getImplementationVersion();
		try
		{
	        if (version != null)
	        	db2MEETWriter[idx].write("-- IBM Data Movement Tool Version    : " + version + linesep);
	        db2MEETWriter[idx].write("-- This MEET output for schema '"+schema+"' was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
    }
    
    private void zipFiles()
    {
    	File f;
    	boolean success;
    	FileInputStream in;
    	int len;
    	byte[] buf = new byte[1024*100];
    	try 
    	{
    		String outFilename = "meet.zip";
    		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(OUTPUT_DIR + filesep + outFilename));

			log("Adding file "+runMeetCommand+" to meet.zip");
    		in = new FileInputStream(OUTPUT_DIR + filesep + runMeetCommand);
    		out.putNextEntry(new ZipEntry(runMeetCommand));
			while ((len = in.read(buf)) > 0) 
			{
				out.write(buf, 0, len);
			}
			in.close();    		
    		
			log("Adding file Meet.log to meet.zip");
    		in = new FileInputStream(OUTPUT_DIR + filesep + "Meet.log");
    		out.putNextEntry(new ZipEntry("Meet.log"));
			while ((len = in.read(buf)) > 0) 
			{
				out.write(buf, 0, len);
			}
			in.close();    		

			log("Adding file MeetError.log to meet.zip");
    		in = new FileInputStream(OUTPUT_DIR + filesep + "MeetError.log");
    		out.putNextEntry(new ZipEntry("MeetError.log"));
			while ((len = in.read(buf)) > 0) 
			{
				out.write(buf, 0, len);
			}
			in.close();    		

			if (dbSourceName.equalsIgnoreCase("oracle"))
			{
				log("Adding file "+meetPublicFileName+" to meet.zip");
	    		in = new FileInputStream(OUTPUT_DIR + filesep + meetPublicFileName);
	    		out.putNextEntry(new ZipEntry(meetPublicFileName));
				while ((len = in.read(buf)) > 0) 
				{
					out.write(buf, 0, len);
				}
				in.close();  
			} 		
    		for (int i = 0; i < meetFileName.length; i++) 
    		{
    			log("Adding file "+meetFileName[i]+" to meet.zip");
    			in = new FileInputStream(OUTPUT_DIR + filesep + meetFileName[i]);
    			out.putNextEntry(new ZipEntry(meetFileName[i]));
    			while ((len = in.read(buf)) > 0) 
    			{
    				out.write(buf, 0, len);
    			}
    			out.closeEntry();
    			in.close();
    		}
    		out.close();
    		
    		f = new File(OUTPUT_DIR + filesep + runMeetCommand);
    		success = f.delete();
    		if (!success)
    			log("Unable to delete file " + runMeetCommand);

    		if (dbSourceName.equalsIgnoreCase("oracle"))
    		{
	    		f = new File(OUTPUT_DIR + filesep + meetPublicFileName);
	    		success = f.delete();
	    		if (!success)
	    			log("Unable to delete file " + meetPublicFileName);
    		}
    		for (int i = 0; i < meetFileName.length; i++) 
    		{
        		f = new File(OUTPUT_DIR + filesep + meetFileName[i]);
        		success = f.delete();
        		if (!success)
        			log("Unable to delete file " + meetFileName[i]);    			
    		}
    	} catch (IOException e) 
    	{
    		e.printStackTrace();
    	}
    }
    
    public void generateMeetOutput()
    {
    	if (dbSourceName.equalsIgnoreCase("oracle"))
    		generateOracleMeetOutput();
    	else if (dbSourceName.equalsIgnoreCase("sybase"))
    		generateSybaseMeetOutput();
    	else
    		generateMSSQLMeetOutput();
    }
    
    public void generateMSSQLMeetOutput()
    {
    	try
    	{
	    	String[] schemas = schemaList.split(":");	 
	    	schemaList = "";
			for (int idx = 0; idx < schemas.length; ++idx)
        	{
				if (idx > 0)
					schemaList += ",";
				schemaList += "'" + IBMExtractUtilities.removeQuote(schemas[idx]) + "'";
        	}
        	for (int idx = 0; idx < schemas.length; ++idx)
        	{
    			genMEETInputForMSSQL(IBMExtractUtilities.removeQuote(schemas[idx]));
        	}            			
        	genMTKConfigFile(OUTPUT_DIR);
        	genMTKRunScript(OUTPUT_DIR);
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}    	
    }
    
    public void generateSybaseMeetOutput()
    {
    	try
    	{
	    	String[] schemas = schemaList.split(":");	    	
			if (majorSourceDBVersion >= 11)
			{		    	
	    		try
	    		{
	    			String schemaList = "";
	    			for (int idx = 0; idx < schemas.length; ++idx)
	            	{
	    				if (idx > 0)
	    					schemaList += ",";
	    				schemaList += "'" + IBMExtractUtilities.removeQuote(schemas[idx]) + "'";
	            	}
	    			db2MEETWriter = new BufferedWriter[schemas.length];
	    			meetFileName = new String[schemas.length];
	            	for (int idx = 0; idx < schemas.length; ++idx)
	            	{
	            		meetFileName[idx] = "meet_" + schemas[idx].toLowerCase() + "_input.sql";
	            		db2MEETWriter[idx] =  new BufferedWriter(new FileWriter(OUTPUT_DIR + filesep + meetFileName[idx], false));
		    			writeHelp(idx, schemas[idx]);
	            	    genMEETInputForSybase(idx, IBMExtractUtilities.removeQuote(schemas[idx]), "P", "Procedures");
	            	    genMEETInputForSybase(idx, IBMExtractUtilities.removeQuote(schemas[idx]), "V", "Views");
	            	    genMEETInputForSybase(idx, IBMExtractUtilities.removeQuote(schemas[idx]), "TR", "Triggers");
	            	    db2MEETWriter[idx].close();
	            	}            			
            		GenerateRunMeetCommand();
            		zipFiles();
	    		} catch (Exception e)
	        	{
	    			e.printStackTrace();
	        	}
			} else
			{
				log("Sybase version < 12 is not yet supported to generate MEET output. Please contact vikram.khatri@us.ibm.com");
			}
			log("Work completed");
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}    	
    }
    
    public void generateOracleMeetOutput()
    {
    	try
    	{
	    	String[] schemas = schemaList.split(":");
	    	
			if (majorSourceDBVersion >= 7)
			{		    	
	    		try
	    		{
	    			String schemaList = "";
	    			for (int idx = 0; idx < schemas.length; ++idx)
	            	{
	    				if (idx > 0)
	    					schemaList += ",";
	    				schemaList += "'" + IBMExtractUtilities.removeQuote(schemas[idx]) + "'";
	            	}
	    			genMEETOutputForPublicSchema(schemaList);

	    			db2MEETWriter = new BufferedWriter[schemas.length];
	    			meetFileName = new String[schemas.length];
	            	for (int idx = 0; idx < schemas.length; ++idx)
	            	{
	            		meetFileName[idx] = "meet_" + schemas[idx].toLowerCase() + "_input.sql";
	            		db2MEETWriter[idx] =  new BufferedWriter(new FileWriter(OUTPUT_DIR + filesep + meetFileName[idx], false));
		    			writeHelp(idx, schemas[idx]);
	            	    genMEETInputForOracle(idx, IBMExtractUtilities.removeQuote(schemas[idx]));
	            	    db2MEETWriter[idx].close();
	            	}            			
            		GenerateRunMeetCommand();
            		zipFiles();
	    		} catch (Exception e)
	        	{
	    			e.printStackTrace();
	        	}
			} else
			{
				log("Oracle version < 7 is not yet supported to generate MEET output. Please contact vikram.khatri@us.ibm.com");
			}
			log("Work completed");
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    private static void log(String msg)
    {
    	IBMExtractUtilities.log(msg);
    }

	private String getVendor(int num)
	{
		String vendor = "";
		switch (num)
		{
		   case 1 : vendor = "oracle"; break;
		   case 2 : vendor = "sybase"; break;
		   default : vendor = "";
		}
		return vendor;
	}

	private int getVendorNum()
	{
		if (dbSourceName.equals("") || dbSourceName.equals("oracle"))
		   return 1;
		else if (dbSourceName.equals("sybase"))
		   return 2;
		else
		   return 0;
	}
	
	private void getSrcVendorConsoleInput()
	{
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		int num = 1;
		String inputa;
		do
		{
			int defNum = getVendorNum();
			num = 1;
			System.out.println("Oracle                  : 1 ");
			System.out.println("Sybase                  : 2 ");
			System.out.print("Enter a number (Default "+defNum+") : ");
			try
			{
				inputa = stdin.readLine();
				num = (inputa.equals("")) ? defNum : Integer.parseInt(inputa);
			}
			catch(Exception e)
			{
				num = -1;
			}
		} while (!(num >= 1 && num <= 2));		
		dbSourceName = getVendor(num);		
	}
	
    private boolean collectInput()
    {
    	boolean ok = false;
		IBMExtractConfig cfg = new IBMExtractConfig();
		cfg.loadConfigFile();
		
		getSrcVendorConsoleInput();
		server = cfg.getStringConsoleInput(server,"Enter "+dbSourceName+" Host Name or IP Address");
	    port = cfg.getIntConsoleInput(port, "Enter "+dbSourceName+"'s port number");
	    if (dbSourceName.equalsIgnoreCase("oracle"))
		   dbName = cfg.getStringConsoleInput(dbName, "Enter Oracle Service Name or Instance Name");
	    else
	       dbName = cfg.getStringConsoleInput(dbName, "Enter Sybase database name");	
		uid = cfg.getStringConsoleInput(uid, "Enter User ID of "+dbSourceName+" database");
	    pwd = cfg.getStringConsoleInput(pwd, "Enter "+dbSourceName+" database Passsword");	
	    String srcJDBC = cfg.getJDBCLocations(dbSourceName, "");
	    cfg.AddJarsToClasspath(srcJDBC);
		System.out.println("Now we will try to connect to "+dbSourceName+" to extract schema names.");
		if (cfg.getYesNoQuitConsoleInput(1, 
				"Do you want to continue (Yes) : 1 ", 
				"Quit program                  : 2 ",
				"Enter a number") == 2)
			return false;
		if (IBMExtractUtilities.TestConnection(true, true, dbSourceName, server, port, dbName, uid, pwd, ""))
		{
			String srcSchName2 = IBMExtractUtilities.GetSchemaList(dbSourceName, server, port, dbName, uid, pwd);
			if (IBMExtractUtilities.Message.equals(""))
			{
				System.out.println(dbSourceName+"'s schema List extracted=" + srcSchName2); 
				if (cfg.getYesNoQuitConsoleInput(1, 
						"Do you want to use '"+uid.toUpperCase()+"' as schema : 1 ", 
						"Or the extracted List                   : 2 ",
						"Enter a number") == 1)
				{
					schemaList = uid.toUpperCase();
				} else
					schemaList = srcSchName2;
				if (dbSourceName.equalsIgnoreCase("oracle"))
				{
					if (cfg.getYesNoQuitConsoleInput(2, 
							"Do you want to extract all objects : 1 ", 
							"Or only VALID objects              : 2 ",
							"Enter a number") == 2)
					{
						validObjects = true;
					} else
						validObjects = false;
				} else if (dbSourceName.equalsIgnoreCase("mssql"))
				{
					mtkHome = cfg.getStringConsoleInput(mtkHome, "Enter MTK Home directory Name ");
				}
				ok = true;
			}
			else
				System.out.println(IBMExtractUtilities.Message);				
		} else
		{
			System.out.println(IBMExtractUtilities.Message);
		}	
		return ok;
    }
    
    public static void runMeet()
    {
        IBMExtractUtilities.replaceStandardOutput(OUTPUT_DIR + filesep + "Meet.log");
        IBMExtractUtilities.replaceStandardError(OUTPUT_DIR + filesep + "MeetError.log");
    	GenerateMeet meet = new GenerateMeet();
    	if (meet.collectInput())
    	{
    		meet.initDBSources();
    		meet.generateMeetOutput();
    	}
    }
    
    public static void main(String[] args)
    {        
        if (args.length < 8)
        {
            System.out.println("usage: java -Xmx600m -DOUTPUT_DIR=. ibm.ExtractMeet " +
                               "source server dbname portnum uid pwd schemaList validObjects[true/false] [MTKHome]");
            System.exit(-1);
        }
        IBMExtractUtilities.replaceStandardOutput(OUTPUT_DIR + filesep + "Meet.log");
        IBMExtractUtilities.replaceStandardError(OUTPUT_DIR + filesep + "MeetError.log");
        dbSourceName = args[0];
        server = args[1];
        dbName = args[2];
        port = Integer.parseInt(args[3]);
        uid = args[4];
        pwd = args[5]; 
        schemaList = args[6]; 
        validObjects = Boolean.parseBoolean(args[7]);
        if (dbSourceName.equalsIgnoreCase("mssql"))
        {
        	if (args.length == 9 && args[8] != null)
        	   mtkHome = args[8];
        }
        pwd = IBMExtractUtilities.Decrypt(pwd);
        fetchSize = 1000;
        String version = GenerateMeet.class.getPackage().getImplementationVersion();
        if (version != null)
        	log("Version " + version);
        log("OS Type:" + System.getProperty("os.name"));
        log("Java Version:" + System.getProperty("java.version") + ": " + System.getProperty("java.vm.version") + ": " 
        		+ System.getProperty("java.vm.name") + ": " + System.getProperty("java.vm.vendor") + ": " + System.getProperty("sun.arch.data.model") + " bit");
        log("Default encoding " + System.getProperty("file.encoding"));
        log("dbSourceName:" + dbSourceName);
        log("server:" + server);
        log("dbName:" + dbName);
        log("port:" + port);
        log("uid:" + uid);
        log("fetchSize:" + fetchSize);
        log("Timezone = " + System.getProperty("user.timezone") + " Offset=" + IBMExtractUtilities.getTimeZoneOffset());
        sqlTerminator = (dbSourceName.equalsIgnoreCase("oracle")) ? "/" : "go";
        GenerateMeet meet = new GenerateMeet();
        meet.initDBSources();
        meet.generateMeetOutput();
    }
}
