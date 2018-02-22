package ibm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class zOSLoader implements Runnable
{
	private static String OUTPUT_DIR = Constants.OUTPUT_DIR;
	public static PrintStream ps = null;
	private IBMExtractConfig cfg = null;
	private TableList t;
	private String tableFileName;
	private byte colsep;
	private zOSLoad z;
	private int numThreads, commitCount;
	private java.util.concurrent.ExecutorService threadPool = null;
	private DBData[] data;
	private String userid, pwd;
	private HashMap dataTypeMap, columnList, columnLengthMap;
	private boolean replace;
	List<Future<Integer>> futures;
	
	public zOSLoader(String tableFileName, String colsep, int numThreads, int commitCount, boolean replace)
	{
		this.tableFileName = tableFileName;
		this.colsep = (byte) colsep.charAt(0);
		this.numThreads = numThreads;
		this.commitCount = commitCount;
		this.replace = replace;
    	cfg = new IBMExtractConfig();    	
    	cfg.loadConfigFile();
    	cfg.getParamValues();    
    	if (!cfg.paramPropFound)
    	{
    		IBMExtractUtilities.log("Error in loading "+IBMExtractUtilities.getConfigFile()+" file.");
    		System.exit(-1);
    	}
        userid = cfg.getDstUid();
        pwd = IBMExtractUtilities.Decrypt(cfg.getDstPwd());
    	data = new DBData[numThreads];
    	t = new TableList(cfg.getCaseSensitiveTabColName(), cfg.getDstVendor(), tableFileName, cfg.getExceptSchemaSuffix(), cfg.getExceptTableSuffix());
    	threadPool = java.util.concurrent.Executors.newFixedThreadPool(numThreads);
    	futures = new ArrayList<Future<Integer>>(t.totalTables);
    	open();
	}
	
	private void open()
	{
    	for (int i = 0; i < numThreads; i++)
        {
        	data[i] = new DBData(cfg.getDstVendor(), cfg.getDstServer(), cfg.getDstPort(), cfg.getDstDBName(), 
        			userid, pwd, "", 0);
        	data[i].setPrintStream(ps);
        	data[i].getConnection();    	
        }
        buildDataTypeMap();		
    	buildColumnList();
    	buildColLengthList();
	}
	
	private void close()
	{
    	for (int i = 0; i < numThreads; i++)
        {
    		data[i].commit();
    		data[i].close();
        }		
	}
	
    private String trim(String name)
    {
    	if (name != null)
    		name = name.trim();
    	return name;
    }
    
    private String getDstSchema(String schema)
    {
    	for (int i = 0; i < t.srcSchName.length; ++i)
    	{
    		if (schema.equals(IBMExtractUtilities.removeQuote(t.srcSchName[i])))
    		{
        		return IBMExtractUtilities.removeQuote(t.dstSchemaName[i]);
    		}
    	}
    	return schema;
    }
    
    private String getDstSelectedSchemaNames()
    {
    	String tmp2 = "", srcString = cfg.getSelectSchemaName(), sep = ":";
    	String[] tmp = srcString.split(sep);
    	for (int i = 0; i < tmp.length; ++i)
    	{
    		if (i > 0)
    			tmp2 += ":";
    		tmp2 += getDstSchema(tmp[i]);
    	}
    	return tmp2;
    }


    private void buildDataTypeMap()
    {
    	DBData d = data[0];
        String methodName = "BuildDataTypeMap";
        String key = "", sql = "";
        String dataType = "", allDataTypes = "";
        String prevSchema = "", prevTable = "", schema, table; 
        boolean added = false;

        String schemaList = IBMExtractUtilities.GetSingleQuotedString(getDstSelectedSchemaNames(),":");
        
        dataTypeMap = new HashMap();
        
        if (d.zDB2())
        {
     	   sql = "SELECT C.TBCREATOR, C.TBNAME, C.COLTYPE " +
	               "FROM   SYSIBM.SYSCOLUMNS C, SYSIBM.SYSTABLES T " +
	               "WHERE C.TBCREATOR = T.CREATOR AND C.TBNAME = T.NAME AND T.TYPE = 'T' AND " +
	               "C.TBCREATOR IN (" + schemaList + ") AND C.HIDDEN = 'N' ORDER BY C.TBCREATOR, C.TBNAME, C.COLNO";
     	   
        } else if (d.DB2())
        {
     	   sql = "SELECT TABSCHEMA, TABNAME, TYPENAME " +
	               "FROM   SYSCAT.COLUMNS " +
	               "WHERE  TABSCHEMA IN (" + schemaList + ") ORDER BY TABSCHEMA, TABNAME, COLNO";
     	}

        if (sql.length() == 0)
     	   return;
        
        try
        {
            SQL s = new SQL(d.connection);
            s.PrepareExecuteQuery(methodName, sql);        	

            int i = 0;
            boolean comma = false;
        	while (s.next())
        	{
        		schema = trim(s.rs.getString(1));
        		table = trim(s.rs.getString(2));
        		dataType = trim(s.rs.getString(3));
        		if (i == 0)
        		{
        			prevSchema = schema;
        			prevTable = table;
        		}
        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
        		{
        			dataTypeMap.put(key, allDataTypes);
        			allDataTypes = "";
        			added = true;
        			comma = false;
        		}
        		allDataTypes += (comma ? "," : "") + dataType;
    		    added = false;
    		    comma = true;
        		++i;
    			prevSchema = schema;
    			prevTable = table;
        		key = schema + "." + table;
        	}
        	if (!added && key.length() > 0)
        	{
        		dataTypeMap.put(key, allDataTypes);	        		
        	}
            s.close(methodName);
            IBMExtractUtilities.log(ps, i + " data types cached in dataTypeMap", 1);
        } catch (SQLException e)
        {
        	e.printStackTrace(ps);
        }
    }
    
    private void buildColLengthList()
    {
    	DBData d = data[0];
        String methodName = "BuildColumnList";
        String key = "", sql = "", length, allLengths = "";
        String prevSchema = "", prevTable = "", schema, table; 
        boolean added = false;
        
        String schemaList = IBMExtractUtilities.GetSingleQuotedString(getDstSelectedSchemaNames(),":");
        
        columnLengthMap = new HashMap();
        
        if (d.zDB2())
        {
     	   sql = "SELECT C.TBCREATOR, C.TBNAME, CASE C.COLTYPE WHEN 'VARCHAR' THEN C.LENGTH WHEN 'VARCHAR2' THEN C.LENGTH " +
     	   		"WHEN 'TIMESTAMP' THEN 30 WHEN 'DATE' THEN 20 WHEN 'TIME' THEN 20 " +
     	   		"WHEN 'BIGINT' THEN 20 WHEN 'INT' THEN 10 WHEN 'INTEGER' THEN 10 WHEN 'SMALLINT' THEN 6 WHEN 'CHAR' THEN C.LENGTH " +
     	   		"WHEN 'CHARACTER' THEN C.LENGTH ELSE 255 END " +
	               "FROM   SYSIBM.SYSCOLUMNS C, SYSIBM.SYSTABLES T " +
	               "WHERE C.TBCREATOR = T.CREATOR AND C.TBNAME = T.NAME AND T.TYPE = 'T' AND " +
	               "C.TBCREATOR IN (" + schemaList + ") AND C.HIDDEN = 'N' ORDER BY C.TBCREATOR, C.TBNAME, C.COLNO";
        } else if (d.DB2())
        {
     	   sql = "SELECT TABSCHEMA, TABNAME, CASE TYPENAME WHEN 'VARCHAR' THEN LENGTH WHEN 'VARCHAR2' THEN LENGTH " +
     	   		"WHEN 'TIMESTAMP' THEN 30 WHEN 'DATE' THEN 20 WHEN 'TIME' THEN 20 " +
     	   		"WHEN 'BIGINT' THEN 20 WHEN 'INT' THEN 10 WHEN 'INTEGER' THEN 10 WHEN 'SMALLINT' THEN 6 WHEN 'CHAR' THEN LENGTH WHEN 'CHARACTER' THEN LENGTH ELSE 255 END " +
	               "FROM   SYSCAT.COLUMNS " +
	               "WHERE  TABSCHEMA IN (" + schemaList + ") ORDER BY TABSCHEMA, TABNAME, COLNO";
     	}

        if (sql.length() == 0)
     	   return;
        
        try
        {
            SQL s = new SQL(d.connection);
            s.PrepareExecuteQuery(methodName, sql);        	

            int i = 0;
            boolean comma = false;
        	while (s.next())
        	{
        		schema = trim(s.rs.getString(1));
        		table = trim(s.rs.getString(2));
        		length = trim(s.rs.getString(3));
        		if (i == 0)
        		{
        			prevSchema = schema;
        			prevTable = table;
        		}
        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
        		{
        			columnLengthMap.put(key, allLengths);
        			allLengths = "";
        			added = true;
        			comma = false;
        		}
        		allLengths += (comma ? "," : "") + length;
    		    added = false;
    		    comma = true;
        		++i;
    			prevSchema = schema;
    			prevTable = table;
        		key = schema + "." + table;
        	}
        	if (!added && key.length() > 0)
        	{
        		columnLengthMap.put(key, allLengths);	        		
        	}
            s.close(methodName);
            IBMExtractUtilities.log(ps, i + " data types cached in columnLengthMap", 1);
        } catch (SQLException e)
        {
        	e.printStackTrace(ps);
        }
    }
    
    private void buildColumnList()
    {
    	DBData d = data[0];
        String methodName = "BuildColLengthList";
        String key = "", sql = "", column, allColumns = "";
        String prevSchema = "", prevTable = "", schema, table; 
        boolean added = false;
        
        String schemaList = IBMExtractUtilities.GetSingleQuotedString(getDstSelectedSchemaNames(),":");
        
        columnList = new HashMap();
        
        if (d.zDB2())
        {
        	sql = "SELECT C.TBCREATOR, C.TBNAME, C.NAME " +
        			"FROM   SYSIBM.SYSCOLUMNS C, SYSIBM.SYSTABLES T " +
        			"WHERE  C.TBCREATOR = T.CREATOR " +
        			"AND C.TBNAME = T.NAME " +
        			"AND T.TYPE = 'T' " +
        			"AND C.TBCREATOR IN (" + schemaList + ") " +
        			"AND C.HIDDEN = 'N' " +
        			"ORDER BY C.TBCREATOR, C.TBNAME, C.COLNO";        			
        } else if (d.DB2())
        {
     	   sql = "SELECT TABSCHEMA, TABNAME, COLNAME " +
	               "FROM   SYSCAT.COLUMNS " +
	               "WHERE  TABSCHEMA IN (" + schemaList + ") ORDER BY TABSCHEMA, TABNAME, COLNO";
     	}

        if (sql.length() == 0)
     	   return;
        
        try
        {
            SQL s = new SQL(d.connection);
            s.PrepareExecuteQuery(methodName, sql);        	

            int i = 0;
            boolean comma = false;
        	while (s.next())
        	{
        		schema = trim(s.rs.getString(1));
        		table = trim(s.rs.getString(2));
        		column = trim(s.rs.getString(3));
        		if (i == 0)
        		{
        			prevSchema = schema;
        			prevTable = table;
        		}
        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
        		{
        			columnList.put(key, allColumns);
        			allColumns = "";
        			added = true;
        			comma = false;
        		}
        		allColumns += (comma ? "," : "") + IBMExtractUtilities.putQuote(column);
    		    added = false;
    		    comma = true;
        		++i;
    			prevSchema = schema;
    			prevTable = table;
        		key = schema + "." + table;
        	}
        	if (!added && key.length() > 0)
        	{
        		columnList.put(key, allColumns);	        		
        	}
            s.close(methodName);
            IBMExtractUtilities.log(ps, i + " data types cached in columnList", 1);
        } catch (SQLException e)
        {
        	e.printStackTrace(ps);
        }
    }
    
    private void truncateTables()
    {
    	DBData d = data[0];
        String methodName = "truncateTables";
        String sql = "";
        String schema, table; 
        boolean added = false;
        
        SQL s = new SQL(d.connection);
        for (int i = 0; i < t.totalTables; i++)
        {
        	schema = t.dstSchemaName[i];
        	table = t.dstTableName[i];
        	if (d.zDB2())
        	{
             	sql = "TRUNCATE TABLE " + schema + "." + table + " REUSE STORAGE IGNORE DELETE TRIGGERS IMMEDIATE";
        	} else
        	{
            	if (d.majorSourceDBVersion >= 9 && d.minorSourceDBVersion >= 7)
             	   sql = "TRUNCATE TABLE " + schema + "." + table;
             	else
             	{
             	   sql = "ALTER TABLE " + schema + "." + table + " ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE";
             	}        		
        	}
        	try
			{
				d.connection.createStatement().execute(sql);
				IBMExtractUtilities.log(ps, IBMExtractUtilities.padRight(schema + "." + table, 50) + " truncated.", 1);
			} catch (SQLException e)
			{
				if (e.getErrorCode() == -204)
				   IBMExtractUtilities.log(ps, IBMExtractUtilities.padRight(schema + "." + table, 50) + " does not exist.", 1);
				else if (e.getErrorCode() == -904)
				{
			       IBMExtractUtilities.blog(ps, IBMExtractUtilities.padRight(schema + "." + table, 50) + " can not truncate since this table is in check pending.", 1);
				   e.printStackTrace(ps);
				}
				else
				{
			       IBMExtractUtilities.blog(ps, IBMExtractUtilities.padRight(schema + "." + table, 50) + "Table truncate problem Message="+e.getMessage(), 1);
				   e.printStackTrace(ps);
				}
			}
        }
    }
    
	public void run()
	{
		if (replace)
			truncateTables();
		
        for (int i = 0; i < t.totalTables; i++)
        {
        	futures.add(threadPool.submit(new zOSLoad(i, colsep, OUTPUT_DIR, commitCount, cfg, ps, t, data[i%numThreads], dataTypeMap, columnList, columnLengthMap)));
        }
        long result = 0;
        Iterator itr = futures.iterator();
        while (itr.hasNext())
        {
        	Future<Integer> future = (Future<Integer>) itr.next();
        	try
			{
				result += future.get();
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			} catch (ExecutionException e)
			{
				e.printStackTrace();
			}
        }
        threadPool.shutdown();
	}

    public static void main(String[] args)
    {
    	String logDir = "", logFile = "";
    	long start = System.currentTimeMillis();
  	    String appHome = IBMExtractUtilities.getAppHome();
    	if (appHome == null || appHome.equals(""))
    	{
    		System.out.println("Specify location of application home using -DAppHome=<Location of the Tool Jar file directory>");
            System.exit(-1);
    	}
    	
        if (args.length < 5)
        {
            System.out.println("usage: java -Xmx600m -DAppHome=<Location of the Tool Jar file directory> ibm.zOSLoader tableFileName colsep numThreads commitCount replace|insert");
            System.exit(-1);
        }

        logDir = OUTPUT_DIR + "logs" + Constants.filesep;
        File tmpfile = new File(logDir);
        tmpfile.mkdirs();

        File f = new File(args[0]);
    	int pos = f.getName().lastIndexOf(".");
    	logFile = f.getName().substring(0, pos) + ".log";
    	try
		{
			ps = new PrintStream(new FileOutputStream(logDir + logFile, false));
		} catch (FileNotFoundException e)
		{
			ps = null;
			e.printStackTrace();
		}
		zOSLoader d = new zOSLoader(args[0], args[1], Integer.valueOf(args[2]), Integer.valueOf(args[3]), Boolean.valueOf(args[4]));
		d.run();
		d.close();
        IBMExtractUtilities.blog(ps,"Please do not forget to check the log file " + logDir + logFile, 1);
        IBMExtractUtilities.blog(ps,"==== Total time: " + IBMExtractUtilities.getElapsedTime(start), 1);
        if (ps != null)
            ps.close();
    }
}
