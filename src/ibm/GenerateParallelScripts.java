package ibm;

import java.io.*;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;

public class GenerateParallelScripts
{
	private static String outputDirectory;
    private static Hashtable<String, TableFileInfo> tabInfo = new Hashtable<String, TableFileInfo>();
    private static ArrayList sortedList = null;
    private static String dbSourceName, tableFileName, server, dbName, userid, pwd;
    private static int numThreads, port, numJVM;
    private static boolean optimize = false;

    private Connection[] bladeConn;
	private ArrayList al = null;
	ArrayList[] distributedArrayList;
	private java.util.concurrent.ExecutorService s;
	
    public class TableFileInfo
    {
    	public String schemaName, query, tableName, srcSchName, srcTableName, countSQL;
    	public int multiTables, colPosition;
    	public long rowCount, tableSize;    	
    }

	public GenerateParallelScripts(String tableFileName)
	{
		s = java.util.concurrent.Executors.newFixedThreadPool(numThreads);
		al = TableList.readTableList(tableFileName);
		bladeConn = new Connection[numThreads];
		for (int i = 0; i < numThreads; ++i)
		{
			bladeConn[i] = ConnectMe();
		}
		ParseTableFile();
	}
	
	public void DisconnectMe()
	{
		for (int i = 0; i < numThreads; ++i)
		{
			if (bladeConn[i] != null)
				try
				{
					bladeConn[i].close();
				} catch (SQLException e)
				{
					e.printStackTrace();
				}
		}
	}
	
	private Connection ConnectMe()
	{
		Connection conn = null;
		DBData data = new DBData(dbSourceName, server, port, dbName, userid, pwd, "", 0);
		data.setAutoCommit(true);
		conn = data.getConnection();
        return conn;
	}
	
	private void GetTableCount()
	{
		long now, last = System.currentTimeMillis();
		GetTableCount task = null;
        Enumeration<String> keys = tabInfo.keys();
        int i = 0;
        while (keys.hasMoreElements()) 
        {
            String key = keys.nextElement();
            TableFileInfo value = tabInfo.get(key);
            task = new GetTableCount(dbSourceName, dbName, bladeConn[i%numThreads], value);
            s.execute(task);
            ++i;
        }
        s.shutdown();
        while (!s.isTerminated()) 
        {
        	try
			{
				Thread.sleep(1000);
			} catch (InterruptedException e) { }
		}
        DisconnectMe();
        now = System.currentTimeMillis();
        DecimalFormat myFormatter = new DecimalFormat("###.###");
        IBMExtractUtilities.log("It took " + myFormatter.format((now - last)/1000.0) + 
        		" seconds to get count of " + i + " tables");
	}
	
	private void ParseTableFile()
	{
		TableFileInfo tableInfo = null;
		String line, values, tmpStr;
		int i = 0;
        Iterator itr = al.iterator();
        while (itr.hasNext())
        {
           tableInfo = new TableFileInfo();	
           line = (String) itr.next();
           values = line.substring(0, line.indexOf(":"));
           tableInfo.schemaName = values.substring(0, values.indexOf("."));
           tableInfo.query = line.substring(line.indexOf(":")+1);
           tmpStr = tableInfo.query.toUpperCase();
           tableInfo.tableName = values.substring(values.indexOf(".")+1);
           tableInfo.srcSchName = tableInfo.query.substring(tmpStr.indexOf("FROM ")+5);
           tableInfo.srcTableName = tableInfo.srcSchName.substring(tableInfo.srcSchName.indexOf(".")+1);
           int m = tableInfo.srcTableName.indexOf('"', 1);
           if (m > 0)
        	   tableInfo.srcTableName = tableInfo.srcTableName.substring(0,m+1);
           int fromPos = tableInfo.srcSchName.indexOf(".");
           if (fromPos > 0)
           {
        	  tableInfo.srcSchName = tableInfo.srcSchName.substring(0, fromPos);
              if (dbSourceName.equalsIgnoreCase("sybase")) 
              {
            	  //tableInfo.countSQL = "SELECT COUNT_BIG(*) FROM " + tableInfo.srcSchName+"."+tableInfo.tableName;                   
            	  tableInfo.countSQL = "{call sp_spaceused '" + IBMExtractUtilities.removeQuote(dbName)+".."+IBMExtractUtilities.removeQuote(tableInfo.tableName) + "'}";
            	  tableInfo.colPosition = 4;
              }
              else if (dbSourceName.equalsIgnoreCase("mssql"))
              {
            	  tableInfo.countSQL = "exec sp_spaceused '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"."+IBMExtractUtilities.removeQuote(tableInfo.tableName) + "'";
            	  tableInfo.colPosition = 4;
              }
              else if (dbSourceName.equalsIgnoreCase("db2")) 
              {
            	  tableInfo.countSQL = "SELECT (CASE WHEN CARD = -1 THEN 1000 ELSE CARD END) * (CASE WHEN AVGROWSIZE = -1 THEN 0 ELSE AVGROWSIZE END) FROM SYSIBM.SYSTABLES WHERE " +
            	  		"CREATOR = '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"' AND NAME = '"+
            	  		  IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
              } 
              else if (dbSourceName.equalsIgnoreCase("zdb2")) 
              {
            	  tableInfo.countSQL = "SELECT (CASE WHEN CARDF = -1 THEN 1000 ELSE CARDF END) * RECLENGTH FROM SYSIBM.SYSTABLES WHERE " +
            	  		"CREATOR = '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"' AND NAME = '"+
            	  		  IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
              } 
              else if (dbSourceName.equalsIgnoreCase("oracle")) 
              {
            	  tableInfo.countSQL = "SELECT NUM_ROWS * AVG_ROW_LEN FROM DBA_TABLES WHERE " +
            	  		"OWNER = '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"' AND TABLE_NAME = '"+
            	  		  IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
              } 
              else if (dbSourceName.equalsIgnoreCase("mysql")) 
              {
            	  tableInfo.countSQL = "SELECT coalesce(data_length,0) FROM information_schema.tables WHERE " +
            	  		"TABLE_SCHEMA = '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"' AND TABLE_NAME = '"+
            	  		  IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
              } 
              else if (dbSourceName.equalsIgnoreCase("teradata"))
              {
            	  tableInfo.countSQL = "select sum(currentperm) from dbc.tablesize where databasename = '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"' " +
            	  		"and tablename = '"+IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";
              }
              else 
              {
            	  tableInfo.countSQL = "SELECT COUNT(*) FROM " + tableInfo.srcSchName+"."+tableInfo.tableName;                    
              }                    
           } else
           {
        	   tableInfo.srcSchName = tableInfo.schemaName;
               if (dbSourceName.equalsIgnoreCase("sybase")) 
               {
             	  tableInfo.countSQL = "{call sp_spaceused '" + IBMExtractUtilities.removeQuote(dbName)+".."+IBMExtractUtilities.removeQuote(tableInfo.tableName) + "'}";
             	  tableInfo.colPosition = 4;
               }
               else if (dbSourceName.equalsIgnoreCase("mssql"))
               {
             	  tableInfo.countSQL = "exec sp_spaceused '" + IBMExtractUtilities.removeQuote(tableInfo.srcSchName)+"."+IBMExtractUtilities.removeQuote(tableInfo.tableName) + "'";
             	  tableInfo.colPosition = 4;
               }
               else if (dbSourceName.equalsIgnoreCase("db2"))   
               {
             	  tableInfo.countSQL = "SELECT (CASE WHEN CARD = -1 THEN 1000 ELSE CARD END) * (CASE WHEN AVGROWSIZE = -1 THEN 0 ELSE AVGROWSIZE END) FROM SYSIBM.SYSTABLES WHERE " +
      	  		         " NAME = '"+IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
               } 
               else if (dbSourceName.equalsIgnoreCase("zdb2"))   
               {
             	  tableInfo.countSQL = "SELECT (CASE WHEN CARDF = -1 THEN 1000 ELSE CARDF END) * RECLENGTH FROM SYSIBM.SYSTABLES WHERE " +
      	  		         " NAME = '"+IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
               } 
               else if (dbSourceName.equalsIgnoreCase("oracle")) 
               {
             	  tableInfo.countSQL = "SELECT NUM_ROWS * AVG_ROW_LEN FROM USER_TABLES WHERE " +
             	  		"TABLE_NAME = '"+IBMExtractUtilities.removeQuote(tableInfo.tableName)+"'";                               	  
               } 
               else 
               {
            	   tableInfo.countSQL = "SELECT COUNT(*) FROM " + tableInfo.tableName;
               }
           }
           tabInfo.put(line, tableInfo);
           ++i;               
        }
	}
	
	class Compare implements Comparator
	{
		public int compare(Object obj1, Object obj2)
		{
		    int result = 0;
		    Map.Entry e1 = (Map.Entry) obj1;
		    Map.Entry e2 = (Map.Entry) obj2; //Sort based on values.
		    
		    TableFileInfo value1 = (TableFileInfo) e1.getValue();
		    TableFileInfo value2 = (TableFileInfo) e2.getValue();
		    
		    if (Long.valueOf(value1.rowCount).compareTo(value2.rowCount) == 0)
		    {
		       String word1 = (String) e1.getKey();
		       String word2 = (String) e2.getKey();
		       //Sort String in an alphabetical order
		       result = word1.compareToIgnoreCase(word2);
		    } else
		    {
		        //Sort values in a descending order
		        result = Long.valueOf(value1.rowCount).compareTo(value2.rowCount);
		     }
		     return result;
		}
	}
			
	private void SortHashtable()
	{
		sortedList = new ArrayList(tabInfo.entrySet());
		Collections.sort(sortedList, new Compare());
		
		//Show sorted results
		Iterator itr = sortedList.iterator();
		String key = "";
		long value = 0;
		int cnt = 0;
		while (itr.hasNext())
		{
		   Map.Entry e = (Map.Entry)itr.next();
		   key = (String) e.getKey();
		   value = ((TableFileInfo) e.getValue()).rowCount;
		   IBMExtractUtilities.log(key+",("+cnt+")"+value);
		   cnt++;
		}
	}
	
	private void DistributeTables()
	{
		int size = tabInfo.size();
		distributedArrayList = new ArrayList[numJVM];
		
		for (int i= 0; i < numJVM; ++i)
		{
			if ((size/numJVM) > 0)
			   distributedArrayList[i] = new ArrayList(size/numJVM);
			else
			   distributedArrayList[i] = new ArrayList();
		}
		
		Iterator itr = sortedList.iterator();
		String line = "", schema, table, prevSchema = "", prevTable = "";
		TableFileInfo value;
		int cnt = 0, pos, prevPos = 0;
		while (itr.hasNext())
		{
			pos = cnt%numJVM;
		    Map.Entry e = (Map.Entry) itr.next();
		    line = (String) e.getKey();
		    value = (TableFileInfo) e.getValue();
		    schema = value.schemaName;
		    table = value.tableName;
		    if (cnt == 0)
		    {
			    prevSchema = schema;
			    prevTable = table;		 
			    prevPos = pos;
		    }
		    if (schema.equals(prevSchema) && table.equals(prevTable))
		    {
		    	distributedArrayList[prevPos].add(line);
		    } else
		    {
		    	distributedArrayList[pos].add(line);
		    	prevPos = pos;
		    }
		    prevSchema = schema;
		    prevTable = table;		 
		    cnt++;
		}		
	}
	
	private void CreateWorkInMultipleJVM()
	{
		long start = System.currentTimeMillis();
		HashMap ht;
		String dirName;
		TableFileInfo value;
		BufferedWriter[] inputFileWriter = new BufferedWriter[numJVM];
		try
		{
			if (numJVM > 1)
			{
				for (int i = 0; i < numJVM; ++i)
				{
					dirName = outputDirectory + IBMExtractUtilities.filesep + "work" +
					IBMExtractUtilities.pad((i+1), 2, "0");
					File tmpfile = new File(dirName);
			        tmpfile.mkdirs();
			        IBMExtractUtilities.log("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());
			        inputFileWriter[i] = new BufferedWriter(new FileWriter(dirName + IBMExtractUtilities.filesep 
			        		+ dbName+".tables", false));			        
				}
			} else
			{
				dirName = outputDirectory;
				File tmpfile = new File(dirName);
		        tmpfile.mkdirs();
		        IBMExtractUtilities.log("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());
		        inputFileWriter[0] = new BufferedWriter(new FileWriter(dirName + IBMExtractUtilities.filesep 
		        		+ dbName+".tables", false));			        
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
		if (optimize)
		  GetTableCount();
		
		SortHashtable();
		DistributeTables();
		
		try
		{
			for (int i = 0; i < numJVM; ++i)
			{
				Iterator itr = distributedArrayList[i].iterator();
				while (itr.hasNext())
				{
					inputFileWriter[i].write((String) itr.next() + IBMExtractUtilities.linesep);
				}
	            inputFileWriter[i].close();
			}			
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		IBMExtractUtilities.log("====  Elapsed Time to build work directories: " + IBMExtractUtilities.getElapsedTime(start));
	}
	
	public static void GenParallelScripts(String outputDir, String dbSrcName, String tabFileName, int numjvm, int numthread,
			String srvr, String db, int portnum, String uid, String passwd, boolean opt)
	{
		outputDirectory = outputDir;
		dbSourceName = dbSrcName;
		tableFileName = tabFileName;
		numJVM = numjvm;
		numThreads = numthread;
		server = srvr;
		dbName = db;
		port = portnum;
		userid = uid;
		pwd = passwd;
		optimize = opt;
		GenerateParallelScripts ps = new GenerateParallelScripts(tableFileName);
		ps.CreateWorkInMultipleJVM();
	}
	
	public static void main(String[] args)
    {
        if (args.length < 11)
        {
            System.out.println("usage: java ibm.GenerateParallelScripts outputDir dbSourceName tableFileName numJVM numThreads server dbname portnum uid pwd optimize[true|false]");
            System.exit(-1);
        }
        GenParallelScripts(args[0], args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]), args[5],
        		args[6], Integer.parseInt(args[7]), args[8], args[9], Boolean.parseBoolean(args[10]));
    }
}
