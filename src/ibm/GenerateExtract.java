/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import ibm.lexer.OraToDb2Converter;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenerateExtract
{
	private static String OUTPUT_DIR = Constants.OUTPUT_DIR;
	private ArrayList loadWarningList;
	private HashMap messageMap, fileMap;
	private static String convertOracleTimeStampWithTimeZone2Varchar = "false";
	private static String convertTimeStampWithTimeZone2UTCTime = "false";
	private static String REEL_NUM = System.getProperty("REEL_NUM") == null ? "0" : System.getProperty("REEL_NUM");
    private static String filesep = System.getProperty("file.separator");
    private static String linesep = Constants.linesep;
    private static String sqlTerminator, sqlsep = ".", sep = Constants.win() ? ";" : ":";
    private static String colsep, encoding = "UTF-8", sqlFileEncoding = "UTF-8";
    private static String TABLES_PROP_FILE = "";
    private static boolean deleteLoadFile = false, netezza, zdb2, zos, db2;
    private static Properties propDataMap, propDataMapNZ, custDatamap, custNullMap, custColNameMap, custTSBP, propParams, udtProp = new Properties();
    private static String server = "", dstServer, dbName = "", dstDBName = "SAMPLE", dstUid, dstPwd, connectStr = "";
    private static String uid, pwd;
    private static String sybConnectStr = "", sybConnectStrMaster = "", varcharLimit = "4096", zConnectStr;
    private static String db2Instance = "", appJAR = "", sourceSchemaList, destinationSchemaList, selectSchemaName, schemaList, javaHome = null;
    private static boolean autoCommit = false, dataUnload, ddlGen, remoteLoad=false, retainColName = true, graphic = false;
    private static boolean usePipe = false, extractObjects = true, syncLoad = false, clusteredIndexes = false;
    private static String oracleNumberMapping = "false";
    private static boolean loadstats = false, norowwarnings = true, udfcreated = false, lobsToFiles = false, debug = false;
    private static boolean loadReplace = true, db2_compatibility = true, dbclob = false;
    private static boolean trimTrailingSpaces = false, regenerateTriggers = false, compressTable = false, compressIndex = false, extractPartitions = true;
    private static boolean extractHashPartitions = false, mysqlZeroDateToNull = false;
    private static boolean retainConstraintsName = false, useBestPracticeTSNames = true, loadException = true;
    private static String limitExtractRows = "ALL", limitLoadRows = "ALL", extentSizeinBytes, dstDB2Home, shell, batchSizeDisplay = "0", loadDirectory, tempFilesPath;
    private static String sybaseConvClass = "", mapCharToVarchar = "false", caseSensitiveTabColName = "false", mapTimeToTimestamp = "false", customMapping = "false";
    private static String exceptSchemaSuffix = "_EXP", exceptTableSuffix = "";
    private static int bytesBufferSize = 36000;
    private static int threads, port, dstPort, fetchSize, numIndex = 1, numFkey = 1, numUniq = 1, nameSeq = 0;
    private static int majorSourceDBVersion = -1, minorSourceDBVersion = -1, batchSize;
    private static float releaseLevel = -1.0F;
    private static long triggerCount = 1;
    private static int[] pipeHandles;
    private static DataOutputStream[] fp;
    private static FileChannel[] fc;
    private static BufferedWriter[] db2ObjectsWriter, db2PipeLoadWriter, db2SyncLoadWriter;
    private static Hashtable<String, Integer> plsqlHashTable = new Hashtable<String, Integer>();
    private static TableList t;
    private ZFileProcess z;
    private static BufferedWriter db2LoginWriter, db2LoadWriter, nzLoadScript, db2TablesWriter, db2FKWriter, db2DropWriter, db2DropExpTablesWriter, db2PKeysWriter, db2IndexesWriter, db2ViewsWriter, db2rolePrivsWriter;
    private static BufferedWriter db2FKDropWriter, db2RunstatWriter, db2TabStatusWriter, db2CheckPendingWriter, db2TabCountWriter, db2ExceptTabCountWriter, db2objPrivsWriter, db2GroupsWriter;
    private static BufferedWriter db2udfWriter, db2tsbpWriter, db2SynonymWriter, db2LoadTerminateWriter, db2FixLobsWriter;
    private static BufferedWriter db2ScriptWriter, db2CheckScriptWriter, db2CheckWriter, db2SeqWriter, db2DefaultWriter, db2TruncNameWriter;
    private static BufferedWriter db2mviewsWriter, db2DropSynWriter, db2DropSeqWriter, db2droptsbpWriter, db2DropScriptWriter, db2DropObjectsWriter, db2TempTablesWriter, db2ExceptionTablesWriter;
    
    private static BladeRunner[] blades;
    private static final Object empty = new Object();
    private static DBData mainData;
    private static Properties mapiDB2TableNames = null;
    private static int jobsCompleted = 1;
    private static String jobsStatus, sybaseUDTConversionToBaseType = "true", dstJDBC, commitCount, saveCount, warningCount; 
    private static double bytesUnloaded = 0L;
    
    private static HashMap synonymMap, columnLengthMap, lobsLengthMap, dataTypeMap, defaultValuesMap, partitionTypeMap, tempTableMap;
    private static HashMap tableCommentsMap, columnCommentsMap, checkConstraintMap, uniqueConstraintMap, notNullColumnsMap, checkColumnsMap;
    private static HashMap uniqueIndexNullMap, uniqueIndexAllMap, fkMap, partitionsMap;
    private static HashMap indexMap, indexExpressionMap , functionalIndexColMap, columnsMap, authColumnsMap, routineOptsMap, indexClusterMap;
    private static HashMap xmlIndexMap, tableAttributeMap, tableAttributeMap2, columnAttribMap, tabColumnsMap, partitionColumnsMap;
    private static HashMap procColumnsMap, procColumnsMap2, dataCaptureChangesMap;
    
    private static PingWorker ping = null;
    
    private static String fmtl(String cmd, String prefix, int len, String suffix)
    {
    	String str = IBMExtractUtilities.padRight(prefix, len) + " " + suffix;
    	return cmd + (Constants.win() ? str : putQuote(str));   
    }
    
    private static String FixString(String s)
    {
    	if (s != null && s.length() > 0)
    	{
    		s = s.replaceAll("-","_");
    		s = s.replaceAll(" ","_");
    	}
    	return s;
    }

    private static String sybSchema(String schemaName)
    {
    	return dbName + "_" + (schemaName.equalsIgnoreCase("dbo") ? schemaName.toUpperCase() : schemaName);
    }
    
    private static boolean db2Skin()
    {
    	return mainData.Sybase() && db2_compatibility;
    }
    
    private static String getCustomColumnName(String schema, String table, String oldColumnName)
    {
    	String newColName = IBMExtractUtilities.CustomColNameMapping(custColNameMap, schema, table, oldColumnName);    	
    	return newColName;
    }
    
    static private String getDstPwd()
    {
    	if (Constants.win())
    		return dstPwd;
    	else
    		return IBMExtractUtilities.escapeUnixChar(dstPwd);
    }
    
    static private String getJobsStatus()
    {
    	if (jobsStatus == null)
    		return "";
    	else 
    	{
    		if (REEL_NUM.equals("0"))
    			return " [" + jobsStatus + "]";
    		else
    			return " [" + jobsStatus + ":" + REEL_NUM + "]";
    	}    	
    }
    
    static private void setCaseSensitiveTableID()
    {
    	int count;
    	TableInfo tab1, tab2;
    	Enumeration<String> keys1 = t.tabInfo.keys();
    	Enumeration<String> keys2;
		int n1 = t.tabInfo.size();
    	while (keys1.hasMoreElements())
        {
    		count = 0;
    		String key1 = keys1.nextElement();
			keys2 = t.tabInfo.keys();
    		while (keys2.hasMoreElements())
	        {
    			String key2 = keys2.nextElement();
    			if (!key1.equals(key2))
    			{
	    			if (key1.equalsIgnoreCase(key2))
	    			{
	    				tab1 = t.tabInfo.get(key1);
	    				if (tab1.getDupID() == 0)
	    				{
	    				    tab1.setDupID(count++);
	    				}
	    				tab2 = t.tabInfo.get(key2);
	    				if (tab2.getDupID() == 0)
	    				{
	    				    tab2.setDupID(count++);
	    				}
	    			}
    			}
	        }
        }
    }
    
    static public ResultSet ExecuteQuery(String procName, PreparedStatement statement, String sql) throws SQLException
    {
    	long start = 0L;
    	if (debug) start = System.currentTimeMillis();
    	ResultSet rs = null;
    	rs = statement.executeQuery();
    	if (debug) log(procName + " ("+IBMExtractUtilities.getElapsedTime(start)+") SQL=" + sql);
    	return rs;
    }
    
    static public ResultSet ExecuteQuery(String procName, Statement statement, String sql) throws SQLException
    {
    	long start = 0L;
    	if (debug) start = System.currentTimeMillis();
    	ResultSet rs = null;
    	rs = statement.executeQuery(sql);
    	if (debug) log(procName + " ("+IBMExtractUtilities.getElapsedTime(start)+") SQL=" + sql);   
    	return rs;
    }
    
    static public String GetMethodName (StackTraceElement e[]) 
    {
       String methodName = "";
	   boolean doNext = false;
	   for (StackTraceElement s : e) 
	   {
	       if (doNext) 
	       {
	    	   return s.getMethodName();
	       }
	       doNext = s.getMethodName().equals("getStackTrace");
       }
	   return methodName;
    }

    static public String getCaseName(String name)
    {
         return caseSensitiveTabColName.equalsIgnoreCase("true") ? name : name.toUpperCase();
    }
    
    static public String putQuote(String name)
    {
    	return IBMExtractUtilities.putQuote(name);
    }
    
    static public String removeQuote(String name)
    {
    	return IBMExtractUtilities.removeQuote(name);
    }
    
    static public String trim(String name)
    {
    	if (name != null)
    		name = name.trim();
    	return name;
    }
    
    static public String byteToHex(byte b) 
    {
       char hexDigit[] = {
          '0', '1', '2', '3', '4', '5', '6', '7',
          '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
       };
       char[] array = { hexDigit[(b >> 4) & 0x0f], hexDigit[b & 0x0f] };
       return new String(array);
    }

    static public String getDstSchema(String schema)
    {
    	for (int i = 0; i < t.srcSchName.length; ++i)
    	{
    		if (schema.equals(removeQuote(t.srcSchName[i])))
    			return removeQuote(t.dstSchemaName[i]);
    	}
    	return schema;
    }
    
    static public String charToHex(char c) 
    {
       byte hi = (byte) (c >>> 8);
       byte lo = (byte) (c & 0xff);
       return byteToHex(hi) + byteToHex(lo);
    }
    
    static public String printBytes(byte[] array, int start, int len)
    {
        String tmp = "";
        for (int k = start; k < len; k++) {
           tmp += byteToHex(array[k]) + " ";
        }
        return tmp;    	
    }
    
    static public String printBytes(byte[] array) 
    {
    	return printBytes(array, 0, array.length);
    }
    
    private static String getNameSeq(String name)
    {
       Calendar now = Calendar.getInstance();
       SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
       nameSeq = (nameSeq == 9) ? 0 : nameSeq + 1;
       return (name + nameSeq + formatter.format(now.getTime())).substring(0,16);
    }

    private static String getHexCode(String singleChar) throws Exception
    {
       byte[] bytes;
       bytes = singleChar.getBytes(encoding);
       return Integer.toHexString(bytes[0] & 0xff).toUpperCase();
    }
        
    private static String getTruncName(int id, String type, String name, int defLen) throws IOException
    {
    	int len;
        if (!(name == null || name == ""))
        {
            name = getCaseName(name);
            if (releaseLevel != -1.0F && releaseLevel < 9.1F)
               len = defLen;
            else
            {
            	if (retainColName)
            		len = 128;
            	else
            		len = defLen;
            }            
            if (name.length() > len)
            {
            	if (type != null)
            	{
	                if (type.equals("COL"))
	                {
	                   db2TruncNameWriter.write(t.dstSchemaName[id]+sqlsep+t.dstTableName[id] + " Column" + name + " truncated to "+ name.substring(0,len) + linesep);
	                } else
	                {
	                   db2TruncNameWriter.write(t.dstSchemaName[id]+sqlsep+t.dstTableName[id] + " Cons" + name + " truncated to "+ name.substring(0,len) + linesep);                	
	                }
            	}
                name = name.substring(0,len);
            }
        }
        return name;
    }

    private static void assignDataFileFP(int i) throws IOException
    {
    	String schema = removeQuote(t.dstSchemaName[i].toLowerCase());
    	String table = removeQuote(t.dstTableName[i].toLowerCase());
    	int dupID = t.tabInfo.get(t.dstSchemaName[i]+"."+t.dstTableName[i]).getDupID();
    	String dupStr = (dupID == 0) ? "" : Integer.toString(dupID);
    	
		if (usePipe)
		{
			int pipeBuffer = 131072;
			String pipeName = IBMExtractUtilities.FixSpecialChars(schema+"_"+table) + dupStr;
			if (Constants.win())
			{
				pipeName = "\\\\.\\pipe\\"+pipeName;
				if (t.multiTables[i] == 0)
				   pipeHandles[i] = Pipes.CreateNamedPipe(pipeName, 0x00000003, 0x00000000, 2, pipeBuffer, pipeBuffer, 0xffffffff, 0);
				else
				{
					int tid = t.tabInfo.get(t.dstSchemaName[i]+"."+t.dstTableName[i]).getID();
				    pipeHandles[i] = pipeHandles[tid]; // Use same pipe handle for parallel unload of same table						
				}
				if (pipeHandles[i] > 0)
					log("Named Pipe " + pipeName + " created. Handle=" + pipeHandles[i]);
			} else
			{
				Process p = null;
				InputStream stdInput = null, stdError = null;
				pipeName = OUTPUT_DIR + "data" + filesep + pipeName + ".pipe";
				File pipeFile = new File(pipeName);
				pipeFile.deleteOnExit();
				if (!pipeFile.exists())
				{
					try
					{
						String[] cmd;
						String os = "";
						String pipeCmd = "mkfifo ";
						os = System.getProperty("os.name");
						if(os.equalsIgnoreCase("AIX"))
						{
							pipeCmd = "mkfifo ";
						}					
						cmd = new String[] {shell, "-c", pipeCmd + pipeName};
						p = Runtime.getRuntime().exec(cmd);
						stdInput = p.getInputStream();
						stdError = p.getErrorStream();
						new InputGUIStreamHandler(null, stdInput, true);
						new InputGUIStreamHandler(null, stdError, true);
						p.waitFor();
						stdInput.close();
						stdError.close();
						p.getInputStream().close();
						p.getErrorStream().close();
						p.destroy();
						log("Unix pipe " + pipeName + " created.");
					} catch (Exception e)
					{
						e.printStackTrace();
					}
				}
			}
		} else
		{
            String fileName = IBMExtractUtilities.FixSpecialChars(schema+"_"+table);
            if (t.multiTables[i] == 0)
            {
               fileName = OUTPUT_DIR + "data" + filesep + fileName + dupStr + ".txt";
               fp[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
            } else
            {
				int tid = t.tabInfo.get(t.dstSchemaName[i]+"."+t.dstTableName[i]).getID();
            	fp[i] = fp[tid]; // Use previous since this is a same table unloaded.
            }
		}
    }
    
    private  void initDBSources(String vendor)
    {
        int i;
        String retainCol, msgraphic, loadstat, nocopypend = "true";
        String tmpFileName, debugStr;
        String norowwarningsStr;
        String dbclobStr, trimTrailingSpacesStr, regenerateTriggersStr, retainConstraintsNameStr;
        String remoteLoadStr = "false", loadExceptionStr = "true", compressTableStr = "false", compressIndexStr = "false", dstDBReleaseStr = "-1.0F";
        String extractPartitionsStr = "true", useBestPracticeTSNamesStr = "true", extractHashPartitionsStr = "false", deleteLoadFileStr = "false";
        String mysqlZeroDateToNullStr = "false", clusteredIndexesStr = "false";

        File tmpfile = new File(OUTPUT_DIR);
        tmpfile.mkdirs();
        try {log("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());  } catch (IOException e1) { log(e1.getMessage());}
        custTSBP = new Properties();
        propDataMap = new Properties();
        propDataMapNZ = new Properties();
        custDatamap = new Properties();
        custNullMap = new Properties();
        propParams = new Properties();
        custColNameMap = new Properties();
        
        try
        {
        	String propFile = IBMExtractUtilities.getConfigFile();
        	try 
        	{
				propParams.load(new FileInputStream(propFile));
				log("Configuration file loaded: '" + propFile + "'");
			} catch (Exception e) {
				log("Configuration file : '" + propFile + "' not found. You need to run 'IBMDataMovementTool' command to create proper setup.");
				System.exit(-1);
			}
            
            javaHome = (String) propParams.getProperty("javaHome"); 
            shell = IBMExtractUtilities.getShell();
            appJAR = (String) propParams.getProperty("appJAR");
            dstJDBC = (String) propParams.getProperty("dstJDBC");
            debugStr = (String) propParams.getProperty("debug");
            retainCol = (String) propParams.getProperty("RetainColName");
            encoding = (String) propParams.getProperty("encoding");
            sqlFileEncoding = (String) propParams.getProperty("sqlFileEncoding");
            if (sqlFileEncoding == null || sqlFileEncoding.length() == 0 || sqlFileEncoding.equalsIgnoreCase("null"))
            	sqlFileEncoding = "UTF-8";
            msgraphic = (String) propParams.getProperty("graphic");
            loadstat = (String) propParams.getProperty("loadStats");
            norowwarningsStr = (String) propParams.getProperty("norowwarnings");
            dstDBName = (String) propParams.getProperty("dstDBName");
            customMapping = (String) propParams.getProperty("customMapping");
            dbclobStr = (String) propParams.getProperty("dbclob");
            trimTrailingSpacesStr = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("trimTrailingSpaces"), "false");
            regenerateTriggersStr = (String) propParams.getProperty("regenerateTriggers");
            remoteLoadStr = (String) propParams.getProperty("remoteLoad");
            loadExceptionStr = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("loadException"), "true");
            exceptSchemaSuffix = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("exceptSchemaSuffix"), "_EXP", true);
            exceptTableSuffix = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("exceptTableSuffix"), "", true);
            compressTableStr = (String) propParams.getProperty("compressTable");
            compressIndexStr = (String) propParams.getProperty("compressIndex");
            extractPartitionsStr = (String) propParams.getProperty("extractPartitions");
            extractHashPartitionsStr = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("extractHashPartitions"), "false");
            mysqlZeroDateToNullStr = (String) propParams.getProperty("mysqlZeroDateToNull");
            if (mysqlZeroDateToNullStr == null)
            	mysqlZeroDateToNullStr = "false";
            deleteLoadFileStr = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("deleteLoadFile"), "false");
            retainConstraintsNameStr = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("retainConstraintsName"), "false");
            useBestPracticeTSNamesStr = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("useBestPracticeTSNames"), "true");
            dstDB2Home = (String) propParams.getProperty("dstDB2Home");
            db2Instance = (String) propParams.getProperty("dstDB2Instance");            
            dstDBReleaseStr = (String) propParams.getProperty("dstDBRelease");
            limitExtractRows = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("limitExtractRows"),"ALL");
            mapCharToVarchar = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("mapCharToVarchar"), "false");
            varcharLimit = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("varcharLimit"), "4096");
            caseSensitiveTabColName = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("caseSensitiveTabColName"), "false");
            mapTimeToTimestamp = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("mapTimeToTimestamp"), "false");
            convertOracleTimeStampWithTimeZone2Varchar = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("convertOracleTimeStampWithTimeZone2Varchar"), "false");
            convertTimeStampWithTimeZone2UTCTime = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("convertTimeStampWithTimeZone2UTCTime"), "false");
            limitLoadRows = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("limitLoadRows"), "ALL");
            extentSizeinBytes = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("extentSizeinBytes"),"36000");
            commitCount = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("commitCount"),"100");
            saveCount = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("saveCount"),"0");            
            warningCount = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("warningCount"),"0");            
            batchSizeDisplay = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("batchSizeDisplay"),"0");
            loadDirectory = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("loadDirectory"), OUTPUT_DIR);
            tempFilesPath = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("tempFilesPath"), ""); 
            sourceSchemaList = (String) propParams.getProperty("srcSchName");
            destinationSchemaList = (String) propParams.getProperty("dstSchName");
            selectSchemaName = (String) propParams.getProperty("selectSchemaName");
            schemaList = IBMExtractUtilities.GetSingleQuotedString(selectSchemaName,":");
            clusteredIndexesStr = (String) propParams.getProperty("clusteredIndexes");
            oracleNumberMapping = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("oracleNumberMapping"), "false"); 
            sybaseConvClass = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("sybaseConvClass"), "");
            sybaseUDTConversionToBaseType = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("sybaseUDTConversionToBaseType"),"false");
            if (usePipe && syncLoad)
            {
            	log("You can either use pipe or sync load. Please correct unload script and re-run.");
            	System.exit(-1);
            }
            if (usePipe)
            {
            	log("You are using pipes. Turning objectsExtrcation to off");
            	extractObjects = false;
            }
            if (syncLoad)
            {
            	log("You are using sync load. Turning objectsExtrcation to off");
            	extractObjects = false;
            	if (!dataUnload)
            	{
            		log("For Sync Load, turning dataUnload=true");
            		dataUnload = true;
            	}            		
            }
            
        	try
        	{
        	   batchSize = Integer.parseInt(batchSizeDisplay);
        	} catch (Exception e)
        	{
        	   batchSize = 0;
        	}
        	if (batchSize <= 0)
        		batchSize = 0;
            
        	try
        	{
        		bytesBufferSize = Integer.valueOf(extentSizeinBytes);
        	} catch (Exception e)
        	{
        		bytesBufferSize = 36000;
        	}
                        
            remoteLoad = Boolean.valueOf(remoteLoadStr).booleanValue();
            loadException = Boolean.valueOf(loadExceptionStr).booleanValue();
            
        	dstServer = (String) propParams.getProperty("dstServer");
            if (dstServer == null || dstServer.length() == 0)
            {
            	log("Valid value for DB2 Server Address (dstServer) not found in "+IBMExtractUtilities.getConfigFile()+" file.");
            	System.exit(-1);                	
            }            	
        	String portStr = (String) propParams.getProperty("dstPort");
        	try 
        	{ 
        		dstPort = Integer.valueOf(portStr); 
        	} catch (Exception e)
        	{
        		log("Valid value for DB2 port number (dstPort) not found in "+IBMExtractUtilities.getConfigFile()+" file.");
            	System.exit(-1);
        	}
            if (portStr == null || portStr.length() == 0)
            {
        		log("Valid value for DB2 port number (dstPort) not found in "+IBMExtractUtilities.getConfigFile()+" file.");
            	System.exit(-1);                	
            }            	
            dstUid = (String) propParams.getProperty("dstUid");
            dstPwd = (String) propParams.getProperty("dstPwd");  
            if (dstUid == null || dstUid.length() == 0)
            {
            	log("Valid value for DB2 user id (dstUid) not found in "+IBMExtractUtilities.getConfigFile()+" file.");
            	System.exit(-1);                	
            }
            if (dstPwd == null || dstPwd.length() == 0)
            {
            	log("Valid value for DB2 user password (dstPwd) not found in "+IBMExtractUtilities.getConfigFile()+" file.");
            	System.exit(-1);                	
            }
            dstPwd = IBMExtractUtilities.Decrypt(dstPwd);
            connectStr = "CONNECT TO " + dstDBName + " USER " + dstUid + " USING " + dstPwd + ";" + linesep;
            zConnectStr = "CONNECT TO " + dstServer + ":" + dstPort + "/" + dstDBName + 
 		                      " USER " + dstUid + " USING " + dstPwd + ";";
            sybConnectStr = "--#SET TERMINATOR go" + linesep + linesep + "use " + dbName + linesep + "go" + linesep;
            sybConnectStrMaster = "--#SET TERMINATOR go" + linesep + linesep + "use master" + linesep + "go" + linesep;
           
            if (dbclobStr == null)
            	dbclobStr = "false";
            
            if (trimTrailingSpacesStr == null)
            	trimTrailingSpaces = true;
                      
            debug = Boolean.valueOf(debugStr).booleanValue();
            IBMExtractUtilities.debug = debug;
            retainColName = Boolean.valueOf(retainCol).booleanValue();
            encoding = encoding.toUpperCase();
            sqlFileEncoding = sqlFileEncoding.toUpperCase();
            graphic = Boolean.valueOf(msgraphic).booleanValue();
            loadstats = Boolean.valueOf(loadstat).booleanValue();
            norowwarnings = Boolean.valueOf(norowwarningsStr).booleanValue(); 
            dstDBName = dstDBName.toUpperCase();
            sqlTerminator = IBMExtractUtilities.sqlTerminator;
            dbclob = Boolean.valueOf(dbclobStr).booleanValue();
            trimTrailingSpaces = Boolean.valueOf(trimTrailingSpacesStr).booleanValue();
            regenerateTriggers = Boolean.valueOf(regenerateTriggersStr).booleanValue();
            compressTable = Boolean.valueOf(compressTableStr).booleanValue();
            compressIndex = Boolean.valueOf(compressIndexStr).booleanValue();
            extractPartitions = Boolean.valueOf(extractPartitionsStr).booleanValue();
            extractHashPartitions = Boolean.valueOf(extractHashPartitionsStr).booleanValue();
            mysqlZeroDateToNull= Boolean.valueOf(mysqlZeroDateToNullStr).booleanValue();
            deleteLoadFile = Boolean.valueOf(deleteLoadFileStr).booleanValue();
            retainConstraintsName = Boolean.valueOf(retainConstraintsNameStr).booleanValue();
            useBestPracticeTSNames = Boolean.valueOf(useBestPracticeTSNamesStr).booleanValue();
            
           
        	mainData = new DBData(vendor, server, port, dbName, uid, pwd, sybaseConvClass, 0);
            
            if (!(mainData.Oracle() || mainData.zDB2()))
            {
            	extractPartitions = false;
            }
            if (!mainData.Oracle())
            {
            	extractHashPartitions = false;
            }
            if (!(mainData.Oracle() || mainData.zDB2()))
            {
            	useBestPracticeTSNames = true;            	
            }
            
            try
            {
            	releaseLevel = Float.parseFloat(dstDBReleaseStr);
            } catch (Exception e1)
            {
            	releaseLevel = -1.0F;
            	log("Missing values for dstDBRelease in the property file");
            }

            if (!IBMExtractUtilities.CheckValidEncoding(encoding))
            {
                log("Invalid encoding specified: " + encoding);
                System.exit(-1);            	
            }
            
            if (!IBMExtractUtilities.CheckValidEncoding(sqlFileEncoding))
            {
                log("Invalid encoding specified: " + sqlFileEncoding);
                System.exit(-1);            	
            }
            if (clusteredIndexesStr == null || clusteredIndexesStr.equalsIgnoreCase("null"))
            {
            	if (!customMapping.equalsIgnoreCase("false"))
            		clusteredIndexes = true;
            	else
            		clusteredIndexes = false;
            } else
            {
            	clusteredIndexes = Boolean.valueOf(clusteredIndexesStr).booleanValue();
            }
        	if (compressTable) 
        	{
        		if (!(releaseLevel != -1.0F && releaseLevel >= 9.0F))
        		{
    				log("compressTable=true can not be used since database release = " + releaseLevel);
    				compressTable = false;
        		}            			
        	}
    		if (compressIndex)
    		{
        		if (!(releaseLevel != -1.0F && releaseLevel >= 9.7F))
        		{
    				log("compressIndex=true can not be used since database release = " + releaseLevel);
    				compressIndex = false;
        		}            			
    		}
            
            log("debug                 : " + ((debug) ? "True" : "False"));
			log("Encoding              : " + encoding);
			log("sqlFileEncoding       : " + sqlFileEncoding);
			log("RetainColName         : " + ((retainColName) ? "True" : "False"));
			log("graphic               : " + ((graphic) ? "True" : "False"));
			log("loadstats             : " + ((loadstats) ? "True" : "False"));
			log("norowwarnings         : " + ((norowwarnings) ? "True" : "False"));
			log("dstDBName             : " + dstDBName);
			log("customMapping         : " + customMapping);
			log("db2_compatibility     : " + ((db2_compatibility) ? "True" : "False"));
			log("dbclob                : " + ((dbclob) ? "True" : "False"));
			log("trimTrailingSpaces    : " + ((trimTrailingSpaces) ? "True" : "False"));
			log("remoteLoad            : " + ((remoteLoad) ? "True" : "False"));
			if (netezza || zdb2)
			{
				loadException = false;
			    log("loadException         : False (Override)");
			} else
			{
			    log("loadException         : " + ((loadException) ? "True" : "False"));
			}
			log("compressTable         : " + ((compressTable) ? "True" : "False"));
			log("compressIndex         : " + ((compressIndex) ? "True" : "False"));
			log("extractPartitions     : " + ((extractPartitions) ? "True" : "False"));
			log("extractHashPartitions : " + ((extractHashPartitions) ? "True" : "False"));
			log("retainConstraintsName : " + ((retainConstraintsName) ? "True" : "False"));
			log("useBestPracticeTSNames: " + ((useBestPracticeTSNames) ? "True" : "False"));
			log("limitExtractRows      : " + limitExtractRows);
			log("limitLoadRows         : " + limitLoadRows);
			log("extentSizeinBytes     : " + extentSizeinBytes);
			log("dstDB2Home            : " + dstDB2Home);
			log("mapCharToVarchar      : " + mapCharToVarchar);
			log("varcharLimit:" + varcharLimit);
			log("caseSensitiveTabColName:" + caseSensitiveTabColName);
			log("mapTimeToTimestamp    :"  + mapTimeToTimestamp);
			log("deleteLoadFile        : " + ((deleteLoadFile) ? "True" : "False"));
            if (zos && (usePipe || syncLoad))
            {
            	usePipe = false;
            	syncLoad = false;
            	if (usePipe)
            	   log("Pipe not yet supported on zdb2");
            	else
            	   log("Sync load is not yet supported on zdb2");
            } else
            {
			   log("usePipe               : " + ((usePipe) ? "True" : "False"));
			   log("syncLoad              : " + ((syncLoad) ? "True" : "False"));
            }
			if (!(db2Instance == null || db2Instance.equals("") || db2Instance.equals("null")))
			   log("DB2 Instance name     : " + db2Instance);
			if (!(releaseLevel == -1.0F))
		       log("DB2 Release         : " + releaseLevel);
			if (mainData.Mysql()) 
			{
				if (mysqlZeroDateToNull)
				{
					log("mysqlZeroDateToNull         : " + ((mysqlZeroDateToNull) ? "True" : "False"));
					log("mysqlZeroDateToNull=true will cause mysql 0000-00-00 date to be treated as NULL");
				}
			}
			
        	t = new TableList(caseSensitiveTabColName, mainData.getDBSourceName(), TABLES_PROP_FILE, exceptSchemaSuffix, exceptTableSuffix);

            if (t.totalTables < threads)
                threads = t.totalTables;
            
            int numFileLimit = IBMExtractUtilities.GetFileNumLimit();
            if (numFileLimit != Integer.MAX_VALUE)
            {
            	if (t.totalTables > (numFileLimit/4))
            	{
            		log("The output of 'ulimit -n' reported is " + numFileLimit);
            		log("Please ask your system administrator to set this limit to unlimited or to 65535(the maximum valid value). Exiting ....");
            		System.exit(-1);
            	}
            }

            lobsToFiles = false;
            if (zos)
            {
                lobsToFiles = true;
            } else 
            {
            	if (usePipe)
            	{
                    db2PipeLoadWriter = new BufferedWriter[t.totalTables];
            		if (Constants.win())
                      pipeHandles = new int[t.totalTables];
            		else
            	      fc = new FileChannel[t.totalTables];
            	} else if (syncLoad)
            	{
                    fp = new DataOutputStream[t.totalTables];
            		db2SyncLoadWriter = new BufferedWriter[t.totalTables];
            	}
            	else
                   fp = new DataOutputStream[t.totalTables];               
            }
            try
            {
            log("query size " + t.query.length + " schemaName size = " + t.dstSchemaName.length);
            }
            catch (NullPointerException ex)
            {
            	log("Nothing to Extract. IDMT is exiting.");
            	System.exit(-1);
            }
            catch (Exception ex)
            {
            	log("Something went wrong. IDMT is exiting.");
            	System.exit(-1);
            }
            for (i = 0; i < t.totalTables; ++i)
            {
                if (usePipe)
                {
                	if (Constants.win())
                       pipeHandles[i] = -1;
                }
            }

            if (!zos && (dataUnload || usePipe || syncLoad)) 
            {
                 new File(OUTPUT_DIR + "data").mkdirs();
              	 if (db2 || netezza)
              	 {
              		 if (netezza)
              		 {
	              		 new File(OUTPUT_DIR + "nzctl").mkdirs();
	              		 new File(OUTPUT_DIR + "nzbad").mkdirs();
	      	             new File(OUTPUT_DIR + "nzlog").mkdirs();
              		 } else
              		 {
	              		 tmpFileName = OUTPUT_DIR + "dump";
	              		 new File(tmpFileName).mkdirs();
	      	             if (!Constants.win())
	      	             {
	      	            	Runtime rt= Runtime.getRuntime();
	      	                try
	      					{
	      						rt.exec("chmod 755 "+ tmpFileName);
	      					} catch (IOException e)
	      					{
	      						e.printStackTrace();
	      					}
	      	             }              			 
	      	             new File(OUTPUT_DIR + "msg").mkdirs();
              		 }
              	 }
                 setCaseSensitiveTableID();
 	             for (i = 0; i < t.totalTables; ++i)
 	             {
 	                assignDataFileFP(i);	            	
 	             }
            }
            
            if (zos)
               z = new ZFileProcess(propParams, t);

            if (dataUnload && !usePipe) 
            {
            	if (db2Skin())
            	{
	   		       db2FixLobsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.fixlobs, false), sqlFileEncoding));
   	   		       IBMExtractUtilities.putHelpInformation(db2FixLobsWriter, Constants.fixlobs);
            	}
         	    db2LoadWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.load, false), sqlFileEncoding));
            	if (zos && z.generateJCLScripts)
            	{
            		z.zLoadWriter(db2LoadWriter);
            	} else
            	{
    	            IBMExtractUtilities.putHelpInformation(db2LoadWriter, Constants.load);
            	}
		        if (netezza)
		        {
		        	nzLoadScript = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.nzLoadScript, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(nzLoadScript, Constants.nzLoadScript);
			        if (!Constants.win())
     	            {
     	            	Runtime rt= Runtime.getRuntime();
     	                try
     					{
     						rt.exec("chmod 755 "+ OUTPUT_DIR + Constants.nzLoadScript);
     					} catch (IOException e)
     					{
     						e.printStackTrace();
     					}
     	            }
		        }
		        if (loadException)
		        {
            	    db2ExceptionTablesWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.exceptiontables, false), sqlFileEncoding));
		            IBMExtractUtilities.putHelpInformation(db2ExceptionTablesWriter, Constants.exceptiontables);
			        db2ExceptTabCountWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.excepttabcount, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2ExceptTabCountWriter, Constants.excepttabcount);
		        }
		        if (db2 || netezza || (zdb2 &&  zos))
		        {
		           db2RunstatWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.runstats, false), sqlFileEncoding));
		           IBMExtractUtilities.putHelpInformation(db2RunstatWriter, Constants.runstats);
		        }
		        if (!netezza)
		        {
			        db2TabStatusWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.tabstatus, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2TabStatusWriter, Constants.tabstatus);
		        }
		        if (db2)
		        {
			        db2LoadTerminateWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.loadterminate, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2LoadTerminateWriter, Constants.loadterminate);		        	
		        }
		        if (db2 || (zdb2 && zos))
		        {
		           db2CheckPendingWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.checkpending, false), sqlFileEncoding));
		           IBMExtractUtilities.putHelpInformation(db2CheckPendingWriter, Constants.checkpending);
		        }
		        db2TabCountWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.tabcount, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2TabCountWriter, Constants.tabcount);
            }
            if (extractObjects)
            {
            	db2DropObjectsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.dropobjects, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2DropObjectsWriter, Constants.dropobjects);        			
		        
	    		if (!IBMExtractUtilities.FileExists(OUTPUT_DIR + Constants.tempTables))
	    		{
	            	db2TempTablesWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.tempTables, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2TempTablesWriter, Constants.tempTables);		        
	    		}

                db2ViewsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.views, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2ViewsWriter, Constants.views);
		        if (db2Skin())
	               db2ViewsWriter.write(sybConnectStr);
		        else
		        {
                    db2ViewsWriter.write("--#SET TERMINATOR " + sqlTerminator + linesep);
		        }
            }
            
            if (extractObjects)
            {
		        db2SeqWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.sequences, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2SeqWriter, Constants.sequences);
		        db2DropSeqWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.dropsequences, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2DropSeqWriter, Constants.dropsequences);
                if (mainData.Oracle() || mainData.DB2() || mainData.Mssql() || mainData.zDB2())
                {
                	db2SynonymWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.synonyms, false), sqlFileEncoding));
		            IBMExtractUtilities.putHelpInformation(db2SynonymWriter, Constants.synonyms);
		            db2DropSynWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.dropsynonyms, false), sqlFileEncoding));
  		            IBMExtractUtilities.putHelpInformation(db2DropSynWriter, Constants.dropsynonyms);
                }
                if (mainData.Oracle())
                {
                	db2rolePrivsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.roleprivs, false), sqlFileEncoding));
   		            IBMExtractUtilities.putHelpInformation(db2rolePrivsWriter, Constants.roleprivs);
   		            db2objPrivsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.objprivs, false), sqlFileEncoding));
   		            IBMExtractUtilities.putHelpInformation(db2objPrivsWriter, Constants.objprivs);
                }
            }

            if (ddlGen && !usePipe) 
            {
                db2TablesWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.tables, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2TablesWriter, Constants.tables);
		        db2FKWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.fkeys, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2FKWriter, Constants.fkeys);
		        db2DropWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.droptables, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2DropWriter, Constants.droptables);
		        db2DropExpTablesWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.dropexceptiontables, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2DropExpTablesWriter, Constants.dropexceptiontables);
		        db2PKeysWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.pkeys, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2PKeysWriter, Constants.pkeys);
		        db2FKDropWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.dropfkeys, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2FKDropWriter, Constants.dropfkeys);
		        if (!netezza)
		        {
			        db2IndexesWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.indexes, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2IndexesWriter, Constants.indexes);
		            db2CheckWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.check, false), sqlFileEncoding));
		            IBMExtractUtilities.putHelpInformation(db2CheckWriter, Constants.check);
		        }
	            if (mainData.Oracle() || mainData.DB2())
	            {
  		            db2mviewsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.mviews, false), sqlFileEncoding));
  		            IBMExtractUtilities.putHelpInformation(db2mviewsWriter, Constants.mviews);
	            }
                if (mainData.Sybase())
                {
   		            db2objPrivsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.objprivs, false), sqlFileEncoding));
   		            IBMExtractUtilities.putHelpInformation(db2objPrivsWriter, Constants.objprivs);
   		            if (db2_compatibility)
   		            {
   	   		            db2LoginWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.logins, false), sqlFileEncoding));
   	   		            IBMExtractUtilities.putHelpInformation(db2LoginWriter, Constants.logins);   		            	
   	   		            db2GroupsWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.groups, false), sqlFileEncoding));
   	   		            IBMExtractUtilities.putHelpInformation(db2GroupsWriter, Constants.groups);     	   		            
   		            }
                }
                if (db2 || netezza)
                {
			        db2tsbpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.tsbp, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2tsbpWriter, Constants.tsbp);
			        db2droptsbpWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.droptsbp, false), sqlFileEncoding));
			        IBMExtractUtilities.putHelpInformation(db2droptsbpWriter, Constants.droptsbp);
                }
                if (!netezza)
                {
                    db2udfWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.udf, false), sqlFileEncoding));
    		        IBMExtractUtilities.putHelpInformation(db2udfWriter, Constants.udf);
                }
		        db2TruncNameWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.truncname, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2TruncNameWriter, Constants.truncname);
		        db2DefaultWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.defalt, false), sqlFileEncoding));
		        IBMExtractUtilities.putHelpInformation(db2DefaultWriter, Constants.defalt);
                if (mainData.Oracle() || mainData.DB2())
                {
                	db2mviewsWriter.write("--#SET TERMINATOR " + sqlTerminator + linesep);
                }
                if (db2Skin())
                {
                	db2FKDropWriter.write(sybConnectStr);
                	if (extractObjects)
                	{
                       db2DropObjectsWriter.write(sybConnectStr);
                	}
                    db2DropWriter.write(sybConnectStr);
                    db2LoginWriter.write(sybConnectStrMaster);
                    db2GroupsWriter.write(sybConnectStr);
                    db2TablesWriter.write(sybConnectStr);                    
                    db2CheckWriter.write(sybConnectStr);
                    db2FKWriter.write(sybConnectStr);
                    db2IndexesWriter.write(sybConnectStr);
                	db2PKeysWriter.write(sybConnectStr);
                	db2objPrivsWriter.write(sybConnectStr);
                }
            }
            tmpFileName = IBMExtractUtilities.db2ScriptName(OUTPUT_DIR, ddlGen, dataUnload);
            if (!usePipe)
            {
            	db2ScriptWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFileName, false), sqlFileEncoding));
            	db2DropScriptWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.dropscripts, false), sqlFileEncoding));
            	if (db2)
            	{
	            	db2CheckScriptWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + Constants.checkremoval, false), sqlFileEncoding));
            	}

	            if (!Constants.win())
	            {
	                Runtime rt= Runtime.getRuntime();
	                rt.exec("chmod 755 "+tmpFileName);
	                rt.exec("chmod 755 "+OUTPUT_DIR + Constants.dropscripts);
	            	if (!netezza)
	            	{
		                rt.exec("chmod 755 "+OUTPUT_DIR + Constants.checkremoval);
	            	}
	            }
            }
            
            if (netezza)
               custTSBP = IBMExtractUtilities.InstantiateCustomProperties(mainData.dbSourceName, "CustomColumnDistribution");
            else
               custTSBP = IBMExtractUtilities.InstantiateCustomProperties(mainData.dbSourceName, "CustTableSpaceMapPropFile");            	
            custDatamap = IBMExtractUtilities.InstantiateCustomProperties(mainData.dbSourceName, "CustDataMapPropFile");
            custNullMap = IBMExtractUtilities.InstantiateCustomProperties(mainData.dbSourceName, "CustNullPropFile");
            custColNameMap = IBMExtractUtilities.InstantiateCustomProperties(mainData.dbSourceName, "CustColNamePropFile");
            propDataMap = IBMExtractUtilities.InstantiateProperties("DataMapPropFile");
            propDataMapNZ = IBMExtractUtilities.InstantiateProperties("DataMapNZPropFile");
        } catch (IOException ex)
        {
            log("Correct this serious error");
            ex.printStackTrace();
            System.exit(-1);
        }

    	Connection mainConn = mainData.getConnection();
    	
    	if (mainConn == null)
            System.exit(-1);

    	majorSourceDBVersion = mainData.majorSourceDBVersion;
    	minorSourceDBVersion = mainData.minorSourceDBVersion;
    	BuildMainMemoryMap();
        if (mainData.Oracle())
        {
     	   IBMExtractUtilities.CheckOracleRequisites(mainConn, uid, majorSourceDBVersion);
        } else if (mainData.Teradata())
        {
     	   IBMExtractUtilities.CheckTeradataRequisites(mainConn, uid);
        }

        if (!autoCommit)
        	mainData.commit();
        if (usePipe || syncLoad)
        {
        	IBMExtractUtilities.checkTargetTablesStatus(null, loadException, caseSensitiveTabColName, exceptSchemaSuffix, exceptTableSuffix, TABLES_PROP_FILE, Constants.getDbTargetName(), dstServer, dstPort, dstDBName, dstUid, dstPwd);
    		if (!IBMExtractUtilities.Message.equals(""))
    		{
    			log(IBMExtractUtilities.Message);
    			System.exit(-1);
    		}		
        }    
        
        if (usePipe || syncLoad)
        {
        	loadWarningList = new ArrayList();
        	messageMap = new HashMap();
        	fileMap = new HashMap();
        }        
    }
    
    public GenerateExtract(String vendor)
    {
        int i;
        ping = new PingWorker(server, 60);
		ping.start();
        initDBSources(vendor);

        blades = new BladeRunner[threads];

        synchronized (blades)
        {
            if (blades[0] == null)
            {
                for (i = 0; i < threads; i++)
                {
                    blades[i] = new BladeRunner(i, vendor);
                }
            }
        }   
    }
    
    private void genNZDatabases()
    {
        try
		{
        	String[] schemas = t.getTargetSchemas();
        	for (int i = 0; i < schemas.length; ++i)
        	{
               db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "SYSTEM" + ":" + removeQuote(schemas[i]) + linesep);           	
			   db2tsbpWriter.write("CREATE DATABASE " + removeQuote(schemas[i]) + linesep  + ";" + linesep + linesep);
		       db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "SYSTEM" + ":" + removeQuote(schemas[i]) + linesep);           	
		       db2droptsbpWriter.write("DROP DATABASE " + removeQuote(schemas[i]) + linesep + ";" + linesep + linesep);
        	}
		} catch (IOException e)
		{
			e.printStackTrace();
		}    	
    }
    
    private void genDB2TSBP()
    {
    	String methodName = "genDB2TSBP";
        String free = "", used = "", allocated = "";
        long freeSpace = 0L, allocatedSpace = 0L, usedSpace = 0L;
        SQL s = new SQL(mainData.connection);
        
    	if (mainData.Oracle() && (REEL_NUM.equals("0") || REEL_NUM.equals("1")))
    	{
    		if (majorSourceDBVersion != -1 && majorSourceDBVersion > 8)
    		{
	            try
	    		{
	    			s.PrepareExecuteQuery(methodName, "SELECT SUM(BYTES) FROM DBA_FREE_SPACE");
	    			if (s.next())
	    			{
	    				freeSpace = s.rs.getLong(1);
	    			}
	    			s.close(methodName);
	    			s.PrepareExecuteQuery(methodName, "SELECT SUM(BYTES) FROM v$datafile");
	    			if (s.next())
	    			{
	    				allocatedSpace = s.rs.getLong(1);
	    			}
	    			usedSpace = allocatedSpace - freeSpace;
	    			allocated = IBMExtractUtilities.getSize((double) allocatedSpace, "");
	    			used = IBMExtractUtilities.getSize((double) usedSpace, "");
	    			free = IBMExtractUtilities.getSize((double) freeSpace, "");
	    			s.close(methodName);
	                    db2tsbpWriter.write("-- **** Oracle Database Size Information for Planning Purpose **** " + linesep);
	                    db2tsbpWriter.write("-- **** This information is extracted at the Oracle instance level **** " + linesep);
	                    db2tsbpWriter.write("-- Database Size Total = " + allocated + linesep);
	                    db2tsbpWriter.write("-- Database Size Used  = " + used + linesep);
	                    db2tsbpWriter.write("-- Database Size Free  = " + free + linesep + linesep);
	    		} catch (SQLException e)
	    		{
	    			e.printStackTrace();
	    		} catch (IOException e)
				{
					e.printStackTrace();
				}
	    		log("Oracle database Total Size = " + allocated + " Used Size = " + used + " Free Size = " +  free);
    		}
    	}
    	
    	try
    	{
            db2tsbpWriter.write("-- **** Best practice recommendations *** " + linesep+ linesep);    	     
            db2tsbpWriter.write("-- Create your DB2 database with a PAGESIZE of 32K and enable AUTOMATIC STORAGE by using storage paths." + linesep);
            db2tsbpWriter.write("-- (Windows) db2 create db testdb automatic storage yes on C:,D: DBPATH ON E: PAGESIZE 32 K" + linesep);
            db2tsbpWriter.write("-- (Unix)    db2 create db testdb automatic storage yes on /db2data1,/db2data2,/db2data3 DBPATH ON /db2system PAGESIZE 32 K" + linesep+ linesep); 
            
	        if (!useBestPracticeTSNames && mainData.zDB2())
            {
	            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + "BP4" + linesep);           	
		        db2tsbpWriter.write("create bufferpool bp4 size automatic pagesize 4K" + linesep + ";" + linesep);	        	
            }
            
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + "BP8" + linesep);           	
	        db2tsbpWriter.write("create bufferpool bp8 size automatic pagesize 8K" + linesep + ";" + linesep);

	        if (!useBestPracticeTSNames && mainData.zDB2())
            {
	            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + "BP16" + linesep);           	
		        db2tsbpWriter.write("create bufferpool bp16 size automatic pagesize 16K" + linesep + ";" + linesep);	        	
            }

	        db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + "BP32" + linesep);           	
	        db2tsbpWriter.write("create bufferpool bp32 size automatic pagesize 32K" + linesep + ";" + linesep);
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + "BPU8" + linesep);           	
	        db2tsbpWriter.write("create bufferpool bpu8 size automatic pagesize 8K" + linesep + ";" + linesep);
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + "BPU32" + linesep);           	
	        db2tsbpWriter.write("create bufferpool bpu32 size automatic pagesize 32K" + linesep + ";" + linesep);
	        
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + "TSUTMP8" + linesep);           	
	        db2tsbpWriter.write("create user temporary tablespace tsutmp8 pagesize 8K managed by automatic storage bufferpool bpu8" + linesep +   ";" + linesep + linesep);
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + "TSUTMP32" + linesep);           	
	        db2tsbpWriter.write("create user temporary tablespace tsutmp32 pagesize 32K managed by automatic storage bufferpool bpu32" + linesep +  ";" + linesep);
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + "TSSTMP8" + linesep);           	
	        db2tsbpWriter.write("create system temporary tablespace tsstmp8 pagesize 8K managed by automatic storage bufferpool bpu8" + linesep +   ";" + linesep + linesep);
            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + "TSSTMP32" + linesep);           	
	        db2tsbpWriter.write("create system temporary tablespace tsstmp32 pagesize 32K managed by automatic storage bufferpool bpu32" + linesep +   ";" + linesep + linesep);

	        
	        if (useBestPracticeTSNames)
            {
	            db2tsbpWriter.write("-- **** Best practice recommendations *** " + linesep+ linesep);    	
		        
	            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + "TS8" + linesep);           	
		        db2tsbpWriter.write("create large tablespace ts8 pagesize 8k bufferpool bp8" + linesep  + ";" + linesep + linesep);
		        db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + "TS32" + linesep);           	
		        db2tsbpWriter.write("create large tablespace ts32 pagesize 32k bufferpool bp32" + linesep  + ";" + linesep + linesep);	        
		        
		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + "TS32" + linesep);           	
		        db2droptsbpWriter.write("DROP TABLESPACE TS32;" + linesep);
		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + "TS8" + linesep);           	
		        db2droptsbpWriter.write("DROP TABLESPACE TS8;" + linesep);
		        
            } else
            {
            	if (custTSBP.size() > 0)
            	{
            		String key;
            		Iterator iter = custTSBP.keySet().iterator();
            		while (iter.hasNext())
            		{
            			key = (String) iter.next();
            			if (key.matches("(?i)^BUFFERPOOL.*"))
            			{
            				String bpName = key.split("\\.")[1];
                			String sizeBP = custTSBP.getProperty(key);            				
                	        db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "BUFFERPOOL" + ":" + bpName + linesep);           	
                	        db2tsbpWriter.write("create bufferpool "+bpName+" size automatic pagesize "+sizeBP+"K" + linesep + ";" + linesep);
	        		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "BUFFERPOOL" + ":" + bpName + linesep);           	
	        		        db2droptsbpWriter.write("DROP BUFFERPOOL " + bpName + linesep + ";" + linesep);
            			}
            		}
            		iter = custTSBP.keySet().iterator();
            		while (iter.hasNext())
            		{
            			key = (String) iter.next();
            			if (key.matches("(?i)^TABLESPACE.*"))
            			{
            				String tsName = key.split("\\.")[1];
                			String value = custTSBP.getProperty(key);
            	            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + tsName + linesep);           	
                			if (value.charAt(0) == '[')
                			{
                		        db2tsbpWriter.write(value.substring(1,value.length()-1) + linesep  + ";" + linesep + linesep);                				
                			} else
                			{
                				String pageSize = custTSBP.getProperty("BUFFERPOOL."+value);
                		        db2tsbpWriter.write("create large tablespace "+tsName+" pagesize "+pageSize+"k bufferpool "+value + linesep  + ";" + linesep + linesep);                				
                			}
            		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + tsName + linesep);           	
            		        db2droptsbpWriter.write("DROP TABLESPACE " + tsName + ";" + linesep);
            			}
            		}            		
            	} else
            	{
	                String tsName;	                
	                String sql = "";	                
	                if (mainData.Oracle())
	                   sql = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_TABLES WHERE TABLESPACE_NAME = 'SYSTEM' OR TABLESPACE_NAME NOT LIKE 'SYS%' " +
	                		"UNION SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_INDEXES WHERE TABLESPACE_NAME = 'SYSTEM' OR TABLESPACE_NAME NOT LIKE 'SYS%'";                      	        		
	                if (!sql.equalsIgnoreCase(""))
	                {	
		                try
		        		{
		        			s.PrepareExecuteQuery(methodName, sql);
		        			while (s.next())
		        			{
		        				tsName = s.rs.getString(1);	
		        	            db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + tsName + linesep);           	
		        		        db2tsbpWriter.write("create large tablespace "+tsName+" pagesize 32k bufferpool bp32" + linesep  + ";" + linesep + linesep);
		        		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + tsName + linesep);           	
		        		        db2droptsbpWriter.write("DROP TABLESPACE "+tsName+";" + linesep);
		        			}
		        			s.close(methodName);
		        		} catch (SQLException e)
		        		{
		        			e.printStackTrace();
		        		}
	                }
            	}
            }	        
	        
	        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + "TSSTMP32" + linesep);           	
	        db2droptsbpWriter.write("DROP TABLESPACE TSSTMP32;" + linesep);
	        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + "TSUTMP32" + linesep);           	
	        db2droptsbpWriter.write("DROP TABLESPACE TSUTMP32;" + linesep);
	        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + "TSU8" + linesep);           	
	        db2droptsbpWriter.write("DROP TABLESPACE TSU8;" + linesep);

	        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "BUFFERPOOL" + ":" + "BPU32" + linesep);           	
	        db2droptsbpWriter.write("DROP BUFFERPOOL BPU32;" + linesep);
	        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "BUFFERPOOL" + ":" + "BPU8" + linesep);           	
	        db2droptsbpWriter.write("DROP BUFFERPOOL BPU8;" + linesep);
	        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "BUFFERPOOL" + ":" + "BP32" + linesep);           	
	        db2droptsbpWriter.write("DROP BUFFERPOOL BP32;" + linesep);
	        if (!useBestPracticeTSNames && mainData.zDB2())
            {
		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "BUFFERPOOL" + ":" + "BP16" + linesep);           	
		        db2droptsbpWriter.write("DROP BUFFERPOOL BP16;" + linesep);
            }
	        db2droptsbpWriter.write("DROP BUFFERPOOL BP8;" + linesep);
	        if (!useBestPracticeTSNames && mainData.zDB2())
            {
		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "BUFFERPOOL" + ":" + "BP4" + linesep);           	
		        db2droptsbpWriter.write("DROP BUFFERPOOL BP4;" + linesep);
            }
	        
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    private String executeSQL(String sql, boolean terminator)
    {
    	return IBMExtractUtilities.executeSQL(mainData.connection, sql, sqlTerminator, terminator);
    }
    
    private String executeSQL(String sql)
    {
    	return IBMExtractUtilities.executeSQL(mainData.connection, sql, sqlTerminator, true);
    }

    private void genSybaseUsersGroups()
    {
    	String methodName = "genSybaseUsersGroups";
    	String name;
    	boolean once = true;
    	String sql = "select name FROM sysusers WHERE uid=gid AND (environ IS NOT NULL)";
    	SQL s = new SQL(mainData.connection);
        try
        {
			s.PrepareExecuteQuery(methodName, sql);				
			while (s.next())
			{
				name = s.rs.getString(1);
				if (name != null && name.length() > 0)
				{
					if (once)
					{
		                db2GroupsWriter.write("-- Following groups will be added through SKIN but DB2 does not" + linesep);
		                db2GroupsWriter.write("-- have concepts of creating user and group in the database." + linesep);
		                db2GroupsWriter.write("-- Grants, if any will be extracted in the db2objprivs.sql file for users and groups." + linesep);
		                db2GroupsWriter.write("-- Please make sure that you get these users and groups created in your OS." + linesep + linesep);
		                once = false;
					}
	                db2GroupsWriter.write("--#SET :" + "LOGIN" + ":" + "02-GROUP" + ":" + name + linesep);
	                db2GroupsWriter.write("EXEC sp_addgroup " + name + linesep + "go" + linesep);
	                db2GroupsWriter.write(linesep);                
				}				
			}
			s.close(methodName);
			
			once = true;
	    	sql = "select name from sysusers where uid < 16384 and name not in ('public','dbo') union " +
			"select suser_name(a.suid) from sysusers u,sysalternates a where a.altsuid=u.suid";

	    	s.PrepareExecuteQuery(methodName, sql);	
			while (s.next())
			{					
				name = s.rs.getString(1);
				if (name != null && name.length() > 0)
				{
					if (once)
					{
		                db2LoginWriter.write("-- Following users and aliases are extracted from Sybase." + linesep);
		                db2LoginWriter.write("-- When these are created through the DB2 SKIN, necessary schemas are created in DB2" + linesep);
		                db2LoginWriter.write("-- and entries are also created in the ACS repository." + linesep + linesep);
		                once = false;
					}
					db2LoginWriter.write("--#SET :" + "LOGIN" + ":" + "01-USERS" + ":" + name + linesep);
					db2LoginWriter.write("EXEC sp_addlogin " + name + linesep + "go" + linesep);
					db2LoginWriter.write(linesep);                
				}
			}
			s.close(methodName);
	    	
        } catch (Exception e)
        {
        	e.printStackTrace();
        }            	
    }
    
    private void genSybaseGrants(String schemaName)
    {
    	String methodName = "genSybaseGrants";
    	String privilege, user, table, to, grantable, grant, dstSchema, userType;
    	String sql = "select distinct v.name, user_name(o.uid) uname, o.name oname, u.name, " +
    			"(case when u.suid < 0 then 'Group' else 'User' end) utype, " +
    			"(case p.protecttype when 0 then 'YES' else 'NO' end) grantable " +
    			"from sysprotects p, master.dbo.spt_values v, sysusers u, sysobjects o " +
    			"where p.uid=u.uid " +
    			"and o.id = p.id " +
    			"and p.action=v.number " +
    			"and p.protecttype=1 " +
    			"and v.type = 'T' " +
    			"and v.name in ('Delete','Execute','Insert','References','Select','Update') " +
    			"and p.id > 100 " +
    			"and user_name(o.uid) = '"+schemaName+"'";
    	
    	SQL s = new SQL(mainData.connection);
        try
        {
			s.PrepareExecuteQuery(methodName, sql);				
			while (s.next())
			{
				privilege = s.rs.getString(1);
				user = s.rs.getString(2);				
				table = s.rs.getString(3);
				to = s.rs.getString(4);
				userType = s.rs.getString(5).toLowerCase();
				grantable = s.rs.getString(6);
				dstSchema = dbName + "_" + (user.equalsIgnoreCase("dbo") ? user.toUpperCase() : user);
				userType = "";
	        	db2objPrivsWriter.write("--#SET :GRANT:"+user+"_"+to+":"+privilege.substring(0,1)+"_" + table + linesep);   
	        	grant = "grant "+ privilege + " ON " + user + "." + table + " to " + userType + " " + to + " " + (grantable.equalsIgnoreCase("YES") ? " with grant option" : "");
	        	db2objPrivsWriter.write(grant + linesep + "go" + linesep);   
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	e.printStackTrace();
        }                		
    }
    
    private void genSybaseDefaultValues(String schemaName)
    {
    	Hashtable h = new Hashtable();
    	
    	String methodName = "genSybaseDefaultValues", defName, tableName, colName, defValue, dstSchema;
    	SQL s = new SQL(mainData.connection);
    	
        String sql = "select c.text from syscomments c, sysobjects o, sysusers u where o.type = 'D' " +
        		"and c.id = o.id and u.uid = o.uid and u.name = '"+schemaName+"'";
        
        dstSchema = sybSchema(schemaName); 
        try
        {
			s.PrepareExecuteQuery(methodName, sql);				
			while (s.next())
			{
				defName = s.rs.getString(1);
				if (defName != null && defName.length() > 0)
				{
					defName = defName.replaceAll("\\r|\\n|\\r\\n","");
					// create default zerodflt as 0
					if (defName.matches("(?i)create\\s+default.*"))
					{
						String[] toks = defName.split("\\s+");
						String key = toks[2], value;
						int pos = defName.toLowerCase().indexOf(" as ");
						if (pos != -1)
						{
							value = defName.substring(pos + 4);
							h.put(key, value);
						}
					}
				}
			}
			s.close(methodName);
			
			sql = "SELECT d.name, o.name, c.name FROM sysobjects o, syscolumns c, sysobjects d, sysusers u " +
					"WHERE o.type = 'U' and o.id = c.id " +
					"and d.type= 'D' and c.cdefault = d.id " +
					"and u.uid = o.uid " +
					"and u.name = '"+schemaName+"'";
			
			s.PrepareExecuteQuery(methodName, sql);	
			while (s.next())
			{					
				defName = s.rs.getString(1);
				if (defName != null && defName.length() > 0)
				{
					defName = s.rs.getString(1);
					tableName = s.rs.getString(2);
					colName = s.rs.getString(3);
					if (h.containsKey(defName))
					{
						defValue = (String) h.get(defName);
			            if (defValue != null && !defValue.equals(""))
			            {
			            	if (defValue.matches("(?i).*getdate\\(\\).*"))
			            	{
			                    defValue = zdb2 ? "" : " CURRENT_TIMESTAMP ";			            		
			            	}
			                db2DefaultWriter.write("--#SET :" + "DEFAULT" + ":" + dstSchema + ":" + tableName + "_" + colName + linesep);
			                db2DefaultWriter.write("ALTER TABLE "+putQuote(dstSchema)+sqlsep+putQuote(tableName)+ " ALTER COLUMN " + 
			                		putQuote(colName) + " SET WITH DEFAULT " + defValue + linesep + ";" + linesep);                
			                db2DefaultWriter.write(linesep);                
			            }
					}
				}
			}
			s.close(methodName);
			
        } catch (Exception e)
        {
        	e.printStackTrace();
        }            
    }
    
    private void BuildMainMemoryMap()
    {
    	if (ddlGen || extractObjects)
    	{
	    	log("Building main memory map. Please wait...");
	    	try
	    	{
		    	synchronized (empty)
				{
					if (synonymMap == null)
						BuildSynonymMap();
					if (columnsMap == null)
						BuildColumnMap();
					if (authColumnsMap == null)
						BuildAuthColumnList();
					if (routineOptsMap == null)
						BuildRoutineOptsMap();
					if (procColumnsMap == null)
						BuildProcColumnsMap();
	            	if (tableCommentsMap == null)
	            	   BuildCommentsMap();				
				}
		    	
	    	} catch (Exception e)
	    	{
	    		e.printStackTrace();
	    		System.exit(-1);
	    	}
    	}
    }

    private void BuildSynonymMap()
    {
    	String methodName = "BuildSynonymMap";
        synonymMap = new HashMap();
    	String key, value, schema, table, synSchema, synName, sql = "";
    	
    	
    	if (mainData.Oracle())
    	    sql = "SELECT S.TABLE_OWNER, S.TABLE_NAME, S.OWNER, S.SYNONYM_NAME  " +
					 "FROM DBA_SYNONYMS S " +
					 "WHERE S.TABLE_OWNER IN ("+schemaList+") " +
					 "AND S.OWNER = 'PUBLIC' " +
					 "AND S.DB_LINK IS NULL ";
    	
    	if (sql.length() == 0)
    		return;
    	
    	SQL s = new SQL(mainData.connection);
    	try
    	{
	        s.PrepareExecuteQuery(methodName, sql);
	        int i = 0;
	        while (s.next())
	        {
	        	schema = trim(s.rs.getString(1));
	        	table = trim(s.rs.getString(2));
	        	synSchema = trim(s.rs.getString(3));
	        	synName = trim(s.rs.getString(4));
				key = schema + "." + table;
				value = synSchema + ":" + synName;
			    synonymMap.put(key, value);
			    ++i;
	        }
	        log(i + " number of public schema cached in synonymMap");
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
        s.close(methodName); 
    }

    private void genSynonyms(String schema)
    {
    	String methodName = "genSynonyms";
    	String sql = "" , sql2 = "", queryOutput;
        String synSchema, dstSchema;
        String owner, tableOwner, tableName, synonymName, objectType, qualifier;
        SQL s = new SQL(mainData.connection);

        if (mainData.Oracle())
        {
        	sql = "SELECT O.OBJECT_NAME, O.OBJECT_TYPE " +
        			"FROM DBA_OBJECTS O " +
        			"WHERE O.OWNER = '"+schema+"' " +
        			"AND O.OBJECT_TYPE IN ('TABLE','VIEW','SEQUENCE','PACKAGE BODY') ";
        } else if (mainData.Mssql())
        {
        	if (majorSourceDBVersion > 8)
        	{
        	   sql = "select c.name, s.name, s.base_object_name " +
        			"from sys.synonyms s, sys.schemas c " +
        			"where s.schema_id = c.schema_id";
        	}
        }
        
        if (sql.equals("")) return;
        

        try
        {
	        s.PrepareExecuteQuery(methodName, sql);
	        int objCount = 0;
	        
	        if (mainData.Oracle())
	        {
		        while (s.next()) 
		        {
		        	tableName = s.rs.getString(1);
		        	objectType = s.rs.getString(2);
		        	if (objectType.equalsIgnoreCase("PACKAGE BODY"))
		        		qualifier = " MODULE ";
		        	else if (objectType.equalsIgnoreCase("SEQUENCE"))
		        		qualifier = " SEQUENCE ";
		        	else
		            	qualifier = "";
		        	
		        	// Get public synonyms
		        	if (synonymMap.containsKey(schema+"."+tableName))
		        	{
		        		String[] tmpStr = ((String) synonymMap.get(schema+"."+tableName)).split(":");
		        		owner = tmpStr[0];
		        		synonymName = tmpStr[1];
		        		tableOwner = schema;
		            	if (owner.equalsIgnoreCase("PUBLIC"))
		            		synSchema = "";
		            	else
		            		synSchema = getDstSchema(schema);
		            	dstSchema = getDstSchema(tableOwner);
		            	queryOutput = "CREATE " + (owner.equalsIgnoreCase("PUBLIC") ? "PUBLIC " : "") +
			              "SYNONYM " + putQuote(synSchema) + (owner.equalsIgnoreCase("PUBLIC") ? "" : ".") + putQuote(synonymName) + " FOR " + qualifier + putQuote(dstSchema) + sqlsep + putQuote(tableName) + linesep + ";" + linesep;
		                if (objCount > 0 && objCount % 20 == 0)
		                	log(objCount + " # public synonyms extracted for schema " + schema);
		                objCount++;
		            	db2SynonymWriter.write("--#SET :SYNONYM:"+dstSchema+":"+synonymName+ linesep);   
		            	db2SynonymWriter.write(queryOutput);   
				        db2DropSynWriter.write("--#SET :DROP SYNONYM:"+dstSchema+":"+synonymName+ linesep);	        	
		            	db2DropSynWriter.write("DROP "+(owner.equalsIgnoreCase("PUBLIC") ? "PUBLIC " : "")+" SYNONYM " + putQuote(synSchema) + "." + putQuote(synonymName) + linesep + ";" + linesep);
		        	}
		        }
		        if (objCount > 0)
					   log(objCount + " Total # public synonyms extracted for schema " + schema);
	        } else if (mainData.Mssql()) 
	        {
	        	String baseObjectName = "";
	        	String[] objs;
		        while (s.next()) 
		        {
		        	synSchema = s.rs.getString(1);
		        	synSchema = getCaseName(getDstSchema(synSchema));
		        	synonymName = getCaseName(s.rs.getString(2));
		        	baseObjectName = s.rs.getString(3).replaceAll("(\\[|\\])", "");
		        	objs = baseObjectName.split("\\.");
		        	dstSchema = getDstSchema(synSchema);
		        	if (objs.length == 1)
		        	{
		        		tableOwner = synSchema;
		        		tableName = getCaseName(objs[0]);
		        	} else if (objs.length == 2)
		        	{
		        		tableOwner = getCaseName(getDstSchema(objs[0]));
		        		tableName = getCaseName(getDstSchema(objs[1]));
		        	} else
		        	{
		        		tableOwner = getCaseName(getDstSchema(objs[1]));
		        		tableName = getCaseName(getDstSchema(objs[2]));
		        	}
	            	queryOutput = "CREATE " + "SYNONYM " + putQuote(dstSchema) + sqlsep +
	            	      putQuote(synonymName) + " FOR " + putQuote(tableOwner) + sqlsep + 
	            	         putQuote(tableName) +  linesep+ ";"+ linesep;
		            if (objCount > 0 && objCount % 20 == 0)
		              	log(objCount + " # synonyms extracted for schema " + schema);
		            objCount++;
	            	db2SynonymWriter.write("--#SET :SYNONYM:"+dstSchema+":"+synonymName+ linesep);   
			        db2SynonymWriter.write(queryOutput);   
			        db2DropSynWriter.write("--#SET :DROP SYNONYM:"+dstSchema+":"+synonymName+ linesep);	        	
			        db2DropSynWriter.write("DROP SYNONYM " + putQuote(synSchema) + "." + putQuote(synonymName) + linesep + ";" + linesep);	        	
		        }        	
		        if (objCount > 0)
				   log(objCount + " Total # synonyms extracted for schema " + schema);
	        }
	        s.close(methodName);
	        
        	// Get private synonyms
	        if (mainData.Oracle())
	        {
				objCount = 0;
	        	sql2 = "SELECT S.OWNER, S.SYNONYM_NAME, S.TABLE_OWNER, S.TABLE_NAME, O.OBJECT_TYPE " +
						"FROM DBA_SYNONYMS S, DBA_OBJECTS O " +
						"WHERE S.OWNER = '"+schema+"' " +
						"AND O.OWNER = S.TABLE_OWNER " +
						"AND O.OBJECT_NAME = S.TABLE_NAME " +
						"AND S.DB_LINK IS NULL " +
						"AND O.OBJECT_TYPE IN ('TABLE','VIEW','SEQUENCE','PACKAGE BODY')";
	        	SQL s2 = new SQL(mainData.connection);
	            s2.PrepareExecuteQuery(methodName, sql2);
	            while (s2.next())
	            {
	            	owner = s2.rs.getString(1);
	            	synonymName = s2.rs.getString(2);
	            	tableOwner = s2.rs.getString(3);
		        	tableName = s2.rs.getString(4);
		        	objectType = s2.rs.getString(5);
		        	if (objectType.equalsIgnoreCase("PACKAGE BODY"))
		        		qualifier = " MODULE ";
		        	else if (objectType.equalsIgnoreCase("SEQUENCE"))
		        		qualifier = " SEQUENCE ";
		        	else
		            	qualifier = "";
	            	synSchema = getDstSchema(owner);
	            	dstSchema = getDstSchema(tableOwner);
	            	queryOutput = "CREATE SYNONYM " + putQuote(synSchema) + "." + putQuote(synonymName) + " FOR " + qualifier + putQuote(dstSchema) + sqlsep + putQuote(tableName) + linesep + ";" + linesep;
	                if (objCount > 0 && objCount % 20 == 0)
	                	log(objCount + " # private synonyms extracted for schema " + schema);
	                objCount++;
	            	db2SynonymWriter.write("--#SET :SYNONYM:"+synSchema+":"+synonymName+ linesep);   
	            	db2SynonymWriter.write(queryOutput);   
			        db2DropSynWriter.write("--#SET :DROP SYNONYM:"+synSchema+":"+synonymName+ linesep);	        	
	            	db2DropSynWriter.write("DROP SYNONYM " + putQuote(synSchema) + "." + putQuote(synonymName) + linesep + ";" + linesep);
	            }
		        if (objCount > 0)
				   log(objCount + " Total # private synonyms extracted for schema " + schema);
	            s2.close(methodName); 
	        }
        } catch (Exception e)
        {
        	log("Exception thrown in method " + methodName + " Error Message " + e.getMessage());
        	e.printStackTrace();
        }
    }
    
    private void BuildCommentsMap()
    {
    	String methodName = "BuildCommentsMap";
    	String key, value, sql = "", sql2 = "";
		String prevSchema = "", prevTable = "", schema, table, columnName; 
		boolean added = false;
		HashMap columnMap = new HashMap();
    	tableCommentsMap = new HashMap();
    	columnCommentsMap = new HashMap();

    	if (mainData.Oracle())
    	{
    	    sql = "SELECT OWNER, TABLE_NAME, COMMENTS FROM DBA_TAB_COMMENTS WHERE OWNER IN (" + schemaList + ") " +
    	          " AND TABLE_TYPE IN ('TABLE','VIEW') AND COMMENTS IS NOT NULL";
        	sql2 = "SELECT OWNER, TABLE_NAME, COLUMN_NAME, COMMENTS FROM DBA_COL_COMMENTS WHERE OWNER IN (" + schemaList + ") " + 
                   " AND COMMENTS IS NOT NULL ORDER BY OWNER, TABLE_NAME";
    	}
    	else if (mainData.Teradata())
     	{
     	    sql = "SELECT DATABASENAME, TABLENAME, COMMENTSTRING FROM DBC.TABLES WHERE DATABASENAME IN (" + schemaList + ") " +
	                  " AND COMMENTSTRING IS NOT NULL";
    		sql2 = "select databasename, tablename, columnname, " +
				" case " +
				"   when columntitle is null then commentstring " +
				"   when commentstring is null then columntitle " +
				"   when columntitle is not null and commentstring is not null then columntitle || ' ' || commentstring " +
				" end comments " +
				"  from dbc.columns where databasename IN (" + schemaList + ") " +
				"  and (columntitle is not null or commentstring is not null) " +
				" order by databasename, tablename";
     	}
    	else if (mainData.zDB2())
     	{
     	    sql = "SELECT CREATOR, NAME, REMARKS FROM SYSIBM.SYSTABLES WHERE CREATOR IN (" + schemaList + ") " +
	                  " AND REMARKS IS NOT NULL";
		      	sql2 = "SELECT TBCREATOR, TBNAME, NAME, REMARKS FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR IN (" + schemaList + ") " +
		                 " AND REMARKS IS NOT NULL ORDER BY TBCREATOR, TBNAME";
     	}
    	else if (mainData.DB2())
     	{
     	    sql = "SELECT TABSCHEMA, TABNAME, REMARKS FROM SYSCAT.TABLES WHERE TABSCHEMA IN (" + schemaList + ") " +
	                  " AND REMARKS IS NOT NULL";
		      	sql2 = "SELECT TABSCHEMA, TABNAME, COLNAME, REMARKS FROM SYSCAT.COLUMNS WHERE TABSCHEMA IN (" + schemaList + ") " +
		                 " AND REMARKS IS NOT NULL ORDER BY TABSCHEMA, TABNAME";
     	}
    	else if (mainData.iDB2())
    	{
    	    sql = "SELECT TABLE_SCHEM, TABLE_NAME, TABLE_TEXT FROM SYSIBM.SQLTABLES WHERE TABLE_SCHEM IN (" + schemaList + ") " +
	                  " AND TABLE_TYPE IN ('TABLE','VIEW') AND TABLE_TEXT IS NOT NULL";
	      	sql2 = "SELECT TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_TEXT FROM SYSIBM.SQLCOLUMNS WHERE TABLE_SCHEM IN (" + schemaList + ") " +
	                 " AND COLUMN_TEXT IS NOT NULL ORDER BY TABLE_SCHEM, TABLE_NAME";
    	}
    	else if (mainData.Mysql())
    	{
    	    sql = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN (" + schemaList + ") " +
	                  " AND TABLE_TYPE = 'BASE TABLE' AND TABLE_COMMENT IS NOT NULL";
	      	sql2 = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN (" + schemaList + ") " +
	                 " AND COLUMN_COMMENT IS NOT NULL ORDER BY TABLE_SCHEMA, TABLE_NAME";
    	}

    	if (sql.equals(""))
    		return;
    	
    	try
    	{
        	SQL s = new SQL(mainData.connection);
			s.PrepareExecuteQuery(methodName, sql);     

			int i = 0;
        	while (s.next())
        	{
        		key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2));
        		value = trim(s.rs.getString(3));
        		if (value != null && value.length() > 0)
        		{
        		   if (value.length() > 255)
        			   value = value.substring(1,255);
        		   value = value.replace("'", "''");
        		   tableCommentsMap.put(key, value);
        		   ++i;
        		}
        	}
        	s.close(methodName);
        	if (debug) log (i + " values cached in tableCommentsMap");
        	key = "";
        	i = 0;
			s.PrepareExecuteQuery(methodName, sql2); 
        	while (s.next())
        	{
        		schema = trim(s.rs.getString(1));
        		table = trim(s.rs.getString(2));
        		if (i == 0)
        		{
        			prevSchema = schema;
        			prevTable = table;
        		}
        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
        		{
        			columnCommentsMap.put(key, columnMap);
        			columnMap = new HashMap();
        			added = true;
        		}
        		columnName = trim(s.rs.getString(3));
        		columnName = getCaseName(getCustomColumnName(schema, table, columnName));
        		key = schema + "." + table;
        		value = trim(s.rs.getString(4));
        		if (value != null && value.length() > 0)
        		{
        			value = value.replace("'", "''");
        		    if (value.length() > 255)
        			   value = value.substring(1,255);
        		    value = value.replace("'", "''");
        		    columnMap.put(columnName, value);
        		    added = false;
	        		++i;
        		}	        			
    			prevSchema = schema;
    			prevTable = table;
        	}
        	if (!added && key.length() > 0)
        	{
    			columnCommentsMap.put(key, columnMap);	        		
        	}
        	s.close(methodName);        		
        	if (debug) log (i + " values cached in columnCommentsMap");
    	} catch (SQLException e)
    	{
    		e.printStackTrace();
    	}        	
    }
    
    private String getComments(String schema, String table, String srcSchema, String srcTable)
    {
    	String key = removeQuote(srcSchema) + "." + removeQuote(srcTable), columnName, comment = "";
    	StringBuffer buffer = new StringBuffer();
    	
    	if (tableCommentsMap.containsKey(key))
    	{
    		comment = (String) tableCommentsMap.get(key);        		
 		   buffer.append("--#SET: COMMENT:"+schema + ":" + table + linesep);
		   buffer.append("COMMENT ON TABLE " + schema + "." + table + " IS '"+ comment + "'" + linesep + ";" + linesep);
    	}
    	
    	if (columnCommentsMap.containsKey(key))
    	{
    		HashMap columnsMap = (HashMap) columnCommentsMap.get(key);
    		Iterator it = columnsMap.entrySet().iterator();
    		while (it.hasNext()) 
    		{
    	        Map.Entry pairs = (Map.Entry) it.next();
    	        columnName = (String) pairs.getKey();
    	        comment = (String) pairs.getValue();
        		if (comment != null && comment.length() > 0 && !comment.equalsIgnoreCase("null"))
        		{
        		   buffer.append("--#SET: COMMENT:"+schema + ":" + table + "." + columnName + linesep);
        		   buffer.append("COMMENT ON COLUMN " + schema + sqlsep + table + "." + putQuote(columnName) + " IS '"+ comment + "'" + linesep + ";" + linesep);
        		}
    	    }
    	}
    	return buffer.toString();        	
    }
    
    private void genPrivs(String schema, String who)
    {
    	String methodName = "genPrivs";
    	String sql = "" ,queryOutput;
        String dstSchema = getDstSchema(schema);
        String privilege, onObject, owner, table_name, column_name, grantee, grantable;
        int objCount = 0;

        SQL s = new SQL(mainData.connection);

        if (mainData.Oracle())
        {
        	sql = "SELECT P.PRIVILEGE, DECODE(O.OBJECT_TYPE,'INDEX',' ON INDEX ','DIRECTORY',' ON DIRECTORY ','FUNCTION', " +
        			"' ON FUNCTION ','PROCEDURE',' ON PROCEDURE ','PACKAGE',  ' ON MODULE ','SEQUENCE', ' ON SEQUENCE ',' ON '), " +
        			"P.OWNER, P.TABLE_NAME, P.GRANTEE, P.GRANTABLE " +
        			"FROM   	DBA_TAB_PRIVS P, DBA_OBJECTS O " +
        			"WHERE 	P."+who+" = '&schemaname&' " +
        			"   AND 	O.OBJECT_NAME = P.TABLE_NAME " +
        			"   AND 	O.OWNER = P.OWNER " +
        			"   AND   O.OBJECT_TYPE IN ('DATABASE LINK','FUNCTION','INDEX', " +
        			"			'INDEX PARTITION','MATERIALIZED VIEW', " +
        			"			'PACKAGE','PROCEDURE', " + // Work Item # 5728 - removed 'PACKAGE BODY'
        			"			'SEQUENCE','SYNONYM','TABLE','TABLE PARTITION', " +
        			"			'TRIGGER','TYPE','DIRECTORY') " + // Work Item # 5728 - removed 'TYPE BODY'
        			"   AND  P.PRIVILEGE NOT LIKE ('%COMMIT REFRESH%') " +
        			"   AND  P.PRIVILEGE NOT LIKE ('%DEBUG%') " +
        			"   AND  P.PRIVILEGE NOT LIKE ('%QUERY REWRITE%') " +
        			"   AND  P.PRIVILEGE NOT LIKE ('%FLASHBACK%') " +
        			"   AND  P.PRIVILEGE NOT LIKE ('%MERGE VIEW%') " +
        			"ORDER BY P.GRANTEE, P.TABLE_NAME";
        }
        
        
        if (sql.equals("")) return;
        

        sql = sql.replace("&schemaname&", schema);
        try
        {
	        s.PrepareExecuteQuery(methodName, sql);
	        while (s.rs.next()) 
	        {
	        	privilege = s.rs.getString(1);
	        	onObject = s.rs.getString(2);
	        	owner = s.rs.getString(3);
	        	table_name = s.rs.getString(4);
	        	grantee = s.rs.getString(5);
	        	grantable = s.rs.getString(6);
	        	queryOutput = "GRANT "+privilege+" "+onObject+" "+putQuote(dstSchema)+sqlsep+putQuote(table_name)+" TO "+putQuote(grantee) + (grantable.equalsIgnoreCase("YES") ? " WITH GRANT OPTION" : "") + ";";
	        	if (objCount == 0)
	        	   db2objPrivsWriter.write("-- Grants listed below are for "+who+" " + schema + linesep);   
	        	db2objPrivsWriter.write(queryOutput + linesep);  
	        	objCount++;
	        }
	        s.close(methodName);
        } catch (Exception e)
        {
        	log("Exception 1 thrown in method " + methodName + " Error Message " + e.getMessage());
        	e.printStackTrace();
        }

    	sql = "SELECT P.PRIVILEGE, DECODE(O.OBJECT_TYPE,'INDEX',' ON INDEX ','DIRECTORY',' ON DIRECTORY ','FUNCTION', " +
			"' ON FUNCTION ','PROCEDURE',' ON PROCEDURE ','PACKAGE',  ' ON MODULE ','SEQUENCE', ' ON SEQUENCE ',' ON '), " +
			"P.OWNER, P.TABLE_NAME, P.COLUMN_NAME, P.GRANTEE, P.GRANTABLE " +
			"FROM   	DBA_COL_PRIVS P, DBA_OBJECTS O " +
			"WHERE 	P."+who+" = '&schemaname&' " +
			"   AND 	O.OBJECT_NAME = P.TABLE_NAME " +
			"   AND 	O.OWNER = P.OWNER " +
			"   AND   O.OBJECT_TYPE IN ('DATABASE LINK','FUNCTION','INDEX', " +
			"			'INDEX PARTITION','MATERIALIZED VIEW', " +
			"			'PACKAGE','PROCEDURE', " + // Work Item # 5728 - removed 'PACKAGE BODY'
			"			'SEQUENCE','SYNONYM','TABLE','TABLE PARTITION', " +
			"			'TRIGGER','TYPE','DIRECTORY') " +  // Work Item # 5728 - removed 'TYPE BODY'
			"   AND  P.PRIVILEGE NOT LIKE ('%COMMIT REFRESH%') " +
			"   AND  P.PRIVILEGE NOT LIKE ('%DEBUG%') " +
			"   AND  P.PRIVILEGE NOT LIKE ('%QUERY REWRITE%') " +
			"   AND  P.PRIVILEGE NOT LIKE ('%FLASHBACK%') " +
			"   AND  P.PRIVILEGE NOT LIKE ('%MERGE VIEW%') " +
			"ORDER BY P.GRANTEE, P.TABLE_NAME";


    	sql = sql.replace("&schemaname&", schema);
      	objCount = 0;
    	
    	try
    	{
        	db2objPrivsWriter.write(linesep);   
	        s.PrepareExecuteQuery(methodName, sql);
	        while (s.next()) 
	        {
	        	privilege = s.rs.getString(1);
	        	onObject = s.rs.getString(2);
	        	owner = s.rs.getString(3);
	        	table_name = s.rs.getString(4);
	        	column_name = s.rs.getString(5);        	
	        	grantee = s.rs.getString(6);
	        	grantable = s.rs.getString(7);
	        	
	        	column_name = getCaseName(getCustomColumnName(schema, table_name, column_name));
	        	
	        	if (objCount == 0)
	         	   db2objPrivsWriter.write("-- Column Level Grants listed below are for "+who+" " + schema + linesep);   
	        	queryOutput = "GRANT "+privilege+"("+putQuote(column_name)+") "+onObject+" "+putQuote(dstSchema)+sqlsep+putQuote(table_name)+" TO "+putQuote(grantee) + (grantable.equals("YES") ? " WITH GRANT OPTION" : "") + ";";
	        	db2objPrivsWriter.write(queryOutput + linesep);   
	            objCount++;
	        }
	        if (objCount > 0)
	    	   log(objCount + " # privileges extracted for schema " + schema);
	        s.close(methodName);
    	} catch (Exception e)
    	{
        	log("Exception 2 thrown in method " + methodName + " Error Message " + e.getMessage());
        	e.printStackTrace();    		
    	}
        
    }

    private String getTriggerUpdateColumnsList(String schemaTrigger, String triggerName)
    {
    	String methodName = "getTriggerUpdateColumnsList";
    	String sql = "";
    	String column, columnList = "";
    	
    	SQL s = new SQL(mainData.connection);
        ResultSet Reader = null;

        if (mainData.iDB2())
        	sql = "SELECT COLUMN_NAME " +
        		//"FROM "+putQuote(schemaTrigger)+".SYSTRIGCOL " +
        		"FROM QSYS2.SYSTRIGCOL " +
        		"WHERE TRIGGER_SCHEMA = '"+schemaTrigger+"' " +
        		"AND TRIGGER_NAME = '"+triggerName+"'";
           
        if (sql.equals("")) return "";
        
        try
        {
          s.PrepareExecuteQuery(methodName,  sql);

          int objCount = 0;
          while (s.next()) 
          {
        	  column = s.rs.getString(1);
        	  // Needs to be fixed for custom column name (find table name)
        	  //column = getCaseName(getCustomColumnName(schemaTrigger, table, column));
        	  if (objCount == 0)
        	  {
        		  columnList = putQuote(column) + " ";
         	  } else
        	  {
         		  columnList = columnList + "," + putQuote(column) + " ";
        	  }
        	  objCount++;
          }
          s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting trigger columns for " + triggerName);
        	e.printStackTrace();
        }    
        return " OF " + columnList;
    }
    
    private String getTriggerSource(String schemaTrigger, String triggerName)
    {
    	String methodName = "getTriggerSource";
    	String sql = "";
    	String dstSchema = getCaseName(getDstSchema(schemaTrigger));
    	
    	SQL s = new SQL(mainData.connection);
        ResultSet Reader = null;
        StringBuffer buffer = new StringBuffer();

        if (mainData.iDB2())
	        sql = "select action_timing, trigger_mode, action_orientation, " +
		    		"action_reference_old_row, action_reference_new_row, " +
		    		"action_reference_old_table, action_reference_new_table, " +
		    		"action_condition, " +
		    		"event_object_schema, event_object_table, " +
		    		"event_manipulation, action_statement " +
		    		//"from "+putQuote(schemaTrigger)+".systriggers " +
		    		"from qsys2.systriggers " +
		    		"where trigger_schema = '"+schemaTrigger+"' " +
		    		"and trigger_name = '"+triggerName+"'";
        else if (mainData.Mysql())
        {
	        sql = "select action_timing, sql_mode, action_orientation, " +
    		      "action_reference_old_row, action_reference_new_row, " +
    		      "action_reference_old_table, action_reference_new_table, " +
    		      "action_condition, " +
    		      "event_object_schema, event_object_table, " +
    		      "event_manipulation, action_statement " +
    		      "from INFORMATION_SCHEMA.TRIGGERS " +
    		      "where upper(trigger_schema) = '"+schemaTrigger.toUpperCase()+"' " +
    		      "and trigger_name = '"+triggerName+"'";
        }
        else if (mainData.Oracle())
        	sql = "select description, trigger_body " +
        			"from dba_triggers where owner = '"+schemaTrigger+"' " +
        			"and trigger_name = '"+triggerName+"'";
           
        if (sql.equals("")) return "";
        
        try
        {
          s.PrepareExecuteQuery(methodName, sql);	

          while (s.next()) 
          {
	          buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep + sqlTerminator + linesep);    
		      buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep + sqlTerminator + linesep);
    	      if (mainData.Mysql())
    	      {
    	    	  buffer.append("-- **Warning** The trigger body is extracted as it is from MySQL database. Please check the syntax." + linesep);
    	    	  buffer.append("-- If you figure out a rule that can be automated, please send an email to vikram.khatri@us.ibm.com" + linesep + "--" + linesep);
    	      }
	          buffer.append("--#SET :TRIGGER:" + dstSchema + ":" + getCaseName(triggerName) + linesep);
        	  if (mainData.iDB2() || mainData.Mysql())
        	  {
        		  String actionTiming = "", triggerMode = "";
        	      String actionOrientation = "", actionReferenceOldRow = "", actionReferenceNewRow = "", actionReferenceOldTable = "";
        	      String actionReferenceNewTable = "", actionCondition = "", eventObjectSchema = "", eventObjectTable = "";
        	      String eventManipulation, actionStatement = "";
        	      boolean ref = false;

        	      buffer.append("CREATE TRIGGER " + putQuote(dstSchema) + "." + putQuote(getCaseName(triggerName)) + linesep);        	  
	        	  actionTiming = s.rs.getString(1);
	        	  triggerMode = s.rs.getString(2);
	        	  actionOrientation = s.rs.getString(3);
	        	  actionReferenceOldRow = s.rs.getString(4);
	        	  actionReferenceNewRow = s.rs.getString(5);
	        	  actionReferenceOldTable = s.rs.getString(6);
	        	  actionReferenceNewTable = s.rs.getString(7);
	        	  actionCondition = s.rs.getString(8);
	        	  eventObjectSchema = s.rs.getString(9);
	        	  eventObjectTable = s.rs.getString(10);
	        	  eventManipulation = s.rs.getString(11);
	        	  actionStatement = s.rs.getString(12);        	  
	        	  buffer.append(actionTiming + " ");
	        	  buffer.append(eventManipulation + " ");
	        	  if (eventManipulation.equalsIgnoreCase("UPDATE"))
	        	  {
	        		  buffer.append(getTriggerUpdateColumnsList(schemaTrigger, triggerName) + linesep);
	        	  } else
	        		  buffer.append(linesep);
	        	  buffer.append("ON " + putQuote(dstSchema) + "." + putQuote(getCaseName(eventObjectTable)) + linesep);
	        	  if ((actionReferenceOldRow != null && actionReferenceOldRow.length() > 0) ||
	        		  (actionReferenceNewRow != null && actionReferenceNewRow.length() > 0) ||
	        		  (actionReferenceOldTable != null && actionReferenceOldTable.length() > 0) ||
	        		  (actionReferenceNewTable != null && actionReferenceNewTable.length() > 0))        		  
	        		  ref = true;
	        	  if (ref)
	        		  buffer.append("REFERENCING ");
	        	  if (actionReferenceOldRow != null && actionReferenceOldRow.length() > 0)
	        		  buffer.append("OLD ROW AS " + actionReferenceOldRow + linesep);
	        	  if (actionReferenceNewRow != null && actionReferenceNewRow.length() > 0)
	        		  buffer.append("NEW ROW AS " + actionReferenceNewRow + linesep);
	        	  if (actionReferenceOldTable != null && actionReferenceOldTable.length() > 0)
	        		  buffer.append("OLD TABLE AS " + actionReferenceNewRow + linesep);
	        	  if (actionReferenceNewTable != null && actionReferenceNewTable.length() > 0)
	        		  buffer.append("NEW TABLE AS " + actionReferenceNewTable + linesep);
	        	  if (actionOrientation != null && actionOrientation.length() > 0)
	        	  {
	        		  buffer.append("FOR EACH " + actionOrientation + linesep);
	        	  }
	        	  if (triggerMode != null && triggerMode.length() > 0)
	        	  {
	        		  buffer.append("MODE " + triggerMode + linesep);
	        	  }
	        	  if (actionCondition != null && actionCondition.length() > 0)
	        	  {
	        		  buffer.append("WHEN " + actionCondition + linesep);
	        	  }
	        	  buffer.append(actionStatement + linesep + sqlTerminator + linesep + linesep);
        	  }
        	  else
        	  {
        		  String description;
        		  description = s.rs.getString(1);
        		  StringBuffer chunks = new StringBuffer();
        		  IBMExtractUtilities.getStringChunks(s.rs, 2, chunks);
            	  buffer.append("CREATE TRIGGER " + description + linesep);
            	  buffer.append(chunks + linesep + sqlTerminator + linesep + linesep);
        	  }
          }
          s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting Trigger for " + triggerName);
        	e.printStackTrace();
        }    
        return buffer.toString();
    }

    private void genTriggers(String schema)
    {
    	String methodName = "genTriggers";
    	String sql = "", triggerSchema, triggerName;
        SQL s = new SQL(mainData.connection);
        ResultSet Reader = null;
        StringBuffer buffer = new StringBuffer();
        StringBuffer chunks = new StringBuffer();
        String dstSchema = getDstSchema(schema);

        if (mainData.iDB2())
	        sql = "select trigger_schema, trigger_name " +
	        		//"from "+putQuote(schema)+".systriggers " +
	        		"from qsys2.systriggers " +
	        		"where  " +
	        		" event_object_schema = '"+schema+"'";
        else if (mainData.DB2())
        	sql = "SELECT trigschema, trigname, text, func_path FROM SYSCAT.TRIGGERS " +
        			"WHERE trigschema = '"+schema+"' " +
        			"ORDER BY CREATE_TIME";
        else if (mainData.zDB2())
        	sql = "SELECT schema, name, text, '' as func_path FROM SYSIBM.SYSTRIGGERS " +
        			"WHERE tbowner = '"+schema+"' " +
        			"ORDER BY CREATEDTS";
        else if (mainData.Oracle())
        	sql = "SELECT owner, TRIGGER_NAME, TRIGGER_BODY, '' AS FUNC_PATH FROM DBA_TRIGGERS " +
			      "WHERE owner = '"+schema+"' ";
        else if (mainData.Mysql())
        	sql = "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, '' AS TRIGGER_BODY, '' AS FUNC_PATH FROM INFORMATION_SCHEMA.TRIGGERS " +
			      "WHERE upper(TRIGGER_SCHEMA) = '"+schema.toUpperCase()+"' ";
        
        if (sql.equals("")) return;
        
        try
        {
            s.PrepareExecuteQuery(methodName, sql);	

	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            buffer.setLength(0);
		            chunks.setLength(0);
		            triggerSchema = trim(s.rs.getString(1));
		            triggerName = trim(s.rs.getString(2));
		            db2DropObjectsWriter.write("--#SET :DROP:TRIGGER:" + triggerName + linesep);
		            db2DropObjectsWriter.write("DROP TRIGGER " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(triggerName)) + ";" + linesep);
		            if (mainData.DB2() || mainData.zDB2())
		            {
		            	IBMExtractUtilities.getStringChunks(s.rs, 3, chunks);
			  	        buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep + sqlTerminator + linesep);            	
				        buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep + sqlTerminator + linesep);            	
				        buffer.append("--#SET :TRIGGER:" + dstSchema + ":" + triggerName + linesep);
				        buffer.append(chunks);
			            buffer.append(linesep + sqlTerminator + linesep);
		            	db2ObjectsWriter[(Integer) plsqlHashTable.get("TRIGGER".toLowerCase())].write(buffer.toString());
		            } else if (mainData.iDB2() || mainData.Mysql())
		            {
		               db2ObjectsWriter[(Integer) plsqlHashTable.get("TRIGGER".toLowerCase())].write(getTriggerSource(triggerSchema, triggerName));
		            }
		            else
		            {
		               db2ObjectsWriter[(Integer) plsqlHashTable.get("TRIGGER".toLowerCase())].write(getTriggerSource(triggerSchema, triggerName));		            	
		            }
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " # Triggers extracted for schema " + schema);
		            objCount++;
	        	} catch (IOException e)
	        	{
	        		log("Error writing Triggers in file " + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting Triggers " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " Total # Triggers extracted for schema " + schema);
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }
    }
    
    private String replaceSybUDTWithBaseType(String buffer)
    {    
    	if (sybaseUDTConversionToBaseType.equalsIgnoreCase("false") || udtProp.size() == 0)
    		return buffer;
        if (buffer != null && buffer.length() > 0)
        {
            Enumeration e = udtProp.propertyNames();
            String key, value;            
	        while (e.hasMoreElements()) 
	        {
	           key = (String) e.nextElement();
	           value = (String) udtProp.getProperty(key);
	           buffer = buffer.replaceAll("(?ism)"+key, value);
	        }
        }
        return buffer;
    }
    
    private void genSybaseObjects(String schema, String objTypeCode, String objType)
    {
    	String methodName = "genSybaseObjects";
    	SQL s = new SQL(mainData.connection);
    	String sql = "", sql2 = "", id, objectName, schemaName, line;

        StringBuffer buffer = new StringBuffer();
        String dstSchema = getCaseName(getDstSchema(schema));
        String sourceCode = "";

        if (mainData.Sybase())
        {
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
	            " AND o1.name not like 'sys%' " + 
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
        }
        
        if (sql.equals("")) return;
        
        try
        {
            s.PrepareExecuteQuery(methodName, sql);	
	        int objCount = 0, fileID = 0;
	        if (plsqlHashTable.containsKey(objType.toLowerCase()))
	        {
	        	fileID = (Integer) plsqlHashTable.get(objType.toLowerCase());
	        }
	        while (s.next()) 
	        {
	        	try
	        	{
		            buffer.setLength(0);
		            objectName = getCaseName(trim(s.rs.getString(1)));
		            schemaName = getCaseName(trim(s.rs.getString(2)));
		            id = s.rs.getString(3);		            
		            if (db2Skin())
		            {
			            db2DropObjectsWriter.write("--#SET :DROP:"+objType+":" + objectName + linesep);
			            db2DropObjectsWriter.write("DROP "+objType+" " + getCaseName(objectName) + linesep + "go" + linesep);
		            } else
		            {
			            db2DropObjectsWriter.write("--#SET :DROP:"+objType+":" + objectName + linesep);
			            db2DropObjectsWriter.write("DROP "+objType+" " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(objectName)) + ";" + linesep);
			  	        buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep + sqlTerminator + linesep);            	
				        buffer.append("SET PATH = SYSTEM PATH, " + putQuote(getCaseName(dstSchema)) + linesep + sqlTerminator + linesep);		            	
		            }
			        buffer.append("--#SET :"+objType.toUpperCase()+":" + dstSchema + ":" + objectName + linesep);
			        try
		            {
		                String tmpsql = sql2.replace("&ID&", id);
		            	SQL s2 = new SQL(mainData.connection);
		                s2.PrepareExecuteQuery(methodName, tmpsql);	
		    	        while (s2.next()) 
		    	        {
		    	        	line = s2.rs.getString(1);
					        buffer.append(line);
		    	        }
		    	        s2.close(methodName);
		    	        sourceCode = replaceSybUDTWithBaseType(buffer.toString());
			            if (db2Skin())
			            {
			            	sourceCode = sourceCode + linesep + "go" + linesep;
			            } else
			            {
			            	sourceCode = sourceCode + linesep + sqlTerminator + linesep;		            	
			            }
		            } catch (Exception es)
		            {
		            	es.printStackTrace();		            	
		            }
		            if (objTypeCode.equalsIgnoreCase("V"))
		            {
		            	db2ViewsWriter.write(sourceCode);
		            } else
		            {
		            	db2ObjectsWriter[fileID].write(sourceCode);		            	
		            }
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " # "+objType+" extracted for schema " + schema);
		            objCount++;
	        	} catch (IOException e)
	        	{
	        		log("Error writing "+objType+" in file " + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting "+objType+" " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " # of "+objType+" extracted for schema " + schema);
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }
    }
    
    private String getRoutineBody(String srcType, String schema, String specificName)
    {
    	String methodName = "getRoutineBody";
    	String sql = "";
    	String routineSource = "";
    	
    	SQL s = new SQL(mainData.connection);
        
        if (mainData.iDB2())
        {
        	if (srcType.equals("1"))
	        	sql = "SELECT ROUTINE_DEFINITION " +
	        			"FROM SYSIBM.ROUTINES " +
	        			"WHERE SPECIFIC_SCHEMA = '"+schema+"' " +
	        			"AND   SPECIFIC_NAME = '"+specificName+"'";
        	else
        		sql = "SELECT ROUTINE_DEFINITION " +
        				"FROM QSYS2.SYSFUNCS " +
        				"WHERE SPECIFIC_SCHEMA = '"+schema+"' " +
        				"AND   SPECIFIC_NAME = '"+specificName+"'";
        }
        else if (mainData.Mysql())
        {
    		sql = "SELECT ROUTINE_DEFINITION " +
			"FROM QSYS2.SYSFUNCS " +
			"WHERE SPECIFIC_SCHEMA = '"+schema+"' " +
			"AND   SPECIFIC_NAME = '"+specificName+"'";        	
        }

        	
           
        if (sql.equals("")) return "";
        
        try
        {
          s.PrepareExecuteQuery(methodName, sql);
          while (s.next()) 
          {
        	  routineSource = s.rs.getString(1);
        	  routineSource = OraToDb2Converter.fixiDB2CursorForReturn(routineSource);
          }
          s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting procedure body for " + specificName);
        	e.printStackTrace();
        }    
        return routineSource;
    }

    private String getSourceColumnList(String schema, String sourcespecific, String sourceschema)
    {
    	String methodName = "getSourceColumnList";
    	String sql = "", typeSchema, typeName, columnList = "";
    	
    	if (mainData.DB2())
    	{
    		sql = "SELECT typeschema, typename " +
    				"FROM SYSCAT.ROUTINEPARMS " +
    				"WHERE specificname = '"+sourcespecific+"' " +
    				"AND routineschema = '"+sourceschema+"' " +
    				"AND rowtype IN ('B', 'O', 'P') " +
    				"ORDER BY ordinal";
    	}


        if (sql.equals("")) return "";
        
    	SQL s = new SQL(mainData.connection);

        try
        {
          s.PrepareExecuteQuery(methodName, sql);		 	

          int objCount = 0;
          while (s.next()) 
          {
        	  typeSchema = s.rs.getString(1);
        	  typeName = s.rs.getString(2);
        	  String tmp = composeType(typeSchema, typeName, -1, 0, -1, schema);
        	  if (objCount == 0)
        	  {
        		 columnList = tmp;
         	  } else
        	  {
        		 columnList = columnList + "," + tmp;
        	  }
        	  objCount++;
          }
          s.close(methodName); 
         } catch (Exception e)
         {
         	log("Error in getting procedure columns for " + sourcespecific);
         	e.printStackTrace();
         }   
         return columnList;
    }
    
    private void BuildProcColumnsMap()
    {
    	boolean added = false;
    	ArrayList al = new ArrayList();
    	String methodName = "BuildProcColumnsMap";
    	procColumnsMap = new HashMap();
    	procColumnsMap2 = new HashMap();
    	String key = "", columnType, schema, specificname, sql = "", sql2 = "";
    	String prevSchema = "", prevSpecificName = "";
    	
    	
        if (mainData.iDB2())
        {
	        	sql = "SELECT PROCEDURE_SCHEM, SPECIFIC_NAME, CASE " +
	        			"WHEN COLUMN_TYPE = 1 THEN 'IN' " +
	        			"WHEN COLUMN_TYPE  = 2 THEN 'INOUT' " +
	        			"WHEN COLUMN_TYPE = 4 THEN 'OUT' " +
	        			"ELSE 'UNKNOWN' " +
		        		"END, " +
		        		"COLUMN_NAME, " +
		        		"TYPE_NAME, " +
		        		"COLUMN_SIZE, " +
		        		"DECIMAL_DIGITS " +
		        		"FROM SYSIBM.SQLPROCEDURECOLS " +
		        		"WHERE PROCEDURE_SCHEM IN ("+schemaList+") " +
		        		"ORDER BY PROCEDURE_SCHEM, SPECIFIC_NAME";
	        	sql2 = "SELECT PROCEDURE_SCHEM, SPECIFIC_NAME, ' ', " +
			    		"COLUMN_NAME, " +
			    		"TYPE_NAME, " +
			    		"COLUMN_SIZE, " +
			    		"DECIMAL_DIGITS " +
			    		"FROM SYSIBM.SQLPROCEDURECOLS " +
			    		"WHERE PROCEDURE_SCHEM IN ("+schemaList+") " +
			    		"ORDER BY PROCEDURE_SCHEM, SPECIFIC_NAME";        
        				// columnType = 1 Function, columnType = 4 Returns Table Type
        } 
        else if (mainData.Mysql())
        {
        	sql = "SELECT db, name, param_list FROM mysql.proc WHERE upper(db) IN  ("+schemaList.toUpperCase()+")";        	
        }
        else if (mainData.DB2())
        {
        	sql = "SELECT routineschema, specificname, CASE WHEN rowtype = 'P' THEN 'IN' " +
        			"WHEN ROWTYPE = 'O' THEN 'OUT' " +
        			"ELSE 'INOUT'" +
        			"END, " +
        			"parmname, typename, length, scale, codepage, typeschema, " +
        			"CASE WHEN locator = 'Y' THEN 'AS LOCATOR' ELSE ' ' END " +
        			"FROM SYSCAT.ROUTINEPARMS " +
        			"WHERE routineschema IN ("+schemaList+")  " +
        			"AND rowtype IN ('B', 'O', 'P') " +
        			"ORDER BY routineschema, specificname, ordinal";
        }
           
    	if (sql.length() == 0)
    		return;
    	
    	try
    	{
        	SQL s = new SQL(mainData.connection);
	        s.PrepareExecuteQuery(methodName, sql);
	        int i = 0;
	        while (s.next())
	        {
	        	schema = trim(s.rs.getString(1));
	        	specificname = trim(s.rs.getString(2));
        		if (i == 0)
        		{
        			prevSchema = schema;
        			prevSpecificName = specificname;
        		}
        		if (!(schema.equals(prevSchema) && specificname.equals(prevSpecificName)))
        		{
        			procColumnsMap.put(key, al);
        			al = new ArrayList();
        			added = true;
        		}
				key = schema + "." + specificname;
    		    al.add(new ProcColumnsData(s.rs, mainData));
    		    added = false;
        		++i;
    			prevSchema = schema;
    			prevSpecificName = specificname;
        	}
        	if (!added && key.length() > 0)
        	{
        		procColumnsMap.put(key, al);	        		
        	}
        	added = false;
	        log(i + " values cached in procColumnsMap");
        	if (sql2.length() > 0)
        	{
		        s.PrepareExecuteQuery(methodName, sql2);
		        while (s.next())
		        {
		        	schema = trim(s.rs.getString(1));
		        	specificname = trim(s.rs.getString(2));
		        	columnType = trim(s.rs.getString(3));
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevSpecificName = specificname;
	        		}
	        		if (!(schema.equals(prevSchema) && specificname.equals(prevSpecificName)))
	        		{
	        			procColumnsMap2.put(key, al);
	        			al = new ArrayList();
	        			added = true;
	        		}
					key = schema + "." + specificname + "." + columnType;
        		    al.add(new ProcColumnsData(s.rs, mainData));
        		    added = false;
	        		++i;
	    			prevSchema = schema;
	    			prevSpecificName = specificname;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		procColumnsMap2.put(key, al);	        		
	        	}
		        log(i + " values cached in procColumnsMap2");
        	}
            s.close(methodName);
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    private String getProcColumnList(int srcType, String schema, String specificName, String columnType)
    {

    	ArrayList al = null;
    	String key;
    	String columnList = "", typeName = "";
    	ProcColumnsData p = null;
    	
        if (mainData.iDB2())
        {
        	if (srcType == 1)
        	{
        		key  = schema + "." + specificName;
        		if (procColumnsMap.containsKey(key))
        			al = (ArrayList) procColumnsMap.get(key);
        	}
        	else
        	{
        		key = schema + "." + specificName + "." + columnType;
        		if (procColumnsMap2.containsKey(key))
        			al = (ArrayList) procColumnsMap.get(key);
        	}
        } 
        else
        {
    		key  = schema + "." + specificName;  
    		if (procColumnsMap.containsKey(key))
    			al = (ArrayList) procColumnsMap.get(key);
    		
        }
        if (al == null)
        	return "";
        int objCount = 0;
        Iterator itr = al.iterator();
        while (itr.hasNext()) 
        {
        	  p = (ProcColumnsData) itr.next();
	    	  String tmp = "";
	    	  if (mainData.iDB2())
	    	  {
	        	  typeName = p.typeName;
	        	  if (typeName.equalsIgnoreCase("VARCHAR") || typeName.equalsIgnoreCase("CHAR") ||        	      
	        	      typeName.equalsIgnoreCase("CHARACTER") || typeName.equalsIgnoreCase("GRAPHIC") ||
	        	      typeName.equalsIgnoreCase("VARGRAPHIC") || typeName.equalsIgnoreCase("CLOB") || 
	        	      typeName.equalsIgnoreCase("DBCLOB") || typeName.equalsIgnoreCase("BLOB"))
	        	  {
	        		  String dec = p.decimalDigits;
	        		  if (dec != null)
	        		  {
	        			  if (dec.length() == 0)
	        			  {
	        				  dec = p.columnSize;
	        			  }
	        		      tmp = typeName + "(" + dec + ")";
	        		  }
	        		  else
	        			 tmp = typeName;
	        	  } 
	        	  else if (typeName.equalsIgnoreCase("NUMERIC") || typeName.equalsIgnoreCase("DECIMAL"))
	        	  {
	        		  String dec = p.decimalDigits;
	        		  if (dec != null)
	        		     tmp = typeName + "(" + p.length + "," + dec + ")";
	        		  else
	        			 tmp = typeName + "(" + p.length + ")";
	        	  }
	        	  else if (typeName.equalsIgnoreCase("CHARACTER FOR BIT DATA"))
	        	  {
	        		  tmp = "CHARACTER " + "(" + p.length + ") FOR BIT DATA";
	        	  }
	        	  else if (typeName.equalsIgnoreCase("CHAR FOR BIT DATA"))
	        	  {
	        		  tmp = "CHAR " + "(" + p.length + ") FOR BIT DATA";
	        	  }
	        	  else if (typeName.equalsIgnoreCase("VARCHAR FOR BIT DATA"))
	        	  {
	        		  tmp = "VARCHAR " + "(" + p.length + ") FOR BIT DATA";
	        	  }
	        	  else
	        	  {
	        		  tmp = typeName;
	        	  }
	    	  } else if (mainData.DB2())
	    	  {
	        	  typeName = p.typeName;
	        	  String typeSchema = p.typeSchema;
	        	  int length = p.length;
	        	  int scale = p.scale;
	        	  int codePage = p.codePage;
	        	  tmp = composeType(typeSchema, typeName, length, scale, codePage, schema) + p.locator;        		  
	    	  }
	    	  if (objCount == 0)
	    	  {
	        	  if (mainData.Mysql())
	        	  {
	        		  columnList = p.paramList;
	        	  }
	        	  else if (mainData.iDB2())
	        	  {
	        		  columnList = p.columnType + " " + putQuote(p.columnName) + " " + tmp;
	        	  }
	        	  else
	        	  {
	    		      columnList = p.columnType + " " + putQuote(p.parmName) + " " + tmp;
	        	  }
	     	  } else
	    	  {
	        	  if (mainData.Mysql())
	        	  {
	        		  columnList = p.paramList;
	        	  }
	        	  else if (mainData.iDB2())
	        	  {
	        		  columnList = columnList + "," + linesep + p.columnType + " " + putQuote(p.columnName) + " " + tmp; 
	        	  }
	        	  else
	        	  {
	    		      columnList = columnList + "," + linesep + p.columnType + " " + putQuote(p.parmName) + " " + tmp;
	        	  }
	    	  }
	    	  objCount++;
        }
	    return columnList;
    }

    private void BuildAuthColumnList()
    {
    	String methodName = "BuildAuthColumnList";
        authColumnsMap = new HashMap();
    	String key, value, schema, specificname, typeschema, typename, sql = "";
    	
    	
        if (mainData.DB2())
        	sql = "SELECT routineschema, specificname, typeschema, typename FROM SYSCAT.ROUTINEPARMS " +
        			"WHERE routineschema IN (" + schemaList + ") AND rowtype IN ('B', 'O', 'P') " +
        					"ORDER BY routineschema, specificname, ordinal ASC";
           
    	
    	if (sql.length() == 0)
    		return;
    	
    	SQL s = new SQL(mainData.connection);
    	try
    	{
	        s.PrepareExecuteQuery(methodName, sql);
	        int i = 0;
	        while (s.next())
	        {
	        	schema = trim(s.rs.getString(1));
	        	specificname = trim(s.rs.getString(2));
	        	typeschema = trim(s.rs.getString(3));
	        	typename = trim(s.rs.getString(4));
				key = schema + "." + specificname;
				value = typeschema + ":" + typename;
				authColumnsMap.put(key, value);
			    ++i;
	        }
	        log(i + " values cached in authColumnsMap");
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
        s.close(methodName);
    }
    
    private String authColumnList(String schema, String colName)
    {
    	int objCount = 0;
    	String key = schema + "." + colName, columnList = "", typeSchema, typeName;
    	
    	if (authColumnsMap.containsKey(key))
    	{
			String[] tmpVar = ((String) authColumnsMap.get(key)).split(":");
			typeSchema = tmpVar[0];
			typeName = tmpVar[1];
  	    	if (objCount == 0)
	    	{
	    	   columnList = composeType(typeSchema, typeName, -1, 0, -1, schema);
	     	} else
	    	{
	    	   columnList = columnList + "," + composeType(typeSchema, typeName, -1, 0, -1, schema);
	    	}
	    	objCount++;
    	}
        return columnList;
    }
    
    private void genDB2Grants(String schema)
    {
    	String methodName = "genDB2Grants";
    	String sql = "", dstSchema = getDstSchema(schema);
    	String granteeType, grantee, name, colName, auth, type;
    	StringBuffer buffer = new StringBuffer();
    	int objCount = 0;
    	
    	if (mainData.DB2())
    	{
    		sql =   "WITH AUTH AS ( " +
    				"SELECT granteetype, " +
    				"grantee, " +
    				"tabname as name, " +
    				"CAST(NULL AS VARCHAR(128)) AS colname, " +
    				"auth, " +
    				"'TABLE' as type " +
    				"FROM SYSCAT.TABAUTH, " +
    				" LATERAL(VALUES " +
    				"    (case when controlauth = 'Y' then 'CONTROL' else NULL end), " +
    				"    (case when alterauth = 'Y' then 'ALTER' when alterauth = 'N' then NULL else 'ALTER      GRANT' end), " +
    				"    (case when deleteauth = 'Y' then 'DELETE' when deleteauth = 'N' then NULL else 'DELETE     GRANT' end), " +
    				"    (case when indexauth = 'Y' then 'INDEX' when indexauth = 'N' then NULL else 'INDEX      GRANT' end), " +
    				"    (case when insertauth = 'Y' then 'INSERT' when insertauth = 'N' then NULL else 'INSERT     GRANT' end), " +
    				"    (case when selectauth = 'Y' then 'SELECT' when selectauth = 'N' then NULL else 'SELECT     GRANT' end), " +
    				"    (case when refauth = 'Y' then 'REFERENCE' when refauth = 'N' then NULL else 'REFERENCES GRANT' end), " +
    				"    (case when updateauth = 'Y' then 'UPDATE' when updateauth = 'N' then NULL else 'UPDATE     GRANT' end) " +
    				" ) " +
    				" AS A(auth) " +
    				"WHERE tabschema = '"+schema+"' " +
    				"UNION ALL " +
    				"SELECT granteetype, " +
    				"       grantee, " +
    				"       tabname as name, " +
    				"       colname, " +
    				"       CASE privtype WHEN 'U' THEN 'UPDATE' WHEN 'R' THEN 'REFERENCES' END || ' GRANT' AS auth, " +
    				"       'COLUMN' AS type " +
    				"FROM SYSCAT.COLAUTH " +
    				"WHERE tabschema = '"+schema+"' " +
    				"UNION ALL " +
    				"SELECT granteetype, " +
    				"       grantee, " +
    				"       indname as name, " +
    				"       CAST(NULL AS VARCHAR(128)) AS colname, " +
    				"       (case when controlauth = 'Y' then 'CONTROL' else NULL end) AS auth, " +
    				"       'INDEX' as type " +
    				"FROM SYSCAT.INDEXAUTH " +
    				"WHERE indschema = '"+schema+"' " +
    				"UNION ALL " +
    				"SELECT granteetype, " +
    				"       grantee, " +
    				"       R.routinename as name," +
    				"       R.specificname as colname, " +
    				"       (case when executeauth = 'Y' then 'EXECUTE' when executeauth = 'N' then NULL else 'EXECUTE    GRANT' end) AS auth, " +
    				"       CASE R.routinetype WHEN 'F' THEN 'FUNCTION' " +
    				"                          WHEN 'P' THEN 'PROCEDURE' " +
    				"                          END as type " +
    				"FROM SYSCAT.ROUTINEAUTH A, " +
    				"     SYSCAT.ROUTINES R " +
    				"WHERE A.schema = '"+schema+"' " +
    				"AND A.schema = R.routineschema " +
    				"AND A.specificname = R.specificname " +
    				"AND A.routinetype IN ('F', 'P') " +
    				"UNION ALL " +
    				"SELECT granteetype, " +
    				"       grantee, " +
    				"       '"+schema+"' as name, " +
    				"       CAST(NULL AS VARCHAR(128)) AS colname, " +
    				"       auth, " +
    				"       'SCHEMA' as type " +
    				"FROM SYSCAT.SCHEMAAUTH, " +
    				"  LATERAL(VALUES " +
    				"          (case when alterinauth = 'Y' then 'ALTERIN' when alterinauth = 'N' then NULL else 'ALTERIN    GRANT' end), " +
    				"          (case when createinauth = 'Y' then 'CREATEIN' when createinauth = 'N' then NULL else 'CREATEIN   GRANT' end), " +
    				"          (case when dropinauth = 'Y' then 'DROPIN' when dropinauth = 'N' then NULL else 'DROPIN     GRANT' end) " +
    				"       ) " +
    				"       AS A(auth) " +
    				"WHERE schemaname = '"+schema+"' " +
    				"UNION ALL " +
    				"SELECT granteetype, " +
    				"       grantee, " +
    				"       seqname as name, " +
    				"       CAST(NULL AS VARCHAR(128)) AS colname, " +
    				"       auth, " +
    				"       'SEQUENCE' as type " +
    				"FROM SYSCAT.SEQUENCEAUTH, " +
    				"  LATERAL(VALUES " +
    				"            (case when usageauth = 'Y' then 'USAGE' when usageauth = 'N' then NULL else 'USAGE      GRANT' end), " +
    				"            (case when alterauth = 'Y' then 'ALTER' when alterauth = 'N' then NULL else 'ALTER      GRANT' end) " +
    				"          ) " +
    				"   AS A(auth) " +  
    				"WHERE seqschema = '"+schema+"' " + 
    				((majorSourceDBVersion >= 9 && minorSourceDBVersion >= 7) ?
    				"UNION ALL " +
    				"SELECT granteetype, grantee, varname as name, CAST(NULL AS VARCHAR(128)) AS colname, auth, 'VARIABLE' as type " +
    				"FROM SYSCAT.VARIABLEAUTH, " +
    				"   LATERAL(VALUES " +
    				"      (case when readauth = 'Y' then 'READ' when readauth = 'N' then NULL else 'READ      GRANT' end), " +
    				"      (case when writeauth = 'Y' then 'WRITE' when writeauth = 'N' then NULL else 'WRITE     GRANT' end)) " +
    				"   AS A(auth) " +
    				"WHERE varschema = '"+schema+"' " 
    				: " ") +
    				((majorSourceDBVersion >= 9 && minorSourceDBVersion >= 5) ?
    				"UNION ALL " +
    				"SELECT granteetype, grantee, MODULENAME as name, CAST(NULL AS VARCHAR(128)) AS colname, " +
    				"       (case when EXECUTEAUTH = 'Y' then 'MODULE' when EXECUTEAUTH = 'N' then NULL else 'EXECUTE    GRANT' end) auth, 'EXECUTE' as type " +
    				"FROM SYSCAT.MODULEAUTH " +
    				"WHERE MODULESCHEMA = '"+schema+"' " +
    				"UNION ALL " +
    				"SELECT granteetype, grantee, rolename as name, CAST(NULL AS VARCHAR(128)) AS colname, " +
    				"       (case when admin = 'Y' then 'ROLE       GRANT' else 'ROLE' end) auth, 'ROLE' as type " +
    				"FROM SYSCAT.ROLEAUTH " +
    				"WHERE grantor = '"+schema+"' " +
    				"and rolename NOT LIKE 'SYSROLE%' "
    				: " ") +
    				") SELECT * FROM AUTH WHERE AUTH IS NOT NULL " +
    				"ORDER BY CASE type WHEN 'SCHEMA' THEN 1 " +
    				"WHEN 'TABLE' THEN 2 " +
    				"WHEN 'COLUMN' THEN 3 " +
    				"ELSE 4 END";
            
    		if (majorSourceDBVersion >= 9 && minorSourceDBVersion >= 5)
            {
            	String role = executeSQL("select rolename from syscat.roles where rolename not like 'SYS%'", false);
            	if (role != null && role.length() > 0)
            	{
            		String[] roles = role.split("~");
            		for (int i = 0; i < roles.length; ++i)
            		{
                  	    try
    					{
    						db2ObjectsWriter[(Integer) plsqlHashTable.get("GRANTS".toLowerCase())].write("CREATE ROLE " + roles[i] + linesep + sqlTerminator + linesep);
    					} catch (IOException e)
    					{
    						e.printStackTrace();
    					}        			
            		}
            	}
            }            
    	}
        
    	if (sql.equals("")) return;
        
    	SQL s = new SQL(mainData.connection);

        try
        {
          s.PrepareExecuteQuery(methodName, sql);	
          while (s.next()) 
          {
        	  buffer.setLength(0);
        	  granteeType = trim(s.rs.getString(1));
        	  grantee = trim(s.rs.getString(2));
        	  name = trim(s.rs.getString(3));
        	  colName = trim(s.rs.getString(4));
        	  auth = trim(s.rs.getString(5));
        	  type = trim(s.rs.getString(6));
        	  if (auth.length() >= 10)
        		  buffer.append("GRANT " + auth.substring(0,9));
        	  else
        		  buffer.append("GRANT " + auth);
        	  if (type.equals("COLUMN"))
        	     buffer.append(" (\"" + colName + "\")");
        	  if (!type.equals("ROLE"))
        	     buffer.append(" ON ");
        	  if (type.equals("COLUMN") || type.equals("TABLE") || type.equals("ROLE"))
                 buffer.append(" \""+name+"\" ");
        	  else
        		 buffer.append(type + " \""+name+"\" ");
        	  if ((type.equals("FUNCTION") || type.equals("PROCEDURE")))
        	  {
        	     buffer.append("(" + authColumnList(schema, colName) + ")");        		  
        	  }
        	  buffer.append(" TO ");
        	  if (grantee.equals("PUBLIC"))
        		  buffer.append(grantee);
        	  else
        	  {
	        	  if (granteeType.equals("U"))
	        		  buffer.append("USER \"" + grantee + "\"");	 
	        	  else if (granteeType.equals("R"))
	        		  buffer.append("ROLE \"" + grantee + "\"");	 
	        	  else
	        		  buffer.append("GROUP \"" + grantee + "\"");
        	  }
        	  if (auth.endsWith("GRANT"))
        		  buffer.append(" WITH GRANT OPTION");
        	  buffer.append(linesep + sqlTerminator + linesep);
        	  db2ObjectsWriter[(Integer) plsqlHashTable.get("GRANTS".toLowerCase())].write(buffer.toString());
        	  if (objCount > 0 && objCount % 20 == 0)
	            	log(objCount + " # grants extracted for schema " + schema);
        	  objCount++;
          }
          if (objCount > 0)
       	    log(objCount + " Total # Grants extracted for schema " + schema);
          s.close(methodName); 
         } catch (Exception e)
         {
         	log("Error in genInstallJavaJars for " + sql);
         	e.printStackTrace();
         }   
    }
    
    private String genInstallJavaJars()
    {
    	String methodName = "genInstallJavaJars";
    	boolean once = true;
    	String sql = "", jarSchema, jarID, jarPath = "";
    	StringBuffer buffer = new StringBuffer();
    	
    	if (mainData.DB2())
    	{
    		sql = "select distinct jarschema, jar_id from syscat.routines where jar_id is not null ";
    	}


        if (sql.equals("")) return "";
        
    	SQL s = new SQL(mainData.connection);

        try
        {
          s.PrepareExecuteQuery(methodName, sql);		
          while (s.next()) 
          {
        	  buffer.setLength(0);
        	  jarSchema = trim(s.rs.getString(1));
        	  jarID = trim(s.rs.getString(2));
        	  jarPath = getJavaJarPath(jarSchema, jarID);
        	  buffer.append("--#SET :JavaJars:" + jarSchema + ":" + jarID + linesep);
        	  buffer.append("CALL SQLJ.REMOVE_JAR('"+jarSchema+"."+jarID+"')" + linesep);
	          buffer.append(sqlTerminator + linesep);
        	  buffer.append("CALL SQLJ.INSTALL_JAR('file:"+jarPath+"','"+jarSchema+"."+jarID+"')" + linesep);
	          buffer.append(sqlTerminator + linesep);
        	  buffer.append("CALL SQLJ.REFRESH_CLASSES()" + linesep);
	          buffer.append(sqlTerminator + linesep);
        	  db2ObjectsWriter[(Integer) plsqlHashTable.get("JAVAJARS".toLowerCase())].write(buffer.toString());
          }
          s.close(methodName); 
         } catch (Exception e)
         {
         	log("Error in genInstallJavaJars for " + sql);
         	e.printStackTrace();
         }   
         return buffer.toString();    	
    }
    
    private String getJavaJarPath(String jarSchema, String jarID)
    {
    	String jarPath = "", sql = "";
		if (Constants.win())
		{
	    	if (majorSourceDBVersion > 8)
	    	{
				sql = "select REG_VAR_VALUE from sysibmadm.REG_VARIABLES " +
						"where reg_var_name = 'DB2INSTPROF' " +
						"and dbpartitionnum = 0";
				jarPath = executeSQL(sql, false) + "\\function\\jar\\"+jarSchema+"\\"+jarID+".jar";
	    	} else
	    	{
	    		Process p = null;
	    		BufferedReader stdInput = null;
	    		String line;
	    		String[] tok = null;
    			try
    			{
    				p = Runtime.getRuntime().exec("db2cmd /c /i /w set ");
    				stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
    				while ((line = stdInput.readLine()) != null)
    				{
    					if (line.startsWith("DB2PATH"))
    					{
    						tok = line.split("=");		
    						jarPath = tok[1] + "\\java";
    					}
    				}
    				if (stdInput != null)
    					stdInput.close();
    			} catch (Exception e)
    			{
    				jarPath = "not detected";
    			}
	    	}	    		
		} else
		{
			String userHome = System.getProperty("user.home");
			if (IBMExtractUtilities.FileExists(userHome + "/sqllib"))
			{
				jarPath = userHome + "/sqllib/function/jar/" + jarSchema + "/" + jarID + ".jar";
			} else
				jarPath = "not detected";
		}
		return jarPath;
    }
    
    // Work in progress for this routine
    private void getNonSQLProcedureSource(String schema)
    {
    	String methodName = "getNonSQLProcedureSource";
    	boolean once = true;
    	String tmp, sql = "", procType;
    	String routineName = "", dstSchema = getDstSchema(schema), specificName = "", routineType, functionType;
    	String origin, sourceSchema, sourceSpecific, language, implementation, classStr;
    	String parameterStyle;
    	int pos;
    	
    	SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();

        if (mainData.DB2())
        	sql = "SELECT routineschema, routinename, specificname, routinetype, origin, functiontype, language, sourceschema, " +
        			"sourcespecific, deterministic, external_action, nullcall, scratchpad, scratchpad_length, " +
        			"parallel, parameter_style, fenced, sql_data_access, dbinfo, result_sets, threadsafe, " +
        			"class, implementation, finalcall, cardinality, jar_id, jarschema " +
        			"FROM SYSCAT.ROUTINES " +
        			"WHERE routinetype IN ('F', 'P') " +
        			"AND routineschema = '"+schema+"' " +
        			"AND language <> 'SQL' " +
        			"AND origin IN ('U', 'E', 'M') " +
        			"ORDER BY specificname";
           
        if (sql.equals("")) return;
        
        try
        {
          s.PrepareExecuteQuery(methodName, sql);

          while (s.next()) 
          {
        	  buffer.setLength(0);
        	  if (once)
        	  {
	             buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep);            	
	             buffer.append(sqlTerminator + linesep);
	             once = false;
        	  }
        	  routineName = trim(s.rs.getString(2));        	  
        	  specificName = trim(s.rs.getString(3));
        	  routineType = trim(s.rs.getString(4));
        	  origin = trim(s.rs.getString(5));
        	  functionType = trim(s.rs.getString(6));
        	  language = trim(s.rs.getString(7));
        	  sourceSchema = trim(s.rs.getString(8));
        	  sourceSpecific = trim(s.rs.getString(9));
        	  procType = (routineType.equals("F") ? "FUNCTION" : "PROCEDURE");
	          db2DropObjectsWriter.write("--#SET :DROP:"+procType+":" + specificName + linesep);
	          db2DropObjectsWriter.write("DROP SPECIFIC "+procType+" " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(specificName)) + ";" + linesep);
	          buffer.append("--#SET :"+procType+":" + dstSchema + ":" + routineName + linesep);
        	  buffer.append("CREATE "+procType+" " + putQuote(dstSchema) + "." + putQuote(routineName));
        	  tmp = getProcColumnList(1, schema, specificName, null);
        	  if (tmp != null && tmp.length() == 0)
        	     buffer.append("()"+ linesep );
        	  else
        		  buffer.append(linesep + "(" + linesep + tmp + linesep + ")" + linesep ); 
           	  if (routineType.equals("F"))
           	  {
         		  buffer.append("RETURNS ");
           		  if (functionType.equals("T"))
           		    buffer.append("TABLE (" + linesep);
        		  buffer.append(getFunctionReturnMarker("2", null, null, schema, specificName)); 
           		  if (functionType.equals("T"))
             		buffer.append(")" + linesep);
           	  }
              buffer.append("SPECIFIC " + putQuote(dstSchema) + "." + putQuote(specificName) + linesep);
              if (origin.equals("M"))
              {
           		  buffer.append("AS TEMPLATE " + linesep);            	  
              } else if (origin.equals("U"))
              {
           		  buffer.append("SOURCE ");            	              	  
            	  if (sourceSchema.startsWith("SYSIBM"))
            	  {
            		  implementation = s.rs.getString(23);
            		  pos = implementation.indexOf(".");
            		  tmp = implementation.substring(0, pos+1);
            		  buffer.append(tmp);
            		  String name = implementation.substring(pos + 1, implementation.length());
            		  pos = name.indexOf(" (");
            		  buffer.append("\"");
            		  if (pos > 0)
            			  buffer.append(name.substring(0, pos) + "\"" + name.substring(pos, name.length()));            			  
            		  else
            			  buffer.append(name + "\"");
            		  buffer.append(linesep);
            	  } else
            	  {
            		  if (!sourceSchema.equalsIgnoreCase(schema))
            		  {
            			  buffer.append(putQuote(sourceSchema) + ".");
            		  }
        			  tmp = executeSQL("SELECT routinename FROM SYSCAT.ROUTINES " +
        			  		"WHERE routineschema = '" + sourceSchema + "' " +
        			  		"AND specificname = '" + sourceSpecific + "'", false);
        			  buffer.append(putQuote(tmp) + linesep + "(" + linesep + 
        					  getSourceColumnList(schema, sourceSchema, sourceSpecific) + 
        					  linesep + ")" + linesep);
            	  }
              } else
              {
            	  buffer.append("EXTERNAL NAME ");
            	  implementation = trim(s.rs.getString(23));
            	  if (language.equalsIgnoreCase("JAVA"))
            	  {
            		  String jarID = trim(s.rs.getString(26));
            		  String jarSchema = trim(s.rs.getString(27));
            		  classStr = trim(s.rs.getString(22));
            		  buffer.append("'"+jarSchema+"."+jarID+":"+classStr);            		  
            		  pos = implementation.indexOf("(");
            		  if (pos > 0)
            		     buffer.append("!" + implementation.substring(0, pos) + "'");
            		  else
            			 buffer.append("!" + implementation + "'");
            	  } else
            	  {
            		  buffer.append("'"+implementation+"'"); 
            	  }
            	  buffer.append(linesep);  
            	  buffer.append("LANGUAGE " + language + linesep);
            	  parameterStyle = trim(s.rs.getString(16));
            	  if (parameterStyle != null)
            	  {
	            	  if (parameterStyle.equalsIgnoreCase("DB2SQL"))
	            		  tmp = "DB2SQL";
	            	  else if (parameterStyle.equalsIgnoreCase("SQL"))
	            		  tmp = "SQL";
	            	  else if (parameterStyle.equalsIgnoreCase("DB2GENRL"))
	            		  tmp = "DB2GENERAL";
	            	  else if (parameterStyle.equalsIgnoreCase("GENERAL"))
	            		  tmp = "GENERAL";
	            	  else if (parameterStyle.equalsIgnoreCase("JAVA"))
	            		  tmp = "JAVA";
	            	  else if (parameterStyle.equalsIgnoreCase("DB2DARI"))
	            		  tmp = "DB2DARI";
	            	  else if (parameterStyle.equalsIgnoreCase("GNRLNULL"))
	            		  tmp = "GENERAL WITH NULLS";
	            	  else
	            		  tmp = "";
	            	  buffer.append("PARAMETER STYLE " + tmp + linesep);
            	  }
            	  String externalAction = s.rs.getString(11);
            	  if (externalAction != null && externalAction.equals("Y"))
            		  buffer.append("EXTERNAL ACTION" + linesep);
            	  else
            		  buffer.append("NO EXTERNAL ACTION" + linesep);
            	  String scratchPad = s.rs.getString(13);
            	  String scratchPadLength = s.rs.getString(14);
            	  if (scratchPad != null && scratchPad.equals("Y"))
            		  buffer.append("SCRATCHPAD " + scratchPadLength + linesep);
            	  else if (scratchPad != null && scratchPad.equals("N") && !routineType.equals("P"))
            		  buffer.append("NO SCRATCHPAD " + linesep);
            	  String finalCall = s.rs.getString(24);
            	  if (finalCall != null && finalCall.equals("Y"))
            		  buffer.append("FINAL CALL" + linesep);
            	  else
            		  buffer.append("NO FINAL CALL" + linesep);
            	  String parallel = s.rs.getString(15);
            	  if (parallel != null && parallel.equals("Y"))
            		  buffer.append("ALLOW PARALLEL" + linesep);
            	  else if (parallel != null && parallel.equals("Y") && !routineType.equals("P"))
            		  buffer.append("DISALLOW PARALLEL" + linesep);
            	  String dbInfo = s.rs.getString(19);
            	  if (dbInfo != null && dbInfo.equals("Y"))
            		  buffer.append("DBINFO" + linesep);
            	  else
            		  buffer.append("NODBINFO" + linesep);
            	  int cardinality = s.rs.getInt(25);
            	  if (cardinality > 0)
            		  buffer.append("CARDINALITY " + cardinality + linesep);
              }
          	  buffer.append(linesep + sqlTerminator + linesep + linesep);   
          	  db2ObjectsWriter[(Integer) plsqlHashTable.get("ROUTINE".toLowerCase())].write(buffer.toString());
          }
          s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting procedure for " + schema + "." + specificName);
        	e.printStackTrace();
        }
    }
    
    private String getProcedureSource(String schema, String specificName)
    {
    	String methodName = "getProcedureSource";
    	String sql = "";
    	String procName = "", dstSchema = getDstSchema(schema);
    	
    	SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();

        if (mainData.iDB2())
        	sql = "SELECT PROCEDURE_SCHEM, PROCEDURE_NAME, NUM_RESULT_SETS, SPECIFIC_NAME " +
        			"FROM SYSIBM.SQLPROCEDURES " +
        			"WHERE PROCEDURE_SCHEM = '"+schema+"' " +
        			"AND   SPECIFIC_NAME = '"+specificName+"'";
           
        if (sql.equals("")) return "";
        
        try
        {
          s.PrepareExecuteQuery(methodName, sql);        	
          while (s.next()) 
          {
        	  procName = s.rs.getString(2);
	          buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep);            	
	          buffer.append(sqlTerminator + linesep);    
	          buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep + sqlTerminator + linesep); 
	          buffer.append("ECHO PROCEDURE:" + dstSchema + ":" + procName + linesep + sqlTerminator + linesep);
	          buffer.append("--#SET :PROCEDURE:" + dstSchema + ":" + procName + linesep);
        	  buffer.append("CREATE PROCEDURE " + putQuote(dstSchema) + "." + putQuote(procName) + linesep);
        	  buffer.append("(" + linesep + getProcColumnList(1, schema, specificName, null) + linesep + ")" + linesep );
        	  String numRS = s.rs.getString(3);
        	  if (numRS != null && numRS.length() > 0)
        		  buffer.append("RESULT SETS " + numRS + linesep);
        	  buffer.append("LANGUAGE SQL" + linesep);
        	  buffer.append("SPECIFIC " + putQuote(dstSchema) + "." + putQuote(s.rs.getString(4)) + linesep);
        	  buffer.append(getRoutineBody("1", schema, specificName));
          	  buffer.append(linesep + sqlTerminator + linesep + linesep);            	          
          }
          s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting procedure for " + procName);
        	e.printStackTrace();
        }    
        return buffer.toString();
    }
        
    private String getFunctionReturnMarker(String functionType, String returnType, 
    		String charMaxLength, String schema, String specificName)
    {
    	String methodName = "getFunctionReturnMarker";
    	String returnStr = "", parmName = "";
    	if (functionType.equals("1"))
    	{
    		if (returnType == null || returnType.length() == 0)
    		{
    			return "";
    		} else
    		{
    	      	if (returnType.equalsIgnoreCase("CHARACTER VARYING"))
    	    	{
    	    	   if (charMaxLength != null && charMaxLength.length() > 0)
    	    	   {
    	    		 returnStr = "VARCHAR("+charMaxLength+")";
    	    	   } else
    	    	   {
    	    	     returnStr = "VARCHAR";
    	    	   }
    	        } else if (returnType.equalsIgnoreCase("CHARACTER") || returnType.equalsIgnoreCase("CHAR"))
    	        {
    	    	   if (charMaxLength != null && charMaxLength.length() > 0)
    	    	   {
    	    		  returnStr = "CHARACTER("+charMaxLength+")";
    	    	   } 
    	    	   else
    	    	   {
    	    		  returnStr = "CHARACTER";
    	    	   }
    	        } else if (returnType.toUpperCase().startsWith("INT") || returnType.toUpperCase().startsWith("INTEGER"))
    	        {
    	        	returnStr = "INTEGER";
    	        }
    	        else if (returnType.matches("(?i).*charset.*"))
    	        {
    	        	int pos = returnType.toUpperCase().indexOf("CHARSET");
    	        	if (pos > 1)
    	        	{
    	        		returnStr = returnType.substring(0, pos-1);
    	        	} else
    	        		returnStr = returnType;
    	        }
    	        else
    	        {
    	        	  returnStr = returnType;
    	        }
    	      	return "RETURNS " + returnStr + linesep;    			
    		}
    	} else
    	{
    		int colCount = 0;
        	String sql = "";
        	String columnList = "", typeName = "";
        	
        	SQL s = new SQL(mainData.connection);
            
            if (mainData.iDB2())
            	sql = "SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, " +
            			"NUM_PREC_RADIX, IS_NULLABLE " +
            			"FROM SYSIBM.SQLFUNCTIONCOLS " +
            			"WHERE FUNCTION_SCHEM = '"+schema+"' " +
            			"AND   SPECIFIC_NAME = '"+specificName+"' " +
            			"AND COLUMN_TYPE <> 1 " +
            			"ORDER BY ORDINAL_POSITION ";
            else if (mainData.DB2())
            {
            	sql = "SELECT c.parmname parmname, C.typeschema as ctypeschema, C.typename ctypename, " +
            			"C.length clength, C.scale cscale, C.codepage ccodepage, R.typeschema as rtypeschema, " +
            			"R.typename rtypename, R.length rlength, R.scale rscale, R.codepage rcodepage, " +
            			"CASE WHEN c.locator = 'Y' THEN 'AS LOCATOR' ELSE ' ' END clocator " +
            			"FROM SYSCAT.ROUTINEPARMS C " +
            			"LEFT OUTER JOIN SYSCAT.ROUTINEPARMS R " +
            			"ON C.routineschema = R.routineschema " +
            			"AND C.specificname = R.specificname " +
            			"AND C.ordinal = R.ordinal " +
            			"AND R.rowtype = 'R' " +
            			"WHERE '"+specificName+"' = C.specificname " +
            			"AND '"+schema+"' = C.routineschema " +
            			"AND C.rowtype = 'C' " +
            			"ORDER BY C.ordinal";
            }
               
            if (sql.equals("")) return "";
            
            try
            {
              s.PrepareExecuteQuery(methodName, sql);    
              while (s.next()) 
              {
            	  String tmp = "";
            	  if (mainData.iDB2())
            	  {
            		  parmName = s.rs.getString(1);
            		  if (parmName == null) parmName = "";
	            	  typeName = s.rs.getString(2);
	            	  if (typeName.equalsIgnoreCase("VARCHAR") || typeName.equalsIgnoreCase("CHAR") ||        	      
	            	      typeName.equalsIgnoreCase("CHARACTER") || typeName.equalsIgnoreCase("GRAPHIC") ||
	            	      typeName.equalsIgnoreCase("VARGRAPHIC") || typeName.equalsIgnoreCase("CLOB") || 
	            	      typeName.equalsIgnoreCase("DBCLOB") || typeName.equalsIgnoreCase("BLOB"))
	            	  {
	            		  String dec = s.rs.getString(4);
	            		  if (dec != null)
	            		     tmp = typeName + "(" + dec + ")";
	            		  else
	            			 tmp = typeName;
	            	  } 
	            	  else if (typeName.equalsIgnoreCase("NUMERIC") || typeName.equalsIgnoreCase("DECIMAL"))
	            	  {
	            		  String dec = s.rs.getString(5);
	            		  if (dec != null)
	            		  {
	            			  if (dec.equals("0"))
	            				  dec = s.rs.getString(3);
	            		     tmp = typeName + "(" + dec + "," + s.rs.getString(6) + ")";
	            		  }
	            		  else
	            			 tmp = typeName + "(" + s.rs.getString(5) + ")";
	            	  }
	            	  else if (typeName.equalsIgnoreCase("CHARACTER FOR BIT DATA"))
	            	  {
	            		  tmp = "CHARACTER " + "(" + s.rs.getString(4) + ") FOR BIT DATA";
	            	  }
	            	  else if (typeName.equalsIgnoreCase("CHAR FOR BIT DATA"))
	            	  {
	            		  tmp = "CHAR " + "(" + s.rs.getString(4) + ") FOR BIT DATA";
	            	  }
	            	  else if (typeName.equalsIgnoreCase("VARCHAR FOR BIT DATA"))
	            	  {
	            		  tmp = "VARCHAR " + "(" + s.rs.getString(4) + ") FOR BIT DATA";
	            	  }
	            	  else
	            	  {
	            		  tmp = typeName;
	            	  }
            	  } else if (mainData.DB2())
            	  {
            		  parmName = s.rs.getString(1);
            		  if (parmName == null) parmName = "";
            		  String ctypeSchema = trim(s.rs.getString(2));
            		  String ctypeName = trim(s.rs.getString(3));
            		  int clength = s.rs.getInt(4);
            		  int cscale = s.rs.getInt(5);
            		  int ccodePage = s.rs.getInt(6);
            		  tmp = composeType(ctypeSchema, ctypeName, clength, cscale, ccodePage, schema);
            		  String rtypeSchema = s.rs.getString(7);
            		  if (rtypeSchema != null && rtypeSchema.length() > 0)
            		  {
            		     String rtypeName = s.rs.getString(8);
            		     int rlength = s.rs.getInt(9);
            		     int rscale = s.rs.getInt(10);
            		     int rcodePage = s.rs.getInt(11);
            		     tmp = tmp + composeType(rtypeSchema, rtypeName, rlength, rscale, rcodePage, schema);
            		  }
            		  tmp = tmp + s.rs.getString(12);
            		  
            	  }
            	  if (colCount == 0)
            	  {
            		 columnList = putQuote(parmName) + " " + tmp + linesep;
             	  } else
            	  {
            		 columnList = columnList + "," + putQuote(parmName) + " " + tmp + linesep;
            	  }
            	  colCount++;
              }
              s.close(methodName); 
            } catch (Exception e)
            {
            	log("Error in getting sql table function columns " + specificName);
            	e.printStackTrace();
            }    
            if (mainData.iDB2())
               return "RETURNS TABLE " + linesep + "(" + linesep + columnList + linesep + ")" + linesep;
            else
               return columnList;
    	}    	
    }
    
    private String getFunctionSource(String schema, String specificName, String functionType)
    {
    	String methodName = "getFunctionSource";
    	String sql = "", routineSchema = "", returnType = "", charMaxLength = "";;
    	String procName = "", dstSchema, isDeterministic = "", sqlDataAccess = "", externalAction = "";
    	
    	SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();

        if (mainData.iDB2())
        {
        	if (functionType.equals("1"))
	        	sql = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DATA_TYPE, " +
	        			"SPECIFIC_SCHEMA, SPECIFIC_NAME, " +
	        			"IS_DETERMINISTIC, SQL_DATA_ACCESS, CHARACTER_MAXIMUM_LENGTH " +
	        			"FROM SYSIBM.ROUTINES " +
	        			"WHERE SPECIFIC_SCHEMA = '"+schema+"' " +
	        			"AND   SPECIFIC_NAME = '"+specificName+"'";
        	else
        		sql = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, '' AS DATA_TYPE, " +
        				"SPECIFIC_SCHEMA, SPECIFIC_NAME, " +
        				"IS_DETERMINISTIC, SQL_DATA_ACCESS, 0 AS CHARACTER_MAXIMUM_LENGTH, EXTERNAL_ACTION " +
        				"FROM QSYS2.SYSFUNCS " +
        				"WHERE SPECIFIC_SCHEMA = '"+schema+"' " +
	        			"AND   SPECIFIC_NAME = '"+specificName+"'"; 
        }
        else if (mainData.Mysql())
        {
        	sql = "SELECT upper(ROUTINE_SCHEMA), upper(ROUTINE_NAME), DTD_IDENTIFIER AS DATA_TYPE, " +
        			"upper(ROUTINE_SCHEMA), upper(SPECIFIC_NAME), " +
        			"IS_DETERMINISTIC, SQL_DATA_ACCESS, 0 AS CHARACTER_MAXIMUM_LENGTH, ROUTINE_DEFINITION " +
        			"FROM INFORMATION_SCHEMA.ROUTINES " +
        			"WHERE ROUTINE_SCHEMA = '"+schema+"' " +
        			"AND   SPECIFIC_NAME = '"+specificName+"'";
        }
           
        if (sql.equals("")) return "";
        
        try
        {
          s.PrepareExecuteQuery(methodName, sql);      
          while (s.next()) 
          {
        	  routineSchema = trim(s.rs.getString(1));
        	  procName = trim(s.rs.getString(2));
        	  returnType = trim(s.rs.getString(3));
        	  isDeterministic = s.rs.getString(6);
        	  sqlDataAccess = s.rs.getString(7);
        	  charMaxLength = s.rs.getString(8);
        	  dstSchema = getDstSchema(routineSchema);
        	  dstSchema = getCaseName(dstSchema);
	          buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep);            	
	          buffer.append(sqlTerminator + linesep);         
		      buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep);            	
	          buffer.append(sqlTerminator + linesep);            	
	          if (functionType.equalsIgnoreCase("PROCEDURE"))
	          {
		          buffer.append("--#SET :PROCEDURE:" + dstSchema + ":" + procName + linesep);
        	      buffer.append("CREATE PROCEDURE " + putQuote(dstSchema) + "." + putQuote(procName) + linesep);
	          }
	          else
	          {
		          buffer.append("--#SET :FUNCTION:" + dstSchema + ":" + procName + linesep);
	        	  buffer.append("CREATE FUNCTION " + putQuote(dstSchema) + "." + putQuote(procName) + linesep);
	          }
        	  buffer.append("(" + linesep + getProcColumnList(2, schema, specificName, "1") + linesep + ")" + linesep );
        	  if (mainData.Mysql())
        	  {
        		  if (functionType.equalsIgnoreCase("FUNCTION"))
                     buffer.append(getFunctionReturnMarker("1", returnType, charMaxLength, schema, specificName));
        	  }
        	  else
                  buffer.append(getFunctionReturnMarker(functionType, returnType, charMaxLength, schema, specificName));
        	  buffer.append("LANGUAGE SQL ");
        	  buffer.append("SPECIFIC " + putQuote(dstSchema) + "." + putQuote(s.rs.getString(5)) + linesep);
        	  if (isDeterministic != null)
        	  {
        		  if (isDeterministic.equalsIgnoreCase("YES"))
                	  buffer.append("DETERMINISTIC  ");
        		  else
                	  buffer.append("NOT DETERMINISTIC ");
        	  }
        	  if (!(functionType.equals("1") || functionType.equals("FUNCTION")))
        	  {
        		  externalAction = s.rs.getString(9);
        		  if (externalAction != null && externalAction.equals("N"))
        			  buffer.append("NO EXTERNAL ACTION" + linesep);
        	  }
        	  if (sqlDataAccess != null)
        	  {
        		  if (sqlDataAccess.startsWith("READS"))
            	     buffer.append("READS SQL DATA" + linesep);
        		  else if (sqlDataAccess.startsWith("MODIFIES"))
             	     buffer.append("MODIFIES SQL DATA" + linesep);
        		  else if (sqlDataAccess.startsWith("CONTAINS"))
              	     buffer.append("CONTAINS SQL" + linesep);
        		  else
               	     buffer.append(sqlDataAccess + linesep);
        	  }        	  
        	  if (mainData.Mysql())
        	  {
        		  buffer.append("-- Function body is extracted as it is from MySQL." + linesep);
        		  buffer.append("-- You have to make changes to fit this to DB2 syntax." + linesep);
        		  StringBuffer chunks = new StringBuffer();
        		  IBMExtractUtilities.getStringChunks(s.rs, 9, chunks);
         	      buffer.append(chunks);
        	  }
        	  else
        	     buffer.append(getRoutineBody(functionType, routineSchema, procName));
          	  buffer.append(linesep + sqlTerminator + linesep + linesep);            	          
          }
          s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting function for " + procName);
        	e.printStackTrace();
        }    
        return buffer.toString();
    }
    
    public void BuildColumnMap()
    {
    	String methodName = "BuildColumnMap";
    	columnsMap = new HashMap();
    	String schema, table, columnsList = "";
    	boolean added = false;
    	String key = "", sql = "", prevSchema = "", prevTable = "";

        if (mainData.Oracle())
        	sql = "SELECT OWNER, TABLE_NAME, COLUMN_NAME FROM DBA_TAB_COLUMNS WHERE OWNER IN (" + schemaList + 
        	      ") ORDER BY OWNER, TABLE_NAME, COLUMN_ID ASC";
        else if (mainData.iDB2())
        	sql = "SELECT TABLE_SCHEM, TABLE_NAME, COLUMN_NAME FROM SYSIBM.SQLCOLUMNS WHERE TABLE_SCHEM IN (" + schemaList + 
  	              ") ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION ASC";
        else if (mainData.Mysql())
        	sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN (" + schemaList + 
  	              ") ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION ASC";
        else if (mainData.Teradata())
        	sql = "SELECT databasename, tablename, columnname FROM dbc.columns WHERE databasename IN (" + schemaList + 
  	              ") ORDER BY databasename, tablename";
    	
    	if (sql.length() == 0)
    		return;
    	
    	try
    	{
    		SQL s = new SQL(mainData.connection);
            s.PrepareExecuteQuery(methodName, sql); 
            int i = 0, j = 0;
        	while (s.next())
        	{
        		schema = trim(s.rs.getString(1));
        		table = trim(s.rs.getString(2));
        		if (i == 0)
        		{
        			prevSchema = schema;
        			prevTable = table;
        			j = 0;
        		}
        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
        		{
        			
        			columnsMap.put(key, columnsList);
        			added = true;
        			j = 0;
        		}
        		key = schema + "." + table;
          	    if (j == 0)
        	    {
        		    columnsList = putQuote(getCaseName(trim(s.rs.getString(3))));
         	    } else
        	    {
         	    	columnsList = columnsList + "," + putQuote(getCaseName(trim(s.rs.getString(3))));
        	    }
          	    ++j;
    		    added = false;
    			prevSchema = schema;
    			prevTable = table;
        		++i;
        	}
        	if (!added && key.length() > 0)
        	{
        		
        		columnsMap.put(key, columnsList);	        		
        	}
        	s.close(methodName);
            log(i + " values cached in columnsMap");
    	} catch (SQLException e)
    	{
    		e.printStackTrace();
    	}        	
    }
        
    private String getColumnList(String schema, String table)
    {
    	String key = removeQuote(schema) + "." + removeQuote(table), columnList = "";
    	
		if (columnsMap.containsKey(key))
		{
			columnList = (String) columnsMap.get(key);
		}
	    return columnList;
    }
    
    private void genDB2GVariables (String schema)
    {
    	String methodName = "genDB2GVariables";
    	StringBuffer sb = new StringBuffer();
    	String sql = "", readOnly, remarks, defaultValue;
    	String varName, typeSchema, typeName, expression = "", dstSchema = getDstSchema(schema);
    	int length, scale, codePage;
    	String varType = "";
    	
    	SQL s = new SQL(mainData.connection);

        if (mainData.DB2())
        {
        	if (majorSourceDBVersion >= 9 && minorSourceDBVersion >= 7)
        	{
        	   sql = "SELECT VARNAME,TYPESCHEMA,TYPENAME,LENGTH, " +
        			"SCALE,CODEPAGE,READONLY,REMARKS,DEFAULT " +
        			"FROM SYSCAT.VARIABLES  " +
        			"WHERE VARSCHEMA = '" + schema + "' " +
        			"AND VARMODULENAME IS NULL ";
        	}
        }
        
        if (sql.equals("")) return;
        
        try
        {
            s.PrepareExecuteQuery(methodName, sql);        	

	        int objCount = 0;
	        while (s.next()) 
	        {
	        	sb.setLength(0);
	        	varName = trim(s.rs.getString(1));
	        	typeSchema = trim(s.rs.getString(2));
	        	typeName = trim(s.rs.getString(3));
	        	length = s.rs.getInt(4);
	        	scale = s.rs.getInt(5);
	        	codePage = s.rs.getInt(6);
	        	readOnly = trim(s.rs.getString(7));
	        	remarks = trim(s.rs.getString(8));
	        	defaultValue = trim(s.rs.getString(9));
	        	if (defaultValue == null)
	        		defaultValue = "";
	        	sb.append("--#SET :" + "VARIABLE" + ":" + dstSchema + ":" + varName + linesep);  
		        db2DropObjectsWriter.write("--#SET :DROP:VARIABLE:" + varName + linesep);
	        	db2DropObjectsWriter.write("DROP VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + ";" + linesep);
	        	if (mainData.DB2())
	        	{
	        		varType = composeType(typeSchema, typeName, length, scale, codePage, schema);	        		
	        		if (readOnly.equals("C") && defaultValue.length() > 0)
		        	   sb.append("CREATE VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + " " + varType + " CONSTANT " + defaultValue);
	        		else if (readOnly.equals("N") && defaultValue.length() > 0)	        			
			           sb.append("CREATE VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + " " + varType + " DEFAULT " + defaultValue);
	        		else
	        		   sb.append("CREATE VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + " " + varType);
	        		sb.append(linesep + sqlTerminator + linesep);
	        	}
	            if (objCount > 0 && objCount % 20 == 0)
	            	log(objCount + " # Global Variables extracted for schema " + schema);
	            objCount++;
	            db2ObjectsWriter[(Integer) plsqlHashTable.get((String) "VARIABLE".toLowerCase())].write(sb.toString());
	        }
	        if (objCount > 0)
	    	   log(objCount + " Total # Global Variables extracted for schema " + schema);
	        s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting Global Variables  for schema " + schema + " SQL=" + sql);
        	e.printStackTrace();
        }    	
    }
    
    private void genMaterializedViews (String schema)
    {
    	String methodName = "genMaterializedViews";
    	StringBuffer sb = new StringBuffer();
    	String sql = "";
    	String mview_name, query = "", dstSchema = getDstSchema(schema);
    	String columnList = "";
    	
    	SQL s = new SQL(mainData.connection);

        if (mainData.Oracle())
        	sql = "SELECT MVIEW_NAME, QUERY FROM DBA_MVIEWS WHERE OWNER = '" + schema + "'";
        else if (mainData.DB2())
        	sql = "SELECT V.VIEWNAME, V.TEXT FROM SYSCAT.VIEWS V, SYSCAT.TABLES T " +
        			"WHERE V.VIEWSCHEMA = T.TABSCHEMA " +
        			"AND V.VIEWNAME = T.TABNAME " +
        			"AND T.TYPE = 'S' " +
        			"AND V.VIEWSCHEMA = '" + schema + "'";
        
        if (sql.equals("")) return;
        
        try
        {
            s.PrepareExecuteQuery(methodName, sql);        	
	        int objCount = 0;
	        while (s.next()) 
	        {
	        	sb.setLength(0);
	        	mview_name = s.rs.getString(1);
	        	query = s.rs.getString(2);
	        	sb.append("--#SET :" + "MQT" + ":" + dstSchema + ":" + mview_name + linesep);      
		        db2DropObjectsWriter.write("--#SET :DROP TABLE:"+ dstSchema + ":" + mview_name + linesep);
	        	db2DropObjectsWriter.write("DROP TABLE " + putQuote(dstSchema) + "." + putQuote(mview_name) + ";" + linesep);
	        	if (mainData.DB2())
	        	{
	        		sb.append(query + linesep + sqlTerminator + linesep);
	        	} else
	        	{
		        	columnList = getColumnList(schema, mview_name);
		        	sb.append("CREATE TABLE " + putQuote(dstSchema) + "." + putQuote(mview_name) + linesep);
		        /*	if (columnList.length() > 0)
		        	{
		        		sb.append("(" + columnList + ")" + linesep);
		        	} */ //Removed Column list  as suggested by Sam. Incorrect  logic for column list.Could be replaced by dbms_getmetadata.
		        	
		        	// Inserting alias if not present in subquery.
		        	
		        	query = genMVSubqueryAlias(schema, mview_name,query);
		        	
		        	
		        	sb.append("AS ("+ linesep + query + linesep + ")" + linesep);
		        	sb.append("DATA INITIALLY DEFERRED REFRESH IMMEDIATE" + linesep);
		        	sb.append("ENABLE QUERY OPTIMIZATION" + linesep);
		        	sb.append("MAINTAINED BY SYSTEM" + linesep + sqlTerminator + linesep + linesep);
		        	sb.append("REFRESH TABLE " + putQuote(schema) + "." + putQuote(mview_name) + linesep + sqlTerminator + linesep + linesep);
		        	sb.append("RUNSTATS ON TABLE " + putQuote(schema) + "." + putQuote(mview_name) + linesep); 
		        	sb.append("ON ALL COLUMNS AND DETAILED INDEXES ALL" + linesep + sqlTerminator + linesep + linesep);	        		
	        	}
	            if (objCount > 0 && objCount % 20 == 0)
	            	log(objCount + " # materialized views extracted for schema " + schema);
	            objCount++;
	            db2mviewsWriter.write(sb.toString());   
	        }
	        if (objCount > 0)
	    	   log(objCount + " Total # materialized views extracted for schema " + schema);
	        s.close(methodName); 
        } catch (Exception e)
        {
        	log("Error in getting materialized query table for schema " + schema + " SQL=" + sql);
        	e.printStackTrace();
        }    	
    }
    
    private String genMVSubqueryAlias(String schema, String mviewName,String query)
    {
    	String methodName = "genMVSubqueryAlias";
    	String sql, subSchema,subObjName,subObjAlias,tempsubObjAlias ;
    	String expr = "",exprQuotes = "";
    	StringBuffer tempQuery = new StringBuffer(query);
    	int index;

    	SQL s = new SQL(mainData.connection);

    	sql = "SELECT DETAILOBJ_OWNER, DETAILOBJ_NAME, DETAILOBJ_ALIAS FROM DBA_MVIEW_DETAIL_RELATIONS WHERE OWNER = '" + schema + "' AND MVIEW_NAME = '" + mviewName + "'";
    	try {
    		s.PrepareExecuteQuery(methodName, sql);

    		while (s.next())
    		{
    			subSchema = s.rs.getString(1);
    			subObjName = s.rs.getString(2);
    			subObjAlias = s.rs.getString(3);
    			expr = subSchema + "." + subObjName;
    			exprQuotes = putQuote(subSchema) + "." + putQuote(subObjName);
    			if(subObjAlias == null || subObjAlias.equals("") || subObjAlias.length() == 0 || subObjAlias.equals(null))
    			{	
    				
    				if(query.contains(expr))
    				{	
    					index =  query.indexOf(expr) + expr.length();
    					tempsubObjAlias = " " + subObjName;
    					tempQuery.insert(index, tempsubObjAlias);
    				}
    				else if(query.contains(exprQuotes))
    				{	
    					index =  query.indexOf(exprQuotes) + exprQuotes.length();
    					tempsubObjAlias = " " + putQuote(subObjName);
    					tempQuery.insert(index, tempsubObjAlias);
    				}
    			}
    			else if (subObjAlias.equalsIgnoreCase(subObjName))
    			{
    				
    				if(!query.contains(expr + " " + subObjAlias))
    				{	
    				index =  query.indexOf(expr) + expr.length();
    				tempsubObjAlias = " " + subObjAlias;
    				tempQuery.insert(index, tempsubObjAlias);
    				}
    				else if(!query.contains(exprQuotes + " " + putQuote(subObjAlias)))
    				{	
    					index =  query.indexOf(exprQuotes) + exprQuotes.length();
    					tempsubObjAlias = " " + putQuote(subObjAlias);
    					tempQuery.insert(index, tempsubObjAlias);
    				}

    			}
    		}
    	} catch (SQLException e) {

    		log("Error in getting table alias for MVIEW " + mviewName + " SQL=" + sql);
    		e.printStackTrace();
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	} 
    	s.close(methodName);

    	query = tempQuery.toString();
    	return query;
    }

    private void genDirectories()
    {
    	String methodName = "genDirectories";
    	String sql = "";
        int objCount = 0;
    	
        if (majorSourceDBVersion >= 9 && mainData.Oracle())
        	sql = "SELECT DIRECTORY_NAME, DIRECTORY_PATH  FROM dba_directories " +
        			"WHERE DIRECTORY_NAME NOT IN ('DATA_PUMP_DIR','ORACLECLRDIR') ";
        
        if (sql.equals("")) return;

        SQL s = new SQL(mainData.connection);

        try
        {
        	String directoryName, directoryPath;
        	BufferedWriter db2DirectoryWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OUTPUT_DIR + "db2directory.db2", false), sqlFileEncoding));
	        IBMExtractUtilities.putHelpInformation(db2DirectoryWriter, "db2directory.db2");
	        db2DirectoryWriter.write("--#SET TERMINATOR " + sqlTerminator + linesep);

            s.PrepareExecuteQuery(methodName, sql);        	

	        while (s.next()) 
	        {
	        	objCount++;
	        	directoryName = trim(s.rs.getString(1));
	        	directoryPath = trim(s.rs.getString(2));
	        	db2DirectoryWriter.write("--#SET :" + "DIRECTORY" + ":" + "NAME" + ":" + directoryName + linesep);           	
	        	db2DirectoryWriter.write("BEGIN" + linesep + "   UTL_DIR.CREATE_OR_REPLACE_DIRECTORY('"+directoryName+"', '"+directoryPath+"');" + 
	        			linesep + "END;" + linesep + sqlTerminator + linesep);   
	        }
	        s.close(methodName); 
	         db2DirectoryWriter.write(linesep);
	         db2DirectoryWriter.close();
        } catch (Exception e)
        {
        	e.printStackTrace();
        }
        if (objCount > 0)
	       log(objCount + " # directory names extracted");
    }
    
    private void genRoles()
    {
    	String methodName = "genRoles";
    	String privilege, onObject, objName, owner;
    	String sql = "" , ddlSQL = "",privSQL = "", roleName, outStr, grantee,grantable;
    	SQL s2 = new SQL(mainData.connection);
    	/*String templateSQL = "SELECT P.PRIVILEGE, DECODE(O.OBJECT_TYPE,'INDEX',' ON INDEX ','DIRECTORY',' ON DIRECTORY ','FUNCTION', " +
    			"' ON FUNCTION ','PROCEDURE',' ON PROCEDURE ','PACKAGE',  ' ON MODULE ','SEQUENCE', ' ON SEQUENCE ',' ON '), " +
    			"P.OWNER, P.TABLE_NAME, P.ROLE " +
    			"FROM   	ROLE_TAB_PRIVS P, DBA_OBJECTS O " +
    			"WHERE 	P.ROLE = 'ROLETEST' " +
    			"AND 	O.OBJECT_NAME = P.TABLE_NAME " +
    			"AND 	O.OWNER = P.OWNER " +
    			"AND    P.OWNER IN (" + schemaList + ") " + 
    			"AND   O.OBJECT_TYPE IN ('DATABASE LINK','FUNCTION','INDEX', " +
    			"'INDEX PARTITION','MATERIALIZED VIEW', " +
    			"'PACKAGE','PACKAGE BODY','PROCEDURE', " +
    			"'SEQUENCE','SYNONYM','TABLE','TABLE PARTITION', " +
    			"'TRIGGER','TYPE','TYPE BODY','DIRECTORY') " +
    			"AND  P.PRIVILEGE NOT LIKE ('%COMMIT REFRESH%') " +
    			"AND  P.PRIVILEGE NOT LIKE ('%DEBUG%') " +
    			"AND  P.PRIVILEGE NOT LIKE ('%QUERY REWRITE%') " +
    			"AND  P.PRIVILEGE NOT LIKE ('%FLASHBACK%') " +
    			"AND  P.PRIVILEGE NOT LIKE ('%MERGE VIEW%') " +
    			"ORDER BY P.ROLE, P.TABLE_NAME"; */
    	SQL s = new SQL(mainData.connection);

    	String exclRoleList =  "'AQ_ADMINISTRATOR_ROLE','AQ_USER_ROLE','AUTHENTICATEDUSER','CONNECT','CSW_USR_ROLE','CTXAPP','CWM_USER','DATAPUMP_EXP_FULL_DATABASE','DATAPUMP_IMP_FULL_DATABASE','DBA'" +
    	",'DELETE_CATALOG_ROLE','DMUSER_ROLE','DM_CATALOG_ROLE','EJBCLIENT','EXECUTE_CATALOG_ROLE','EXP_FULL_DATABASE','GATHER_SYSTEM_STATISTICS','GLOBAL_AQ_USER_ROLE','ADM_PARALLEL_EXECUTE_TASK'" +
    	",'IMP_FULL_DATABASE','JAVADEBUGPRIV','JAVAIDPRIV','JAVASYSPRIV','JAVAUSERPRIV','JAVA_ADMIN','JAVA_DEPLOY','JMXSERVER','LOGSTDBY_ADMINISTRATOR','MGMT_USER','OEM_ADVISOR'" +
    	",'OEM_MONITOR','OLAPI_TRACE_USER','OLAP_DBA','OLAP_USER','OLAP_XS_ADMIN','ORDADMIN','OWB$CLIENT','OWB_DESIGNCENTER_VIEW','OWB_USER','PLUSTRACE','RECOVERY_CATALOG_OWNER'" +
    	",'RESOURCE','SCHEDULER_ADMIN','SELECT_CATALOG_ROLE','SPATIAL_CSW_ADMIN','SPATIAL_WFS_ADMIN','WFS_USR_ROLE','WKUSER','WM_ADMIN_ROLE','XDBADMIN','XDB_SET_INVOKER','XDB_WEBSERVICES'" +
    	",'XDB_WEBSERVICES_OVER_HTTP','XDB_WEBSERVICES_WITH_PUBLIC','CONNECT', 'RESOURCE', 'DBA','HS_ADMIN_ROLE','HS_ADMIN_EXECUTE_ROLE','HS_ADMIN_SELECT_ROLE','DBFS_ROLE', 'XDBWEBSERVICES','APEX_ADMINISTRATOR_ROLE'";

    	String exclGranteeList = "'ANONYMOUS','BI','CTXSYS','DBSNMP','DIP','DMSYS','EXFSYS','FLOWS_020100','FLOWS_FILES','HR','IX','MDDATA','MDSYS','MGMT_VIEW','ODM','ODM_MTR','OE','XS$NULL'," +
    	"'OLAPSYS','ORDPLUGINS','ORDSYS','ORDDATA','OUTLN','PM','PUBLIC','RMAN','SCOTT','SH','SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','TSMSYS','WKSYS','WMSYS','XDB'," +
    	"'QS','QS_ADM','QS_WS','QS_ES','QS_OS','QS_CBADM','QS_CB','QS_CS', 'APPQOSSYS','ORACLE_OCM','OWBSYS','OWBSYS_AUDIT','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','APEX_040000','CMS_ADMIN','WKPROXY'";

    	if (mainData.Oracle())
    	{
    		/*sql = "SELECT ROLE FROM ROLE_TAB_PRIVS WHERE ROLE NOT IN ('IMP_FULL_DATABASE','EXECUTE_CATALOG_ROLE'," +
        			"'DELETE_CATALOG_ROLE','HS_ADMIN_ROLE','GATHER_SYSTEM_STATISTICS','SELECT_CATALOG_ROLE','PLUSTRACE'," +
        			"'DBA','XDBADMIN','EXP_FULL_DATABASE', 'WM_ADMIN_ROLE', 'AQ_ADMINISTRATOR_ROLE') GROUP BY ROLE"; */
    		// Replaced roles extraction query  from DBAnalysis script. 

    		sql =   "SELECT ROLE FROM DBA_ROLES " +
    		" WHERE ROLE NOT IN (" +exclRoleList +") " + 
    		" AND ROLE IN (SELECT  GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE NOT IN ("+exclGranteeList+"))";
    	}
    	if (sql.equals("")) return;

    	try
    	{
    		s.PrepareExecuteQuery(methodName, sql);        	

    		int objCount = 0;
    		while (s.next()) 
    		{
    			roleName = s.rs.getString(1);
    			
    			db2rolePrivsWriter.write("--#SET :" + "ROLE" + ":" + "ROLE" + ":" + roleName + linesep); 
    			db2DropObjectsWriter.write("--#SET :DROP:ROLE:" + roleName + linesep);
    			db2DropObjectsWriter.write("DROP ROLE " + roleName + ";" + linesep);           	
    			db2rolePrivsWriter.write("CREATE ROLE \"" + roleName + "\";" + linesep + linesep);   
    			/*  ddlSQL = templateSQL.replace("&roleName&", roleName);

	            s2.PrepareExecuteQuery(methodName, ddlSQL);   
	            while (s2.next())
	            {
		        	privilege = s2.rs.getString(1);
		        	onObject = s2.rs.getString(2);
		        	owner = getDstSchema(s2.rs.getString(3));
		        	objName = s2.rs.getString(4);
		        	roleName = s2.rs.getString(5);
	                if (objCount > 0 && objCount % 20 == 0)
	                	log(objCount + " # Roles extracted");
	                objCount++;
	            	db2rolePrivsWriter.write("GRANT "+privilege+" "+onObject+" "+putQuote(owner)+"."+putQuote(objName)+" TO ROLE "+putQuote(roleName)+";" + linesep);
	            }
	            s2.close(methodName); */
    			/*   ddlSQL = "SELECT USERNAME FROM USER_ROLE_PRIVS WHERE GRANTED_ROLE = '"+roleName+"'";
	            s2.PrepareExecuteQuery(methodName, ddlSQL); 
	            while (s2.next())
	            {
	            	outStr = getDstSchema(s2.rs.getString(1));
	            	db2rolePrivsWriter.write(linesep + "GRANT ROLE "+putQuote(roleName)+" TO USER "+putQuote(outStr)+";" + linesep + linesep);
	            	++objCount;
	            }
	            s2.close(methodName); */
    		}
    		ddlSQL = " SELECT GRANTED_ROLE, GRANTEE, decode(admin_option, 'YES', ' WITH ADMIN OPTION') " +
    				" FROM DBA_ROLE_PRIVS " +
    				" WHERE GRANTEE NOT IN ( "+exclGranteeList +" ) AND GRANTED_ROLE NOT IN ( " +exclRoleList+" )";
    		s2.PrepareExecuteQuery(methodName, ddlSQL); 
    		while (s2.next())
    		{
    			roleName = s2.rs.getString(1);
    			outStr = getDstSchema(s2.rs.getString(2));
    			grantable = s2.rs.getString(3);
    			if(grantable == null || grantable.equals("") || grantable.length() == 0 || grantable.equals(null))
    				grantable = "";
    			
    			
    			db2rolePrivsWriter.write("GRANT "+putQuote(roleName)+" TO "+putQuote(outStr)+ " " +grantable +" ;" + linesep);
    			++objCount;
    		}
    		s2.close(methodName);

    		privSQL = "SELECT PRIVILEGE, OWNER, TABLE_NAME, GRANTEE, decode(grantable, 'YES', ' WITH GRANT OPTION')" + 
    		" FROM DBA_TAB_PRIVS" +
    		" WHERE" +
    			" OWNER NOT IN ('SYS','SYSTEM','CTXSYS','MDSYS','OUTLN','DBSNMP','XDB') " +
    			" AND OWNER NOT LIKE 'FLOWS_%'"+
    			" AND OWNER NOT LIKE 'APEX_%'"+
    		" AND " +
    			"(" +
    				"(" +
    				" GRANTEE IN " +
    					"(" +
    						" SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE NOT IN " +
    						"(" 
    							+exclGranteeList+
    						")" +
    					") OR GRANTEE NOT IN " +
    						"("
    							+exclGranteeList+
    						")" +
    				")" +
    			")" +
    		"AND GRANTEE NOT IN " +
    			"(" 
    				+exclRoleList+
    			")"+
    		" ORDER BY OWNER, GRANTEE";
    		
    		s2.PrepareExecuteQuery(methodName, privSQL); 
    		while (s2.next())
    		{
    			privilege = s2.rs.getString(1);
    			owner = getDstSchema(s2.rs.getString(2));
    			onObject = s2.rs.getString(3);
    			grantee = s2.rs.getString(4);
    			grantable = s2.rs.getString(5);
    			if(grantable == null || grantable.equals("") || grantable.length() == 0 || grantable.equals(null))
    				grantable = "";
    			objCount++;
    			if (objCount > 0 && objCount % 20 == 0)
    				log(objCount + " # Roles extracted");
    			db2rolePrivsWriter.write("GRANT "+privilege+" ON "+putQuote(owner)+"."+putQuote(onObject)+" TO "+putQuote(grantee)+ " " +grantable +" ;" + linesep);
    		}
    		s2.close(methodName);
    		if (objCount > 0)
    			log(objCount + " # grants/roles extracted");
    		s.close(methodName);
    	} catch (Exception e)
    	{
    		log("Exception thrown in method " + methodName + " Error Message " + e.getMessage());
    		e.printStackTrace();
    	}
    }
    
    private void genAllSequences()
    {
	    try
	    {
	    	if (mainData.Oracle())
	    	   genOraSequences();
	    	else if (mainData.DB2())
	    	{
	    		genDB2Aliases();
	    	    genDB2Sequences();
	    	}
	    	else if (mainData.zDB2())
	    	{
	    		genDB2Aliases();
	    	    genDB2Sequences();
	    	}
		} catch (SQLException e)
		{
		    e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
    }
    
    private void openPLSQLFiles()
    {
    	String methodName = "openPLSQLFiles";
    	String plSQL = "", type, fileName = "", filName = "";
    	String countSQLStr = "";
        SQL s = new SQL(mainData.connection);

        if (mainData.Oracle())
        {
        	if (majorSourceDBVersion <= 8)
        	{
            	plSQL = "select distinct type from dba_source union all select 'TRIGGER' AS type from dual";
            	countSQLStr = "select sum(C1) from (select count(distinct type) as \"C1\" from dba_source union all select 1 as \"C1\" from dual)";        		
        	} else
        	{
            	plSQL = "select distinct type from dba_source";
            	countSQLStr = "select count(distinct type) from dba_source";        		
        	}
        } 
        else if (mainData.iDB2())
        {
        	plSQL = "SELECT 'PROCEDURE' FROM SYSIBM.SYSDUMMY1 UNION " +
        			"SELECT 'TRIGGER' FROM SYSIBM.SYSDUMMY1 UNION " +
        			"SELECT 'FUNCTION' FROM SYSIBM.SYSDUMMY1";
        	countSQLStr = "SELECT 3 FROM SYSIBM.SYSDUMMY1";        	
        }
        else if (mainData.zDB2())
        {
        	plSQL = "SELECT 'PROCEDURE' FROM SYSIBM.SYSDUMMY1 UNION " +
        			"SELECT 'TRIGGER' FROM SYSIBM.SYSDUMMY1 UNION " +
        			"SELECT 'FUNCTION' FROM SYSIBM.SYSDUMMY1";
        	countSQLStr = "SELECT 3 FROM SYSIBM.SYSDUMMY1";        	
        }
        else if (mainData.DB2())
        {
        	plSQL = "SELECT 'MODULE' FROM SYSIBM.SYSDUMMY1 UNION  " +
        	        "SELECT 'VARIABLE' FROM SYSIBM.SYSDUMMY1 UNION  " +
        	        "SELECT 'GRANTS' FROM SYSIBM.SYSDUMMY1 UNION  " +
        		    "SELECT 'JAVAJARS' FROM SYSIBM.SYSDUMMY1 UNION  " +
        			"SELECT 'XMLSCHEMA' FROM SYSIBM.SYSDUMMY1 UNION " +
        		    "SELECT 'TYPE' FROM SYSIBM.SYSDUMMY1 UNION " +
        		    "SELECT 'ROUTINE' FROM SYSIBM.SYSDUMMY1 UNION " + 
        			"SELECT 'PROCEDURE' FROM SYSIBM.SYSDUMMY1 UNION " +
        			"SELECT 'TRIGGER' FROM SYSIBM.SYSDUMMY1 UNION " +
        			"SELECT 'FUNCTION' FROM SYSIBM.SYSDUMMY1";
        	countSQLStr = "SELECT 10 FROM SYSIBM.SYSDUMMY1";        	
        }
        else if (mainData.Mysql())
        {
        	plSQL = "SELECT 'TRIGGER' FROM DUAL UNION " +
        			"SELECT 'FUNCTION' FROM DUAL";
        	countSQLStr = "SELECT 2 FROM DUAL";        	
        }
        else if (mainData.Sybase())
        {
        	plSQL = "SELECT 'PROCEDURE' UNION " +
        			"SELECT 'TRIGGER'";
        	countSQLStr = "SELECT 2";        	
        } /*else if (mainData.Mssql())
        {
        	plSQL = "SELECT 'VIEWS'";
	        countSQLStr = "SELECT 1";        	        	
        }*/
                
        if (plSQL.equals(""))
        	return;
        
        try
        {
            s.PrepareExecuteQuery(methodName, countSQLStr);        	
        	int n = 0;
        	if (s.next())
        	{
        		n = s.rs.getInt(1);
        	}      
            s.close(methodName); 
        	
        	db2ObjectsWriter = new BufferedWriter[n];
        	
            s.PrepareExecuteQuery(methodName, plSQL);        	
	        int i = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            type = s.rs.getString(1).trim();
		            type = type.replace(" ", "_").toLowerCase();
		            filName = "db2"+type+".db2";
		            fileName = OUTPUT_DIR + filName;
		            plsqlHashTable.put(type,new Integer(i));
		            db2ObjectsWriter[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, false), sqlFileEncoding));
	                if (zdb2 || db2)
	                {
			           if (db2Skin())
			           {
				           IBMExtractUtilities.putHelpInformation(db2ObjectsWriter[i], "sybase");
		                   db2ObjectsWriter[i].write(sybConnectStr);			        	   
			           } else
			           {
				           IBMExtractUtilities.putHelpInformation(db2ObjectsWriter[i], filName);
		                   db2ObjectsWriter[i].write("--#SET TERMINATOR " + sqlTerminator + linesep);
			           }
	                }
	                ++i;
	        	} catch (IOException e)
	        	{
	        		log("Error creating file " + fileName + ":" + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting openPLSQLFiles " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }        	
	        s.close(methodName); 
        }
        catch (SQLException e)
        {
        	e.printStackTrace();
        }    
	}
    
    private void regenerateTriggers()
    {
    	String fileName = "db2trigger.db2";
    	String inFileName = OUTPUT_DIR + fileName;
    	String outFileName;
    	try
		{    		
    		if (IBMExtractUtilities.FileExists(inFileName))
    		{
		   		outFileName =  OUTPUT_DIR + fileName.substring(0, fileName.lastIndexOf('.')) + "_Original" + fileName.substring(fileName.lastIndexOf('.'));
				IBMExtractUtilities.copyFile(new File(inFileName), new File(outFileName));
				String text = OraToDb2Converter.getSplitTriggers(IBMExtractUtilities.FileContents(outFileName));
				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(inFileName, false), sqlFileEncoding));
				//BufferedWriter out =  new BufferedWriter(new FileWriter(inFileName, false));
		        out.write(text);
		        out.close();
		        log("File " + inFileName + " saved as " + outFileName);
    		}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
    }
    
    private void fixiDB2Code(String fileName)
    {
	   	String inFileName = OUTPUT_DIR + fileName;
	   	String outFileName = OUTPUT_DIR;	   	
	   	try			
	   	{    	
    		if (IBMExtractUtilities.FileExists(inFileName))
    		{
		   		outFileName =  OUTPUT_DIR + fileName.substring(0, fileName.lastIndexOf('.')) + "_Original" + fileName.substring(fileName.lastIndexOf('.'));
				IBMExtractUtilities.copyFile(new File(inFileName), new File(outFileName));
				String text = OraToDb2Converter.fixiDB2Procedures(IBMExtractUtilities.FileContents(outFileName));
				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(inFileName, false), sqlFileEncoding));
				//BufferedWriter out =  new BufferedWriter(new FileWriter(inFileName, false));
		        out.write(text);
		        out.close();
		        log("File " + inFileName + " saved as " + outFileName);
    		}
		} catch (IOException e)
		{
			e.printStackTrace();
		}    	
    }
    
    private void closePLSQLFiles()
    {
    	int len = (db2ObjectsWriter == null) ? 0 : db2ObjectsWriter.length;
    	for (int i = 0; i < len; ++i)
    	{
			try
			{
				db2ObjectsWriter[i].close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
    	}
    }
    
    private void getPLSQLSource(String schema, String type, String objectName, StringBuffer buffer)
    {
    	String methodName = "getPLSQLSource";
        SQL s = new SQL(mainData.connection);
        String dstSchema = getDstSchema(schema);
    	String plSQL = "select text from dba_source where owner = '"+dstSchema+"' " +
    			"and name = '"+objectName+"' and type = '"+type+"' order by line asc";
        try
        {
            s.PrepareExecuteQuery(methodName, plSQL);     
	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            if (objCount ==  0)
		            {
		            	buffer.append("CREATE OR REPLACE " + s.rs.getString(1));
		            	objCount++;
		            } else
		               buffer.append(s.rs.getString(1));
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting PL/SQL " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }   
    }
    
    private boolean invalidOracleObject(String owner, String objectType, String objectName)
    {
    	String sql = "SELECT 'X' FROM DBA_OBJECTS " +
    			     "WHERE OWNER = '" + owner + "' " +
    			     "AND OBJECT_TYPE = '" + objectType + "' " +
    			     "AND OBJECT_NAME = '" + objectName + "' " +
    			     "AND STATUS = 'INVALID'";
    	String found = executeSQL(sql, false);
    	return (found.equals("X")) ? true : false;
    }
    
    private void genPLSQL(String schema)
    {
    	String methodName = "genPLSQL";
    	// select type, name from dba_source where owner = 'TESTCASE' group by type, name;
    	String plSQL = "", plsqlTemplate = "select dbms_metadata.get_ddl('&type&','&name&','&schema&') from dual", type, origType, name, ddlSQL;
    	SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        StringBuffer chunks = new StringBuffer();
        String newType, dstSchema = getDstSchema(schema);

        plSQL = "select type, name from dba_source where owner = '"+schema+"' group by type, name";
        
        if (plSQL.equals("")) return;
        
        try
        {
            s.PrepareExecuteQuery(methodName, plSQL);        
	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            buffer.setLength(0);
		            chunks.setLength(0);
		            origType = s.rs.getString(1);
		            name = s.rs.getString(2);
		            type = origType.replace(" ", "_");
		            newType = type;
		            if (type.equals("PACKAGE"))
		            	newType = "PACKAGE_SPEC";
		        	
		        	buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep);            	
		        	buffer.append(sqlTerminator + linesep);   
				    buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep);            	
			        buffer.append(sqlTerminator + linesep);            	

		            buffer.append("--#SET :" + type + ":" + dstSchema + ":" + name + linesep);
		            if (invalidOracleObject(schema, type, name))
		            	buffer.append("--#WARNING :" + type + " : " + schema + "." + name + " is found to be invalid in source database" + linesep);
		            if (!(type.equalsIgnoreCase("trigger") || type.equalsIgnoreCase("package_body")))
		            {
		                db2DropObjectsWriter.write("--#SET :DROP:" + origType + ":" + name + linesep);		            	
		                db2DropObjectsWriter.write("DROP " + origType + " " + putQuote(dstSchema) + "." + putQuote(name) + ";" + linesep);
		            }
		        	ddlSQL = plsqlTemplate.replace("&schema&", schema);
		            ddlSQL = ddlSQL.replace("&name&", name);
		            ddlSQL = ddlSQL.replace("&type&", newType);
		            
		        	try
		        	{
		        		SQL s2 = new SQL(mainData.connection);
		        		s2.PrepareExecuteQuery(methodName, ddlSQL);   
			            if (s2.next())
			            {
			            	IBMExtractUtilities.getStringChunks(s2.rs, 1, chunks);
			            	if (chunks.length() == 0)
			            		getPLSQLSource(schema, origType, name, chunks);
			            }
			            s2.close(methodName);
		        	} catch (Exception ex)
		        	{
		        		if (chunks.length() == 0)
		        		    getPLSQLSource(schema, origType, name, chunks);
		        	}
		            if (chunks.length() > 0)
		            {
                        buffer.append(OraToDb2Converter.getDb2PlSql(chunks.toString()));  
		            	buffer.append(linesep + sqlTerminator + linesep);            	
		            }
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " # PL/SQL objects extracted for schema " + schema);
		            objCount++;
		            db2ObjectsWriter[(Integer) plsqlHashTable.get((String) type.toLowerCase())].write(buffer.toString());
	        	} catch (IOException e)
	        	{
	        		log("Error writing PL/SQL in file " + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting PL/SQL " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " Total # PL/SQL objects extracted for schema " + schema);
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }
    }

    private void BuildRoutineOptsMap()
    {
    	String methodName = "BuildRoutineOptsMap";
    	routineOptsMap = new HashMap();
    	String key, schema, specificname, sql = "";
    	    	
        if (mainData.DB2())
        	sql = "SELECT routineschema, specificname, isolation, blocking, insert_buf, " +
				"reoptvar, queryopt, sqlmathwarn, " +
				"degree, intra_parallel, refreshage " +
				"FROM SYSCAT.PACKAGES, SYSCAT.ROUTINEDEP " +
				"WHERE pkgschema = bschema " +
				"AND pkgname = bname " +
				"AND btype = 'K' " +
				"AND routineschema IN ("+schemaList+") " +
				"ORDER BY routineschema, specificname";
           
    	
    	if (sql.length() == 0)
    		return;
    	
    	SQL s = new SQL(mainData.connection);
    	try
    	{
	        s.PrepareExecuteQuery(methodName, sql);
	        int i = 0;
	        while (s.next())
	        {
	        	schema = trim(s.rs.getString(1));
	        	specificname = trim(s.rs.getString(2));
				key = schema + "." + specificname;
				routineOptsMap.put(key, new RoutineOptData(s.rs));
			    ++i;
	        }
	        log(i + " values cached in routineOptsMap");
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
        s.close(methodName); 
    }
    
    private String getRoutineOpts(String schema, String specificName)
    {
    	String key = schema + "." +  specificName;
    	StringBuffer buffer = new StringBuffer();
    	
    	if (routineOptsMap.containsKey(key))
    	{
    		RoutineOptData o = (RoutineOptData) routineOptsMap.get(key);
        	String blocking = o.blocking;
        	if (blocking.equalsIgnoreCase("N"))
        		buffer.append("BLOCKING NO");
        	else if (blocking.equalsIgnoreCase("U"))
        		buffer.append("BLOCKING UNAMBIG");
        	else if (blocking.equalsIgnoreCase("B"))
        		buffer.append("BLOCKING ALL");	        			
        	buffer.append(" DEGREE " + o.degree);
        	String insert = o.insert_buf;
        	if (insert.equalsIgnoreCase("Y"))
        		buffer.append(" INSERT BUF ");
        	else if (insert.equalsIgnoreCase("N"))
        		buffer.append(" INSERT DEF ");
        	buffer.append(" ISOLATION " + o.isolation);
        	buffer.append(" QUERYOPT " + o.queryopt);
        	String reoptVar = o.reoptvar;
        	if (reoptVar.equalsIgnoreCase("N"))
        		buffer.append(" REOPT NONE ");
        	else if (reoptVar.equalsIgnoreCase("A"))
        		buffer.append(" REOPT ALWAYS ");
        	else if (reoptVar.equalsIgnoreCase("O"))
        		buffer.append(" REOPT ONCE ");
        }
	    return buffer.toString();
    }
    
    private String genTypeCols(int code, String schema, String typeModule, String typeName)
    {
    	String methodName = "genTypeCols";
    	String sql = "";
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();

        if (mainData.DB2())
        {
        	if (majorSourceDBVersion >= 9 && minorSourceDBVersion >= 7)
        	{
        		if (code == 1)
        		{
        		   sql = "SELECT FIELDNAME, FIELDTYPESCHEMA, FIELDTYPEMODULENAME, FIELDTYPENAME, " +
        				"LENGTH, SCALE, CODEPAGE " +
        				"FROM SYSCAT.ROWFIELDS " +
        				"WHERE TYPENAME = '"+typeName+"' " +
        				"AND TYPEMODULENAME ='"+typeModule+"' " +
        				"AND TYPESCHEMA='"+schema+"' " +
        				"ORDER BY ORDINAL";
        		} else if (code == 2)
        		{
        			sql = "SELECT FIELDNAME, FIELDTYPESCHEMA, FIELDTYPEMODULENAME, FIELDTYPENAME, " +
        					"LENGTH, SCALE, CODEPAGE " +
        					"FROM SYSCAT.ROWFIELDS " +
        					"WHERE TYPESCHEMA ='"+schema+"' " +
        					"AND TYPEMODULENAME IS NULL " +
        					"AND TYPENAME = '"+typeName+"' " +
        					"ORDER BY ORDINAL";
        		} else
        		{
        			// code = 3
        			sql = "SELECT ATTR_NAME, ATTR_TYPESCHEMA, TYPEMODULENAME, ATTR_TYPENAME , " +
        					"LENGTH, SCALE, CODEPAGE, " +
        					"TARGET_TYPESCHEMA,TARGET_TYPENAME,LOGGED,COMPACT " +
        					"FROM SYSCAT.ATTRIBUTES " +
        					"WHERE TYPENAME = SOURCE_TYPENAME " +
        					"AND TYPESCHEMA = SOURCE_TYPESCHEMA " +
        					"AND TYPESCHEMA='"+schema+"' " +
        					"AND TYPENAME='"+typeName+"' " +
        					"ORDER BY ORDINAL";
        		}
        	}
        }
        
        if (sql.equals("")) return "";
        try
        {
        	s.PrepareExecuteQuery(methodName, sql);        	

	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            String fieldName = trim(s.rs.getString(1));
		            String fieldtypeSchema = trim(s.rs.getString(2));
		            String fieldTypeName = trim(s.rs.getString(4));
		            int length = s.rs.getInt(5);
		            int scale = s.rs.getInt(6);
		            int codePage = s.rs.getInt(7);
		            String dataType = composeType(fieldtypeSchema, fieldTypeName, length, scale, codePage, schema);
		            if (objCount == 0)
		            {
		            	buffer.append(putQuote(fieldName) + " " + dataType);
		            } else
		            {
		            	buffer.append("," + linesep + putQuote(fieldName) + " " + dataType);
		            }
		            objCount++;
	        	}
	        	catch (SQLException ex)
	        	{
	        		log("Error getting field names for type " + ex.getMessage() + sql);
	        		ex.printStackTrace();
	        	}
	        }
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
	    	log("Error getting field names for type " + sql);
        	e.printStackTrace();
        }    	        	
        return (buffer.length() > 0) ? linesep + buffer.toString() + linesep : "";
    }

    private void genSQLModules(String schema)
    {
    	String methodName = "genSQLModules";
    	String[] modules;
    	String sql = "", moduleList = "";
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        StringBuffer chunks = new StringBuffer();
        String dstSchema = getDstSchema(schema);
        String module;

        if (mainData.DB2())
        {
        	if (majorSourceDBVersion >= 9 && minorSourceDBVersion >= 7)
        	{
        		moduleList = executeSQL("SELECT MODULENAME FROM SYSCAT.MODULES WHERE MODULESCHEMA ='"+schema+"' AND DIALECT = 'DB2 SQL PL'", false);
        	}
        }
        
        if (moduleList.equals("")) return;
        
        modules = moduleList.split("~");
        for (int i = 0; i < modules.length; ++i)
        {
        	buffer.setLength(0);
        	module = modules[i];
        	buffer.append("CREATE MODULE " + putQuote(dstSchema) + "." + putQuote(modules[i]) + linesep + sqlTerminator + linesep);
        	// Extract all conditions for the given module
            try
            {
            	sql = "SELECT DISTINCT B.CONDNAME,B.SQLSTATE,B.REMARKS, A.PUBLISHED " +
            			"FROM SYSCAT.MODULEOBJECTS A, SYSCAT.CONDITIONS B " +
            			"WHERE A.OBJECTTYPE ='CONDITION' " +
            			"AND A.OBJECTNAME= B.CONDNAME " +
            			"AND A.OBJECTMODULENAME= B.CONDMODULENAME " +
            			"AND A.OBJECTSCHEMA= B.CONDSCHEMA " +
            			"AND A.OBJECTSCHEMA='"+schema+"' " +
            			"AND A.OBJECTMODULENAME ='"+module+"' ";  
            	s.PrepareExecuteQuery(methodName, sql);        	

    	        int objCount = 0;
    	        while (s.next()) 
    	        {
    	        	try
    	        	{
    		            chunks.setLength(0);
    		            String condName = trim(s.rs.getString(1));
    		            String sqlState = trim(s.rs.getString(2));
    		            String published = trim(s.rs.getString(4));
    		            db2DropObjectsWriter.write("--#SET :DROP:CONDITION:"+condName + linesep);
    		            db2DropObjectsWriter.write("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
    		            db2DropObjectsWriter.write("DROP CONDITION " + putQuote(condName) + linesep + sqlTerminator + linesep);
    		            buffer.append("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
    		            published = (published != null && published.equals("Y")) ? "PUBLISH" : "ADD";
    		            buffer.append(published + " CONDITION " + putQuote(condName) + " FOR SQLSTATE '" + sqlState + "'" + linesep + sqlTerminator + linesep);
			            db2ObjectsWriter[(Integer) plsqlHashTable.get("MODULE".toLowerCase())].write(buffer.toString());
    		            if (objCount > 0 && objCount % 20 == 0)
    		            	log(objCount + " # SQL Module conditions extracted for module " + schema+"."+module);
    		            objCount++;
    	        	} catch (IOException e)
    	        	{
    	        		log("Error writing DB2 SQL Module conditions in file " + e.getMessage() + sql);
    	        	} catch (SQLException ex)
    	        	{
    	        		log("Error getting DB2 SQL Modules Conditions " + ex.getMessage() + sql);
    	        		ex.printStackTrace();
    	        	}
    	        }
    	        if (objCount > 0)
            	   log(objCount + " Total # DB2 SQL Module Conditions extracted for module " + schema+"."+module);
    	        s.close(methodName); 
            } 
    	    catch (SQLException e)
            {
    	    	log("Error in extracting DB2 SQL Modules " + sql);
            	e.printStackTrace();
            }    	        	
        	// Extract functions and procedures for the given module
            try
            {
            	sql = "SELECT B.ROUTINENAME, B.ROUTINETYPE,B.SPECIFICNAME,B.LANGUAGE,A.PUBLISHED,B.TEXT " +
            			"FROM SYSCAT.MODULEOBJECTS A, SYSCAT.ROUTINES B " +
            			"WHERE A.OBJECTTYPE IN ('PROCEDURE','FUNCTION') " +
            			"AND A.OBJECTMODULENAME= B.ROUTINEMODULENAME  " +
            			"AND A.OBJECTSCHEMA= B.ROUTINESCHEMA " +
            			"AND A.SPECIFICNAME= B.SPECIFICNAME " +
            			"AND A.OBJECTNAME= B.ROUTINENAME " +
            			"AND A.OBJECTSCHEMA='"+schema+"' " +
            			"AND A.OBJECTMODULENAME ='"+module+"' " +
            			"AND ORIGIN <> 'S'";            		
            	s.PrepareExecuteQuery(methodName, sql);        	

    	        int objCount = 0;
    	        while (s.next()) 
    	        {
    	        	try
    	        	{
    	            	buffer.setLength(0);
    		            chunks.setLength(0);
    		            String routineType = trim(s.rs.getString(2));
    		            routineType = (routineType.equals("P")) ? "PROCEDURE" : "FUNCTION";
    		            String specificName = trim(s.rs.getString(3));
    		            String language = trim(s.rs.getString(4));
    		            if (!language.equals("SQL"))
    		            {
    		            	buffer.append("-- Extraction for NON-SQL Procedure from modules is not yet supported. Please report this issue.");
    		            }
    		            IBMExtractUtilities.getStringChunks(s.rs, 6, chunks);
    		            db2DropObjectsWriter.write("--#SET :DROP:"+routineType+":"+specificName + linesep);    		            
    		            db2DropObjectsWriter.write("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
    		            db2DropObjectsWriter.write("DROP SPECIFIC "+routineType+" " + putQuote(specificName) + linesep + sqlTerminator + linesep);
    		            buffer.append(chunks + linesep + sqlTerminator + linesep);
			            db2ObjectsWriter[(Integer) plsqlHashTable.get("MODULE".toLowerCase())].write(buffer.toString());
    		            if (objCount > 0 && objCount % 20 == 0)
    		            	log(objCount + " # SQL Module Procedures / Functions extracted for module " + schema+"."+module);
    		            objCount++;
    	        	} catch (IOException e)
    	        	{
    	        		log("Error writing DB2 SQL Module Procedures / Functions  in file " + e.getMessage() + sql);
    	        	} catch (SQLException ex)
    	        	{
    	        		log("Error getting DB2 SQL Modules Procedures / Functions  " + ex.getMessage() + sql);
    	        		ex.printStackTrace();
    	        	}
    	        }
    	        if (objCount > 0)
            	   log(objCount + " Total # DB2 SQL Module Procedures / Functions  extracted for module " + schema+"."+module);
    	        s.close(methodName); 
            } 
    	    catch (SQLException e)
            {
    	    	log("Error in extracting DB2 SQL Modules Procedures / Functions " + sql);
            	e.printStackTrace();
            }    	        	
        	// Extract Types for the given module
            try
            {
            	sql = "SELECT B.TYPENAME, B.METATYPE, B.SOURCESCHEMA, B.SOURCEMODULENAME,B.SOURCENAME, " +
            			"B.LENGTH,B.SCALE, B.CODEPAGE, B.INSTANTIABLE, B.FINAL, B.ARRAY_LENGTH, A.PUBLISHED " +
            			"FROM SYSCAT.MODULEOBJECTS A, SYSCAT.DATATYPES B " +
            			"WHERE A.OBJECTTYPE ='TYPE' " +
            			"AND A.OBJECTMODULENAME= B.TYPEMODULENAME " +
            			"AND A.OBJECTSCHEMA= B.TYPESCHEMA " +
            			"AND A.OBJECTNAME = B.TYPENAME " +
            			"AND A.OBJECTSCHEMA='"+schema+"' " +
            			"AND A.OBJECTMODULENAME ='"+module+"' ";
            	s.PrepareExecuteQuery(methodName, sql);        	

    	        int objCount = 0;
    	        while (s.next()) 
    	        {
    	        	try
    	        	{
    	            	buffer.setLength(0);
    		            String typeName = trim(s.rs.getString(1));
    		            String metaType = trim(s.rs.getString(2));
    		            String sourceSchema = trim(s.rs.getString(3));
    		            String sourceName = trim(s.rs.getString(5));
    		            int length = s.rs.getInt(6);
    		            int scale = s.rs.getInt(7);
    		            int codePage = s.rs.getInt(8);
    		            int arrayLength = s.rs.getInt(11);
    		            String colType = "";
    		            String published = trim(s.rs.getString(12));
    		            published = (published.equals("Y")) ? "PUBLISH" : "ADD";
    		            db2DropObjectsWriter.write("--#SET :DROP:TYPE:"+typeName + linesep);    		            
    		            db2DropObjectsWriter.write("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
    		            db2DropObjectsWriter.write("DROP TYPE "+ putQuote(typeName) + linesep + sqlTerminator + linesep);
    		            buffer.append("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
    		            if (metaType.equals("F"))
    		            {
    		            	// Type = F - Row data type
    		            	colType = genTypeCols(1, schema, module, typeName);
    		                buffer.append(published + " TYPE " + putQuote(typeName) + " AS ROW (" + colType + ")");
    		            }
    		            else if (metaType.equals("A"))
    		            {
    		            	// Type = A - Array Type
    		            	colType = composeType(sourceSchema, sourceName, length, scale, codePage, schema);
     		                buffer.append(published + " TYPE " + putQuote(typeName) + " AS " + colType + " ARRAY [" + arrayLength + "]");
    		            } 
    		            else if (metaType.equals("T"))
    		            {
    		            	// Type = T -  User defined type
    		            	colType = composeType(sourceSchema, sourceName, length, scale, codePage, schema);
    			            String comp = "";
    			            if (sourceSchema.equalsIgnoreCase("SYSIBM") && 
    			               (!(sourceName.equals("BLOB") || sourceName.equals("CLOB") || sourceName.equals("DBCLOB"))))
    			            {
    			            	comp = " WITH COMPARISONS ";
    			            }
     		                buffer.append(published + " TYPE " + putQuote(typeName) + " AS " + colType + comp);    		            	
    		            } else
    		            {
    		            	// Type = C - Cursor Type
    		            	String sql2 = "SELECT  distinct BSCHEMA||'~'||NVL(BMODULENAME,'NULL')||'~'|| " +
    		            			"BNAME||'~'||NVL(T.TYPESCHEMA,'NULL')||'~'||NVL(T.TYPEMODULENAME,'NULL') " +
    		            			" FROM SYSCAT.DATATYPEDEP D  LEFT OUTER JOIN SYSCAT.DATATYPES T " +
    		            			" ON  NVL(D.BMODULENAME,'')  = NVL(T.TYPEMODULENAME,'') " +
    		            			" AND NVL (D.BMODULEID,0) = NVL(T.TYPEMODULEID,0) " +
    		            			" AND D.BNAME = T.TYPENAME " +
    		            			" AND D.BSCHEMA = T.TYPESCHEMA " +
    		            			" WHERE D.TYPESCHEMA = '"+schema+"' " +
    		            			" AND D.TYPEMODULENAME ='"+module+"' " +
    		            			" AND D.TYPENAME = '"+typeName+"' " +
    		            			" AND D.BTYPE ='R'";
    		            	String curType = executeSQL(sql2, false);
    		            	String types[] = curType.split("~");
    		            	if (types[0].equals(types[3]) && types[1].equals(types[4]))
    		            	   colType = putQuote(types[1]) + "." + putQuote(types[2]);
    		            	else
    		            	   colType = putQuote(types[3]) + "." + putQuote(types[4]) + "." + putQuote(types[2]);
     		                buffer.append(published + " TYPE " + putQuote(typeName) + " AS " + colType);
    		            }
    		            buffer.append(linesep + sqlTerminator + linesep);
			            db2ObjectsWriter[(Integer) plsqlHashTable.get("MODULE".toLowerCase())].write(buffer.toString());
    		            if (objCount > 0 && objCount % 20 == 0)
    		            	log(objCount + " # SQL Module Types extracted for module " + schema+"."+module);
    		            objCount++;
    	        	} catch (IOException e)
    	        	{
    	        		log("Error writing DB2 SQL Module Types  in file " + e.getMessage() + sql);
    	        	} catch (SQLException ex)
    	        	{
    	        		log("Error getting DB2 SQL Modules Types  " + ex.getMessage() + sql);
    	        		ex.printStackTrace();
    	        	}
    	        }
    	        if (objCount > 0)
            	   log(objCount + " Total # DB2 SQL Module Types  extracted for module " + schema+"."+module);
    	        s.close(methodName); 
            } 
    	    catch (SQLException e)
            {
    	    	log("Error in extracting DB2 SQL Modules Types " + sql);
            	e.printStackTrace();
            }    	        	
        	// Extract Variables for the given module
            try
            {
            	sql = "SELECT A.PUBLISHED,VARNAME,TYPESCHEMA,TYPEMODULENAME,TYPENAME,LENGTH,SCALE,CODEPAGE, " +
            			"READONLY,REMARKS, DEFAULT " +
            			"FROM SYSCAT.MODULEOBJECTS A, SYSCAT.VARIABLES B " +
            			"WHERE A.OBJECTTYPE ='VARIABLE' " +
            			"AND A.OBJECTMODULENAME= B.VARMODULENAME " +
            			"AND A.OBJECTSCHEMA= B.VARSCHEMA " +
            			"AND A.OBJECTNAME = B.VARNAME " +
            			"AND A.OBJECTSCHEMA='TESTCASE' " +
            			"AND A.OBJECTMODULENAME ='CLASSES' ";   
            	s.PrepareExecuteQuery(methodName, sql);   
    	        int objCount = 0;
    	        while (s.next()) 
    	        {
    	        	try
    	        	{
    	            	buffer.setLength(0);
    		            String published = trim(s.rs.getString(1));
    		            published = (published.equals("Y")) ? "PUBLISH" : "ADD";
    		        	String varName = trim(s.rs.getString(2));
    		        	String typeSchema = trim(s.rs.getString(3));
    		        	String typeName = trim(s.rs.getString(5));
    		        	int length = s.rs.getInt(6);
    		        	int scale = s.rs.getInt(7);
    		        	int codePage = s.rs.getInt(8);
    		        	String readOnly = trim(s.rs.getString(9));
    		        	String defaultValue = trim(s.rs.getString(11));
    		        	if (defaultValue == null)
    		        		defaultValue = "";
    		            db2DropObjectsWriter.write("--#SET :DROP:VARIABLE:"+varName + linesep);    		            
    		            db2DropObjectsWriter.write("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
    		        	db2DropObjectsWriter.write("DROP VARIABLE " + putQuote(varName) + ";" + linesep);
    		        	buffer.append("ALTER MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
		        		String varType = composeType(typeSchema, typeName, length, scale, codePage, schema);	        		
		        		if (readOnly.equals("C") && defaultValue.length() > 0)
		        			buffer.append(published + " VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + " " + varType + " CONSTANT " + defaultValue);
		        		else if (readOnly.equals("N") && defaultValue.length() > 0)	        			
		        			buffer.append(published + " VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + " " + varType + " DEFAULT " + defaultValue);
		        		else
		        			buffer.append(published + " VARIABLE " + putQuote(dstSchema) + "." + putQuote(varName) + " " + varType);
		        		buffer.append(linesep + sqlTerminator + linesep);
    		            if (objCount > 0 && objCount % 20 == 0)
    		            	log(objCount + " # SQL Module Variables extracted for module " + schema+"."+module);
    		            objCount++;
			            db2ObjectsWriter[(Integer) plsqlHashTable.get("MODULE".toLowerCase())].write(buffer.toString());
    		            if (objCount > 0 && objCount % 20 == 0)
    		            	log(objCount + " # SQL Module Variables extracted for module " + schema+"."+module);
    		            objCount++;
    	        	} catch (IOException e)
    	        	{
    	        		log("Error writing DB2 SQL Module Variables  in file " + e.getMessage() + sql);
    	        	} catch (SQLException ex)
    	        	{
    	        		log("Error getting DB2 SQL Modules Variables  " + ex.getMessage() + sql);
    	        		ex.printStackTrace();
    	        	}
    	        }
    	        if (objCount > 0)
            	   log(objCount + " Total # DB2 SQL Module Variables extracted for module " + schema+"."+module);
    	        s.close(methodName); 
	            try
				{
		            db2DropObjectsWriter.write("--#SET :DROP:MODULE:"+module + linesep);    		            
					db2DropObjectsWriter.write("DROP MODULE " + putQuote(dstSchema) + "." + putQuote(module) + linesep);
	        		String comment = executeSQL("SELECT REMARKS FROM SYSCAT.MODULES WHERE MODULESCHEMA ='"+schema+"' " +
	        				"AND DIALECT = 'DB2 SQL PL' AND MODULENAME = '"+module+"'", false);
	        		if (comment != null && comment.length() > 0)
	        		{
	        			buffer.setLength(0);
		        		if (comment.length() > 255)
		        			comment = comment.substring(1,255);
		        		comment = comment.replace("'", "''");
		        		buffer.append("COMMENT ON MODULE " + putQuote(dstSchema) + "." + putQuote(module) + " IS '" + comment + "'" + linesep + sqlTerminator + linesep);
			            db2ObjectsWriter[(Integer) plsqlHashTable.get("MODULE".toLowerCase())].write(buffer.toString());
	        		}
				} catch (IOException e)
				{
					e.printStackTrace();
				}
            } 
    	    catch (SQLException e)
            {
    	    	log("Error in extracting DB2 SQL Modules Variable " + sql);
            	e.printStackTrace();
            }    	        	
        }
    }
    
    private void genSQLPL(String schema)
    {
    	String methodName = "genSQLPL";
    	String sqlPL = "", specificSchema, specificName, routineType = "";
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        StringBuffer chunks = new StringBuffer();
        String dstSchema = getDstSchema(schema);

        if (mainData.iDB2())
        {
            sqlPL = "select specific_schema, specific_name " +
        		"from sysibm.routines " +
        		"where routine_body = 'SQL' " +
        		"and routine_type = 'PROCEDURE' " +
        		"and routine_schema = '"+schema+"'";
        } else if (mainData.DB2())
        {
            sqlPL = "select routineschema, specificname, " +
            		"text, case when routinetype = 'P' then 'PROCEDURE' when routinetype = 'F' then 'FUNCTION' end routinetype " +             		 
        		"from syscat.routines " +
        		"where routinetype in ('F','P') " +
        		"and language = 'SQL' " +
        		"and text is not null " +
        		"and routineschema = '"+schema+"' " +
        		((majorSourceDBVersion >= 9 && minorSourceDBVersion >= 7) ?
        		"union all " +
        		"select MODULESCHEMA routineschema, MODULENAME specificname, " +
        		"SOURCEHEADER text, case when moduletype = 'P' then 'PACKAGE' end routinetype " +
        		"from SYSIBM.SYSMODULES " +
        		"where moduletype = 'P' " +
        		"and MODULESCHEMA='"+schema+"' " +
        		"union all " +
        		"select MODULESCHEMA routineschema, MODULENAME specificname, " +
        		"SOURCEBODY text, case when moduletype = 'P' then 'PACKAGE BODY' end routinetype " +
        		"from SYSIBM.SYSMODULES " +
        		"where moduletype = 'P' " +
        		"and MODULESCHEMA='"+schema+"' "
        		: "");
        }
        
        if (sqlPL.equals("")) return;
        
        try
        {
        	s.PrepareExecuteQuery(methodName, sqlPL);        	

	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            buffer.setLength(0);
		            chunks.setLength(0);
		            specificSchema = s.rs.getString(1);
		            specificName = s.rs.getString(2);
		            db2DropObjectsWriter.write("--#SET :DROP:PROCEDURE:"+specificName + linesep);    		            
		            db2DropObjectsWriter.write("DROP SPECIFIC PROCEDURE " + putQuote(dstSchema) + "." + putQuote(specificName) + ";" + linesep);
		            if (mainData.DB2())
		            {
		            	IBMExtractUtilities.getStringChunks(s.rs, 3, chunks);
		            	routineType = trim(s.rs.getString(4));
		  	            buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep + sqlTerminator + linesep);            	
			            buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep + sqlTerminator + linesep); 
			            String preopts = getRoutineOpts(specificSchema, specificName);
			            if (preopts.length() > 0)
			               buffer.append("CALL SET_ROUTINE_OPTS('"+trim(preopts)+"')" + linesep + sqlTerminator + linesep);
			            if (routineType.equalsIgnoreCase("FUNCTION"))
			               buffer.append("--#SET :FUNCTION:" + dstSchema + ":" + specificName + linesep);
			            else if (routineType.equalsIgnoreCase("PROCEDURE"))
			               buffer.append("--#SET :PROCEDURE:" + dstSchema + ":" + specificName + linesep);
			            else if (routineType.equalsIgnoreCase("PACKAGE"))
				           buffer.append("--#SET :PACKAGE:" + dstSchema + ":" + specificName + linesep);
			            else
			               buffer.append("--#SET :PACKAGE BODY:" + dstSchema + ":" + specificName + linesep);
			            buffer.append(chunks);
			            buffer.append(linesep + sqlTerminator + linesep);
			            db2ObjectsWriter[(Integer) plsqlHashTable.get("PROCEDURE".toLowerCase())].write(buffer.toString());
		            } else
		               db2ObjectsWriter[(Integer) plsqlHashTable.get("PROCEDURE".toLowerCase())].write(getProcedureSource(specificSchema, specificName));
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " # SQL PL procedures extracted for schema " + schema);
		            objCount++;
	        	} catch (IOException e)
	        	{
	        		log("Error writing PL/SQL in file " + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting PL/SQL " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " Total # SQL PL Procedures extracted for schema " + schema);
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }
    }

    private void genDB2Types(String schema)
    {
    	String methodName = "genDB2Types";
    	String sql = "", typeName, sourceSchema, sourceName, typeStr = "";
    	int length, scale, codePage;
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        String dstSchema = getDstSchema(schema);

        if (mainData.DB2())
        {
        	if (mainData.version() >= 9.5F)
	        sql = "SELECT typename, sourceschema, sourcename, length, scale, codepage, metatype, array_length " +
	        		"FROM SYSCAT.DATATYPES " +
	        		"WHERE typeschema = '" + schema + "' " +
	        		"AND metatype IN ('T','A','F','R') " +
	        		"AND typemodulename is null " +
	        		"ORDER BY create_time";
        } else
        {
        	sql = "SELECT typename, sourceschema, sourcename, length, scale, codepage, metatype, 0 " +
		    		"FROM SYSCAT.DATATYPES " +
		    		"WHERE typeschema = '" + schema + "' " +
		    		"AND metatype IN ('T','A','F','R') " +
		    		"ORDER BY create_time";
        }
        
        if (sql.equals("")) return;
        
        try
        {
        	s.PrepareExecuteQuery(methodName, sql);        	

	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            buffer.setLength(0);
		            typeName = s.rs.getString(1);
		            sourceSchema = s.rs.getString(2);
		            sourceName = s.rs.getString(3);
		            length = s.rs.getInt(4);
		            scale = s.rs.getInt(5);
		            codePage = s.rs.getInt(6);
		            String metaType = s.rs.getString(7);
		            if (typeName != null)
		            	typeName = typeName.trim();
		            if (sourceSchema != null)
		            	sourceSchema = sourceSchema.trim();
		            if (sourceName != null)
		            	sourceName = sourceName.trim();
		            db2DropObjectsWriter.write("--#SET :DROP:TYPE:"+typeName + linesep);    		            
		            db2DropObjectsWriter.write("DROP TYPE " + putQuote(dstSchema) + "." + putQuote(typeName) + ";" + linesep);
		            if (metaType.equals("T"))
		            {
			            String comp = "";
			            if (sourceSchema.equalsIgnoreCase("SYSIBM") && 
			               (!(sourceName.equals("BLOB") || sourceName.equals("CLOB") || sourceName.equals("DBCLOB"))))
			            {
			            	comp = " WITH COMPARISONS ";
			            }
			            typeStr = "CREATE DISTINCT TYPE " + putQuote(dstSchema) + "." + putQuote(typeName) + linesep +
			                         "AS " + composeType(sourceSchema, sourceName, length, scale, codePage, schema) + comp;
		            }
		            else if (metaType.equals("A"))
		            {
		            	int arrayLength = s.rs.getInt(8);
			            typeStr = "CREATE TYPE " + putQuote(dstSchema) + "." + putQuote(typeName) + linesep +
			                         "AS " + composeType(sourceSchema, sourceName, length, scale, codePage, schema) + " ARRAY [" + arrayLength + "]";
		            } else if (metaType.equals("F"))
		            {
		            	// Type = F - Row data type
		            	String colType = genTypeCols(2, schema, null, typeName);
			            typeStr = "CREATE TYPE " + putQuote(dstSchema) + "." + putQuote(typeName) + linesep +
                                     "AS ROW (" + colType + ")";
		            }
		            else if (metaType.equals("R"))
		            {
		            	// Type = F - Row data type
		            	String colType = genTypeCols(3, schema, null, typeName);
			            typeStr = "CREATE TYPE " + putQuote(dstSchema) + "." + putQuote(typeName) + linesep +
                                     "AS (" + colType + ")" + linesep + "MODE DB2SQL";
		            }
		            db2ObjectsWriter[(Integer) plsqlHashTable.get("TYPE".toLowerCase())].write(typeStr + linesep + sqlTerminator + linesep);
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " # TYPES extracted for schema " + schema);
		            objCount++;
	        	} catch (IOException e)
	        	{
	        		log("Error writing Types in file " + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting Types " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " Total # Types extracted for schema " + schema);
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }    	
    }
    
    private void genSQLFunctions(String schema)
    {
    	String methodName = "genSQLFunctions";
    	String sql = "", specificSchema, specificName, functionType;
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        String dstSchema = getDstSchema(schema);

        if (mainData.iDB2())
	        sql = "select function_schem, function_name, specific_name, function_type " +
	        		"from sysibm.sqlfunctions " +
	        		"where function_schem = '"+schema+"'";
        else if (mainData.Mysql())
        {	
        	dstSchema = getCaseName(dstSchema);
	        sql = "select upper(ROUTINE_SCHEMA), ROUTINE_NAME, SPECIFIC_NAME, ROUTINE_TYPE " +
	        		"from information_schema.routines " +
	        		"where ROUTINE_SCHEMA = '"+schema+"'";
        }
        
        if (sql.equals("")) return;
        
        try
        {
        	s.PrepareExecuteQuery(methodName, sql);   
	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
		            buffer.setLength(0);
		            specificSchema = s.rs.getString(1);
		            specificName = s.rs.getString(3);
		            functionType = s.rs.getString(4);
		            if (functionType.equalsIgnoreCase("PROCEDURE"))
		            {
			            db2DropObjectsWriter.write("--#SET :DROP:PROCEDURE:"+specificName + linesep);    		            
			            db2DropObjectsWriter.write("DROP SPECIFIC PROCEDURE " + putQuote(dstSchema) + "." + putQuote(specificName) + ";" + linesep);		            	
		            } else
		            {
			            db2DropObjectsWriter.write("--#SET :DROP:FUNCTION:"+specificName + linesep);    		            		            	
			            db2DropObjectsWriter.write("DROP SPECIFIC FUNCTION " + putQuote(dstSchema) + "." + putQuote(specificName) + ";" + linesep);		            	
		            }
		            db2ObjectsWriter[(Integer) plsqlHashTable.get("FUNCTION".toLowerCase())].write(getFunctionSource(specificSchema, specificName, functionType));
		            if (objCount > 0 && objCount % 20 == 0)
		            	log(objCount + " # SQL Functions extracted for schema " + schema);
		            objCount++;
	        	} catch (IOException e)
	        	{
	        		log("Error writing Functions in file " + e.getMessage());
	        	} catch (SQLException ex)
	        	{
	        		log("Error getting Functions " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        if (objCount > 0)
        	   log(objCount + " Total # SQL Functions extracted for schema " + schema);
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }
    }

    private StringBuffer fixiDB2TableShortNames(String schema, String idb2TableNames, 
    		StringBuffer buffer)
    {
    	String methodName = "fixiDB2TableShortNames";
    	String sql = "", tabString;
    	SQL s = new SQL(mainData.connection);
    	
    	if (mainData.iDB2())
    	{
    		sql = "select table_name, system_table_name from qsys2.systables " +
    				"where table_schema = '"+schema+"'";
    	}
    	
    	if (sql.equals("")) return buffer;
    	
    	if (mapiDB2TableNames == null)
    	{
    		mapiDB2TableNames = new Properties();
	    	try
	    	{
	    		s.PrepareExecuteQuery(methodName, sql);    
		        while (s.next()) 
		        {
		        	try
		        	{
		        		String tableName = trim(s.rs.getString(1));
		        		String shortName = trim(s.rs.getString(2));
		        		mapiDB2TableNames.setProperty(shortName, tableName);
		        	} catch (Exception ex)
		        	{
		        		log("Error executing " + sql);
		        		ex.printStackTrace();
		        	}
		        }
		        s.close(methodName); 
	    	}
	    	catch (SQLException e)
	        {
	        	e.printStackTrace();
	        }
    	}
    	
    	String[] iDB2tables = idb2TableNames.split(";");
    	for (int i = 0; i < iDB2tables.length; ++i)
	    {
	    	String[] toks = iDB2tables[i].split(":");
	    	String tab = toks[1];
	    	String value = mapiDB2TableNames.getProperty(tab);
	    	if (!tab.equals(value))
	    	{
	    		int fromPos = IBMExtractUtilities.getTokenPosition(".*\\s+(FROM)\\s+.*", buffer.toString());
	    		if (fromPos > 0)
	    			fromPos += 4;
	    		int pos = 0;
	    		do
	    		{
		    		pos = (fromPos > 0) ? buffer.indexOf(tab, fromPos) : -1;
		    		if (pos > 0)
		    		{
		    			buffer.replace(pos, pos + tab.length(), value);
		    			int wherePos = IBMExtractUtilities.getTokenPosition(".*\\s+(WHERE)\\s+.*", buffer.toString());
		    			if (wherePos < 0)
		    			{
		    				wherePos = IBMExtractUtilities.getTokenPosition(".*\\s+(GROUP\\s+BY)\\s+.*", buffer.toString());
		    				if (wherePos < 0)
		    				{
		    					wherePos = IBMExtractUtilities.getTokenPosition(".*\\s+(ORDER\\s+BY)\\s+.*", buffer.toString());
		    				}
		    				if (wherePos < 0)
		    				{
		    					wherePos = IBMExtractUtilities.getTokenPosition(".*\\s+(HAVING)\\s+.*", buffer.toString());
		    				}
		    			}
	    				if (wherePos > 0)
	    					tabString = buffer.substring(pos, wherePos);
	    				else
	    					tabString = buffer.substring(pos);
		    			StringTokenizer st = new StringTokenizer(tabString, "\t\n\r ,", true);
	    				boolean correlCollected = false, tableCollected = false; 
	    				int len = (st.countTokens() == 0) ? Integer.MAX_VALUE - buffer.length() : 0;
		    			while (st.hasMoreTokens())
		    		    {
		    				String token = st.nextToken();
		    				len += token.length();
		    		    	if (!(token.equals("\n") || token.equals("\r") 
		    		    		|| token.equals("\t") || token.equals(" ")))
		    		    	{
		    			    	if (token.equalsIgnoreCase("JOIN") || token.equalsIgnoreCase(","))
		    			    	{
		    			    		tableCollected = correlCollected = false;
		    			    		break;
		    			    	}
		    			    	else if (token.equalsIgnoreCase("AS"))
		    			    	{
		    			    		;
		    			    	}
		    			    	else
		    			    	{
		    			    		if (!tableCollected || !correlCollected)
		    			    		{
		    					    	int dotPos = token.indexOf('.');
		    					    	if (dotPos > 0)
		    					    	{
		    					    		tableCollected = true;
		    					    	} else
		    					    	{
		    					    		if (!tableCollected)
		    					    		{
		    						    		tableCollected = true;
		    					    		} else
		    					    		{
		    						    		correlCollected = true;	    			
		    					    		}
		    					    	}
		    			    		}
		    			    	}		    		    		
		    		    	}
		    		    }
		    			fromPos = pos + ((len == 0) ? value.length() : 0) + len;	    			
		    		}
	    		} while (pos != -1);
	    	}
	    }    	
    	return buffer;
    }
    
    private StringBuffer fixTeradataViews(StringBuffer buffer)
    {
        Pattern p;
        Matcher m;
        
	    p = Pattern.compile("\\s*(create|replace)\\s+view\\s+.*((select|sel).*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    	
		StringBuffer replaceResult = new StringBuffer();
		m = p.matcher(buffer);
		if (m.find()) 
		{
			String tmpResult = m.group(2);
			tmpResult = tmpResult.replace(";", "");
			tmpResult = tmpResult.replaceFirst("(?ism)^SEL[\\s+|\\n]", "SELECT ");
			m.appendReplacement(replaceResult, tmpResult);				        				
		}
		m.appendTail(replaceResult);
		m.reset();
		buffer.setLength(0);
		buffer.append(replaceResult);
		
		return buffer;
    }
    
    private StringBuffer fixIDB2ShortName(String schema, StringBuffer buffer)
    {
    	String methodName = "fixIDB2ShortName";
    	String sql = "", idb2TableNames = "";
    	SQL s = new SQL(mainData.connection);
        Pattern p;
        Matcher m;
    	
    	if (!mainData.iDB2())    		
    		return buffer;
		idb2TableNames = IBMExtractUtilities.getTableNamesiSeriesDB2(buffer.toString());
    	buffer = fixiDB2TableShortNames(schema, idb2TableNames, buffer);
		idb2TableNames = IBMExtractUtilities.getTableNamesiSeriesDB2(buffer.toString());
	    if (idb2TableNames.length() > 0)
	    {
	       String[] iDB2tables = idb2TableNames.split(";");
	       for (int i = 0; i < iDB2tables.length; ++i)
	       {
	    	   String[] toks = iDB2tables[i].split(":");
	    	   String sch = (toks[0].length() == 0) ? removeQuote(getCaseName(schema)) : removeQuote(toks[0]);
	    	   String tab = removeQuote(toks[1]);
	    	   sql = "select column_name, system_column_name " +
	    		   "from qsys2.syscolumns where table_schema = '"+sch+"' " +
	    	       "and (table_name = '"+tab+"' OR system_table_name = '"+tab+"')";
	    	   String corName = (toks.length == 2) ? "" : removeQuote(toks[2]);
		       try
		       {
		    		s.PrepareExecuteQuery(methodName, sql);  
			        while (s.next()) 
			        {
			        	try
			        	{
			        		String columnName = trim(s.rs.getString(1));
			        		String columnShortName = trim(s.rs.getString(2));
			        		if (!columnName.equals(columnShortName))
			        		{
			        			if (corName.length() > 0)
			        			    p = Pattern.compile("\\s+\"?+"+corName+"\"?+\\s*\\.\\s*\"?+"+columnShortName+"\"?+[\\s+,]", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			        			else
			        				p = Pattern.compile("\"?+"+columnShortName+"\"?+[\\s+,]", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
			        			m = p.matcher(buffer);
			        			StringBuffer replaceResult = new StringBuffer();
			        			while (m.find()) 
			        			{
				        			if (corName.length() > 0)
			        				   m.appendReplacement(replaceResult, " " + putQuote(corName) + "." + putQuote(columnName) +
			        				       ((m.group().endsWith(",")) ? "," : " "));
				        			else
				        			   m.appendReplacement(replaceResult, " " + putQuote(columnName) +
					        				       ((m.group().endsWith(",")) ? "," : " "));				        				
			        			}
			        			m.appendTail(replaceResult);
			        			m.reset();
			        			buffer.setLength(0);
			        			buffer.append(replaceResult);
			        		}
			        	} catch (Exception ex)
			        	{
			        		log("Error executing " + sql);
			        		ex.printStackTrace();
			        	}
			        }
			        s.close(methodName); 
		       }
		       catch (SQLException e)
		       {
		        	e.printStackTrace();
		       }    	    	   
	       }
	    }	       
    	return buffer;
    }
    
    private String getViewSource(String schema, String viewName)
    {
    	String methodName = "getViewSource";
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        StringBuffer sb = new StringBuffer();
        String dstSchema = getDstSchema(schema), columnList = "";
    	String plSQL = "", headView = ""; 
    	
    	if (mainData.Oracle())
    	{
    		headView = "CREATE OR REPLACE VIEW " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(viewName)) + linesep;
    		plSQL = "select text from dba_views where owner = '"+schema+"' and view_name = '"+viewName+"'";
    	}
    	else if (mainData.iDB2())
    	{
    		headView = "CREATE VIEW " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(viewName)) + linesep;
    		plSQL = "select view_definition from sysibm.views where table_name = '"+viewName+"' and table_schema = '"+schema+"'";
    	}
    	else if (mainData.Mysql())
    	{
    		headView = "CREATE VIEW " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(viewName)) + linesep;
    		plSQL = "select view_definition from information_schema.views where table_name = '"+viewName+"' and table_schema = '"+schema+"'";
    	}
    	else if (mainData.Mssql())
    	{
    		headView = "";
    		plSQL = "select view_definition from information_schema.views where table_name = '"+viewName+"' and table_schema = '"+schema+"'";
    	}
    	else if (mainData.DB2())
    	{
    		headView = "";
    		plSQL = "select text from syscat.views where viewname = '"+viewName+"' and viewschema = '"+schema+"'";
    	}
    	else if (mainData.zDB2())
    	{
    		headView = "";
    		plSQL = "select text from sysibm.sysviews where name = '"+viewName+"' and creator = '"+schema+"' order by seqno";
    	}
    	else if (mainData.Teradata())
    	{
    		headView = "CREATE VIEW " + putQuote(getCaseName(dstSchema)) + "." + putQuote(getCaseName(viewName)) + linesep;
    		plSQL = "select requesttext from dbc.tables where tablename = '"+viewName+"' and databasename = '"+schema+"'";
    	}
    	
    	if (plSQL.equals(""))
    		return "";	

        try
        {
        	if (!(mainData.DB2() || mainData.zDB2()))
        	    columnList = "(" + getColumnList(schema, viewName) + ")" + linesep + " AS " + linesep;
    		s.PrepareExecuteQuery(methodName, plSQL);   
	        int objCount = 0;
	        while (s.next()) 
	        {
	        	try
	        	{
	        		sb.setLength(0);
	        		IBMExtractUtilities.getStringChunks(s.rs, 1, sb);
	        		if (mainData.iDB2())
	        		   sb = fixIDB2ShortName(schema, sb);	        		
	        		else if (mainData.Teradata())
	        			sb = fixTeradataViews(sb);
		            if (objCount ==  0)
		            {
		            	buffer.append(headView + columnList);
		            	if (mainData.Mysql() || mainData.Teradata() 
		            			|| mainData.Mssql())
		            	{
		            		buffer.append("-- The view definition is extracted as it is from " + mainData.dbSourceName + linesep);
		            		buffer.append("-- Make changes in the syntax to fit to DB2." + linesep);
		            	}
		            	if (mainData.Mysql())
		            		if (sb.length() == 0)
		            			buffer.append("-- View definition could not be extracted from MySQL. This is a known bug in MySQL. " + plSQL);
		            	buffer.append(sb.toString());
		            	objCount++;
		            } else
		               buffer.append(sb.toString());
	        	} catch (Exception ex)
	        	{
	        		log("Error getting views " + ex.getMessage());
	        		ex.printStackTrace();
	        	}
	        }
	        s.close(methodName); 
        } 
	    catch (SQLException e)
        {
        	e.printStackTrace();
        }   
	    return buffer.toString();
    }
    
    private void genViews(String schema)
    {
    	String methodName = "genViews";
    	String viewSQL = "", viewTemplate = "", viewName, ddlSQL, viewDDL;
    	SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        String dstSchema = getDstSchema(schema);

        if (mainData.Oracle())
        {
        	viewTemplate = "select dbms_metadata.get_ddl('VIEW','&viewName&','&schemaName&') from dual";
        	viewSQL = "select view_name from dba_views where owner = '"+schema+"' and view_name not like 'AQ$%'";
        } 
        else if (mainData.iDB2())
        {
        	viewTemplate = "select '' from sysibm.sysdummy1";
        	viewSQL = "select table_name from sysibm.views where table_schema = '"+schema+"' and table_name not like 'SYS%'";
        }
        else if (mainData.Mysql())
        {
        	dstSchema = getCaseName(dstSchema);
        	viewTemplate = "select '' from dual";
        	viewSQL = "select table_name from information_schema.views where table_schema = '"+schema+"' and table_name not like 'SYS%'";
        }
        else if (mainData.Mssql())
        {
        	dstSchema = getCaseName(dstSchema);
        	viewTemplate = "select ''";
        	viewSQL = "select table_name from information_schema.views where table_schema = '"+schema+"' and table_name not like 'SYS%'";
        }
        else if (mainData.DB2())
        {
        	viewTemplate = "select '' from sysibm.sysdummy1";
        	viewSQL = "select viewname from syscat.views where viewschema = '"+schema+"' and valid = 'Y'";
        }
        else if (mainData.zDB2())
        {
        	viewTemplate = "select '' from sysibm.sysdummy1";
        	viewSQL = "select name from sysibm.sysviews where creator = '"+schema+"' and type = 'V'";
        }
        else if (mainData.Teradata())
        {
        	viewTemplate = "select ''";
        	viewSQL = "select tablename from dbc.tables where databasename = '"+schema+"' and tablekind = 'V'";
        }
        
        if (viewSQL.equals("")) return;
        
        try
        {
			s.PrepareExecuteQuery(methodName, viewSQL);   
	        int objCount = 0;
	        while (s.next()) 
	        {
	            buffer.setLength(0);
	        	buffer.append("SET CURRENT SCHEMA = '" + dstSchema + "'" + linesep);  
	        	buffer.append(sqlTerminator + linesep);  
			    buffer.append("SET PATH = SYSTEM PATH, " + putQuote(dstSchema) + linesep);            	
		        buffer.append(sqlTerminator + linesep);            	
	            viewName = trim(s.rs.getString(1));
	            ddlSQL = viewTemplate.replace("&schemaName&", schema);
	            ddlSQL = ddlSQL.replace("&viewName&", viewName);            
	            try 
	            {    
	                viewDDL = null;
	            	try
	            	{
	            		SQL s2 = new SQL(mainData.connection);
	            		s2.PrepareExecuteQuery(methodName, ddlSQL);        	
	
	                    if (s2.next())
	                    {
	                    	viewDDL = s2.rs.getString(1);
	                    	if (viewDDL == null || viewDDL.length() == 0)
	                    		viewDDL = getViewSource(schema, viewName);
	                    }
	                    s2.close(methodName);
	            	} catch (Exception ex)
	            	{
	            		viewDDL = getViewSource(schema, viewName);
	            	}
	                if (viewDDL != null && !viewDDL.equals(""))
	                {
	                    buffer.append("--#SET :VIEW:" + dstSchema + ":" + viewName + linesep);
	                    if (mainData.Oracle() && invalidOracleObject(schema, "VIEW", viewName))
	                    	buffer.append("--#WARNING : VIEW " + schema + "." + viewName + " is found to be invalid in source database" + linesep);
			            db2DropObjectsWriter.write("--#SET :DROP:VIEW:"+viewName + linesep);    		            
	                    db2DropObjectsWriter.write("DROP VIEW " + putQuote(getCaseName(dstSchema)) + sqlsep + putQuote(getCaseName(viewName)) + ";" + linesep);
	                    if (mainData.Oracle())
	                    {
	                       buffer.append(OraToDb2Converter.getDb2PlSql(viewDDL));
	                    } else
	                    {
	                    	buffer.append(viewDDL);
	                    }
	                    // ensure the / is at the beginning of the line
	                	buffer.append(linesep + sqlTerminator +  linesep);            	
	                }
	                db2ViewsWriter.write(buffer.toString()); 
	                db2ViewsWriter.flush();
	                db2ViewsWriter.write(getComments(getDstSchema(schema), viewName, schema, viewName));
	                db2ViewsWriter.flush();
	                if (objCount > 0 && objCount % 20 == 0)
	                	log(objCount + " # views extracted for schema " + schema);
	                objCount++;
	            } catch (Exception e)
	            {
	            	log("genViews SQL=" + ddlSQL);
	            	e.printStackTrace();
	            }
	        }
	        if (objCount > 0)
	    	   log(objCount + " Total # views extracted for schema " + schema);
	        s.close(methodName); 
        } catch (Exception e)
        {
        	log("Exception thrown in method " + methodName + " Error Message " + e.getMessage());
        	e.printStackTrace();    		
        }
    }
    
    private String composeType(String typeSchema, String typeName, 
    		int length, int scale, int codePage, String pSchemaName)
    {
    	String typeStr = "";
    	if (!typeSchema.equalsIgnoreCase("SYSIBM") && !typeSchema.equalsIgnoreCase(pSchemaName))
    		typeStr += "\"" + typeSchema + "\".";
    	if (typeSchema.equalsIgnoreCase("SYSIBM"))
    		typeStr += typeName;
    	else
    		typeStr += "\"" + typeName + "\"";
    	if (typeSchema.equalsIgnoreCase("SYSIBM") && 
    			 (typeName.equalsIgnoreCase("CHARACTER") || typeName.equalsIgnoreCase("VARCHAR") ||
    			  typeName.equalsIgnoreCase("BLOB") || typeName.equalsIgnoreCase("CLOB") ||
    			  typeName.equalsIgnoreCase("DECIMAL")))
    	{
    		if (length == -1)
    			typeStr += "()";
    		else
    		{
    			if (typeName.equalsIgnoreCase("DECIMAL"))
    				typeStr += "(" + length + "," + scale + ")";
    			else
    				typeStr += "(" + length + ")";
    		}
    	}
    	if ((typeName.equalsIgnoreCase("CHARACTER") || typeName.equalsIgnoreCase("VARCHAR")) &&
    			codePage == 0)
    		typeStr += " FOR BIT DATA";
    	return typeStr;
    }
    
    private void genDB2XSRSchema(String schema)
    {
    	String methodName = "genDB2XSRSchema";
    	int num = 0;
        String xsrSQL = "";
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        StringBuffer chunks = new StringBuffer();
        BufferedWriter writer = null;
        String objectSchema, objectName, schemaLocation, schemaLocation2, xmlFile, xsdFile;
        
    	if (mainData.DB2())
        {
            if (majorSourceDBVersion > 9)
            	return;
            xsrSQL = "SELECT OBJECTSCHEMA, OBJECTNAME, SCHEMALOCATION, OBJECTINFO " +
            		"FROM SYSCAT.XSROBJECTS WHERE OWNER = '"+schema+"'";
        }
    	
    	if (xsrSQL.equals("")) return;

    	try
		{
    		new File(OUTPUT_DIR + "xsr").mkdirs();
    		s.PrepareExecuteQuery(methodName, xsrSQL);   
	        while (s.next()) 
	        {
	            buffer.setLength(0);
	            chunks.setLength(0);
	            objectSchema = trim(s.rs.getString(1));
	            objectName = trim(s.rs.getString(2));
	            schemaLocation = trim(s.rs.getString(3));
	            IBMExtractUtilities.getStringChunks(s.rs, 4, chunks);
	            schemaLocation2 = (schemaLocation == null) ? "" : schemaLocation;  
	            xmlFile = OUTPUT_DIR + "xsr" + filesep + objectSchema.toLowerCase() + "_" + objectName.toLowerCase() + ".xml";
	            xsdFile = OUTPUT_DIR + "xsr" + filesep + objectSchema.toLowerCase() + "_" + objectName.toLowerCase() + ".xsd";
	            buffer.append("--#SET :XMLSCHEMA:" + objectSchema + ":" + objectName + linesep);      
	        	buffer.append("REGISTER XMLSCHEMA " + putQuote(schemaLocation2) + linesep);
	        	buffer.append("FROM " + xmlFile + " AS " + putQuote(objectSchema) + "." + putQuote(objectName) + linesep + ";" + linesep);
	            buffer.append("--#SET :XMLCOMPLETE:" + objectSchema + ":" + objectName + linesep);      
	        	buffer.append("COMPLETE XMLSCHEMA " + putQuote(objectSchema) + "." + putQuote(objectName) + linesep);
	        	buffer.append("WITH " + xmlFile + linesep + ";" + linesep);
	        	db2ObjectsWriter[(Integer) plsqlHashTable.get("XMLSCHEMA".toLowerCase())].write(buffer.toString());
	            db2DropObjectsWriter.write("--#SET :DROP:XMLSCHEMA:"+objectName + linesep);    		            
	            db2DropObjectsWriter.write("DROP XMLSCHEMA " + putQuote(objectSchema) + "." + putQuote(objectName) + ";" + linesep);
                if (num > 0 && num % 20 == 0)
                	log(num + " # XML Schema extracted for schema " + schema);
	        	++num;
	        	writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xmlFile, false), sqlFileEncoding));
	        	//writer = new BufferedWriter(new FileWriter(xmlFile, false));
	        	writer.write(chunks.toString());
	        	writer.close();
	        	if (schemaLocation == null)
	        	   xsrSQL = "SELECT COMPONENT " +
	        			"FROM SYSCAT.XSROBJECTCOMPONENTS " +
	        			"WHERE OBJECTSCHEMA = '"+objectSchema+"' " +
	        			"AND OBJECTNAME = '"+objectName+"' " +
	        			"AND SCHEMALOCATION IS NULL";
	        	else
		           xsrSQL = "SELECT COMPONENT " +
	        			"FROM SYSCAT.XSROBJECTCOMPONENTS " +
	        			"WHERE OBJECTSCHEMA = '"+objectSchema+"' " +
	        			"AND OBJECTNAME = '"+objectName+"' " +
	        			"AND SCHEMALOCATION = '"+schemaLocation+"'";
	        	
	        	SQL s2 = new SQL(mainData.connection);
	    		s2.PrepareExecuteQuery(methodName, xsrSQL);        	

		        if (s2.next())
		        {
		            chunks.setLength(0);
		        	writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xsdFile, false), sqlFileEncoding));
		        	//writer = new BufferedWriter(new FileWriter(xsdFile, false));
		            byte[] byteBuffer = new byte[1024*1000];
		            int bytesRead = 0;
		            try
		    		{
		                InputStream input = s2.rs.getBinaryStream(1);
		    			while ((bytesRead = input.read(byteBuffer)) != -1)
		    			{
				        	writer.write(new String(byteBuffer, 0, bytesRead));
		    			}
		    		} catch (Exception e)
		    		{
		    			e.printStackTrace();
		    		}
		        	writer.close();
		        }
		        s2.close(methodName);
	        }
            if (num > 0)
            	log(num + " # XML Schema extracted for schema " + schema);
            s.close(methodName); 
		} catch (Exception e)
		{
			log("seqSQL=" + xsrSQL);
			e.printStackTrace();
		}    	
    }
    
    private void genDB2Aliases()
    {
    	String methodName = "genDB2Aliases";
    	int num = 0;
        String aliasSQL = "";
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        String dstSchema, tabName, tabSchema, baseTabSchema, baseTabName;

        if (mainData.DB2())
        {
            if (majorSourceDBVersion < 8)
            	return;
            aliasSQL = "SELECT tabschema, tabname, base_tabschema, base_tabname " +
            		 "FROM SYSCAT.TABLES WHERE type = 'A' " +
            		 "AND tabschema IN ("+schemaList+") " +
            		 "ORDER BY create_time";
        } else if (mainData.zDB2())
        {
            if (majorSourceDBVersion < 8)
            	return;
        	aliasSQL = "SELECT CREATOR, NAME, TBCREATOR, TBNAME FROM SYSIBM.SYSTABLES " +
        			"WHERE TYPE = 'A' " +
        			"AND  CREATOR IN ("+schemaList+") " +
        			"ORDER BY CREATEDTS";
        }
        
        
        if (aliasSQL.equals("")) return;
                        
        try
		{
    		s.PrepareExecuteQuery(methodName, aliasSQL);    
	        while (s.next()) 
	        {
	        	++num;
	            buffer.setLength(0);
	            tabSchema = trim(s.rs.getString(1));
	            tabName = trim(s.rs.getString(2));
	            baseTabSchema = trim(s.rs.getString(3));
	            baseTabName = trim(s.rs.getString(4));
	            
	            dstSchema = getDstSchema(tabSchema);
	            if (dstSchema == null)
	            	dstSchema = tabSchema;
	            buffer.append("--#SET :ALIAS:" + dstSchema + ":" + tabName + linesep);      
	            buffer.append("CREATE ALIAS " + putQuote(dstSchema) + "." + putQuote(tabName) + linesep);
	            if (baseTabSchema.equalsIgnoreCase(tabSchema))
	               buffer.append("FOR " + putQuote(baseTabName) + linesep);
	            else
	               buffer.append("FOR " + putQuote(baseTabSchema) + "." + putQuote(baseTabName) + linesep);
	        	buffer.append(";" + linesep);
	            db2SynonymWriter.write(buffer.toString());    
	            db2DropSynWriter.write("DROP ALIAS " + putQuote(dstSchema) + "." + putQuote(tabName) + ";" + linesep);
	        }
	        s.close(methodName); 
		} catch (Exception e)
		{
			log("seqSQL=" + aliasSQL);
			e.printStackTrace();
		}
    }
    
    private void genDB2Sequences()
    {
    	String methodName = "genDB2Sequences";
    	int seq_num = 0;
        String seqSQL = "", seqName;
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        String dstSchema, lastSeqStr, typeSchema, typeName, increment, seqSchema;
        String maxvalue, minvalue, cycle, order;
        int cache, precision;

        if (mainData.DB2())
        {
            if (majorSourceDBVersion < 8)
            	return;
            seqSQL = "SELECT seqschema, seqname, typeschema, typename, increment, maxvalue, " +
            		 "minvalue, cycle, cache, order, precision " +
            		 "FROM SYSCAT.SEQUENCES AS S " +
            		 "JOIN SYSCAT.DATATYPES AS D " +
            		 "ON S.datatypeid = D.typeid " +
            		 "WHERE origin = 'U' " +
            		 "AND seqtype = 'S' " +
            		 "AND seqschema IN ("+schemaList+")";
        } else if (mainData.zDB2())
        {
            if (majorSourceDBVersion < 8)
            	return;
            seqSQL = "SELECT S.SCHEMA seqschema, S.NAME seqname, d.sourceschema typeschema, d.sourcetype typename, " +
            		 "s.increment increment, s.maxvalue maxvalue, s.minvalue minvalue, " +
            		 "s.cycle cycle, s.cache cache, s.order order, s.precision precision " +
            		 "FROM SYSIBM.SYSSEQUENCES S, SYSIBM.SYSDATATYPES D " +
            		 "WHERE S.DATATYPEID = D.SOURCETYPEID " +
            		 "AND S.SEQTYPE = 'S' " +
            		 "AND S.SCHEMA IN ("+schemaList+")";        	
        }
        
        
        if (seqSQL.equals("")) return;
                        
        try
		{
    		s.PrepareExecuteQuery(methodName, seqSQL);       
	        while (s.next()) 
	        {
	        	++seq_num;
	            buffer.setLength(0);
	            seqSchema = trim(s.rs.getString(1));
	            seqName = s.rs.getString(2);
	            typeSchema = trim(s.rs.getString(3));
	            typeName = s.rs.getString(4);
	            increment = s.rs.getString(5);
	            maxvalue = s.rs.getString(6);
	            minvalue = s.rs.getString(7);
	            cycle = s.rs.getString(8);
	            cache = s.rs.getInt(9);
	            order = s.rs.getString(10);
	            precision = s.rs.getInt(11);
	            
	            dstSchema = getDstSchema(seqSchema);
	            if (dstSchema == null)
	            	dstSchema = seqSchema;
	            lastSeqStr = executeSQL("SELECT NEXT VALUE FOR " + putQuote(seqSchema) + "." + putQuote(seqName) + " FROM SYSIBM.SYSDUMMY1", false);
	            buffer.append("--#SET :SEQUENCE:" + dstSchema + ":" + seqName + linesep);      
	            buffer.append("CREATE SEQUENCE " + putQuote(dstSchema) + "." + putQuote(seqName) + 
	            		" AS " + composeType(typeSchema, typeName, precision, 0, -1, seqSchema) + linesep);
	            buffer.append("START WITH " + lastSeqStr + linesep);
	            if (netezza)
	            {
		            buffer.append("INCREMENT BY " + increment + linesep);	            	
	            }
	            buffer.append("MAXVALUE " + maxvalue + linesep);
	            buffer.append("MINVALUE " + minvalue + linesep);
	            if (!netezza)
	            {
	               buffer.append("INCREMENT BY " + increment + linesep);
	            }
	            if (cycle.equalsIgnoreCase("Y"))
	               buffer.append("CYCLE " + linesep);
	            else
	               buffer.append("NO CYCLE " + linesep);
	            if (!netezza)
	            {
		            if (order.equalsIgnoreCase("Y"))
			           buffer.append("ORDER " + linesep);
			        else
			           buffer.append("NO ORDER " + linesep);
		            if (cache < 2)
		               buffer.append("NO CACHE " + linesep);
		            else
		               buffer.append("CACHE " + cache + linesep);
	            }
	        	buffer.append(";" + linesep);
	            db2SeqWriter.write(buffer.toString());    
	            db2DropSeqWriter.write("--#SET :DROP:SEQUENCE:" + dstSchema + "." + seqName + linesep);
	            db2DropSeqWriter.write("DROP SEQUENCE " + putQuote(dstSchema) + sqlsep + putQuote(seqName) + linesep + ";" + linesep);
	        }
	        s.close(methodName); 
		} catch (Exception e)
		{
			log("seqSQL=" + seqSQL);
			e.printStackTrace();
		}
    }
    
    private void genOraSequences() throws SQLException, IOException
    {
    	String methodName = "genOraSequences";
        /*
         * select trigger_name, trigger_type, triggering_event, REFERENCING_NAMES, trigger_body from dba_triggers where table_owner = 'VIKRAM' and table_name = 'T1'
         * select last_number, min_value, increment_by, cycle_flag, order_flag, cache_size, sequence_name from dba_sequences where sequence_owner = 'VIKRAM'
         * CREATE SEQUENCE SAM_OWN.D_CUSTOMER_LOCATION_SEQ
         * MINVALUE 1
         * MAXVALUE 999999999
         * INCREMENT BY 1
         * CACHE 50 CYCLE  ORDER;
         * 
         * NOMAXVALUE in DB2 is 2147483647
         */
    	int seq_num = 0;
        long last_number, min_value, max_value, cache_size, increment_by;
        String seqSQL = "", cycle_flag = "", order_flag = "", seqName, seqOwner;
        SQL s = new SQL(mainData.connection);
        StringBuffer buffer = new StringBuffer();
        String dstSchema, dataType = "", maxValueStr = null;

        if (mainData.Oracle())
            seqSQL = "select last_number, min_value, max_value, increment_by, cache_size, cycle_flag, order_flag, sequence_owner, sequence_name " +
                     "from dba_sequences where sequence_owner IN ("+schemaList+")";        
        
        if (seqSQL.equals("")) return;
                
		s.PrepareExecuteQuery(methodName, seqSQL);        	
        while (s.next()) 
        {
        	++seq_num;
            buffer.setLength(0);
            last_number = s.rs.getLong(1);
        	maxValueStr = s.rs.getString(3);
            try 
            {
            	min_value = s.rs.getLong(2);
            } catch (Exception es)
            {
            	min_value = -1;
            }
            try 
            {
               max_value = s.rs.getLong(3);
            } catch (Exception es)
            {
            	max_value = 2147483647;
            }
            increment_by = s.rs.getLong(4);
            cache_size = s.rs.getLong(5);
            cycle_flag = s.rs.getString(6);
            order_flag = s.rs.getString(7);
            seqOwner = s.rs.getString(8);
            seqName = s.rs.getString(9);
            if (customMapping.equalsIgnoreCase("CE") || netezza)
            {
            	dataType = " AS BIGINT";
            } else
            {
            	if (maxValueStr != null)
            	{
            		if (maxValueStr.length() <= 10)
            			dataType = " AS NUMERIC(10)";
            		else
            			dataType = " AS NUMERIC(31)";
            	}
            }
            dstSchema = getDstSchema(seqOwner);
            if (dstSchema == null)
            	dstSchema = seqOwner;
            buffer.append("-- Oracle Sequence Name : " + seqName + linesep);                            
            buffer.append("--#SET :SEQUENCE:" + dstSchema + ":" + seqName + linesep);    
    		if ((releaseLevel != -1.0F && releaseLevel >= 9.7F) || netezza)
               buffer.append("CREATE SEQUENCE " + putQuote(dstSchema) + sqlsep + putQuote(seqName) + dataType + linesep);
            else
               buffer.append("CREATE SEQUENCE " + putQuote(dstSchema) + sqlsep + putQuote(seqName) + linesep);                            
            buffer.append("START WITH " + (last_number + increment_by) + linesep); 
            if (netezza)
            {
                buffer.append("INCREMENT BY " + increment_by + linesep);             	
            }
    		if ((releaseLevel != -1.0F && releaseLevel >= 9.7F) || netezza)
            {            	
                buffer.append("MINVALUE " + s.rs.getString(2) + linesep);                            
                buffer.append("MAXVALUE " + (netezza ? "9223372036854775807" : maxValueStr) + linesep);                            
            } else
            {
            	if (min_value == -1)
            	{
	                buffer.append("-- Oracle MIN VALUE IS " + s.rs.getString(2) + ". Using MIN_VALUE as 0. Please check. " + seqName + linesep); 
		            buffer.append("MINVALUE 0" + linesep);            		
            	} else
            	{
		            buffer.append("MINVALUE " + min_value + linesep);            		            		
            	}
	            if (max_value > 2147483647)
	            {
	                buffer.append("-- Oracle MAX VALUE IS " + maxValueStr + ". Using 2147483647 as max value for DB2 " + seqName + linesep); 
	                max_value = 2147483647;
	            }            	
	            buffer.append("MAXVALUE " + max_value + linesep);                            
            }
    		if (!netezza)
    		{
                buffer.append("INCREMENT BY " + increment_by + linesep);
    		}
    		if (!netezza)
    		{
	            if (cache_size == 0)
	               buffer.append("NO CACHE " + linesep);
	            else
	                buffer.append("CACHE " + cache_size + linesep);
    		}
            if (cycle_flag.equalsIgnoreCase("N"))
            	buffer.append((netezza ? "NO CYCLE" : "NOCYCLE") + linesep);
            else
            	buffer.append("CYCLE" + linesep);
            if (!netezza)
            {
	            if (order_flag.equalsIgnoreCase("N"))
	            	buffer.append("NOORDER" + linesep);
	            else
	            	buffer.append("ORDER" + linesep);
            }
        	buffer.append(";" + linesep);
            db2SeqWriter.write(buffer.toString());    
            db2DropSeqWriter.write("--#SET :DROP SEQUENCE:" + dstSchema + ":" + seqName + linesep);
            db2DropSeqWriter.write("DROP SEQUENCE " + putQuote(dstSchema) + sqlsep + putQuote(seqName) + linesep + ";" + linesep);
        }
        s.close(methodName); 
    }
    
    private void genDB2CheckScript() throws IOException 
    {
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append("::  Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep);
            buffer.append(":: The purpose of this script is to generate SET INTEGRITY commands in the right order." + linesep);
            buffer.append(":: You may need to run this script manually."+linesep);
            buffer.append("::"+linesep);
        } else
        {
             buffer.append("#!"+shell+linesep);
             buffer.append("#  Copyright(r) IBM Corporation"+linesep);
             buffer.append("#"+linesep);
             buffer.append("# The purpose of this script is to generate SET INTEGRITY commands in the right order." + linesep);
             buffer.append("# You may need to run this script manually."+linesep);
             buffer.append("#"+linesep);
             buffer.append("echo "+linesep);        	
        }
        buffer.append("db2 connect to "+dstDBName + linesep);
        buffer.append("db2 -tx +w \"WITH GEN(tabname, seq) AS (SELECT RTRIM(TABSCHEMA)||'.' ||RTRIM(TABNAME) AS TABNAME, ");
        buffer.append("ROW_NUMBER() OVER (PARTITION BY STATUS) as seq FROM SYSCAT.TABLES WHERE STATUS='C'), r(a, seq1) AS ");
        buffer.append("(SELECT CAST(TABNAME as VARCHAR(32000)), SEQ FROM gen WHERE seq=1 UNION ALL ");
        buffer.append("SELECT CAST(r.a ||','||RTRIM(gen.tabname) AS VARCHAR(32000)), gen.seq FROM gen, r WHERE (r.seq1+1)=gen.seq), r1 AS ");
        buffer.append("(SELECT a, seq1 FROM r) SELECT 'SET INTEGRITY FOR ' || a || ' IMMEDIATE CHECKED;' ");
        buffer.append("FROM r1 WHERE seq1=(SELECT MAX(seq1) FROM r1)\" > tmp.sql" + linesep);
        buffer.append("db2 -tvf tmp.sql" + linesep);
        db2CheckScriptWriter.write(buffer.toString());
    }

    private void genDB2DropScript() throws IOException
    {
    	int len = 40;
    	String runcmd = Constants.win() ? "%RUNCMD% " : "$RUNCMD ";
    	String echo = Constants.win() ? "ECHO " : "echo ";
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        
        if (Constants.win())
        {
            buffer.append("::  Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep);
            buffer.append(":: This script can be run to drop objects in the right order."+linesep);
            buffer.append(":: This script can be run either from GUI or from command line."+linesep);
            buffer.append("::"+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Drop Objects in "+Constants.getDbTargetName()+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO START TIME=%DATE% %TIME% "+linesep+linesep);

            buffer.append("SET JAVA_HOME=" + javaHome + linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);
            buffer.append("SET RUNCMDARGS1=\"%JAVA_HOME%\\bin\\java\"" + linesep);
            buffer.append("SET RUNCMDARGS2=-Xmx600m -DAppHome="+IBMExtractUtilities.FormatENVString(IBMExtractUtilities.getHomeDir()) + linesep);
            buffer.append("SET RUNCMDARGS3=-cp %CLASSPATH% ibm.DeployObjects" + linesep);
            buffer.append("SET RUNCMD=%RUNCMDARGS1% %RUNCMDARGS2% %RUNCMDARGS3%" + linesep + linesep);
            buffer.append("echo Executing Script "+Constants.dropscripts+linesep+linesep);
        } else
        {
            buffer.append("#!"+shell+linesep);
            buffer.append("#  Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep);
            buffer.append("# This script can be run to drop objects in the right order."+linesep);
            buffer.append("# This script can be run either from GUI or from command line."+linesep);
            buffer.append("#"+linesep);
            buffer.append("echo "+linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Drop Objects in "+Constants.getDbTargetName()+linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo "+linesep);
            buffer.append("echo $(date) Executing Script "+Constants.dropscripts+linesep+linesep);
            
            buffer.append("JAVA_HOME=" + IBMExtractUtilities.FormatENVString(javaHome) + linesep);
            buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);
            buffer.append("RUNCMDARGS1="+IBMExtractUtilities.FormatENVString("$JAVA_HOME/bin/java") + linesep);
            buffer.append("RUNCMDARGS2="+IBMExtractUtilities.FormatENVString("-Xmx600m -DAppHome="+IBMExtractUtilities.getHomeDir()) + linesep);
            buffer.append("RUNCMDARGS3="+IBMExtractUtilities.FormatENVString("-cp \"$CLASSPATH\" ibm.DeployObjects") + linesep);
            buffer.append("RUNCMD="+IBMExtractUtilities.FormatENVString("$RUNCMDARGS1 $RUNCMDARGS2 $RUNCMDARGS3") + linesep+linesep);
        }
        buffer.append(fmtl(echo,"Running "+Constants.dropfkeys, len, "- Drop foreign keys")+linesep);
        buffer.append(runcmd + Constants.dropfkeys+linesep+linesep);
        if ((mainData.Oracle() || db2Skin()) && !netezza)
        {
            buffer.append(fmtl(echo,"Running "+Constants.dropobjects, len, "- Drop Objects")+linesep);
            buffer.append(runcmd + Constants.dropobjects+linesep+linesep);
        }
        if (db2)
        {
            buffer.append(fmtl(echo,"Running "+Constants.dropexceptiontables, len, "- Drop Exception tables")+linesep);
            buffer.append(runcmd + Constants.dropexceptiontables+linesep+linesep);        	
        }
        buffer.append(fmtl(echo,"Running "+Constants.droptables, len, "- Drop tables")+linesep);
        buffer.append(runcmd + Constants.droptables+linesep+linesep);
        buffer.append(fmtl(echo,"Running "+Constants.dropsequences, len, "- Drop sequences")+linesep);
        buffer.append(runcmd + Constants.dropsequences+linesep+linesep);
        if (mainData.Oracle() || mainData.SKIN())
        {
            buffer.append(fmtl(echo,"Running "+Constants.dropsynonyms, len, "- Drop synonyms")+linesep);
            buffer.append(runcmd + Constants.dropsynonyms+linesep+linesep);
        }
        buffer.append(fmtl(echo,"Running "+Constants.droptsbp, len, (netezza ? "- Drop databases" : "- Drop table space and buffer pools"))+linesep);
        buffer.append(runcmd + Constants.droptsbp+linesep+linesep);

        buffer.append(Constants.win() ? "ECHO END TIME  =%DATE% %TIME%"+linesep : "echo $(date)");
        buffer.append(Constants.win() ? "ECHO. "+linesep : "");
        buffer.append("echo Work Completed. Check "+ OUTPUT_DIR + filesep + "logs directory for errors"+linesep);
        buffer.append(Constants.win() ? "ECHO. "+linesep : "");
        buffer.append(Constants.win() ? ":END "+linesep : "");        
        db2DropScriptWriter.write(buffer.toString());            
    }
    
    private void genExceptTableCount()
    {
    	boolean skip = true;
    	StringBuffer sb = new StringBuffer();
    	String prevSchema = "", prevTable = "";
    	
    	try
    	{
    		db2ExceptTabCountWriter.write("--#SET :QUERY:TABLE:COUNT" + linesep);
	    	for (int i = 0; i < t.exceptTableName.length; ++i)
	    	{
	    		skip = (i == 0 || sb.length() == 0) ? true : false;
	    		if (!(prevSchema.equals(t.exceptSchemaName[i]) && prevTable.equals(t.exceptTableName[i])))
	    		{
		    		if (!skip && i < t.exceptTableName.length)
		    		{
		    			sb.append(" UNION ALL " + linesep);
		    		}
	         	    sb.append("SELECT '"+removeQuote(getCaseName(t.exceptSchemaName[i]))+"."+removeQuote(getCaseName(t.exceptTableName[i]))+"' AS TABLE_NAME, " +
	         	    		"COUNT_BIG(*) AS ROW_COUNT FROM " + getCaseName(t.exceptSchemaName[i]) + "." + getCaseName(t.exceptTableName[i]));
	         	    if (sb.length() > 100000)
	         	    {
	    				db2TabCountWriter.write(sb.toString() + linesep + ";" + linesep);
	    				sb.setLength(0);
	         	    }
	    		}
	            prevSchema = t.exceptSchemaName[i];
	            prevTable = t.exceptTableName[i];
	    	}
	    	db2ExceptTabCountWriter.write(sb.toString() + linesep + ";" + linesep);
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }

    private void genTableCount()
    {
    	boolean skip = true;
    	StringBuffer sb = new StringBuffer();
    	String prevSchema = "", prevTable = "";
    	
    	try
    	{
	    	for (int i = 0; i < t.dstTableName.length; ++i)
	    	{
	    		skip = (i == 0 || sb.length() == 0) ? true : false;
	    		if ((prevSchema.equals(t.dstSchemaName[i])))
	    		{
		    		if (!skip && i < t.dstTableName.length)
		    		{
		    			sb.append(" UNION ALL " + linesep);
		    		}
	         	    sb.append("SELECT '"+removeQuote(getCaseName(t.dstSchemaName[i]))+"."+removeQuote(getCaseName(t.dstTableName[i]))+"' AS TABLE_NAME, " +
	         	    		"COUNT"+(netezza ? "" : "_BIG")+"(*) AS ROW_COUNT FROM " + getCaseName(t.dstSchemaName[i]) + "." + getCaseName(t.dstTableName[i]));
	         	    if (sb.length() > 100000)
	         	    {
	    				db2TabCountWriter.write(sb.toString() + linesep + ";" + linesep);
	    				sb.setLength(0);
	         	    }
	    		} else
	    		{
	    			if (sb.length() > 0)
	    			{
	    				db2TabCountWriter.write(sb.toString() + linesep + ";" + linesep);
	    				sb.setLength(0);	    				
	    			}
	    			sb.append("--#SET :QUERY:"+removeQuote(getCaseName(t.dstSchemaName[i]))+":TABLE_COUNT" + linesep);	    		
	         	    sb.append("SELECT '"+removeQuote(getCaseName(t.dstSchemaName[i]))+"."+removeQuote(getCaseName(t.dstTableName[i]))+"' AS TABLE_NAME, " +
	         	    		"COUNT"+(netezza ? "" : "_BIG")+"(*) AS ROW_COUNT FROM " + getCaseName(t.dstSchemaName[i]) + "." + getCaseName(t.dstTableName[i]));
	        	}
	            prevSchema = t.dstSchemaName[i];
	            prevTable = t.dstTableName[i];
	    	}
	    	db2TabCountWriter.write(sb.toString() + linesep + ";" + linesep);
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    private void genTableStatus()
    {
    	String head, tail, ends;
    	boolean skip = true;
    	StringBuffer sb = new StringBuffer();
    	String prevSchema = "";
    	String tmpPrevSchema = "";
    	    	
    	if (db2)
    	{
	    	head = "select substr(rtrim(tabschema)||'.'||rtrim(tabname),1,50) TABLE_NAME, substr(const_checked,1,1) FK_CHECKED, substr(const_checked,2,1) CC_CHECKED, " +
		                           "case status when 'N' then 'NORMAL' when 'C' then 'CHECK PENDING' when 'X' then 'INOPERATIVE' end STATUS from syscat.tables where tabschema = '";
	    	tail = "' and tabname IN (";
	    	ends = ");" + linesep;
    	} else if (zdb2)
    	{
    		head = "SELECT SUBSTR(A.NAME,1,30) TABLE_NAME, CASE A.STATUS WHEN ' ' THEN 'COMPLETE' WHEN 'X' THEN 'COMPLETE' WHEN 'I' THEN 'SEE TABLE_STATUS2' WHEN 'R' THEN 'REGEN ERROR' ELSE 'UNKNOWN' END AS TABLE_STATUS1, " +
    				"CASE SUBSTR(A.TABLESTATUS,1,2) WHEN 'L' THEN 'AUX TABLE' WHEN 'P' THEN 'PRIMARY INDEX' WHEN 'R' THEN 'ROW ID INDEX' WHEN 'U' THEN 'UNIQUE KEY INDEX' WHEN 'V' THEN 'VIEW REGEN ERROR' WHEN ' ' THEN 'COMPLETE' " +
    				"ELSE 'UNKNOWN' END AS TABLE_STATUS2, CASE A.CHECKFLAG WHEN 'C' THEN 'RI/CHECK ERROR' WHEN ' ' THEN 'CONSISTENT' ELSE 'UNKNOWN' END AS CHECKFLAG, CASE B.STATUS WHEN 'A' THEN 'COMPLETE' WHEN 'C' THEN 'PI MISSING' " +
    				"WHEN 'P' THEN 'CHECK PEND' WHEN 'S' THEN 'CHECK PEND' WHEN 'T' THEN 'NO TABLES' ELSE 'UNKNOWN' END AS TS_STATUS FROM SYSIBM.SYSTABLES A, SYSIBM.SYSTABLESPACE B " +
    				"WHERE A.CREATOR = '";
    		tail = "' AND A.NAME IN (";
    		ends = ") AND B.NAME = A.TSNAME AND B.DBNAME = A.DBNAME;" + linesep;
    	} else
    	{
    		return;
    	}
    	
    	try
    	{
    		db2TabStatusWriter.write("--#SET :QUERY:TABLE:STATUS" + linesep);
    		prevSchema = t.dstSchemaName[0];   //assigning  with first schema 
	    	for (int i = 0; i < t.dstTableName.length; ++i)
	    	{
	    		skip = (i == 0 || sb.length() == 0) ? true : false;
	    		if (!skip && i < t.dstTableName.length)
	    		{
	    			if (prevSchema.equalsIgnoreCase(t.dstSchemaName[i]))
	    			sb.append(",");
	    		}
                if (prevSchema.equalsIgnoreCase(t.dstSchemaName[i]))
                {
            	    sb.append("'"+removeQuote(getCaseName(t.dstTableName[i]))+"'"); 
            	    if (sb.length() > 2000000)
            	    {
                        db2TabStatusWriter.write(head+removeQuote(getCaseName(prevSchema))+tail+sb.toString()+ends);             	    	
                        sb.setLength(0);	 
            	    }
                } else
                {
                	if (sb.length() > 0)
                       db2TabStatusWriter.write(head+removeQuote(getCaseName(prevSchema))+tail+sb.toString()+ends); 
                    sb.setLength(0);	 
                }
                tmpPrevSchema = prevSchema;
	            prevSchema = t.dstSchemaName[i];
	            if(!tmpPrevSchema.equalsIgnoreCase(t.dstSchemaName[i]))
	            {
	            	i--;
	            }
	    	}
	    	if (sb.length() > 0)
	    	{
                db2TabStatusWriter.write(head+removeQuote(getCaseName(prevSchema))+tail+sb.toString()+ends);
	    	}
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    private void genDB2Script() throws IOException
    {
    	int len = 40;
    	String runcmd = Constants.win() ? "%RUNCMD% " : "$RUNCMD ";
    	String runcmd2 = Constants.win() ? "%RUNCMD2% " : "$RUNCMD2 ";
    	String echo = Constants.win() ? "ECHO " : "echo ";
    	String taskNum = Constants.win() ? " %TASKNUM%" : "";
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep);
            buffer.append(":: This is the deployment script."+linesep);
            buffer.append("::"+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Deploy objects in "+Constants.getDbTargetName()+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep);
            buffer.append("IF \"%1%\" == \"\" ("+linesep);
            buffer.append("  SET TASKNUM="+linesep);
            buffer.append(") ELSE (" + linesep);
            buffer.append("  IF \"%1%\" == \"0\" (" + linesep);
            buffer.append("    SET TASKNUM="+linesep);
            buffer.append("  ) ELSE (" + linesep);
            buffer.append("    SET TASKNUM=[%1%]" + linesep);
            buffer.append("  )" + linesep);
            buffer.append(")" + linesep + linesep);
            
            buffer.append("SET JAVA_HOME=" + javaHome + linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);
            buffer.append("SET RUNCMDARGS1=\"%JAVA_HOME%\\bin\\java\"" + linesep);
            buffer.append("SET RUNCMDARGS2=-Xmx600m -DAppHome="+IBMExtractUtilities.FormatENVString(IBMExtractUtilities.getHomeDir()) + linesep);
            buffer.append("SET RUNCMDARGS3=-cp %CLASSPATH% ibm.DeployObjects" + linesep);
            buffer.append("SET RUNCMD=%RUNCMDARGS1% %RUNCMDARGS2% %RUNCMDARGS3%" + linesep);
            if (netezza)
            {
                buffer.append("SET RUNCMDARGS4=-cp %CLASSPATH% ibm.NZLoadStatus" + linesep);
                buffer.append("SET RUNCMD2=%RUNCMDARGS1% %RUNCMDARGS2% %RUNCMDARGS4%" + linesep);            	
            } else if (db2)
            {
                buffer.append("SET RUNCMDARGS4=-cp %CLASSPATH% ibm.FixCheck" + linesep);
                buffer.append("SET RUNCMD2=%RUNCMDARGS1% %RUNCMDARGS2% %RUNCMDARGS4%" + linesep);            	
            } else if (zdb2 && !zos)
            {
                buffer.append("SET RUNCMDARGS4=-cp %CLASSPATH% ibm.zOSLoader" + linesep);
                buffer.append("SET RUNCMDARGS5="+IBMExtractUtilities.FormatENVString(TABLES_PROP_FILE) + " " + IBMExtractUtilities.putQuote(colsep) + 
                		" " + threads + " " + commitCount + " " + String.valueOf(loadReplace) +  linesep);
                buffer.append("SET RUNCMD2=%RUNCMDARGS1% %RUNCMDARGS2% %RUNCMDARGS4% %RUNCMDARGS5%" + linesep);            	            	
            }
            buffer.append(linesep);
            buffer.append("ECHO START TIME=%DATE% %TIME% %TASKNUM% "+linesep+linesep);
        } else
        {
            buffer.append("#!"+shell+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep);
            buffer.append("# This is the deployment script."+linesep);
            buffer.append("#"+linesep);
            buffer.append("echo "+linesep);
            buffer.append("OUTPUT=${0%.*}.log"+linesep);
            
            if (ddlGen && dataUnload)
               buffer.append("echo $(date) Executing Script "+Constants.prefix+"gen.sh"+linesep+linesep);
            else if (ddlGen && !dataUnload)
                buffer.append("echo $(date) Executing Script "+Constants.prefix+"ddl.sh"+linesep+linesep);
            else if (!ddlGen && dataUnload)
                buffer.append("echo $(date) Executing Script "+Constants.prefix+"load.sh"+linesep+linesep);
            
            buffer.append("JAVA_HOME=" + IBMExtractUtilities.FormatENVString(javaHome) + linesep);
            buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);
            buffer.append("RUNCMDARGS1="+IBMExtractUtilities.FormatENVString("$JAVA_HOME/bin/java") + linesep);
            buffer.append("RUNCMDARGS2="+IBMExtractUtilities.FormatENVString("-Xmx600m -DAppHome="+IBMExtractUtilities.getHomeDir()) + linesep);
            buffer.append("RUNCMDARGS3="+IBMExtractUtilities.FormatENVString("-cp \"$CLASSPATH\" ibm.DeployObjects") + linesep);
            buffer.append("RUNCMD="+IBMExtractUtilities.FormatENVString("$RUNCMDARGS1 $RUNCMDARGS2 $RUNCMDARGS3") + linesep);
            if (netezza)
            {
                buffer.append("RUNCMDARGS4="+IBMExtractUtilities.FormatENVString("-cp \"$CLASSPATH\" ibm.NZLoadStatus") + linesep);
                buffer.append("RUNCMD2="+IBMExtractUtilities.FormatENVString("$RUNCMDARGS1 $RUNCMDARGS2 $RUNCMDARGS4") + linesep);            	
            } else if (db2)
            {
                buffer.append("RUNCMDARGS4="+IBMExtractUtilities.FormatENVString("-cp \"$CLASSPATH\" ibm.FixCheck") + linesep);
                buffer.append("RUNCMD2="+IBMExtractUtilities.FormatENVString("$RUNCMDARGS1 $RUNCMDARGS2 $RUNCMDARGS4") + linesep);            	
            } else if (zdb2 && !zos)
            {
                buffer.append("RUNCMDARGS4="+IBMExtractUtilities.FormatENVString("-cp \"$CLASSPATH\" ibm.zOSLoader") + linesep);
                buffer.append("RUNCMDARGS5="+IBMExtractUtilities.FormatENVString(TABLES_PROP_FILE + " " + IBMExtractUtilities.escapeUnixChar(colsep) + 
                		" " + threads + " " + commitCount + " " + String.valueOf(loadReplace)) +  linesep);
                buffer.append("RUNCMD2="+IBMExtractUtilities.FormatENVString("$RUNCMDARGS1 $RUNCMDARGS2 $RUNCMDARGS4 $RUNCMDARGS5") + linesep);            	            	
            }
            buffer.append(linesep);
        }
        
        if (ddlGen && dataUnload)
            buffer.append(echo +" Executing Script "+Constants.genScript+taskNum+linesep+linesep);
         else if (ddlGen && !dataUnload)
            buffer.append(echo +" Executing Script "+Constants.ddlScript+taskNum+linesep+linesep);
         else if (!ddlGen && dataUnload)
            buffer.append(echo + " Executing Script "+Constants.loadScript+taskNum+linesep+linesep);         

        if (ddlGen)
        {
        	if (db2Skin())
        	{
                buffer.append(fmtl(echo,"Running "+Constants.logins+taskNum, len, "- Create logins")+linesep);
                buffer.append(runcmd + Constants.logins+linesep+linesep);
                buffer.append(fmtl(echo,"Running "+Constants.groups+taskNum, len, "- Create groups")+linesep);
                buffer.append(runcmd + Constants.groups+linesep+linesep);
        	}
        	if (netezza)
        	{
                buffer.append(fmtl(echo,"Running "+Constants.tsbp+taskNum, len, "- Create databases")+linesep);
                buffer.append(runcmd + Constants.tsbp+linesep+linesep);
        	} else
        	{
        		if (db2)
        		{
                    buffer.append(fmtl(echo,"Running "+Constants.tsbp+taskNum, len, "- Create buffer pools and tablespaces")+linesep);
                    buffer.append(runcmd + Constants.tsbp+linesep+linesep);        			
        		}
                buffer.append(fmtl(echo,"Running "+Constants.udf+taskNum, len, "- Create UDFs")+linesep);
                buffer.append(runcmd + Constants.udf+linesep+linesep);
        	}
            buffer.append(fmtl(echo,"Running "+Constants.tables+taskNum, len, "- Create tables")+linesep);
            buffer.append(runcmd + Constants.tables+linesep+linesep);
            if (mainData.iDB2())
            {
                buffer.append(fmtl(echo,"Running "+Constants.views+taskNum, len, "- Create views")+linesep);
                buffer.append(runcmd + Constants.views+linesep+linesep);
            }
            buffer.append(fmtl(echo,"Running "+Constants.defalt+taskNum, len, "- Create defaults")+linesep);
            buffer.append(runcmd + Constants.defalt+linesep+linesep);
            if (!netezza)
            {
                buffer.append(fmtl(echo,"Running "+Constants.check+taskNum, len, "- Create check constraints")+linesep);
                buffer.append(runcmd + Constants.check+linesep+linesep);
                buffer.append(fmtl(echo,"Running "+Constants.indexes+taskNum, len, "- Create indexes")+linesep);
                buffer.append(runcmd + Constants.indexes+linesep+linesep);
            }
            buffer.append(fmtl(echo,"Running "+Constants.pkeys+taskNum, len, "- Create primary keys")+linesep);
            buffer.append(runcmd + Constants.pkeys+linesep+linesep);
        }
        if (extractObjects)
        {
            buffer.append(fmtl(echo,"Running "+Constants.sequences+taskNum, len, "- Create sequences")+linesep);
            buffer.append(runcmd + Constants.sequences+linesep+linesep);
            if (mainData.Oracle())
            {
                buffer.append(fmtl(echo,"Running "+Constants.synonyms+taskNum, len, "- Create synonyms")+linesep);
                buffer.append(runcmd + Constants.synonyms+linesep+linesep);
            }
        }
        if (dataUnload)
        {
            if (zdb2 && zos)
            {
               buffer.append("echo Cleaning up DISC, LERR and CERR datasets"+linesep);
               if (appJAR == null || appJAR.length() == 0)
               {
            	   appJAR = new File("..").getCanonicalPath() + "/IBMDataMovementTool.jar";
               }
               buffer.append("$RUNCMD -cp " + appJAR + " ibm.Cleanup" + linesep+linesep);                        
            }
        	if (loadException)
        	{
                buffer.append(fmtl(echo,"Running "+Constants.exceptiontables+taskNum, len, "- Create exception tables for load utility")+linesep);
                buffer.append(runcmd + Constants.exceptiontables+linesep+linesep);
        	}
        	if (zdb2 && !zos)
        	{
                buffer.append(fmtl(echo,"Dropping Foreign Keys"+taskNum, len, "- Required for Inserts")+linesep);
                buffer.append(runcmd + Constants.dropfkeys+linesep+linesep);        		        		
                buffer.append(fmtl(echo,"High Performance Inserts"+taskNum, len, "- Load the data in z/OS DB2")+linesep);
                buffer.append(runcmd2+linesep+linesep);        		        		
        	} else
        	{
                buffer.append(fmtl(echo,"Running "+Constants.load+taskNum, len, "- Load the data")+linesep);
                buffer.append(runcmd + Constants.load+linesep+linesep);        		
        	}
            if (netezza)
            {
                buffer.append(fmtl(echo,"Load Report "+taskNum, len, "- Report status of data load")+linesep);
                buffer.append(runcmd2 + linesep+linesep);
            }
            if (db2Skin())
            {
                buffer.append(fmtl(echo,"Running "+Constants.fixlobs+taskNum, len, "- Fix lobs")+linesep);
                buffer.append(runcmd + Constants.fixlobs+linesep+linesep);
            }
            if (db2 && !zos)
            {
                buffer.append(fmtl(echo,"Running FixCheck "+taskNum, len, "")+linesep);
                buffer.append(runcmd2 + IBMExtractUtilities.FormatENVString(TABLES_PROP_FILE)+linesep+linesep);                	
            }
            buffer.append(fmtl(echo,"Running "+Constants.tabcount+taskNum, len, "- Count rows from all tables")+linesep);
            buffer.append(runcmd + Constants.tabcount+linesep+linesep);
        	if (loadException)
        	{
                buffer.append(fmtl(echo,"Running "+Constants.excepttabcount+taskNum, len, "- Exception table row count")+linesep);
                buffer.append(runcmd + Constants.excepttabcount+linesep+linesep);
        	}
        	if (!netezza)
        	{
                buffer.append(fmtl(echo,"Running "+Constants.tabstatus+taskNum, len, "- Show status of tables after load")+linesep);
                buffer.append(runcmd + Constants.tabstatus+linesep+linesep);
        	}
        }

        if (ddlGen)
        {
        	if (REEL_NUM.equals("0"))
        	{
                buffer.append(fmtl(echo,"Running "+Constants.fkeys+taskNum, len, "- Create foreign keys")+linesep);
                buffer.append(runcmd + Constants.fkeys+linesep+linesep);
        	} else
        	{
                buffer.append(fmtl(echo,"Running "+Constants.fkeys+taskNum, len, "- is turned off")+linesep);
        	}
        }
        buffer.append(Constants.win() ? "ECHO END TIME  =%DATE% %TIME% %TASKNUM%"+linesep : "echo $(date)");
        buffer.append(Constants.win() ? "ECHO. "+linesep : "");
        buffer.append("echo Work Completed. Check "+ OUTPUT_DIR + filesep + "logs directory for errors"+linesep);
        buffer.append(Constants.win() ? "ECHO. "+linesep : "");
        buffer.append(Constants.win() ? ":END "+linesep : "");     
        db2ScriptWriter.write(buffer.toString());            
    }
    
    public void run()
    {
        if (ddlGen && !usePipe)
        {
        	if (netezza)
               genNZDatabases();
        	else if (db2)
               genDB2TSBP();
        }

        for (int i = 0; i < t.totalTables; i++)
        {
            int bladeIndex = i % threads;
            blades[bladeIndex].add(new Integer(i));
        }

        log("Starting Blades");
        for (int i = 0; i < threads; i++)
        {
            blades[i].start();
        }

        boolean done = false;
        while (!done)
        {
            done = true;
            for (int i = 0; i < threads; i++)
            {
                List queue = blades[i].getQueue();
                int size = -1;
                synchronized (queue)
                {
                    size = queue.size();
                }
                if (size > 0)
                {
                    done = false;
                    try
                    {
                        synchronized (empty)
                        {
                            empty.wait();
                        }
                    } catch (InterruptedException ex)
                    {
                        // ignore
                    }

                    break;
                }
            }
        }

        // Shut. It. Down!
        for (int i = 0; i < threads; i++)
        {
            try
            {
                Thread.sleep(250);
                blades[i].shutdown();
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }

        try
        {   
        	if (zos)
        		z.close();
            for (int id = 0; id < t.totalTables; ++id)
            {
                if (!zos && (dataUnload || usePipe || syncLoad)) 
                {
                	if (usePipe)
                	{
                    	if (Constants.win())
                    	{
        					Pipes.FlushFileBuffers(pipeHandles[id]);
        				    Pipes.CloseHandle(pipeHandles[id]);
        				    Pipes.DisconnectNamedPipe(pipeHandles[id]);
                    	} else
                    	{
                    		if (fc[id] != null)
        						fc[id].close();
                    	}	
                    	if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getCount() > 1)
                    	{
                        	// Block till multi table load is done 
                        	log("Blocking outer load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
                        	try
            				{
                        		if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread != null)
                        			t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.join();
                            	log("Released outer load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
            				} catch (InterruptedException e)
            				{
            					e.printStackTrace();
            				}
                    	}
                	} else if (syncLoad)
                	{
                		if (fp[id] != null)
    						fp[id].close();
                    	if (t.multiTables[id] > 0)
                    	{
                        	log("Starting outer load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
                        	try
            				{
                        		if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread != null)
                        		{
                        			t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.start();
                        		}
                            	log("Released outer load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
            				} catch (Exception e)
            				{
            					e.printStackTrace();
            				}
                    	}                    		
                	}
                	else
                	{
                		if (fp[id] != null)
    						fp[id].close();
                	}
                }
            }
            if (syncLoad)
            {
                for (int id = 0; id < t.totalTables; ++id)
                {
                	if (t.multiTables[id] > 0)
                	{
	                	// Block till multi table load is done 
	                	log("Waiting for completion of the outer load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	                	try
	    				{
	                		if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread != null)
	                		{
	                			t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.join();
	                		}
	                    	log("Done - Outer load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	    				} catch (InterruptedException e)
	    				{
	    					e.printStackTrace();
	    				}                	
                	}
                }            	
            }
            
            if (ddlGen)
            {
            	if (mainData.Mssql())
            	{
            		try
            		{
                		genSynonyms("");
            		} catch (Exception e)
            		{
            			e.printStackTrace();
            		}
            	}
            }
            if (dataUnload && !usePipe) 
            {
            	genTableStatus();
            	genTableCount();
            	if (loadException)
            	{
                	genExceptTableCount();
                	db2ExceptionTablesWriter.close();
                	db2ExceptTabCountWriter.close();
            	}
                db2LoadWriter.close();            		
                if (netezza)
                	nzLoadScript.close();
                if (db2Skin())
                	db2FixLobsWriter.close();
                if (db2 || netezza || (zdb2 && zos))
                   db2RunstatWriter.close();     
                if (!netezza)
                {
                    db2TabStatusWriter.close();
                }
                if (db2)
                {
                    db2LoadTerminateWriter.close();                	                	
                }
                if (db2 || (zdb2 && zos))
                {
                    db2CheckPendingWriter.close();                	
                }
                db2TabCountWriter.close(); 
            }
            if (extractObjects)
            {
                log("Starting extract of other metadata. Please wait ....");
                if (!db2_compatibility)
                	log("db2_compatibility is turned off. You may see limited objects extracted.");
            	genAllSequences();
                String[] schemaArray = selectSchemaName.split(":");
            	if (schemaArray.length > 0)
            	{
	            	if (mainData.Oracle())
	            	{
		                genAllSequences();
	            		if (db2_compatibility)
	            		{
		            		genDirectories();
		                	genRoles();
		                	openPLSQLFiles();
			            	for (int idx = 0; idx < schemaArray.length; ++idx)
			            	{
			            	   if (debug) log("Starting extract for schema " + removeQuote(schemaArray[idx]));
			            	   genSynonyms(removeQuote(schemaArray[idx]));
			            	   genPrivs(removeQuote(schemaArray[idx]), "GRANTEE");	
			            	   genPrivs(removeQuote(schemaArray[idx]), "GRANTOR");	
		            		   genViews(removeQuote(schemaArray[idx]));
		            		   genMaterializedViews(removeQuote(schemaArray[idx]));
		            		   genPLSQL(removeQuote(schemaArray[idx]));
		            		   if (majorSourceDBVersion <= 8)
		            			   genTriggers(removeQuote(schemaArray[idx])); 
			            	}
			            	closePLSQLFiles();
			            	if (regenerateTriggers)
			            	   regenerateTriggers();
	            		}
	            	}
	            	else if (mainData.iDB2())
	            	{
	                	openPLSQLFiles();
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug)
		            	   {
		            		   log("Starting extract for schema " + removeQuote(schemaArray[idx]));
		            	   }
		            	   genSQLFunctions(removeQuote(schemaArray[idx]));
	            		   genViews(removeQuote(schemaArray[idx]));
	            		   genSQLPL(removeQuote(schemaArray[idx]));
	            		   genTriggers(removeQuote(schemaArray[idx]));
		            	}
		            	closePLSQLFiles();
	            	}
	            	else if (mainData.zDB2())
	            	{
	            		genAllSequences();
	                	openPLSQLFiles();
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug)
		            	   {
		            		   log("Starting extract for schema " + removeQuote(schemaArray[idx]));
		            	   }
		            	   genSQLFunctions(removeQuote(schemaArray[idx]));
	            		   genViews(removeQuote(schemaArray[idx]));
	            		   genSQLPL(removeQuote(schemaArray[idx]));
	            		   genTriggers(removeQuote(schemaArray[idx]));
		            	}
		            	closePLSQLFiles();
	            	}
	            	else if (mainData.DB2())
	            	{
	            		genAllSequences();
	                	openPLSQLFiles();
	            		genInstallJavaJars();
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug)
		            	   {
		            		   log("Starting extract for schema " + removeQuote(schemaArray[idx]));
		            	   }
		            	   genMaterializedViews(removeQuote(schemaArray[idx]));
		            	   genDB2GVariables(removeQuote(schemaArray[idx]));
		            	   genDB2Grants(removeQuote(schemaArray[idx]));
	            		   getNonSQLProcedureSource(removeQuote(schemaArray[idx]));
		            	   genDB2Types(removeQuote(schemaArray[idx]));
		            	   genDB2XSRSchema(removeQuote(schemaArray[idx]));
	            		   genTriggers(removeQuote(schemaArray[idx]));
	            		   genViews(removeQuote(schemaArray[idx]));
	            		   genSQLPL(removeQuote(schemaArray[idx]));
	            		   genSQLModules(removeQuote(schemaArray[idx]));
		            	}
		            	closePLSQLFiles();
	            	}
	            	else if (mainData.Mssql())
	            	{
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug) log("Starting extract for schema " + removeQuote(schemaArray[idx]));
	            		   genViews(removeQuote(schemaArray[idx]));
		            	}
	            	}
	            	else if (mainData.Mysql())
	            	{
	                	openPLSQLFiles();
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug)
		            	   {
		            		   log("Starting extract for schema " + removeQuote(schemaArray[idx]));
		            	   }
	            		   genTriggers(removeQuote(schemaArray[idx]));
	            		   genViews(removeQuote(schemaArray[idx]));
		            	   genSQLFunctions(removeQuote(schemaArray[idx]));
		            	}
		            	closePLSQLFiles();
	            	}
	            	else if (db2Skin())
	            	{
	                	openPLSQLFiles();
		            	genSybaseUsersGroups();
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug)
		            	   {
		            		   log("Starting extract for schema " + removeQuote(schemaArray[idx]));
		            	   }
		            	   genSybaseObjects(removeQuote(schemaArray[idx]), "P", "PROCEDURE");
		            	   genSybaseObjects(removeQuote(schemaArray[idx]), "V", "VIEW");
		            	   genSybaseObjects(removeQuote(schemaArray[idx]), "TR", "TRIGGER");
		            	   genSybaseDefaultValues(removeQuote(schemaArray[idx]));
		            	   genSybaseGrants(removeQuote(schemaArray[idx]));
		            	}
		            	closePLSQLFiles();
	            	}
	            	else if (mainData.Teradata())
	            	{
	                	openPLSQLFiles();
		            	for (int idx = 0; idx < schemaArray.length; ++idx)
		            	{
		            	   if (debug)
		            	   {
		            		   log("Starting extract for schema " + removeQuote(schemaArray[idx]));
		            	   }
	            		   genViews(removeQuote(schemaArray[idx]));
		            	}
		            	closePLSQLFiles();
	            	}
            	} else
            	{
            		log("No schema were selected for extraction of objects");
            	}
            } else
                log("Extraction of other metadata is turned off.");
            if (extractObjects)
            {
                db2DropObjectsWriter.close();
                if (db2TempTablesWriter != null)
                   db2TempTablesWriter.close();
                db2ViewsWriter.close();
                db2SeqWriter.close();
                db2DropSeqWriter.close();
                if (mainData.Oracle() || mainData.DB2() || mainData.Mssql() || mainData.zDB2())
                {
                    db2SynonymWriter.close();
                    db2DropSynWriter.close();                   
                }
                if (mainData.Oracle())
                {
                   db2rolePrivsWriter.close();
                   db2objPrivsWriter.close();
                }
            }
            if (ddlGen && !usePipe)
            {
                db2TablesWriter.close();
                db2FKWriter.close();
                db2DropWriter.close();
                db2DropExpTablesWriter.close();
                db2PKeysWriter.close();
                db2FKDropWriter.close();
                if (!netezza)
                {
                   db2CheckWriter.close();
                   db2IndexesWriter.close();
                }
                if (mainData.Oracle() || mainData.DB2())
                {
                    db2mviewsWriter.close();
                }
                if (mainData.Sybase())
                {
                    db2objPrivsWriter.close();
                    if (db2_compatibility)
                    {
                    	db2LoginWriter.close();
                    	db2GroupsWriter.close();
                    }
                }
                if (!netezza)
                {
                    db2udfWriter.close();
                }
                db2TruncNameWriter.close();                	
                if (db2 || netezza)
                {
                   db2tsbpWriter.close();
                   db2droptsbpWriter.close();
                }
                db2DefaultWriter.close();
            }
            if ((dataUnload || ddlGen) && !usePipe)
            {
                genDB2DropScript();
                db2DropScriptWriter.close();
            	if (db2)
            	{
            	   genDB2CheckScript();
            	   db2CheckScriptWriter.close();
            	}
                genDB2Script();
                db2ScriptWriter.close();
            }
        	try
			{
        		if (mainData.connection != null)
        			if (!mainData.Informix())
        				mainData.connection.commit();
        		mainData.connection.close();
	            for (int i = 0; i < threads; ++i)
	            {
	            	if (blades[i].data.connection != null)
	            		if (!mainData.Informix())
	            		   blades[i].data.connection.commit();
					blades[i].data.connection.close();
	            }
			} catch (SQLException e)
			{
				e.printStackTrace();
			}
        } catch (IOException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        if (mainData.iDB2() && !usePipe)
        {
    	    fixiDB2Code("db2function.db2");
    	    fixiDB2Code("db2views.db2");
    	    fixiDB2Code("db2procedure.db2");
    	    fixiDB2Code("db2trigger.db2");
        }
        if (usePipe || syncLoad)
        {
        	String logFileName = "";
        	if (netezza)
        	{
        		logFileName = IBMExtractUtilities.combinePipeLogs(OUTPUT_DIR + "nzlog" + filesep, ".nzlog", "unload");
	            log("Check file "+logFileName+" for the LOAD status");
	        	IBMExtractUtilities.deleteFiles(OUTPUT_DIR + "nzlog" + filesep, ".nzlog");  
	        	try
				{
					PrintStream ps = new PrintStream(new FileOutputStream(OUTPUT_DIR + "nzloadstatus.log", false));
					NZLoadStatus ns = new NZLoadStatus(ps, logFileName);
					ns.reportFromFile();
				} catch (FileNotFoundException e)
				{
					e.printStackTrace();
				} catch (Exception e)
				{
					e.printStackTrace();
				}
        	} else
        	{
	        	IBMExtractUtilities.checkTargetTablesStatus(null, loadException, caseSensitiveTabColName, exceptSchemaSuffix, exceptTableSuffix, TABLES_PROP_FILE, Constants.getDbTargetName(), dstServer, dstPort, dstDBName, dstUid, dstPwd);
	    		if (IBMExtractUtilities.Message.length() > 0)
	    		{
	    			log("Running db2removecheckforpipeload.db2 to remove check pending status. Please wait .... ");
					new RunDB2Script(null, dstDB2Home, OUTPUT_DIR + "db2removecheckforpipeload.db2", false).run();				   
	    		}		
	    		logFileName = IBMExtractUtilities.combinePipeLogs(OUTPUT_DIR, ".loadlog", "unload");
	            log("Check file "+logFileName+" for the LOAD status");
	        	IBMExtractUtilities.deleteFiles(OUTPUT_DIR, ".loadlog");
	        	if (usePipe)
	        	   IBMExtractUtilities.deleteFiles(OUTPUT_DIR, "pipeload.sql");
	        	else
	        	   IBMExtractUtilities.deleteFiles(OUTPUT_DIR, "syncload.sql");
        	}
        }
        if (REEL_NUM.equals("0"))
           log("Work completed");
        else
           log("Work("+REEL_NUM+") completed");
    }

    public static class TableInfo
    {
    	public Thread loadThread = null;
    	private int id, dupID = 0;
    	private int countParalleltable = 1;
    	private int initalThreadID;
    	
    	public TableInfo(int id)    	
    	{
    		this.id = id;
    	}
    	
    	public void setInitialThreadID(int initalThreadID)
    	{
    		this.initalThreadID = initalThreadID;
    	}
    	
    	public int getInitialThreadID()
    	{
    		return initalThreadID;
    	}
    	
    	public int getCount()
    	{
    		return countParalleltable;
    	}
    	
    	public void incrTableCount()
    	{
    		++countParalleltable;
    	}

    	public int getID()
    	{
    		return id;
    	}

    	public int getDupID()
    	{
    		return dupID;
    	}
    	
    	public void setDupID(int id)
    	{
    		this.dupID = id;
    	}
    }
    
    public class UDT
    {
    	public String userType, baseType;
    	
    	public UDT(String userType, String baseType)
    	{
    		this.userType = userType;
    		this.baseType = baseType;
    	}
    	
    	public String toString()
    	{
    		return "userType="+userType+":baseType="+baseType;
    	}
    }
    
    public class BladeRunner extends Thread
    {
    	private DumpData dd;
    	private BlobVal bval;
    	private DBData data, dstdata;    	
        private int number;
        private long increment_value, last_value;
        private ArrayList queue = new ArrayList();
        private boolean done = false;
        private byte[] binaryData = new byte[33000];        
        private java.io.ByteArrayOutputStream byteOutputStream = new java.io.ByteArrayOutputStream(36000);
        private java.io.ByteArrayOutputStream fileBuffer = new java.io.ByteArrayOutputStream(bytesBufferSize);
                
    	private DatabaseMetaData dmMetaData = null;
    	
        public BladeRunner(int number, String vendor)
        {
        	int majorDBVersion = -1;
            this.number = number;
        	data = new DBData(vendor, server, port, dbName, uid, pwd, sybaseConvClass, number);
    		if (data.Sybase())
    			data.setAutoCommit(true);
        	data.getConnection();
        	dstdata = new DBData(Constants.getDbTargetName(), dstServer, dstPort, dstDBName, dstUid, dstPwd, "", number);
        	dstdata.setAutoCommit(true);

        	try
			{
        		dmMetaData = data.connection.getMetaData();        			
			} catch (SQLException e)
			{
				e.printStackTrace();
			}
        	
        	if (data.connection == null)
                System.exit(-1);
        	
            dd = new DumpData(data, colsep.charAt(0), fetchSize, netezza, 
            		propDataMapNZ, propDataMap, udtProp,  
            		t, oracleNumberMapping, dataUnload, usePipe, graphic, dbclob, db2_compatibility, lobsToFiles,
            		mapTimeToTimestamp, convertOracleTimeStampWithTimeZone2Varchar, mapCharToVarchar, customMapping, releaseLevel, encoding, varcharLimit);
        }

        public void add(Object call)
        {
            synchronized (queue)
            {
                queue.add(call);
                queue.notify();
            }
        }

        public List getQueue()
        {
            return queue;
        }

        public void shutdown()
        {
            done = true;
            synchronized (queue)
            {
                queue.notifyAll();
            }
        }

        public void run()
        {
            setName("Blade_" + number);
            log("Starting " + Thread.currentThread().getName() + " and building memory map.");
            BuildBladeMemoryMap();

            long count = 0;
            long last = System.currentTimeMillis();
            while (!done)
            {
                int size = -1;
                Object call = null;

                synchronized (queue)
                {
                    call = queue.get(0);                    
                }
                
                if (call != null)
                {
                    int key = ((Integer) call).intValue();
                    count = processTable(key);
                    if (debug) IBMExtractUtilities.RunTimeReport();
                    try
					{
						byteOutputStream.close();
					} catch (IOException e)
					{
						e.printStackTrace();
					}
                    String pingStr = ping.toString();
                    if (pingStr != null && pingStr.length() > 0)
                    {
                    	pingStr = " Latency " + pingStr;
                    }
                    long now = System.currentTimeMillis();
                    String timeStr = IBMExtractUtilities.getElapsedTime(last);
                    if ((dataUnload && ddlGen) || (dataUnload && !ddlGen) || (ddlGen && (usePipe || syncLoad))) 
                       log( "Blade_" + number + " unloaded " + count + " rows in " + timeStr + " " + removeQuote(t.srcSchName[key]) + "." + removeQuote(t.srcTableName[key]) + " " + t.partition[key] + getJobsStatus() + pingStr);
                    else if (!dataUnload && ddlGen && !(usePipe || syncLoad))
                       log( "Blade_" + number + " DDL in " + timeStr + " " + removeQuote(t.srcSchName[key]) + "." + removeQuote(t.srcTableName[key]) + " " + t.partition[key] + getJobsStatus() + pingStr);
                    last = now;
                    count = 0;
                }

                synchronized (queue)
                {
                    size = queue.size();
                    if (size > 0)
                    {
                        call = queue.remove(0);
                        size--;
                    }
                }

                if (size < 1)
                {
                    synchronized (empty)
                    {
                        empty.notify();
                    }
                }

                try
                {
                    synchronized (queue)
                    {
                        if (queue.size() < 1)
                        {
                            queue.wait();
                        }
                    }
                } catch (InterruptedException ex)
                {
                    log("interrupted: " + ex);
                }
            }
            log("done " + Thread.currentThread().getName());
        }
        
        private String getDeleteRule(int rule)
        {
            String tmp = "";
            if (rule == dmMetaData.importedKeyCascade) // 0
               tmp = "CASCADE";
            else if (rule == dmMetaData.importedKeyNoAction) // 3
                tmp = "NO ACTION";
            else if (rule == dmMetaData.importedKeyRestrict) // 1
                tmp = "RESTRICT";
            else if (rule == dmMetaData.importedKeySetNull) // 2
                tmp = "SET NULL";
            else 
                tmp = "";
            return tmp;
        }
        
        private String getUpdateRule(int rule)
        {
            String tmp = "";
            if (rule == dmMetaData.importedKeyNoAction) // 3
                tmp = "NO ACTION";
            else if (rule == dmMetaData.importedKeyRestrict) // 1
                tmp = "RESTRICT";
            else 
                tmp = "";
            return tmp;
        }
        
        
        private void genDB2UDFs() throws Exception
        {
            String  udf = "CREATE FUNCTION DB2.NEWGUID() " + linesep +
                          "RETURNS CHAR(32) " + linesep +
                          "NOT DETERMINISTIC " + linesep +
                          "RETURN hex(generate_unique()) || hex(CHR(CAST(RAND()*255 AS SMALLINT))) || " + linesep + 
                          "       hex(CHR(CAST(RAND()*255 AS SMALLINT))) || " + linesep + 
                          "       hex(CHR(CAST(RAND()*255 AS SMALLINT))) " + linesep +
                          "; " + linesep;
            db2udfWriter.write(udf);
        }
        
        private void genSybFkeys(int id) throws SQLException, IOException
        {
        	Hashtable fkHash = new Hashtable();
        	String schema = removeQuote(t.srcSchName[id]); 
            String key, table = removeQuote(t.srcTableName[id]);
            StringBuffer buffer = new StringBuffer();            
            StringBuffer dropkey = new StringBuffer();            
            String pkTableSchema = "", pkTableName = "", pkColumnName = "", fkTableSchema = "", fkTableName = "", fkColumnName = "", fkConsName = "";
            int keyseq, updateRule, deleteRule;
            String upd, del, sql;
            FKData fkData;
            
            buffer.setLength(0);
            dropkey.setLength(0);
            
            key = removeQuote(t.srcSchName[id]) + "." + removeQuote(t.srcTableName[id]);
            
            if (fkMap.containsKey(key))
            {
            	Iterator itr = ((ArrayList) fkMap.get(key)).iterator();
                while (itr.hasNext())
                {                
            		fkData = (FKData) itr.next();
                	fkConsName = fkData.fkName;
                	if (fkConsName != null) 
                		fkConsName = getCaseName(fkConsName);
                	else
                		fkConsName = "" + numFkey;
                	if (fkHash.containsKey(fkConsName))
                	{
                		buffer = (StringBuffer) fkHash.get(fkConsName);
                	} else
                	{
                		buffer = new StringBuffer();
                		fkHash.put(fkConsName, buffer);
                	}
                	pkTableSchema = fkData.pkTableSchema;
                    pkTableName = fkData.pkTableName;
                    pkColumnName = fkData.pkColumnName;
                    fkTableSchema = fkData.fkTableSchema;
                    if (fkTableSchema == null)
                    	fkTableSchema = schema;
                    fkTableName = fkData.fkTableName;
                    fkColumnName = fkData.fkColumnName;
                    keyseq = fkData.keySeq;
                    updateRule = fkData.updateRule;
                    deleteRule = fkData.deleteRule;
                    if (keyseq == 1)
                    {
                        buffer.setLength(0);
    		            buffer.append("--#SET :" + "FOREIGN_KEYS" + ":" + fkTableSchema + ":" + fkConsName + linesep);           	
                        buffer.append("ALTER TABLE " + fkTableSchema + "." + fkTableName + linesep);
                        buffer.append("ADD CONSTRAINT " + putQuote(fkConsName) + " FOREIGN KEY" + linesep);
                        buffer.append("(" + linesep);
                        buffer.append(fkColumnName+",abcxyz");
                        buffer.append(linesep);
                        buffer.append(")" + linesep);
                        buffer.append("REFERENCES " + pkTableSchema + "." + pkTableName + "" + linesep);
                        buffer.append("(" + linesep);
                        buffer.append(pkColumnName+",xyzabc");
                        buffer.append(linesep);
                        buffer.append(")" + linesep);
                        upd = getUpdateRule(updateRule);
                        del = getDeleteRule(deleteRule);
                        buffer.append("go" + linesep);

                        dropkey.setLength(0);
                        dropkey.append("ALTER TABLE " + fkTableSchema + "." + fkTableName + "" + linesep);
                        dropkey.append("DROP CONSTRAINT " + putQuote(fkConsName) + linesep);
                        dropkey.append("go" + linesep);
                        dropkey.append(linesep);
                        db2FKDropWriter.write("--#SET :DROP FOREIGN KEY:"+fkTableSchema+":"+fkConsName+linesep);
                        db2FKDropWriter.write(dropkey.toString());
                    } else
                    {
                        buffer.insert(buffer.indexOf("abcxyz")-1,","+fkColumnName);
                        buffer.insert(buffer.indexOf("xyzabc")-1,","+pkColumnName);
                    }
                }
                Iterator iter = fkHash.keySet().iterator();
                while (iter.hasNext())
                {
                	key = (String) iter.next();
                	buffer = (StringBuffer) fkHash.get(key);
                    int start1 = buffer.indexOf(",abcxyz");
                    buffer = buffer.delete(start1,start1+7);
                    int start2 = buffer.indexOf(",xyzabc");
                    buffer = buffer.delete(start2,start2+7);
                    db2FKWriter.write(buffer.toString());
                }
            }
        }
        
        private void BuildFKeysMap()
        {
        	ArrayList al = new ArrayList();
        	FKData fk;
            String methodName = "BuildFKeysMap";
            String key = "", sql = "";
            Integer count = 0; 
            String prevSchema = "", prevTable = "", schema, table;
            boolean added = false;

            fkMap = new HashMap();
            
            if (data.Oracle())
            {
            	sql = "SELECT NULL AS PKTABLE_CAT, t3.owner AS PKTABLE_SCHEM, t3.table_name AS PKTABLE_NAME, " +
		      	      "t3.column_name AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, t1.owner AS FKTABLE_SCHEM, " +
		      	      "t1.table_name AS FKTABLE_NAME, t1.column_name AS FKCOLUMN_NAME, " +
		      	      "t1.position AS KEY_SEQ, decode(t4.delete_rule,'CASCADE', 0, 'NO ACTION', 3, 'RESTRICT', 1, 'SET NULL', 2, 0) AS UPDATE_RULE, " +
		      	      "decode(t2.delete_rule,'CASCADE', 0, 'NO ACTION', 3, 'RESTRICT', 1, 'SET NULL', 2, 0) AS DELETE_RULE, " +
		      	      "t1.constraint_name AS FK_NAME, t3.constraint_name AS PK_NAME, NULL AS DEFERRABILITY " +
		      	      "from dba_cons_columns t1, dba_constraints t2, dba_cons_columns t3, dba_constraints t4 " +
		      	      "where t1.table_name = t2.table_name " +
		      	      "and t1.constraint_name = t2.constraint_name " +
		      	      "and t1.owner = t2.owner " +
		      	      "and t2.CONSTRAINT_TYPE  = 'R' " +
		      	      "and t4.constraint_name = t2.r_constraint_name " +
		      	      "and t4.OWNER = t2.R_OWNER " +
		      	      "and t4.table_name = t3.table_name " +
		      	      "and t4.CONSTRAINT_NAME = t3.CONSTRAINT_NAME " +
		      	      "and t4.owner = t3.owner " +
		      	      "and t1.position = t3.position " +
		      	      "and t1.owner IN ("+schemaList+") " +
		      	      "order by t1.owner, t1.table_name, t1.constraint_name, t1.position";
            }
            
            try
            {
	            SQL s = new SQL(data.connection);
	            if (data.Oracle())
	            {
		            s.PrepareExecuteQuery(methodName, sql);		
		            int i = 0;
		        	while (s.next())
		        	{
		        		schema = trim(s.rs.getString(6));
		        		table = trim(s.rs.getString(7));
		        		if (i == 0)
		        		{
		        			prevSchema = schema;
		        			prevTable = table;
		        		}
		        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
		        		{
		        			fkMap.put(key, al);
		        			al = new ArrayList();
		        			added = true;
		        		}
		        		key = schema + "." + table;
	        		    al.add(new FKData(s.rs));
	        		    added = false;
	        			prevSchema = schema;
	        			prevTable = table;
		        		++i;
		        	}
		        	if (!added && key.length() > 0)
		        	{
	        			fkMap.put(key, al);	        		
		        	}
		        	s.close(methodName);
		            log(i + " values cached in fkMap");
	            } else
	            {
	            	int cnt = 0;
	            	for (int i = 0;  i < t.totalTables; ++i)
	            	{
	                	schema = removeQuote(t.srcSchName[i]);
	            		table = removeQuote(t.srcTableName[i]);
	                    if (data.Postgres())
	                    {
	                        table = table.toLowerCase();
	                    }
	            		key = removeQuote(schema) + "." + removeQuote(table);
		                if (data.DB2())
		                {
		                    s.rs = dmMetaData.getImportedKeys("", schema, table);                
		                }
		                else if (data.zDB2())
		                {
		                	s.rs = dmMetaData.getImportedKeys("", schema, table);                
		                }
		                else if (data.Teradata())
		                {
		                	s.rs = dmMetaData.getImportedKeys("", schema, table);                
		                }
		                else if (data.iDB2())
		                {
		                	String catalogName = data.connection.getCatalog();
		                	s.rs = dmMetaData.getImportedKeys(catalogName, schema, table);                
		                }
		                else if (data.Hxtt())
		                {
		                	s.rs = dmMetaData.getImportedKeys(null, null, table);                                
		                } else if (data.Informix()) 
		                {
		                	s.rs = dmMetaData.getImportedKeys(dbName, null, table);                                            	
		                }
		                else if (data.Mssql()) 
		                {
		                	s.rs = dmMetaData.getImportedKeys(dbName, schema, table);                                
		                }
		                else
		                {
		                	s.rs = dmMetaData.getImportedKeys(dbName, null, table);                                
		                }
		                while (s.next())
		                {
		        		    al.add(new FKData(s.rs));
		        		    ++cnt;
		                }
		                if (al.size() > 0)
		                {
	                	    fkMap.put(key, FKData.Sort(al));
	                	    al = new ArrayList();
		                }
	            	}
		            log(cnt + " values cached in fkMap");
	            }
            }
            catch (SQLException e)
    	    {
    		   e.printStackTrace();
    	    }
        }
        
        private void genFKeys(int id) throws SQLException, IOException
        {
        	if (db2Skin())
        	{
        		genSybFkeys(id);
        		return;
        	}
        	Hashtable fkHash = new Hashtable();
            String schema = removeQuote(t.srcSchName[id]); 
            String dstSchema = getCaseName(t.dstSchemaName[id]);
            String key, table = "";
            StringBuffer buffer;            
            StringBuffer dropkey = new StringBuffer();            
            String pkTableSchema = "", pkTableName = "", pkColumnName = "", fkTableSchema = "", fkTableName = "", fkColumnName = "", fkConsName = "";
            String pkTableSchema2 = "", pkTableName2 = "", pkColumnName2 = "", fkTableSchema2 = "", fkTableName2 = "", fkColumnName2 = "";
            int keyseq, updateRule, deleteRule;
            String upd, del, sql;
            FKData fkData;
            
            dropkey.setLength(0);
            key = removeQuote(t.srcSchName[id]) + "." + removeQuote(t.srcTableName[id]);
            
            if (fkMap.containsKey(key))
            {
            	Iterator itr = ((ArrayList) fkMap.get(key)).iterator();
            	while (itr.hasNext())
            	{
            		fkData = (FKData) itr.next();
                	fkConsName = fkData.fkName;
                	if (fkConsName != null) 
                		fkConsName = getCaseName(fkConsName);
                	else
                		fkConsName = "" + numFkey;
                	if (fkHash.containsKey(fkConsName))
                	{
                		buffer = (StringBuffer) fkHash.get(fkConsName);
                	} else
                	{
                		buffer = new StringBuffer();
                		fkHash.put(fkConsName, buffer);
                	}
                	pkTableSchema2 = fkData.pkTableSchema;
                	if (pkTableSchema2 == null)
                		pkTableSchema2 = schema;
                	pkTableSchema = getCaseName(pkTableSchema2);
                    pkTableName2 = fkData.pkTableName;
                    pkTableName = pkTableName2;
                    if (pkTableName != null)
                    	pkTableName = getCaseName(pkTableName);
                    pkColumnName2 = fkData.pkColumnName;
                    pkColumnName = getTruncName(0, "", getCaseName(pkColumnName2),30);                	
                    fkTableSchema2 = fkData.fkTableSchema;
                    if (fkTableSchema2 == null)
                    	fkTableSchema2 = schema;
                    fkTableSchema = getCaseName(fkTableSchema2);
                    fkTableName2 = fkData.fkTableName;
                    fkTableName = getCaseName(fkTableName2);
                    fkColumnName2 = fkData.fkColumnName;
                    fkColumnName = getTruncName(0, "", getCaseName(fkColumnName2),18);                	
                    if (data.Teradata())
                        keyseq = fkData.keySeq + 1;
                    else
                        keyseq = fkData.keySeq;
                    updateRule = fkData.updateRule;
                    deleteRule = fkData.deleteRule;
                    if (keyseq == 1)
                    {
                        buffer.setLength(0);
                        String tmp = removeQuote(t.dstTableName[id]);
                        tmp = FixString(tmp);
                        String newFKConsName = getTruncName(0, "", "FK" + numFkey + "_" + tmp,18);
                        if (retainConstraintsName)
                        {
                        	if (!(fkConsName == null || fkConsName.length() == 0))
                        	{
                        		newFKConsName = fkConsName;
                        	}
                        }
    		            buffer.append("--#SET :" + "FOREIGN_KEYS" + ":" + removeQuote(getDstSchema(fkTableSchema)) + ":" + newFKConsName + linesep);           	
                        buffer.append("ALTER TABLE \"" + removeQuote(getDstSchema(fkTableSchema)) + "\".\"" + fkTableName + "\"" + linesep);
                        buffer.append("ADD CONSTRAINT " + putQuote(newFKConsName) + " FOREIGN KEY" + linesep);
                        buffer.append("(" + linesep);
                        buffer.append("\""+getCaseName(getCustomColumnName(fkTableSchema2, fkTableName2, fkColumnName2))+"\",abcxyz");
                        buffer.append(linesep);
                        buffer.append(")" + linesep);
                        buffer.append("REFERENCES " + putQuote(getDstSchema(pkTableSchema)) + ".\"" + pkTableName + "\"" + linesep);
                        buffer.append("(" + linesep);
                        buffer.append("\""+getCaseName(getCustomColumnName(pkTableSchema2, pkTableName2, pkColumnName2))+"\",xyzabc");
                        buffer.append(linesep);
                        buffer.append(")" + linesep);
                        upd = getUpdateRule(updateRule);
                        del = getDeleteRule(deleteRule);
                        if (db2 || netezza)
                          if (!upd.equals("")) 
                             buffer.append("ON UPDATE " + upd + linesep);
                        if (!del.equals("")) buffer.append("ON DELETE " + del + linesep);
                        buffer.append(";" + linesep);

                        dropkey.setLength(0);
                        dropkey.append("ALTER TABLE \"" + removeQuote(getDstSchema(fkTableSchema)) + "\".\"" + fkTableName + "\"" + linesep);
                        dropkey.append("DROP CONSTRAINT " + newFKConsName + (netezza ? " CASCADE" : "") + linesep);
                        dropkey.append(";" + linesep);
                        dropkey.append(linesep);
                        db2FKDropWriter.write("--#SET :DROP FOREIGN KEY:"+ removeQuote(getDstSchema(fkTableSchema)) + ":" + newFKConsName+linesep);
                        db2FKDropWriter.write(dropkey.toString());
                        numFkey++;
                    } else
                    {
                        buffer.insert(buffer.indexOf("abcxyz")-1,","+putQuote(getCaseName(getCustomColumnName(fkTableSchema2, fkTableName2, fkColumnName2))));
                        buffer.insert(buffer.indexOf("xyzabc")-1,","+putQuote(getCaseName(getCustomColumnName(pkTableSchema2, pkTableName2, pkColumnName2))));
                    }
            	}
            }            
            Iterator iter = fkHash.keySet().iterator();
            while (iter.hasNext())
            {
            	key = (String) iter.next();
            	buffer = (StringBuffer) fkHash.get(key);
                int start1 = buffer.indexOf(",abcxyz");
                buffer = buffer.delete(start1,start1+7);
                int start2 = buffer.indexOf(",xyzabc");
                buffer = buffer.delete(start2,start2+7);
                db2FKWriter.write(buffer.toString());
            }
        }
        
        private void BuildIndexMapAllCount()
        {
            String methodName = "BuildIndexMapAllCount";
            String key, sql = "";
            Integer count = 0; 
            
            uniqueIndexAllMap = new HashMap();
            
            if (data.Oracle())
            {
            	sql = "SELECT C.TABLE_OWNER, C.TABLE_NAME, C.INDEX_NAME, COUNT(*) FROM DBA_TAB_COLUMNS A, DBA_IND_COLUMNS C " +
	    			"WHERE A.OWNER = C.TABLE_OWNER " +
	    			"AND A.TABLE_NAME = C.TABLE_NAME " +
	    			"AND A.COLUMN_NAME = C.COLUMN_NAME " +
	    			"AND C.TABLE_OWNER IN (" + schemaList + ") " +
	    			"GROUP BY C.TABLE_OWNER, C.TABLE_NAME, C.INDEX_NAME";
            }

            if (sql.length() == 0)
         	   return;
            
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);        	
	
	            int i = 0;
	            while (s.next())
	            {
	            	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2)) + "." + trim(s.rs.getString(3));
	                count = s.rs.getInt(4);
	                uniqueIndexAllMap.put(key, count);
	                ++i;
	            }
	            s.close(methodName);
	            log(i + " values cached in uniqueIndexAllMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }        	        	
        }
        
        private void BuildIndexMapNullCount()
        {
            String methodName = "BuildIndexMapNullCount";
            String key, sql = "";
            Integer count = 0; 
            
            uniqueIndexNullMap = new HashMap();
            
            if (data.Oracle())
            {
            	sql = "SELECT C.TABLE_OWNER, C.TABLE_NAME, C.INDEX_NAME, COUNT(*) FROM DBA_TAB_COLUMNS A, DBA_IND_COLUMNS C " +
	    			"WHERE A.OWNER = C.TABLE_OWNER " +
	    			"AND A.TABLE_NAME = C.TABLE_NAME " +
	    			"AND A.COLUMN_NAME = C.COLUMN_NAME " +
	    			"AND C.TABLE_OWNER IN (" + schemaList + ") " +
	    			"AND A.NULLABLE = 'Y' " +
	    			"GROUP BY C.TABLE_OWNER, C.TABLE_NAME, C.INDEX_NAME";
            }

            if (sql.length() == 0)
         	   return;
            
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);        	
	
	            int i = 0;
	            while (s.next())
	            {
	            	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2)) + "." + trim(s.rs.getString(3));
	            	count = s.rs.getInt(4);
	                uniqueIndexNullMap.put(key, count);
	                ++i;
	            }
	            s.close(methodName);
	            log(i + " values cached in uniqueIndexNullMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }        	
        }
        
        private boolean getOraUniqueIndex(String schema, String table, String index, boolean result)
        {
        	int indexColCount = 0;
            boolean notNullColumn;
            String key = schema + "." + table + "." + removeQuote(index);
            
			// Convert Oracle 1 column nullable unique index to non-unique			
			if (!result)
			{
				if (uniqueIndexAllMap.containsKey(key))
				{
					indexColCount = (Integer) uniqueIndexAllMap.get(key);
				}
				if (indexColCount == 1)
				{
					notNullColumn = true;
					if (uniqueIndexNullMap.containsKey(key))
					{
						int count = (Integer) uniqueIndexNullMap.get(key);
						if (count == 1) // NULL column
							notNullColumn = false;				
					}
					if (notNullColumn == false)
						result = true;
				}
			}			
			return result;
        }
        
        private void BuildDB2XMLIndexPattern()
        {
            String methodName = "BuildDB2XMLIndexPattern";
        	String xmlPattern = "", hashed = "", ignore = "";
            String key, sql = "";
            
            xmlIndexMap = new HashMap();
            
            if (data.DB2())
            {
    			sql = "SELECT INDSCHEMA, INDNAME, DATATYPE, LENGTH, HASHED, TYPEMODEL, PATTERN FROM SYSCAT.INDEXXMLPATTERNS "
					+ "WHERE INDSCHEMA IN (" + schemaList + ")";
         	}

            if (sql.length() == 0)
         	   return;
            
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);        	
	
	            int i = 0;
	            while (s.next())
	            {
	            	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2));
					if (s.rs.getString("HASHED").equalsIgnoreCase("Y"))
						hashed = " HASHED ";
					if (s.rs.getString("TYPEMODEL").equalsIgnoreCase("Q"))
						ignore = " IGNORE INVALID VALUES ";
					else if (s.rs.getString("TYPEMODEL").equalsIgnoreCase("R"))
						ignore = " REJECT INVALID VALUES ";
					if (s.rs.getInt("LENGTH") > 0)
					   xmlPattern = "GENERATE KEY USING XMLPATTERN '"+s.rs.getString("PATTERN")+
					       "' AS SQL "+s.rs.getString("DATATYPE")+"("+s.rs.getString("LENGTH")+")";
					else
					   xmlPattern = "GENERATE KEY USING XMLPATTERN '"+s.rs.getString("PATTERN")+
					       "' AS SQL "+s.rs.getString("DATATYPE");
					xmlPattern = xmlPattern + hashed + ignore;
	            	xmlIndexMap.put(key, xmlPattern);
	                ++i;
	            }
	            s.close(methodName);
	            log(i + " values cached in xmlIndexMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }        	        	
        }
        
        private String getDB2XMLIndexPattern(String schemaName, String indexName)
        {
			String key, value = "";
						
			key = schemaName + "." + indexName;			
			if (xmlIndexMap.containsKey(key))
			{
				value = (String) xmlIndexMap.get(key);
			}
        	return value;
        }
        
        private void BuildFunctionalIndexColumn()
        {
            String methodName = "BuildFunctionalIndexColumn";
            String key, sql = "";
            String value = ""; 
            
            functionalIndexColMap = new HashMap();
            
            if (data.Oracle())
            {
        		sql =  "SELECT OWNER, TABLE_NAME, COLUMN_NAME, 'X' FROM DBA_TAB_COLUMNS " +
        				"WHERE OWNER IN (" + schemaList + ")";
         	}

            if (sql.length() == 0)
         	   return;
            
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);        	
	
	            int i = 0;
	            while (s.next())
	            {
	            	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2)) + "." + trim(s.rs.getString(3));
	            	value = trim(s.rs.getString(4));
	            	functionalIndexColMap.put(key, value);
	                ++i;
	            }
	            s.close(methodName);
	            log(i + " values cached in functionalIndexColMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }        	
        }

        private boolean isFunctionalIndexColumn(String schema, String table, String column)
        {
        	String key = schema + "." + table + "." + column;
        	String expression = "";        	
        	if (functionalIndexColMap.containsKey(key))
        	{
        		expression = (String) functionalIndexColMap.get(key);
        	}
        	return expression.equalsIgnoreCase("X");
        }
        
        private void BuildIndexExpressionmap()
        {
            String methodName = "BuildIndexExpressionmap";
            String key, sql = "";
            String value = ""; 
            
            indexExpressionMap = new HashMap();
            
            if (data.Oracle())
            {
            	sql = "select index_owner, index_name, column_position, column_expression from dba_ind_expressions " +
	    			  "where index_owner IN ("+schemaList+")";
         	}

            if (sql.length() == 0)
         	   return;
            
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);        	
	
	            int i = 0;
	            while (s.next())
	            {
	            	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2)) + "." + trim(s.rs.getString(3));
	            	value = trim(s.rs.getString(4));
	            	indexExpressionMap.put(key, value);
	                ++i;
	            }
	            s.close(methodName);
	            log(i + " values cached in indexExpressionMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }        	
        }
        
        private String getFunctionIndexExpression(String owner, String indexName, short position)
        {
        	String key = owner + "." + indexName + "." + position;
        	String expression = "";
        	if (indexExpressionMap.containsKey(key))
        	{
        		expression = (String) indexExpressionMap.get(key);
        	}
        	return expression;
        }
        
        private String getDirection(String s)
        {
        	if (s == null || s.length() == 0)
        		return s;
        	else if (s.equalsIgnoreCase("A") || s.equalsIgnoreCase("ASC"))
        		return "";
        	else if (s.equalsIgnoreCase("D") || s.equalsIgnoreCase("DESC"))
        		return "DESC";
        	else
        		return s;
        }
        
        private void genSybIndexes(String schema, String dstSchema, String table, String pkName) throws SQLException, IOException
        {
        	String methodName = "genSybIndexes";
            String indexName, columnName, direction, newIndexName, expression;
            short position;
            boolean nonUnique, pkfound;        	
        	StringBuffer buffer = new StringBuffer();
        	SQL s = new SQL(data.connection);
            String sql = "";
            
            s.rs = data.connection.getMetaData().getIndexInfo(dbName, null, table, false, true);

            // Indexes
            buffer.setLength(0);
            indexName = "~";
            String oldIndName = "~";
            nonUnique = true;
            while (s.next())
            {
                indexName = s.rs.getString(6);
                if (indexName != null)
                {
                	columnName = s.rs.getString(9);
                    position = s.rs.getShort(8);
                    direction = getDirection(s.rs.getString(10));
                    nonUnique = s.rs.getBoolean(4);                    	
                    pkfound = (indexName.equalsIgnoreCase(pkName)) ? true : false;  
                    if (position == 1)
                    {
                        if (buffer.length() > 0)
                        {
                            buffer.append(linesep);
                            buffer.append(")" + linesep + "go" + linesep);
                            buffer.append(linesep);
                            if (!oldIndName.equalsIgnoreCase(pkName))
                            {
                                db2IndexesWriter.write(buffer.toString());
                            }
                        }
                        oldIndName = indexName;
                        buffer.setLength(0);
                        if (!pkfound)
                        {
                            if (nonUnique)
                            {
                               newIndexName = "IX" + numIndex + "_" + table;
                               newIndexName = FixString(newIndexName);
           		               buffer.append("--#SET :" + "INDEX" + ":" + schema + ":" + newIndexName + linesep);           	
                               buffer.append("CREATE INDEX " + newIndexName + " ON " + 
                                     schema + "." + table + linesep);
                            } else
                            {
                                newIndexName = "UQ" + numIndex + "_" + table;                                
                                newIndexName = FixString(newIndexName);
            		            buffer.append("--#SET :" + "UNIQUE_INDEX" + ":" + schema + ":" + newIndexName + linesep);           	
                                buffer.append("CREATE UNIQUE INDEX " + newIndexName + " ON " + 
                                      schema + "." + table+ "" + linesep);
                            }
                            buffer.append("(" + linesep);
                            buffer.append(columnName + " " + direction + " ");
                            numIndex++;
                        }
                    } else
                    {
                        if (!pkfound && position > 0)
                        {
                            buffer.append(linesep + ",");
                            buffer.append(columnName + " " + direction + " ");
                        }
                    }
                }
            }
            s.close(methodName);
            if (buffer.length() > 0)
            {
                buffer.append(linesep);
                buffer.append(")" + linesep + "go" + linesep);
                buffer.append(linesep);
                if (!oldIndName.equalsIgnoreCase(pkName))
                {
                    db2IndexesWriter.write(buffer.toString());
                }
            }            
        }
        
        private void BuildIndexClusterMap()
        {
            String methodName = "BuildIndexClusterMap";
            String key = "", sql = "", tableName, indexName, clusterType;
        	
            indexClusterMap = new HashMap();
            
            if (data.Mssql())
            {
            	sql = "select object_name(object_id),name,type_desc from sys.indexes where object_name(object_id) not like 'sys%' and type_desc = 'CLUSTERED'";         
            }
            
            try
            {
	            SQL s = new SQL(data.connection);
	            if (data.Mssql())
	            {
		            s.PrepareExecuteQuery(methodName, sql);		
		            int i = 0;
		        	while (s.next())
		        	{
		        		tableName = trim(s.rs.getString(1));
		        		indexName = trim(s.rs.getString(2));
		        		clusterType = trim(s.rs.getString(3));
		        		key = tableName + "." + indexName;
		        		indexClusterMap.put(key, clusterType);
		        		++i;
		        	}
		        	s.close(methodName);
		            log(i + " values cached in indexClusterMap");
	            }
            }
            catch (SQLException e)
    	    {
    		    e.printStackTrace();
    	    }            
        }
        
        private void BuildDataCaptureChangesMap()
        {
            String methodName = "BuildDataCaptureChangesMap";
            String key = "", sql = "";
            String schema, table;
            String value = ""; 

            dataCaptureChangesMap = new HashMap();

            if (data.zDB2())
            {
            	sql = "SELECT CREATOR, NAME, 'Y' FROM SYSIBM.SYSTABLES " +
			          "WHERE CREATOR IN (" + schemaList + ") " +
			          "AND DATACAPTURE = 'Y'";         
            }
            
            try
            {
	            SQL s = new SQL(data.connection);
	            if (data.zDB2())
	            {
		            s.PrepareExecuteQuery(methodName, sql);		
		            int i = 0;
		        	while (s.next())
		        	{
		        		schema = trim(s.rs.getString(1));
		        		table = trim(s.rs.getString(2));
		        		value = trim(s.rs.getString(3));
		        		key = schema + "." + table;
	        		    dataCaptureChangesMap.put(key, value);	        		
		        		++i;
		        	}
		        	s.close(methodName);
		            log(i + " values cached in dataCaptureChangesMap");
	            }
            }
            catch (SQLException e)
    	    {
    		    e.printStackTrace();
    	    }
        }
        
        private void BuildIndexMap()
        {
            String methodName = "BuildIndexMap";
            String key = "", sql = "";
            String prevSchema = "", prevTable = "", schema, table, indexSchema, indexName, colName, ord, asc, type, part, uniq;
            String value = ""; 
            boolean added = false;
            
            indexMap = new HashMap();            
            ArrayList al = new ArrayList();
            
            if (data.Oracle())
            {
            	String ascDesc = (majorSourceDBVersion != -1 && majorSourceDBVersion > 8) ? "DESCEND" : "'ASC'";
            	sql = "SELECT NULL AS TABLE_CAT, C.TABLE_OWNER, C.TABLE_NAME, I.UNIQUENESS, C.INDEX_OWNER, C.INDEX_NAME, " +
	                  "I.INDEX_TYPE, C.COLUMN_POSITION, C.COLUMN_NAME, "+ascDesc+" AS ASC_OR_DSC, 0 AS CARDINALITY, 0 AS PAGES, I.PARTITIONED " +
			          "FROM DBA_IND_COLUMNS C, DBA_INDEXES I " +
			          "WHERE I.TABLE_OWNER IN (" + schemaList + ") " +
			          "AND I.OWNER = C.INDEX_OWNER " +
			          "AND I.INDEX_NAME = C.INDEX_NAME " +
			          "ORDER BY 2, 3, 5, 6, 8 ";         
            }

            try
            {
	            SQL s = new SQL(data.connection);
	            if (data.Oracle())
	            {
		            s.PrepareExecuteQuery(methodName, sql);		
		            int i = 0;
		        	while (s.next())
		        	{
		        		schema = trim(s.rs.getString(2));
		        		table = trim(s.rs.getString(3));
		        		if (i == 0)
		        		{
		        			prevSchema = schema;
		        			prevTable = table;
		        		}
		        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
		        		{
		        			indexMap.put(key, al);
		        			al = new ArrayList();
		        			added = true;
		        		}
		        		key = schema + "." + table;
	        		    al.add(new IndexData(s.rs, data));
	        		    added = false;
	        			prevSchema = schema;
	        			prevTable = table;
		        		++i;
		        	}
		        	if (!added && key.length() > 0)
		        	{
		        		indexMap.put(key, al);	        		
		        	}
		        	s.close(methodName);
		            log(i + " values cached in indexMap");
	            } else
	            {
	            	int cnt = 0;
	            	for (int i = 0;  i < t.totalTables; ++i)
	            	{
	                	schema = removeQuote(t.srcSchName[i]);
	            		table = removeQuote(t.srcTableName[i]);
	            		key = schema + "." + table;
	                    if (data.Postgres())
	                    {
	                        table = table.toLowerCase();
	                    }
	            		key = removeQuote(schema) + "." + removeQuote(table);
	                    if (data.Mssql())
	                        s.rs = data.connection.getMetaData().getIndexInfo(dbName, schema, table, false, true);
	                    else if (data.zDB2())
	                    	s.rs = data.connection.getMetaData().getIndexInfo(null, schema, table, false, true);
	                    else if (data.iDB2())
	                    	s.rs = data.connection.getMetaData().getIndexInfo(null, schema, table, false, true);
	                    else if (data.DB2())
	                    	s.rs = data.connection.getMetaData().getIndexInfo(null, schema, table, false, true);
	                    else if (data.Hxtt())
	                    	s.rs = data.connection.getMetaData().getIndexInfo(null, null, table, false, true);
	                    else if (data.Mysql())
	                    	s.rs = data.connection.getMetaData().getIndexInfo(null, schema, table, false, true);
	                    else if (data.Teradata())
	                    	s.rs = data.connection.getMetaData().getIndexInfo(null, schema, table, false, true);
	                    else
	                    	s.rs = data.connection.getMetaData().getIndexInfo(dbName, null, table, false, true);
	                    added = false;
		                while (s.next())
		                {
			        		key = schema + "." + table;
		        		    al.add(new IndexData(s.rs, data));
		        		    added = false;
		        		    ++cnt;
		                }
		                if (al.size() > 0)
		                {
		        		   indexMap.put(key, al);	        		
		                   al = new ArrayList();
		                }
	            	}
		            if (debug) log(cnt + " values cached in indexMap");
	            }
            }
            catch (SQLException e)
    	    {
    		    e.printStackTrace();
    	    }
        }
        
        private void genIndexes(String schema, String dstSchema, String table, String pkName) throws IOException
        {
        	if (netezza)
        		return;
        	
            String key, indexName, columnName, columnName2, direction, newIndexName, expression, colTag = "";
            short position, type;
            boolean nonUnique, pkfound, localIndex, oldLocalIndex = false, clusterdIndex = false, prevClusterdIndex = false, clusteredIndexDefined = false;        	
        	StringBuffer buffer = new StringBuffer();
        	StringBuffer generatedColumns = new StringBuffer();
           
            // Indexes
            buffer.setLength(0);
            generatedColumns.setLength(0);
            indexName = "~";
            String oldIndName = "~";
            nonUnique = true;
            key = schema + "." + table;
           
            if (indexMap.containsKey(key))
            {
            	ArrayList al = (ArrayList) indexMap.get(key);
            	Iterator itr = al.iterator();
            	while (itr.hasNext())
            	{
            		clusterdIndex = false;
            		IndexData idx = (IndexData) itr.next();
                    indexName = idx.indexName;
                    String tmpType = idx.type;
                    
                    if (data.Oracle())
                    {
                    	clusterdIndex = idx.type.equalsIgnoreCase("CLUSTER");
                    } else
                    {
                       try { type = Short.valueOf(tmpType); } catch (Exception e) { type = -1; } 
                       if (type == DatabaseMetaData.tableIndexClustered)
                       {
                    	   clusterdIndex = true;
                       }
                    }
                    if (!clusteredIndexes)
                    	prevClusterdIndex = false; // disable it when clusteredIndexes=false is set in IBMExtrcat.properties file. 
                    localIndex = idx.partitioned.equalsIgnoreCase("YES");
                    if (indexName != null)
                    {
                    	columnName2 = idx.columnName;
                        columnName = getTruncName(0, "", columnName2,30);                		
                        position = idx.ordinalPosition;
                        direction = getDirection(idx.ascDesc);
                        if (data.Oracle())
                        {
                        	colTag = "";
                        	if (columnName.startsWith("SYS"))
                        	{
                        	    expression = getFunctionIndexExpression(schema, indexName, position);              
                        		if (expression != null && expression.length() > 0)
                        		{
    	                    		boolean ok = isFunctionalIndexColumn(schema, table, removeQuote(expression));
    	                    		if (ok)
    	                    		{
    	                    		    columnName2 = removeQuote(expression);
    	                                columnName = getTruncName(0, "", columnName2,30);
                                 	} 
    	                    		else
    	                    		{
    	                    			generatedColumns.append("--#SET :" + "INTEGRITY" + ":" + removeQuote(dstSchema) + ":" + removeQuote(table) + linesep); 
    	                    		    generatedColumns.append("SET INTEGRITY FOR " + putQuote(schema) + "." + 
    	                    				   putQuote(table) + " OFF" + linesep + ";" + linesep);
    	                    		   generatedColumns.append("--#SET :" + "ALTER_TABLE" + ":" + removeQuote(dstSchema) + ":" + removeQuote(table)+"_" + getCaseName(getCustomColumnName(schema, table, columnName2)) + linesep); 
    	                    		   generatedColumns.append("-- ALTER TABLE TO ADD GENERATED COLUMN TO TAKE CARE OF Oracle's FUNCTION BASED INDEX COLUMNS" + linesep);
    	                    		   generatedColumns.append("ALTER TABLE " + putQuote(schema) + "." + 
    	                    				   putQuote(table) + " ADD COLUMN " + putQuote(getCaseName(getCustomColumnName(schema, table, columnName2))) +
    	                    				   " GENERATED ALWAYS AS (" + expression + ")" + linesep + ";" + linesep);
    	                    		  
    	                    		   generatedColumns.append("--#SET :" + "INTEGRITY" + ":" + removeQuote(dstSchema) + ":" + removeQuote(table) + linesep);  	      
    	                    		   generatedColumns.append("SET INTEGRITY FOR \"" + schema + "\".\"" + table + "\"" + 
    	                    				   " IMMEDIATE CHECKED FORCE GENERATED" + linesep + ";" + linesep + linesep);
    	                    		   colTag = " -- '" + expression + "' converted to generated column";
    	                    		}
                        		}
                        	}   	
                        	if (!(oldIndName.equals(indexName)))
                        	{
                               nonUnique = getOraUniqueIndex(schema, table, indexName, idx.nonUnique);
                        	}
                        } else 
                        {
                            nonUnique = idx.nonUnique;                    	
                        }
                        if (data.iDB2())
                        	pkfound = (indexName.equalsIgnoreCase(table)) ? true : false;
                        else
                            pkfound = (indexName.equalsIgnoreCase(pkName)) ? true : false;  
                        if (position == 1)
                        {
                            if (buffer.length() > 0)
                            {
                             
                                buffer.append(linesep);
                                if (db2 || netezza)
                                {
                               	   buffer.append(")" + linesep); 
                                   String xmlPattern;
                                   if (data.DB2())
                                   {
                                      xmlPattern = getDB2XMLIndexPattern(schema, oldIndName); 
                                      if (!xmlPattern.equals(""))
                                	      buffer.append(xmlPattern + linesep);
                                   }
                                   if (oldLocalIndex)
                                   {
                                	   if (releaseLevel != -1.0F && releaseLevel >= 9.7F)
                                	   {
                                		   buffer.append(" PARTITIONED ");
                                	   }
                                   }
                                   if (!clusteredIndexDefined)
                                   {
                                	   if (prevClusterdIndex)
                                	   {
                                		   buffer.append(" CLUSTER ");
                                		   clusteredIndexDefined = true;
                                	   }
                                   }
                                   buffer.append("ALLOW REVERSE SCANS" + linesep);
                                   if (loadstats)
                                   {
                                	   buffer.append("COLLECT DETAILED STATISTICS" + linesep);
                                   }
                                   if (compressIndex)
                                   {
                                       buffer.append("COMPRESS YES" + linesep);
                                   }
                                   buffer.append(";" + linesep);
                                }
                                else
                                   buffer.append(")" + linesep + ";" + linesep);
                                buffer.append(linesep);
                               if (!oldIndName.equalsIgnoreCase(pkName))
                                {
                                	db2IndexesWriter.write(generatedColumns.toString());
                                    db2IndexesWriter.write(buffer.toString());
                                    buffer.setLength(0);
                                    generatedColumns.setLength(0);
                                }
                            }
                            oldIndName = indexName;
                            oldLocalIndex = localIndex;
                         // buffer.setLength(0);
                          // generatedColumns.setLength(0);
                            if (!pkfound)
                            {
                                if (nonUnique)
                                {
                                   newIndexName = getTruncName(0, "", "IX" + numIndex + "_" + removeQuote(table),18);
                                   newIndexName = FixString(newIndexName);
                                   if (retainConstraintsName)
                                   {
                                	   if (!(indexName == null || indexName.length() == 0))
                                	   {
                                		   newIndexName = getCaseName(indexName);
                                	   }
                                   }
               		               buffer.append("--#SET :" + "INDEX" + ":" + removeQuote(dstSchema) + ":" + newIndexName + linesep);  
                                   buffer.append("CREATE INDEX " + dstSchema + ".\"" + newIndexName + "\" ON " + 
                                         dstSchema + ".\"" + getCaseName(table) + "\"" + linesep);
                                } else
                                {
                                    newIndexName = getTruncName(0, "", "UQ" + numIndex + "_" + removeQuote(table),18);
                                    newIndexName = FixString(newIndexName);
                                    if (retainConstraintsName)
                                    {
                                 	   if (!(indexName == null || indexName.length() == 0))
                                 	   {
                                 		   newIndexName = getCaseName(indexName);
                                 	   }
                                    }
                		            buffer.append("--#SET :" + "UNIQUE_INDEX" + ":" + removeQuote(dstSchema) + ":" + newIndexName + linesep);           	
                		            buffer.append("CREATE UNIQUE INDEX " + dstSchema + ".\"" + newIndexName + "\" ON " + 
                                          dstSchema + ".\"" + getCaseName(table)+ "\"" + linesep);
                                }
                                buffer.append("(" + linesep);
                                buffer.append(putQuote(getCaseName(getCustomColumnName(schema, table, columnName2))) + " " + direction + " " + colTag);
                                numIndex++;
                            }
                        } else
                        {
                            if (!pkfound && position > 0)
                            {
                                buffer.append(linesep + ",");
                                buffer.append(putQuote(getCaseName(getCustomColumnName(schema, table, columnName2))) + " " + direction + " " + colTag);
                            }
                        }
                    }
                    prevClusterdIndex = clusterdIndex;
            	}
            }
            if (buffer.length() > 0)
            {
                buffer.append(linesep);
                if (db2 || netezza)
                {
             	   buffer.append(")" + linesep); 
                   String xmlPattern;
                   if (data.DB2())
                   {
                      xmlPattern = getDB2XMLIndexPattern(schema, oldIndName); 
                      if (!xmlPattern.equals(""))
                	     buffer.append(xmlPattern + linesep);
                   }
                   if (!clusteredIndexDefined)
                   {
                	   if (prevClusterdIndex)
                	   {
                		   buffer.append(" CLUSTER ");
                		   clusteredIndexDefined = true;
                	   }
                   }
                   if (loadstats)
                      buffer.append("ALLOW REVERSE SCANS COLLECT STATISTICS" + linesep);
                   else
                      buffer.append("ALLOW REVERSE SCANS" + linesep);
                   if (compressIndex)
                   {
                       buffer.append("COMPRESS YES" + linesep + ";" + linesep);                	   
                   } else
                   {
                       buffer.append(";" + linesep);                	                   	   
                   }
                } else
                   buffer.append(")" + linesep + ";" + linesep);
                buffer.append(linesep);
               
                if (!oldIndName.equalsIgnoreCase(pkName))
                {
                	db2IndexesWriter.write(generatedColumns.toString());
                    db2IndexesWriter.write(buffer.toString());
                }
            }            
        }
                
        private void genSybaseTableKeys(int id) throws SQLException, IOException
        {
        	Hashtable pkCols = new Hashtable();
        	short keySeq = 0;
            String schema = removeQuote(t.srcSchName[id]); 
            String table = removeQuote(t.srcTableName[id]); 
            ResultSet pkeys;
            String pkName = "", columnName, columnName2, newPKName;
            boolean found = false;
            StringBuffer buffer = new StringBuffer();
            pkeys = data.connection.getMetaData().getPrimaryKeys(dbName, null, table);

            buffer.setLength(0);
            
            // Primary keys
            pkCols.clear();
            while (pkeys.next())
            {
                pkName = pkeys.getString(6);
                keySeq = pkeys.getShort(5);
                columnName = pkeys.getString(4);
                pkCols.put(new Integer(keySeq), columnName);
            }
            newPKName = "PK_"+table;
            newPKName = FixString(newPKName);
            buffer.append("--#SET :" + "PRIMARY_KEY" + ":" + schema + ":" + newPKName + linesep);           	
            buffer.append("ALTER TABLE " + putQuote(schema) + sqlsep + putQuote(table) + linesep);
            buffer.append("ADD CONSTRAINT " + putQuote(newPKName) + " PRIMARY KEY" + linesep);
            buffer.append("(" + linesep);
            Vector v = new Vector(pkCols.keySet());
            Collections.sort(v);	            
            for (Enumeration e = v.elements(); e.hasMoreElements();) {
                if (found) buffer.append(",");
                String val = (String)pkCols.get((Integer)e.nextElement());
                buffer.append(val);
                found = true;
            }
            if (pkeys != null) 
                pkeys.close();
            buffer.append(linesep);
            buffer.append(")" + linesep + "go" + linesep);
            if (found) 
               db2PKeysWriter.write(buffer.toString());
            genSybIndexes(schema, schema, table, pkName);
        }
        
        private void genUniqClusKeys(int id, String schema, String table, Hashtable pkCols, String newPKName, String dstSchema, String isTempTable) throws IOException
        {
            StringBuffer uniqBuffer = new StringBuffer();
            boolean found = false;
            uniqBuffer.append("--#SET :" + "UNIQUE_INDEX" + ":" + removeQuote(dstSchema) + ":" + newPKName + linesep);           	
            uniqBuffer.append("CREATE UNIQUE INDEX " + dstSchema + "." + putQuote(newPKName) + " ON " + dstSchema + "." + getCaseName(t.dstTableName[id]) + linesep);
            uniqBuffer.append("(" + linesep);        	
            Vector v = new Vector(pkCols.keySet());
            Collections.sort(v);	            
            for (Enumeration e = v.elements(); e.hasMoreElements();) 
            {
                String val = (String)pkCols.get((Integer)e.nextElement());
                if (found) uniqBuffer.append(",");
                uniqBuffer.append(putQuote(putQuote(getCaseName(getCustomColumnName(schema, table, val)))));
                found = true;
            }
  	    	uniqBuffer.append(linesep + ")" + linesep + "CLUSTER" + linesep + "ALLOW REVERSE SCANS" + linesep);
            if (compressIndex && isTempTable.equals(""))
            {
                uniqBuffer.append("COMPRESS YES" + linesep);
            }
            if (loadstats)
            {
         	    uniqBuffer.append("COLLECT DETAILED STATISTICS" + linesep);
            }
            uniqBuffer.append(";" + linesep);	 
            if (found && clusteredIndexes)
                db2IndexesWriter.write(uniqBuffer.toString());
        }
        
        private void genTableKeys(int id) throws SQLException, IOException
        {
        	if (db2Skin())
        	{
        		genSybaseTableKeys(id);
        		return;
        	}
        	
        	Hashtable pkCols = new Hashtable();
        	short keySeq = 0;
            String schema = removeQuote(t.srcSchName[id]); 
            String dstSchema = getCaseName(t.dstSchemaName[id]);
            String table = removeQuote(t.srcTableName[id]); 
            String isTempTable = "";
            ResultSet pkeys;
            String pkName = "", columnName, columnName2, newPKName;
            boolean found = false, pkClustered = false;
            StringBuffer buffer = new StringBuffer();
            if (data.Postgres())
                table = removeQuote(t.srcTableName[id].toLowerCase());
            else
                table = removeQuote(t.srcTableName[id]); 
            if (data.Oracle())
                pkeys = data.connection.getMetaData().getPrimaryKeys(null, schema, table);
            else if (data.Hxtt())
                pkeys = data.connection.getMetaData().getPrimaryKeys(null, null, table);
            else if (data.zDB2())
                pkeys = data.connection.getMetaData().getPrimaryKeys(null, schema, table);
                //pkeys = data.connection.getMetaData().getPrimaryKeys(null, null, table);
            else if (data.iDB2())
                pkeys = data.connection.getMetaData().getPrimaryKeys(null, schema, table);
            else if (data.DB2())
                pkeys = data.connection.getMetaData().getPrimaryKeys(null, schema, table);
            else if (data.Mysql())
                pkeys = data.connection.getMetaData().getPrimaryKeys(schema, null, table);
            else if (data.Mssql())
                pkeys = data.connection.getMetaData().getPrimaryKeys(dbName, schema, table);
            else
                pkeys = data.connection.getMetaData().getPrimaryKeys(dbName, null, table);

            buffer.setLength(0);
            
            isTempTable = isTableTemporary(schema, table);
            // Primary keys
            if (isTempTable.equals(""))
            {
	            pkCols.clear();
	            while (pkeys.next())
	            {
	                pkName = pkeys.getString(6);
	                keySeq = pkeys.getShort(5);
	                columnName2 = pkeys.getString(4);
                    columnName = getTruncName(0, "", columnName2,30);                	
	                pkCols.put(new Integer(keySeq), columnName);
	            }
	            if (indexClusterMap.containsKey(table+"."+pkName))
	            	pkClustered = true;
	            newPKName = "PK_"+getCaseName(table);
	            newPKName = FixString(newPKName);
        		if (releaseLevel != -1.0F && releaseLevel < 9.1F)
        		{
    	            if (newPKName.length() > 18) 
    	            	newPKName = newPKName.substring(0,18);        			
        		}
	            if (retainConstraintsName)
	            {
	            	if (!(pkName == null || pkName.length() == 0))
	            		newPKName = pkName;
	            }
	            buffer.append("--#SET :" + "PRIMARY_KEY" + ":" + removeQuote(dstSchema) + ":" + newPKName + linesep);           	
	            buffer.append("ALTER TABLE " + dstSchema + sqlsep + getCaseName(t.dstTableName[id]) + linesep);
	            buffer.append("ADD CONSTRAINT " + putQuote(newPKName) + " PRIMARY KEY" + linesep);
	            buffer.append("(" + linesep);
	            Vector v = new Vector(pkCols.keySet());
	            Collections.sort(v);	            
	            for (Enumeration e = v.elements(); e.hasMoreElements();) {
	                if (found) buffer.append(",");
	                String val = (String)pkCols.get((Integer)e.nextElement());
	                buffer.append(putQuote(getCaseName(getCustomColumnName(schema, table, val))));
	                found = true;
	            }
	            if (pkeys != null) 
	                pkeys.close();
                buffer.append(linesep + ")" + linesep + ";" + linesep);
	            if (found) 
	            {
	                db2PKeysWriter.write(buffer.toString());
		            if (data.Mssql() && pkClustered)
		            {
		            	genUniqClusKeys(id, schema, table, pkCols, newPKName, dstSchema, isTempTable);
		            }
	            }
	        }        
            else
            {
	            pkCols.clear();
	            while (pkeys.next())
	            {
	                pkName = pkeys.getString(6);
	                columnName2 = pkeys.getString(4);
                    columnName = getTruncName(0, "", columnName2,30);                	
	                pkCols.put(new Integer(keySeq), columnName);
	            }
	            newPKName = "PK_"+getCaseName(table);
	            newPKName = FixString(newPKName);
	            if (newPKName.length() > 18) newPKName = newPKName.substring(0,18);
	            newPKName = removeQuote(newPKName);
	            if (retainConstraintsName)
	            {
	            	if (!(pkName == null || pkName.length() == 0))
	            		newPKName = pkName;
	            }
	            buffer.append("--#SET :" + "UNIQUE_INDEX" + ":" + removeQuote(dstSchema) + ":" + newPKName + linesep);           	
	            buffer.append("CREATE UNIQUE INDEX " + dstSchema + "." + putQuote(newPKName) + " ON " + dstSchema + "." + getCaseName(t.dstTableName[id]) + linesep);
	            buffer.append("(" + linesep);
	            Vector v = new Vector(pkCols.keySet());
	            Collections.sort(v);	            
	            for (Enumeration e = v.elements(); e.hasMoreElements();) {
	                if (found) buffer.append(",");
	                String val = (String)pkCols.get((Integer)e.nextElement());
	                buffer.append(putQuote(putQuote(getCaseName(getCustomColumnName(schema, table, val)))));
	                found = true;
	            }
	            if (pkeys != null) 
	                pkeys.close();
                buffer.append(")" + linesep + "ALLOW REVERSE SCANS" + linesep);
	            if (compressIndex && isTempTable.equals(""))
	            {
	                buffer.append("COMPRESS YES");
	            }
                if (loadstats)
                {
             	    buffer.append(linesep + "COLLECT DETAILED STATISTICS");
                }
                buffer.append(linesep + ";" + linesep);
	            if (found) 
	               db2PKeysWriter.write(buffer.toString());
            }
            genIndexes(schema, dstSchema, table, pkName);
        }
        
        private String massageOraDate(String def)
        {
        	if (!(def == null || def.length() == 0))
        		def = def.trim();
        	if (def.equalsIgnoreCase("sysdate"))
        	{
        		if (releaseLevel != -1.0F && releaseLevel >= 9.7F)
        		   return " SYSDATE ";
        		else
         		   return " CURRENT_TIMESTAMP ";
        	}
        	else 
        	{
        	    return def.equalsIgnoreCase("null") ? "NULL" : "'" + formatDate(def) + "'";	
        	}
        }
        
        private String formatsqlServerDate (String strDate, int which)
        {
        	if (which == 0)
        	{
        	   if (mysqlZeroDateToNull)
        	   {
        		   return "NULL";
        		   
        	   } else
        	   {
		           Pattern p = Pattern.compile(".*(\\d{4})\\-(\\d{2})\\-(\\d{2}).*");
		           Matcher m = p.matcher(strDate);
		           if (m.matches())
		           {
		        	   String yr = m.group(1), mo = m.group(2), dy = m.group(3);
		        	   if (yr.equals("0000"))
		        		   yr = "0001";
		        	   if (mo.equals("00"))
		        		   mo = "01";
		        	   if (dy.equals("00"))
		        		   dy = "01";
		               return "'"+yr+"-"+mo+"-"+dy+"-00.00.00.000000'";
		           }
        	   }
        	} else if (which == 1)
        	{
         	   if (mysqlZeroDateToNull)
        	   {
        		   return "NULL";
        		   
        	   } else
        	   {
      	           Pattern p = Pattern.compile(".*(\\d{4})\\-(\\d{2})\\-(\\d{2})");
     	           Matcher m = p.matcher(strDate);
     	           if (m.matches())
     	           {
     	        	   String yr = m.group(1), mo = m.group(2), dy = m.group(3);
    	        	   if (yr.equals("0000"))
    	        		   yr = "0001";
    	        	   if (mo.equals("00"))
    	        		   mo = "01";
    	        	   if (dy.equals("00"))
    	        		   dy = "01";
     	               return "'"+yr+"-"+mo+"-"+dy+"'";
     	           }        		        		   
        	   }
         	} 
        	else
        	{
          	   if (mysqlZeroDateToNull)
        	   {
        		   return "NULL";
        		   
        	   } else
        	   {
	 	           Pattern p = Pattern.compile("(\\d{2})\\:(\\d{2})\\:(\\d{2})");
		           Matcher m = p.matcher(strDate);
		           if (m.matches())
		           {
		               return "'"+m.group(1)+":"+m.group(2)+":"+m.group(3)+"'";
		           }        		
        	   }
        	}
            return strDate;           
        }
        
        private void BuildDefaultValuesMap()
        {
        	String methodName = "BuildDefaultValuesMap";
        	String key, value, sql = "";
        	defaultValuesMap = new HashMap();
        	
            if (data.Oracle())
            {
                sql = "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DEFAULT_LENGTH, DATA_DEFAULT " +
                                                "FROM DBA_TAB_COLUMNS " +
                                                "WHERE OWNER IN (" + schemaList + ")";			                    
            }
            else if (data.zDB2())
            {
                sql = "SELECT TBCREATOR, TBNAME, NAME, DEFAULT, DEFAULTVALUE " +
                             "FROM SYSIBM.SYSCOLUMNS " +
                             "WHERE TBCREATOR IN (" + schemaList + ")";
            }
            else if (data.DB2())
            {
                sql = "SELECT TABSCHEMA, TABNAME, COLNAME, DEFAULT, '' AS DEFAULTVALUE " +
                             "FROM SYSCAT.COLUMNS " +
                             "WHERE TABSCHEMA IN (" + schemaList + ")";
            }
            else if (data.iDB2())
            {
                sql = "SELECT TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_DEF, '' AS DEFAULTVALUE " +
                             "FROM SYSIBM.SQLCOLUMNS " +
                             "WHERE TABLE_SCHEM IN (" + schemaList + ")";
            }
            
            if (sql.length() == 0)
            	return;
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);                
	            int i = 0;
	            String val1, val2;
			    while (s.next())
			    {
			    	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2)) + "." + trim(s.rs.getString(3));
			    	val1 = trim(s.rs.getString(4));
			    	val2 = trim(s.rs.getString(5));
			    	if (val1 != null) 
			    	{
			    	   value = val1 + ":" + val2;
			    	   defaultValuesMap.put(key,value);
			    	}
			    	++i;
		       }           
			   s.close(methodName);
			   log(i + " default values cached in defaultValuesMap");
           } catch (SQLException e)
           {
        	   e.printStackTrace();
           }
        }
        
        private void genDB2DefaultValues(String dstSchemaName, String schemaName, String tabName, String colName, String nullType, String dataType) throws Exception
        {
        	String methodName = "genDB2DefaultValues";
        	String dfv = (netezza) ? " DEFAULT " : " WITH DEFAULT ";
            DatabaseMetaData dbmeta = data.connection.getMetaData();
            SQL s = new SQL(data.connection);
            String key, value, defValue = "";
            String[] tmpVar;
            String dstSchema = getCaseName(dstSchemaName), colName2;
            dstSchema = removeQuote(dstSchema);
            
            key = removeQuote(schemaName) + "." + removeQuote(tabName) + "." + removeQuote(colName);
            if (data.Postgres())
            {
                s.rs = dbmeta.getColumns(null, removeQuote(schemaName.toLowerCase()), removeQuote(tabName.toLowerCase()), colName.toLowerCase());
                while (s.next())
                {
                    defValue = s.rs.getString("COLUMN_DEF");
                }
                s.close(methodName);
                if (defValue != null && !defValue.equals(""))
                {
                    if (defValue.startsWith("nextval"))
                        defValue = "";
                    else
                    {
                        defValue =defValue.replace('(', ' ');
                        defValue =defValue.replace(')', ' ');
                        if(defValue.equalsIgnoreCase("true"))
                            defValue ="1";
                        else if(defValue.equalsIgnoreCase("false"))
                            defValue ="0";
                        else if(defValue.equalsIgnoreCase("\"current_user\"") || defValue.indexOf("current_user") > 0)
                        	defValue = "CURRENT_USER";
                        else if(defValue.indexOf("::bpchar") > 0)
                            defValue = defValue.substring(0, defValue.indexOf("::bpchar"));                        
                        else if(defValue.indexOf("::text") > 0)
                            defValue = defValue.substring(0, defValue.indexOf("::text"));                        
                        else if(defValue.indexOf("::character varying") > 0)
                            defValue = defValue.substring(0, defValue.indexOf("::character varying"));                        
                        else if(defValue.indexOf("'now'") > 0 || defValue.indexOf("now") > 0)
                        {
                           if (dataType.equalsIgnoreCase("TIMESTAMP"))
                               defValue = " CURRENT_TIMESTAMP ";
                           else if (dataType.equalsIgnoreCase("TIME"))
                               defValue = " CURRENT_TIME ";
                           else if (dataType.equalsIgnoreCase("DATE"))
                               defValue = " CURRENT_DATE ";
                           else
                        	   defValue = " CURRENT_TIMESTAMP ";
                        }                        
                        defValue = dfv + defValue;
                    }
                }
            }
            else if (data.Teradata())
            {
                s.rs = dbmeta.getColumns(null, removeQuote(schemaName), removeQuote(tabName), colName);
                while (s.next())
                {
                    defValue = s.rs.getString("COLUMN_DEF");
                }
                s.close(methodName);
                if (defValue != null && !defValue.equals(""))
                {
		        	defValue = dfv + defValue;
                }
            }
            else if (data.Oracle())
            {
                String def_length = null, defaultValue = null;
            	if (defaultValuesMap.containsKey(key))
            	{
            		value = (String) defaultValuesMap.get(key);
            	    tmpVar = value.split(":");
                	def_length = tmpVar[0];
                	if (tmpVar.length > 1)
                		defaultValue = tmpVar[1];
                    if (def_length == null ||def_length.equalsIgnoreCase("null"))
                      def_length = null;
                    if (defaultValue == null ||defaultValue.equalsIgnoreCase("null"))
                      defaultValue = null;
    	            if (defaultValue != null && !defaultValue.equals("")) 
    	            {
    		             if (dataType.startsWith("DATE") || dataType.startsWith("TIMESTAMP"))
    		             {
    		                defValue = dfv + massageOraDate(defaultValue);
    		             }
    		             else
    	                 {
    	                   /* 
    	                    * Oracle may return brackets around the quoted literal ('01') which is
    	                    * the same as just '01', these must be stripped to make the statement db2 compatible
    	                    */
    	                   if (defaultValue.charAt(0) == '(')
    	                   {
    	                      defaultValue = defaultValue.substring(1,defaultValue.lastIndexOf(')'));
    	                   }
    	                   defValue = dfv + defaultValue + " ";
    	                 }			        	      
    	            }   
            	}
            }
            else if (data.zDB2())
            {
                String def = null, defaultValue = null;        	
            	if (defaultValuesMap.containsKey(key))
            	{
            		value = (String) defaultValuesMap.get(key);
            		tmpVar = value.split(":");
                	def = tmpVar[0];
                	if (tmpVar.length > 1)
                       defaultValue = tmpVar[1];
                    if (def == null || def.equalsIgnoreCase("null"))
                      def = null;
                    if (defaultValue == null || defaultValue.equalsIgnoreCase("null"))
                      defaultValue = null;
                    if (def.equalsIgnoreCase("1"))
                    {
                        if (dataType.startsWith("VARCHAR") || dataType.startsWith("CHAR") ||
                            dataType.startsWith("TIMESTAMP") || dataType.startsWith("DATE") ||
                            dataType.startsWith("TIME") || dataType.startsWith("CLOB"))                            
                            defValue = dfv + " '" + defaultValue + "'";
                        else
                            defValue = dfv + " " + defaultValue + " ";
                    } else if (def.equalsIgnoreCase("U"))
                    {
                        defValue = dfv + " USER ";                        
                    }
                    else if (def.equalsIgnoreCase("Y"))
                    {
                        if (nullType.equals(""))
                           defValue = dfv + " NULL ";                        
                        else
                           defValue = dfv;                        
                    } else if (def.equalsIgnoreCase("4"))
                    {
                    	defValue = dfv + defaultValue + " ";
                    }
            	}
            }
            else if (data.DB2())
            {
                String def = null;
            	if (defaultValuesMap.containsKey(key))
            	{
            	   value = (String) defaultValuesMap.get(key);
            	   tmpVar = value.split(":");
            	   def = tmpVar[0];
                   defValue = (def == null || def.equalsIgnoreCase("null")) ? "" : dfv + def + " ";
            	}
            }
            else if (data.iDB2())
            {
                String def = null;
            	if (defaultValuesMap.containsKey(key))
            	{
            		value = (String) defaultValuesMap.get(key);
                	tmpVar = value.split(":");
             	    def = tmpVar[0];
                    defValue = (def == null || def.equalsIgnoreCase("null")) ? "" : dfv + def + " ";            		
            	}
            }
            else if (data.Mssql() || data.Mysql())
            {
            	colName = IBMExtractUtilities.escapeSingleQuote(colName);
                s.rs = data.Mssql() ? dbmeta.getColumns(null, removeQuote(schemaName), removeQuote(tabName), removeQuote(colName)) :
                	dbmeta.getColumns(removeQuote(schemaName), null, removeQuote(tabName), removeQuote(colName));                
                while (s.next())
                {
                    defValue = s.rs.getString("COLUMN_DEF");
                }
                s.close(methodName);
                if (defValue != null && !defValue.equals(""))
                {
                    defValue =defValue.replace('(', ' ');
                    defValue =defValue.replace(')', ' ');
                    if (defValue.trim().equalsIgnoreCase("getdate"))
                    {
                       defValue = zdb2 ? "" : " CURRENT_TIMESTAMP ";
                    } 
                    else if (defValue.trim().equalsIgnoreCase("curdate"))
                    {
                    	defValue = zdb2 ? "" : " CURRENT_DATE ";
                    }
                    else if (defValue.trim().equalsIgnoreCase("user_name"))
                    {
                       defValue = zdb2 ? " CURRENT SQLID " : " CURRENT_USER ";
                    }
                    
                    if (dataType.equalsIgnoreCase("TIMESTAMP"))
                       defValue = dfv + formatsqlServerDate(defValue, 0);
                    else if (dataType.equalsIgnoreCase("DATE"))
                        defValue = dfv + formatsqlServerDate(defValue, 1);
                    else if (dataType.equalsIgnoreCase("TIME"))
                        defValue = dfv + formatsqlServerDate(defValue, 2);
                    else
                    {
                        if (data.Mysql() && (dataType.startsWith("CHAR") ||
                        		dataType.startsWith("VARCHAR") || dataType.startsWith("CLOB")))
                        {
                        	if (defValue.charAt(0) != '\'')
                        	{
                        		defValue = "'" + defValue + "'";
                        	}
                        }
                        defValue = dfv + defValue;
                    }
                    
                    if (defValue.trim().endsWith("newid"))
                    {
                      if (!udfcreated)
                      {
                        genDB2UDFs();
                        udfcreated = true;
                      }
                      String tmpStr = removeQuote(getCaseName(tabName));
                      tmpStr = FixString(tmpStr);
                      String trigName = "TRIG"+(triggerCount)+"_"+tmpStr;
                      trigName = getTruncName(0, "", trigName, 18); 
                      trigName = getCaseName(schemaName) + ".\"" + trigName + "\"";
                      db2DropWriter.write("DROP TRIGGER " + putQuote(dstSchema) + "." + putQuote(trigName) + ";" + linesep);                   
                      db2DefaultWriter.write("--#SET :" + "TRIGGER" + ":" + dstSchema + ":" + trigName + linesep);           	
                      db2DefaultWriter.write("CREATE TRIGGER " + putQuote(dstSchema) + "." + putQuote(trigName) + " NO CASCADE BEFORE INSERT " + linesep +
                            "ON " + getCaseName(schemaName) + "." + getCaseName(tabName) + " REFERENCING NEW AS NEW " + linesep +
                            "FOR EACH ROW MODE DB2SQL " + linesep);
                      colName2 = getCaseName(getCustomColumnName(schemaName, tabName, colName));
                      if (db2 || netezza)
                         db2DefaultWriter.write("SET " + colName2 + " = DB2.NEWGUID()" + linesep);
                      else
                         db2DefaultWriter.write("VALUES DB2.NEWGUID() INTO " + colName2 + linesep);
                      db2DefaultWriter.write(";" + linesep + linesep);    
                      defValue = dfv;
                      triggerCount++;
                    }
                }
            }
            if (defValue != null && !defValue.equals(""))
            {
            	colName =  getCaseName(getCustomColumnName(schemaName, tabName, colName));
                colName = getTruncName(0, "", colName, 30);
                db2DefaultWriter.write("--#SET :" + "DEFAULT" + ":" + removeQuote(getCaseName(schemaName)) + ":" + 
                		removeQuote(getCaseName(tabName)) + "_" + removeQuote(colName) + linesep);           	
                db2DefaultWriter.write("ALTER TABLE "+putQuote(dstSchema)+sqlsep+putQuote(getCaseName(tabName))+ " ALTER COLUMN \"" + 
                		colName + "\" SET " + defValue + linesep + ";" + linesep);                
                db2DefaultWriter.write(linesep);                
            }
        }
        
        private int ModifyTable(String str)
        {
            String regex = "CHAR\\((\\d.*)\\)|VARCHAR\\((\\d.*)\\)|NUMERIC\\((\\d.*)\\)|BLOB\\((\\d.*)\\)|CLOB\\((\\d.*)\\)"+
                           "|CLOB|BLOB|INT|INTEGER|FLOAT|SMALLINT|BIGINT|LONG VARCHAR\\((\\d.*)\\)|LONG VARGRAPHIC\\((\\d.*)\\)|"+
                           "DOUBLE|TIMESTAMP|DATE|TIME|NUMERIC\\((\\d.*,\\d.*)\\)|GRAPHIC\\((\\d.*)\\)|VARGRAPHIC\\((\\d.*)\\)";
            //String regex2 = ".*\\((\\d.*)\\)|.*\\((\\d.*,\\d.*)\\)";
            String regex2 = ".*\\(([0-9]*)\\)|.*\\(([0-9]*,[0-9]*)\\)";
            String tok1, tok2[];
            int p = 0, s = 0;
            //System.out.println(str);
            Pattern pattern, pattern2;
            Matcher matcher = null, matcher2 = null;
            pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
            matcher = pattern.matcher(str);
            int tableSize = 0;
            while (matcher.find()) 
            {
                tok1 = matcher.group().toLowerCase();
                pattern2 = Pattern.compile(regex2);
                matcher2 = pattern2.matcher(tok1);
                p = s = 0;
                if (matcher2.find())
                {
                	if (matcher2.group(1) != null)
                	{
                       tok2 = matcher2.group(1).split(",");
                       try
                       {
                          p = Integer.parseInt(tok2[0]);
                       } catch (Exception e)
                       {
                    	  p = 5; // Poor fix. We should fix regex and not circumvent the problem.
                       }
                       if (tok2.length > 1)
                          s = Integer.parseInt(tok2[1]);
                       //System.out.print("p="+p+" s="+s);
                	}
                	else if (matcher2.group(2) != null)
                	{
                        tok2 = matcher2.group(2).split(",");
                        try
                        {
                           p = Integer.parseInt(tok2[0]);
                        } catch (Exception e)
                        {
                     	  p = 5; // Poor fix. We should fix regex and not circumvent the problem.
                        }
                        if (tok2.length > 1)
                           s = Integer.parseInt(tok2[1]);
                        //System.out.print("p="+p+" s="+s);                		
                	}
                }
                if (tok1.startsWith("int") || tok1.startsWith("integer"))
                {
                    tableSize += 4;
                }
                else if (tok1.startsWith("smallint"))
                {
                    tableSize += 2;
                }
                else if (tok1.startsWith("bigint"))
                {
                    tableSize += 8;
                }
                else if (tok1.startsWith("real"))
                {
                    tableSize += 4;
                }
                else if (tok1.startsWith("double"))
                {
                    tableSize += 8;
                }
                else if (tok1.startsWith("char"))
                {
                    tableSize += p;
                }
                else if (tok1.startsWith("varchar"))
                {
                    tableSize += p;
                }
                else if (tok1.startsWith("graphic"))
                {
                    tableSize += p*2;
                }
                else if (tok1.startsWith("vargraphic"))
                {
                    tableSize += p*2;
                }
                else if (tok1.startsWith("date"))
                {
                    tableSize += 4;
                }
                else if (tok1.startsWith("time"))
                {
                    tableSize += 3;
                }
                else if (tok1.startsWith("xml"))
                {
                    tableSize += 84;
                }
                else if (tok1.startsWith("long varchar"))
                {
                    tableSize += 24;
                }
                else if (tok1.startsWith("long vargraphic"))
                {
                    tableSize += 24;
                }
                else if (tok1.startsWith("blob") || tok1.startsWith("clob"))
                {
                    if (p == 0) p = 1048576; 
                    if (p > 0 && p <= 1024)
                       tableSize += 72;
                    else if (p > 1024 && p <= 8192)
                        tableSize += 96;
                    else if (p > 8192 && p <= 65536)
                        tableSize += 120;
                    else if (p > 65536 && p <= 524000)
                        tableSize += 144;
                    else if (p > 524000 && p <= 4190000)
                        tableSize += 168;
                    else if (p > 4190000 && p <= 134000000)
                        tableSize += 200;
                    else if (p > 134000000 && p <= 536000000)
                        tableSize += 224;
                    else if (p > 536000000 && p <= 1470000000)
                        tableSize += 280;
                    else if (p > 1470000000 && p <= 2147483647)
                        tableSize += 316;
                }
                else if (tok1.startsWith("numeric"))
                {
                    tableSize += (p / 2 + 1);
                }
                //System.out.println(" tok=" + tok1 + " Table Size = " + tableSize + " start= " + matcher.start() + " end=" + matcher.end());
            }
            //System.out.println(" Table Size = " + tableSize);
            return tableSize;
        }
        
        public String ModifyTableAll(String str, String schemaName, String tabName) throws IOException
        {
            String regex = "VARCHAR\\((\\d.*)\\)|VARGRAPHIC\\((\\d.*)\\)|LONG VARCHAR\\((\\d.*)\\)|LONG VARGRAPHIC\\((\\d.*)\\)";
            String regex2 = ".*\\((\\d.*)\\)";
            String tok1;
            int tableSize, p, oldTableSize = 0;
            //str = str.replaceAll("VARCHAR\\(1\\)", "CHAR(1)");
            StringBuffer sb = new StringBuffer();
            boolean found = false;
            sb.setLength(0);
            sb.append(str);
            Pattern pattern, pattern2;
            Matcher matcher = null, matcher2 = null;
            while ((tableSize = ModifyTable(sb.toString())) > 30000)
            {
                if (oldTableSize == 0)
                   oldTableSize = tableSize;
                pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
                matcher = pattern.matcher(sb.toString());
                found = false;
                while (matcher.find()) 
                {
                    tok1 = matcher.group().toLowerCase();
                    pattern2 = Pattern.compile(regex2);
                    matcher2 = pattern2.matcher(tok1);
                    p = 0;
                    if (matcher2.find())
                    {
                        p = Integer.parseInt(matcher2.group(1));
                        //System.out.print("p="+p);                    
                    }                
                    if (matcher.group().toLowerCase().startsWith("varchar"))
                    {
                        if (p > 1000)
                        {
                            //System.out.println("tablesize=" + tableSize + " Group = " + matcher.group());
                            sb.replace(matcher.start(),matcher.end(),"LONG VARCHAR");
                            found = true;
                            break;
                        }
                    }
                    if (matcher.group().toLowerCase().startsWith("vargraphic"))
                    {
                        if (p > 500)
                        {
                            //System.out.println("tablesize=" + tableSize + " Group = " + matcher.group());
                            sb.replace(matcher.start(),matcher.end(),"LONG VARGRAPHIC");
                            found = true;
                            break;
                        }
                    }
                }
                if (!found)
                    break;
            }
            if (oldTableSize > 0)
            {
                sb.insert(0, "-- The original approx table size was " + oldTableSize + linesep);            
                sb.insert(0, "-- or to LONG VARGRAPHIC column" + linesep);            
                sb.insert(0, "-- Some of the VARCHAR or VARGRAPHIC columns have been converted to LONG VARCHAR" + linesep);            
            }
            if (data.Oracle() && db2_compatibility)
            	sb.insert(0, "-- Estimated size of the table is wrong. I need to fix this." + linesep);
            sb.insert(0, "-- Approximate Table Size " + tableSize + linesep);            
            return sb.toString();
        }
        
        private void BuildUniqueConstraintMap()
        {
        	String methodName = "BuildUniqueConstraintMap";
        	String key = "", schema, table, prevSchema = "", prevTable = "", value, sql = "";
        	String consName, columnName;
        	ArrayList al = new ArrayList();
        	UniqData uq;
        	boolean added = false;
        	
        	uniqueConstraintMap = new HashMap();
        	
            if (data.Oracle())
                sql = "SELECT COL.OWNER, COL.TABLE_NAME, COL.COLUMN_NAME, CON.CONSTRAINT_NAME, COL.POSITION FROM DBA_CONS_COLUMNS COL, " +
                		"DBA_CONSTRAINTS CON WHERE COL.OWNER IN ("+schemaList+") AND CONSTRAINT_TYPE <> 'R' " +
                		"AND COL.OWNER = CON.OWNER AND COL.TABLE_NAME = CON.TABLE_NAME AND COL.CONSTRAINT_NAME = CON.CONSTRAINT_NAME AND " +
                		"CON.CONSTRAINT_TYPE = 'U' ORDER BY COL.CONSTRAINT_NAME, COL.POSITION";
            else if (data.Teradata())
            	sql = ""; 
            else if (data.Mssql())
            	sql =  "" +
				"SELECT T.TABLE_SCHEMA, T.TABLE_NAME, C.COLUMN_NAME, C.CONSTRAINT_NAME, U.ORDINAL_POSITION " + linesep +
				"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T, INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C, INFORMATION_SCHEMA.KEY_COLUMN_USAGE U " + linesep +
				"WHERE T.TABLE_SCHEMA IN ("+schemaList+")  " + linesep +
				"AND T.TABLE_SCHEMA = C.TABLE_SCHEMA " + linesep +
				"AND T.TABLE_NAME = C.TABLE_NAME " + linesep +
				"AND T.TABLE_SCHEMA = U.TABLE_SCHEMA " + linesep +
				"AND T.TABLE_NAME = U.TABLE_NAME " + linesep +
				"AND T.CONSTRAINT_TYPE = 'UNIQUE' " + linesep +
				"AND T.CONSTRAINT_NAME = C.CONSTRAINT_NAME " + linesep +
				"AND T.CONSTRAINT_NAME = U.CONSTRAINT_NAME " + linesep +
				"AND C.COLUMN_NAME = U.COLUMN_NAME";
            else if (data.iDB2())
            	sql =  "";
            else if (data.DB2())
            	sql =  "";
            else if (data.zDB2())
            	sql =  "";
            else if (data.Mysql())
            	sql = "";
            else if (data.Sybase())
            	sql = "";
            
            if (sql.equals("")) return;

            try
            {
                SQL s = new SQL(data.connection);
                s.PrepareExecuteQuery(methodName, sql);        	
                int i = 0;
                
	        	while (s.next())
	        	{
	        		uq = new UniqData(s.rs);
	        		schema = uq.schema;
	        		table = uq.table;
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			uniqueConstraintMap.put(key, al);
	        			al = new ArrayList();
	        			added = true;
	        		}
	        		key = schema + "." + table;
            		al.add(uq);
        		    added = false;
        			prevSchema = schema;
        			prevTable = table;
        			++i;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		uniqueConstraintMap.put(key, al);	        		
	        	}
                s.close(methodName);
        		log(i + " values cached in uniqueConstraintMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }            
        }
        
        private void checkUniqueConstraint(int id) throws SQLException, IOException
        {
        	if (netezza)
        		return;
        	
        	String methodName = "checkUniqueConstraint";
            int position = 1;
            String origColumn = "", column = "", newConsName, consName, commentit = "N";
            String schema = removeQuote(t.srcSchName[id]);
            String table = removeQuote(t.srcTableName[id]);
            String key = schema + "." + table;
            StringBuffer buffer = new StringBuffer();
            UniqData uqData;

            buffer.setLength(0);
            
            if (uniqueConstraintMap.containsKey(key))
            {
            	Iterator itr = ((ArrayList) uniqueConstraintMap.get(key)).iterator();
            	while (itr.hasNext())
            	{
            		uqData = (UniqData) itr.next();
                	origColumn = uqData.column;
                	column = getCaseName(getCustomColumnName(schema, table, origColumn));
                    position = uqData.position;
                    consName = uqData.constraintName;
                    if (position == 1)
                    {
                        if (buffer.length() > 0)
                        {
                            buffer.append(linesep);
                            buffer.append(")" + linesep + ";" + linesep);
                            buffer.append(linesep);
                            if (commentit.equals("Y"))
                            	db2PKeysWriter.write(IBMExtractUtilities.CommentString("This unique constraint is commented since the column used " +
                            			"is nullable. This constraint may not enable in DB2.",buffer).toString());
                            else
                                db2PKeysWriter.write(buffer.toString());
                        }
                        buffer.setLength(0);
                        commentit = "N";
                        newConsName = getTruncName(0, "", "UK" + numUniq + "_" + table,18);          
                        if (retainConstraintsName)
                        {
                        	if (!(consName == null || consName.length() == 0))
                        		newConsName = consName;
                        }
        	            buffer.append("--#SET :" + "UNIQUE_CONSTRAINT" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + newConsName + linesep);           	
                        buffer.append("ALTER TABLE " + getCaseName(t.dstSchemaName[id]) + "." + getCaseName(t.dstTableName[id]) + 
                              " ADD CONSTRAINT " + newConsName + " UNIQUE " + linesep);                            
                        buffer.append("(" + linesep);                            
                        buffer.append(putQuote(column));
                    	if (!notNullColumnsMap.containsKey(key+"."+origColumn))
                    	{
                    	   commentit = "Y";
                    	}
                    } else
                    {
                        buffer.append("," + linesep);
                        buffer.append(putQuote(column));
                    }
            	}
            }
            if (buffer.length() > 0)
            {
                buffer.append(linesep);                    
                buffer.append(")" + linesep + ";" + linesep);
                buffer.append(linesep);                    
                if (commentit.equals("Y"))
                {
                	db2PKeysWriter.write(IBMExtractUtilities.CommentString("This unique constraint is commented since the column used " +
                			"is nullable. This constraint may not enable in DB2.",buffer).toString());
                } else
                   db2PKeysWriter.write(buffer.toString());              
            }
        }
        
        private void BuildCheckColumns()
        {
        	String methodName = "BuildCheckColumns";        	
        	String key, value, sql = "";
        	
        	if (data.Oracle())
        	   sql = "select owner, table_name, constraint_name, column_name from dba_cons_columns " +
					"where owner IN ("+schemaList+")";
        	
        	checkColumnsMap = new HashMap();
        	
        	if (sql.length() == 0)
        		return;
        	        	
        	try
        	{
                SQL s = new SQL(data.connection);
                s.PrepareExecuteQuery(methodName, sql);        	
                int i = 0;
                while (s.next()) 
                {
                	key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2)) + "." + trim(s.rs.getString(3));
                	value = trim(s.rs.getString(4));
                	checkColumnsMap.put(key, value);
                	++i;
                }
        		s.close(methodName);
        		log(i + " values cached in checkColumnsMap");
        	} catch (SQLException e)
        	{
        		e.printStackTrace();
        	}
        }
        
        private void BuildNotNullcolumnsMap(int id, ResultSetMetaData rsmd)
        {
        	String methodName = "BuildNotNullcolumnsMap";
        	String key = removeQuote(t.srcSchName[id]) + "." + removeQuote(t.srcTableName[id]);
        	String checkCondition, consName, columnName;
        	ArrayList al = new ArrayList();
        	boolean added = false;
        	int ij, colIndex, colCount;
        	String colName, colType, nullType;
        	
        	if (notNullColumnsMap == null)
        		notNullColumnsMap = new HashMap();
        	        	
            try
			{   
            	colCount = rsmd.getColumnCount();
	            for (colIndex = 1; colIndex <= colCount; ++colIndex)
	            {
	               ij = colIndex - 1;
				   colName = rsmd.getColumnName(colIndex);
	               nullType = (rsmd.isNullable(colIndex) == ResultSetMetaData.columnNoNulls) ? "N" : "Y";
	               if (nullType.equals("N"))
	                  notNullColumnsMap.put(key+"."+colName, nullType);
	            }
		    } catch (SQLException e)
		    {
			   e.printStackTrace();
		    }
        }
        
        private void BuildCheckConstraintMap()
        {
        	String methodName = "BuildCheckConstraintMap";
        	String key = "", schema, table, prevSchema = "", prevTable = "", value, sql = "";
        	String checkCondition, consName, columnName;
        	ArrayList al = new ArrayList();
        	boolean added = false;
        	
        	checkConstraintMap = new HashMap();
        	
            if (data.Oracle())
            	sql = "SELECT OWNER, TABLE_NAME, SEARCH_CONDITION, CONSTRAINT_NAME "+
                           "FROM DBA_CONSTRAINTS WHERE OWNER IN ("+schemaList+") " + 
                           "AND CONSTRAINT_TYPE = 'C'";
            else if (data.Teradata())
            	sql = "SELECT databasename, tablename, ColCheck, '' AS cons_name, ColumnName "+
                           "FROM DBC.showcolchecks WHERE databasename IN ("+schemaList+")"; 
            else if (data.Mssql())
            	sql =  "SELECT TABLE_SCHEMA, B.TABLE_NAME, CHECK_CLAUSE, A.CONSTRAINT_NAME " +
                            "FROM   INFORMATION_SCHEMA.CHECK_CONSTRAINTS A, " + 
                            "       INFORMATION_SCHEMA.TABLE_CONSTRAINTS B " +
                            "WHERE  A.CONSTRAINT_NAME = B.CONSTRAINT_NAME " +
                            "AND    TABLE_SCHEMA IN ("+schemaList+") ";
            else if (data.iDB2())
            	sql =  "SELECT B.TABLE_SCHEMA, B.TABLE_NAME, A.CHECK_CLAUSE, A.CONSTRAINT_NAME " +
                            "FROM   SYSIBM.CHECK_CONSTRAINTS A, " + 
                            "       SYSIBM.TABLE_CONSTRAINTS B " +
                            "WHERE  A.CONSTRAINT_SCHEMA = B.CONSTRAINT_SCHEMA " +
                            "AND    A.CONSTRAINT_NAME = B.CONSTRAINT_NAME " +
                            "AND    B.TABLE_SCHEMA IN ("+schemaList+") " +
                            "AND    B.CONSTRAINT_TYPE = 'CHECK'";
            else if (data.DB2())
            	sql =  "SELECT OWNER, TABNAME, TEXT, CONSTNAME " +
                "FROM   SYSCAT.CHECKS " + 
                "WHERE  TYPE = 'C' " +
                "AND    OWNER IN ("+schemaList+")";
            else if (data.zDB2())
            	sql =  "SELECT TBOWNER, TBNAME, CHECKCONDITION, CHECKNAME " +
                "FROM   SYSIBM.SYSCHECKS " + 
                "WHERE  TBOWNER IN ("+schemaList+")";
            else if (data.Mysql())
            {
            	sql = "select table_schema, table_name, column_type, '' AS cons_name, column_name from information_schema.columns " +
            			"where table_schema IN ("+schemaList+") and data_type = 'enum'";
            }
            else if (data.Sybase())
            {
            	sql = "SELECT user_name(o.uid), o.name, m.text, object_name(c.constrid) " +
            			"FROM sysconstraints c JOIN syscomments m ON c.constrid=m.id " +
            			"JOIN sysobjects o ON user_name(o.uid) IN ("+schemaList+") " +
            			//"JOIN syscolumns col ON col.id = c.colid and col.colid = c.colid " +
            			"JOIN sysusers u ON u.uid = o.uid " +
            			"WHERE  c.status=128 AND c.tableid=o.id ";
            			//"ORDER BY col.colid"
            			
            }            
            if (sql.equals("")) return;

            try
            {
                SQL s = new SQL(data.connection);
                s.PrepareExecuteQuery(methodName, sql);        	
                int i = 0;
                
	        	while (s.next())
	        	{
	        		schema = trim(s.rs.getString(1));
	        		table = trim(s.rs.getString(2));
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			checkConstraintMap.put(key, al);
	        			al = new ArrayList();
	        			added = true;
	        		}
	        		key = schema + "." + table;
                    checkCondition = trim(s.rs.getString(3));
                    consName = trim(s.rs.getString(4));
                    int jk = checkCondition.toUpperCase().indexOf("NOT NULL");
                    if (!(jk > 0))
                    {
    	                if (data.Mysql())
    	                {
	                		columnName = trim(s.rs.getString(5));
    	                } else if (data.Teradata())
    	                {
    	                	columnName = trim(s.rs.getString(5));
    	                } else
    	                {
    	                	columnName = " ";
    	                }
    	                value = checkCondition + "&split&" + consName + "&split&" + columnName;
            		    al.add(value);
    	                ++i;
                    }
        		    added = false;
        			prevSchema = schema;
        			prevTable = table;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		checkConstraintMap.put(key, al);	        		
	        	}

                s.close(methodName);
        		log(i + " values cached in checkConstraintMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }            
        }
        
        private void genCheckConstraints(int id) throws IOException
        {
        	if (netezza)
        		return;
        	        	
        	// Needs to be fixed for custom column name. Fixed for Oracle, Teradata. Remaining mssql, db2, idb2 etc.
        	// Check cons for zdb2 needs to be added.
            int numCheck = 1;
            String checkCondition = "", newConsName, consName, key;
            String schema = removeQuote(t.srcSchName[id]);
            String table = removeQuote(t.srcTableName[id]);
            String columnName;
            String[] tmpVal;
            StringBuffer buffer = new StringBuffer();

            key = schema + "." + table;
            if (checkConstraintMap.containsKey(key))
            {
            	Iterator itr = ((ArrayList) checkConstraintMap.get(key)).iterator();
            	while (itr.hasNext())
            	{
                	tmpVal = ((String) itr.next()).split("&split&");
	                checkCondition = tmpVal[0];
	                consName = tmpVal[1];
	                columnName = tmpVal[2];
	                buffer.setLength(0);
	                String tmp = FixString(table);
	                newConsName = getTruncName(0, "", "CK" + numCheck + "_" + tmp,18);
	                if (retainConstraintsName)
	                {
	                	if (!(consName == null || consName.length() == 0))
	                		newConsName = consName;
	                }
	                if (db2Skin())
	                {
			            buffer.append("--#SET :" + "CHECK_CONSTRAINTS" + ":" + schema + ":" + newConsName + linesep);           	
		                buffer.append("ALTER TABLE " + schema + "." + table + linesep);
	                } else
	                {
			            buffer.append("--#SET :" + "CHECK_CONSTRAINTS" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + newConsName + linesep);           	
		                buffer.append("ALTER TABLE " + getCaseName(t.dstSchemaName[id]) + "." + getCaseName(t.dstTableName[id]) + linesep);	                	
	                }
	                if (data.Mssql())
	                {
	                    int pos1 = checkCondition.indexOf('[') , pos2 = checkCondition.indexOf(']');
	                    checkCondition = checkCondition.replace('[', '"');
	                    checkCondition = checkCondition.replace(']', '"');
	                    if (pos1 >0 && pos2 >0)
	                    {
	                        checkCondition = checkCondition.substring(0,pos1) + 
	                                         checkCondition.substring(pos1,pos2).toUpperCase() + 
	                                         checkCondition.substring(pos2);
	                    }
	                } else if (data.Mysql())
	                {
	                	Pattern p = Pattern.compile("enum(.*)", Pattern.CASE_INSENSITIVE);
	                	Matcher m = p.matcher(checkCondition);
	                	while (m.find()) 
	                	{
	                		columnName = getCaseName(getCustomColumnName(t.srcSchName[id], t.srcTableName[id], columnName));
	                		checkCondition = columnName + " IN " + m.group(1);
	                	}	                	
	                } else if (data.Teradata())
	                {
	            		String newColName = getCaseName(getCustomColumnName(t.srcSchName[id], t.srcTableName[id], columnName));
	            		if (!newColName.equals(columnName))
	            		   checkCondition = checkCondition.replaceAll(columnName, newColName);
		                buffer.append("ADD CONSTRAINT " + newConsName + " " + checkCondition.trim() + " "+ linesep);
	                }
	                else if (data.Oracle())
	                {
	                	if (checkColumnsMap.containsKey(schema+"."+table+"."+consName))
	                	{
	                		columnName = (String) checkColumnsMap.get(schema+"."+table+"."+consName);
	                		String newColName = getCaseName(getCustomColumnName(t.srcSchName[id], t.srcTableName[id], columnName));
	                		if (!newColName.equals(columnName))
	                		   checkCondition = checkCondition.replaceAll(columnName, newColName);
	                        buffer.append("ADD CONSTRAINT " + newConsName + " CHECK (" + checkCondition.trim() + ")"+ linesep);	                	
	                	}
	                } else if (data.Sybase())
	                {
	                	if (checkCondition.matches("(?i).*" + consName + ".*"))
	                	{
	                		checkCondition = checkCondition.substring(checkCondition.indexOf(consName) + consName.length()+1);	                		
	                	}
	                    buffer.append("ADD CONSTRAINT " + newConsName + " " + checkCondition.trim() + " "+ linesep);
	                }
	                else
	                    buffer.append("ADD CONSTRAINT " + newConsName + " CHECK (" + checkCondition.trim() + ")"+ linesep);
	                numCheck++;
	                if (db2Skin())
	                {
	                    db2CheckWriter.write(buffer.toString() + "go" + linesep);
	                } else
	                {
	                    db2CheckWriter.write(buffer.toString() + ";" + linesep);
	                }	                	
	                db2CheckWriter.write(linesep);
            	}
            }
        }
        
        private void getIdentityAttributes(String schema, String table, String column) throws SQLException
        {
        	String methodName = "getIdentityAttributes";
            String identitySQL = "";
            SQL s = new SQL(data.connection);
            
            if (data.Mssql())
            {
               identitySQL =  "SELECT CONVERT(BIGINT,INCREMENT_VALUE) INCREMENT_VALUE, " +
                              "CONVERT(BIGINT,LAST_VALUE) LAST_VALUE " +
                              "FROM   SYS.IDENTITY_COLUMNS " + 
                              "WHERE  OBJECT_NAME(OBJECT_ID) = '"+removeQuote(table)+"' " +
                              "AND    NAME = '"+column+"' ";
            } else if (data.DB2())
            {
            	identitySQL =  "select s.increment, s.lastassignedval from sysibm.syssequences s, sysibm.sysdependencies d " +
            			"where s.seqtype = 'I' " +
            			"and s.seqschema = d.bschema " +
            			"and s.seqname = d.bname " +
            			"and d.btype = 'Q' " +
            			"and s.seqschema = '"+removeQuote(schema)+"' " +
            			"and d.dname = '"+removeQuote(table)+"'";
            } else if (data.zDB2())
            {
            	identitySQL =  "select s.increment, s.MAXASSIGNEDVAL from sysibm.syssequences s, sysibm.SYSSEQUENCESDEP d " +
            			"where s.seqtype = 'I' " +
            			"and d.DTYPE = s.SEQTYPE " + 
            			"and s.SCHEMA = d.dcreator " +
            			"and S.SEQUENCEID = D.BSEQUENCEID " +
            			"and s.SCHEMA = '"+removeQuote(schema)+"' " +
            			"and d.DNAME = '"+removeQuote(table)+"'";
            } else if (data.Mysql())
            {
            	identitySQL =  "select 1, auto_increment from information_schema.tables " +
            			"where table_schema = '"+removeQuote(schema)+"' and table_name = '"+removeQuote(table)+"' ";
            } else if (data.Teradata())
            {
            	identitySQL =  "SELECT Increment, AvailValue FROM DBC.IDCol WHERE (DatabaseID, TableID) = " +
            			"(SELECT t.DatabaseID, t.TVMID FROM DBC.TVM t INNER JOIN DBC.dbase d ON t.DatabaseID = d.DatabaseID " +
            			"WHERE TVMNameI = '"+removeQuote(table)+"' AND d.DatabasenameI = '"+removeQuote(schema)+"')";
            }
            
            if (identitySQL.equals("")) 
            {
               String sql = "SELECT MAX("+putQuote(column)+") FROM " + schema + "." + table;
               String result = IBMExtractUtilities.executeSQL(data.connection, sql, null, false);
               try
               {
            	   last_value = Integer.parseInt(result);
               } catch (Exception e)
               {
            	   last_value = -1;
               }
               increment_value = 1;
               return;
            }
            try
            {
               s.PrepareExecuteQuery(methodName, identitySQL);        	
               while (s.next()) 
               {
                   increment_value = s.rs.getLong(1);
                   last_value = s.rs.getLong(2);
               }
               s.close(methodName); 
            } catch (SQLException e)
            {
            	if (data.Mssql())
            	{
                   identitySQL =  "SELECT CONVERT(BIGINT,IDENT_INCR(TABLE_NAME)) AS INCREMENT_VALUE, " +
                              "CONVERT(BIGINT,IDENT_CURRENT(TABLE_NAME)) AS LAST_VALUE " +
                              "FROM INFORMATION_SCHEMA.TABLES " + 
                              "WHERE OBJECTPROPERTY(OBJECT_ID(TABLE_NAME), 'TableHasIdentity') = 1 " +
                              "AND TABLE_TYPE = 'BASE TABLE' " + 
                              "AND TABLE_NAME = '"+removeQuote(table)+"' ";
            	
	               try
	               {
	                  s.PrepareExecuteQuery(methodName, identitySQL); 
	                  while (s.next()) 
	                  {
	                      increment_value = s.rs.getLong(1);
	                      last_value = s.rs.getLong(2);
	                  }
	                  s.close(methodName); 
	               } catch (SQLException e1)
	               {
	                  throw e1;
	               }
            	}
            }
        }

        private String formatDate(String strDate)
        {
        	String methodName = "formatDate";
        	SQL s = new SQL(data.connection);
        	/*******************************************************************
			 * Pattern p =
			 * Pattern.compile(".*(\\d{4})\\-(\\d{2})\\-(\\d{2})\\s+(\\d{2}):(\\d{2}):(\\d{2}).*");
			 * Matcher m = p.matcher(strDate); if (m.matches()) { return
			 * m.group(1)+"-"+m.group(2)+"-"+m.group(3)+"-"+m.group(4)+"."+m.group(5)+"."+m.group(6)+".000000"; }
			 * return "";
			 ******************************************************************/
			String sql = "";
			if (strDate.toLowerCase().startsWith("to_date")) {
				sql = "SELECT TO_CHAR(" + strDate
						+ ",'YYYY-MM-DD HH24:MI:SS\".00000\"') FROM DUAL";
			} else {
				sql = "SELECT TO_CHAR(TO_DATE(" + strDate
						+ "),'YYYY-MM-DD HH24:MI:SS\".00000\"') FROM DUAL";
			}
			try 
			{
				s.PrepareExecuteQuery(methodName, sql);        	

				if (s.next()) 
				{
					String retDate = s.rs.getString(1);
					s.close(methodName);
					return retDate;
				}
			} catch (SQLException e) {
				return "";
			}
			return "";
        }

        private String TempOnCommitIndicator(String schemaName, String tabName)
        {
        	SQL s = new SQL(data.connection);
        	String methodName = "TempOnCommitIndicator";
        	String commitTok = "ON COMMIT PRESERVE ROWS";
        	String sql = "";
        	
        	if (data.Oracle())   
        	{
        		if (majorSourceDBVersion != -1 && majorSourceDBVersion > 8)
                   sql = "select decode(duration, 'SYS$SESSION','ON COMMIT PRESERVE ROWS','SYS$TRANSACTION','ON COMMIT DELETE ROWS', NULL) COMMIT_TOKEN " +
            		     "from dba_tables " +
            		     "WHERE OWNER = " + "'" + removeQuote(schemaName) + "' " + 
                         "AND TABLE_NAME = '" + removeQuote(tabName) + "' " +         
                         "AND TEMPORARY = 'Y'";
        		else
        			sql = "";
        	}
        	else if (data.Teradata())
        		sql = "select case when t.commitopt = 'D' then 'ON COMMIT DELETE ROWS' else 'ON COMMIT PRESERVE ROWS' end " +
        				"from dbc.tvm t where t.databaseid = (select d.databaseid from dbc.dbase d where upper(d.DatabasenameI) = upper('" + removeQuote(schemaName) + "')) " +
		    			"and t.tvmnamei = '" + removeQuote(tabName) + "' " +
		    			"and t.commitopt in ('D','P')";
        	
        	if (sql.equals(""))
        		return "";
        	
			try
			{
				s.PrepareExecuteQuery(methodName, sql);        	

				if (s.next())
				{
					commitTok = s.rs.getString(1);
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				e.printStackTrace();
			}
			return commitTok;        	
        }
        
        private void BuildTemporaryTableMap()
        {
        	String methodName = "isTableTemporary";
        	String key, value, sql = "";

            tempTableMap = new HashMap();

            if (data.Oracle())
            	sql = "SELECT OWNER, OBJECT_NAME, 'X' FROM DBA_OBJECTS WHERE OWNER IN (" + schemaList + ") " + 
                "AND TEMPORARY = 'Y'";
            else if (data.Teradata())
            	sql = "select d.DatabasenameI, t.tvmnamei, 'X' from dbc.tvm t, dbc.dbase d where t.databaseid = d.databaseid " +
            			"and d.DatabasenameI IN (" + schemaList + ") " +
            			"and t.commitopt in ('D','P')";

            if (sql.length() == 0)
            	return;
            
            try
            {
                SQL s = new SQL(data.connection);
    			s.PrepareExecuteQuery(methodName, sql);        	
    			int i = 0;
    			while (s.next())
    			{
    				key = trim(s.rs.getString(1)) + "." + trim(s.rs.getString(2));
    				tempTableMap.put(key,"");
    			    ++i;
    			}
    			s.close(methodName);
    			log(i + " values cached in tempTableMap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }
            if (debug)
            {
            	IBMExtractUtilities.printHashMap(tempTableMap);
            }
        }
        
        private String isTableTemporary(String schemaName, String tabName)  throws SQLException
        {
        	String key = removeQuote(schemaName) + "." + removeQuote(tabName);
        	String isTemp = "";
                        
            if (tempTableMap.containsKey(key))
            {
			    isTemp = " GLOBAL TEMPORARY ";
            }
			return isTemp;        	
        }
        
        private String getCustomTableSpaces(String schemaName, String tableName)
        {
        	String key = removeQuote(schemaName) + "." + removeQuote(tableName);
        	String tsName = "", dataTS = "", idxTS = "", longTS = "";
        	
        	dataTS = (String) custTSBP.getProperty(key+".DATA");
        	if (dataTS != null && dataTS.length() > 0)
        	{
        		dataTS = "IN " + dataTS + " ";
        	} else
        		dataTS = "";
        	idxTS = (String) custTSBP.getProperty(key+".INDEX");
        	if (idxTS != null && idxTS.length() > 0)
        	{
        		idxTS = " INDEX IN " + idxTS + " ";
        	} else
        		idxTS = "";
        	longTS = (String) custTSBP.getProperty(key+".LONG");
        	if (longTS != null && longTS.length() > 0)
        	{
        		longTS = " LONG IN " + longTS + " ";
        	} else
        		longTS = "";
        	return dataTS + idxTS + longTS;
        }
        
        private String getzDB2TableSpaces(String schemaName, String tableName)
        {
        	String methodName = "getzDB2TableSpaces";
        	SQL s = new SQL(data.connection);

        	String tsName = "", dataTS = "", idxTS = "", longTS = "";
        	String sql1, sql2, sql3, size;
        	
        	sql1 = "select a.dbname, b.name, b.pgsize " +
        			"from sysibm.systables a, sysibm.systablespace b " +
        			"where a.tsname = b.name " +
        			"and a.dbname = b.dbname " +
        			"and a.creator = '"+removeQuote(schemaName)+"' " +
        			"and a.name = '"+removeQuote(tableName)+"' " +
        			"and a.type = 'T'";	

        	sql2 = "select max(pgsize/1024) " +
        			"from sysibm.sysindexes " +
        			"where tbname = '"+removeQuote(tableName)+"' " +
        			"and tbcreator = '"+removeQuote(schemaName)+"'";
        	
        	sql3 = "select max(b.pgsize) " +
        			"from sysibm.systables a, sysibm.systablespace b " +
        			"where a.tsname = b.name " +
        			"and a.dbname = b.dbname " +
        			"and (a.creator, a.name) in (select auxtbowner, auxtbname " +
        			"from sysibm.sysauxrels " +
        			"where tbowner = '"+removeQuote(schemaName)+"' and tbname = '"+removeQuote(tableName)+"')";
        	
			try
			{
				s.PrepareExecuteQuery(methodName, sql1);        	

				if (s.next())
				{
					tsName = trim(s.rs.getString(1)) + "_" + trim(s.rs.getString(2));
					size = trim(s.rs.getString(3));
					if (size == null || size.length() == 0)
					{
						dataTS = "";
					} else
					{
						dataTS = "D" + tsName;
						try
						{
							db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + dataTS + linesep);
		    		        db2tsbpWriter.write("CREATE LARGE TABLESPACE "+dataTS+" pagesize "+size+"K BUFFERPOOL BP" + size + linesep  + ";" + linesep + linesep);
		    		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + dataTS + linesep);
		    		        db2droptsbpWriter.write("DROP TABLESPACE "+dataTS+";" + linesep);
						} catch (IOException e)
						{
							e.printStackTrace();
						}           	
						dataTS = "IN " + dataTS + " ";						
					}
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				dataTS = "";
				e.printStackTrace();
			}
			
			try
			{
				s.PrepareExecuteQuery(methodName, sql2);

				if (s.next())
				{
					size = s.rs.getString(1);
					if (size == null || size.length() == 0)
					{
						idxTS = "";
					} else
					{
						idxTS = "X" + tsName;
						try
						{
							db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + idxTS + linesep);
		    		        db2tsbpWriter.write("CREATE LARGE TABLESPACE "+idxTS+" pagesize "+size+"K BUFFERPOOL BP" + size + linesep  + ";" + linesep + linesep);
		    		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + idxTS + linesep);
		    		        db2droptsbpWriter.write("DROP TABLESPACE "+idxTS+";" + linesep);
						} catch (IOException e)
						{
							e.printStackTrace();
						}           	
						idxTS = "INDEX IN " + idxTS + " ";						
					}
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				idxTS = "";
				e.printStackTrace();
			}

			try
			{
				s.PrepareExecuteQuery(methodName, sql3);   
				if (s.next())
				{
					size = s.rs.getString(1);
					if (size == null || size.length() == 0)
					{
						longTS = "";
					} else
					{
						longTS = "L" + tsName;
						try
						{
							db2tsbpWriter.write("--#SET :" + "BPTS" + ":" + "TABLESPACE" + ":" + longTS + linesep);
		    		        db2tsbpWriter.write("CREATE LARGE TABLESPACE "+longTS+" pagesize "+size+"K BUFFERPOOL BP" + size + linesep  + ";" + linesep + linesep);
		    		        db2droptsbpWriter.write("--#SET :" + "DROP" + ":" + "TABLESPACE" + ":" + longTS + linesep);
		    		        db2droptsbpWriter.write("DROP TABLESPACE "+longTS+";" + linesep);
						} catch (IOException e)
						{
							e.printStackTrace();
						}           	
						longTS = "LONG IN " + longTS + " ";						
					}
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				longTS = "";
				e.printStackTrace();
			}
			return dataTS + idxTS + longTS;
        }
        
        private String getOraTableSpaces(String schemaName, String tableName)
        {
        	String methodName = "getOraTableSpaces";
            String dataTS = "", idxTS = "", longTS = "", sql1, sql2, sql3;
            SQL s = new SQL(data.connection);
            
            // DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) is put since DB2 does not allow SYSTEM in TS Name
            sql1 = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_TABLES " +
            		"WHERE OWNER = '"+removeQuote(schemaName)+"' " +
            		"AND TABLE_NAME = '"+removeQuote(tableName)+"' ";
            sql2 = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_INDEXES " +
		    		"WHERE OWNER = '"+removeQuote(schemaName)+"' " +
		    		"AND TABLE_NAME = '"+removeQuote(tableName)+"' ";
            sql3 = "SELECT DECODE(TABLESPACE_NAME,'SYSTEM','DB2SYSTEM',TABLESPACE_NAME) FROM DBA_LOBS " +
		    		"WHERE OWNER = '"+removeQuote(schemaName)+"' " +
		    		"AND TABLE_NAME = '"+removeQuote(tableName)+"' ";
                  
			try
			{
				s.PrepareExecuteQuery(methodName, sql1);        	
				if (s.next())
				{
					dataTS = s.rs.getString(1);
					dataTS = (dataTS == null || dataTS.length() == 0) ? "" : "IN " + dataTS + " ";					  
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				dataTS = "";
				e.printStackTrace();
			}
			
			try
			{
				s.PrepareExecuteQuery(methodName, sql2);        	
				if (s.next())
				{
					idxTS = s.rs.getString(1);
					idxTS = (idxTS == null || idxTS.length() == 0) ? "" : "INDEX IN " + idxTS + " ";					  
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				idxTS = "";
				e.printStackTrace();
			}

			try
			{
				s.PrepareExecuteQuery(methodName, sql3);        	

				if (s.next())
				{
					longTS = s.rs.getString(1);
					longTS = (longTS == null || longTS.length() == 0) ? "" : "LONG IN " + longTS + " ";					  
				}
				s.close(methodName);
			} catch (SQLException e)
			{
				longTS = "";
				e.printStackTrace();
			}
			return dataTS + idxTS + longTS;
        }
        
        private void BuildDataTypeMap()
        {
        	double result;
            String methodName = "BuildDataTypeMap";
            String key, sql = "", sql2;
            String schema, table, columnName, dataType = "", charUsed, dataLength; 
            
            dataTypeMap = new HashMap();
            
            if (data.Oracle())
            {
               columnLengthMap = new HashMap();
               lobsLengthMap = new HashMap();
               if (data.version() >= 9.0F)
               {
         	       sql = "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHAR_USED, DATA_LENGTH " +
 	                  "FROM   DBA_TAB_COLUMNS " +
 	                  "WHERE  OWNER IN (" + schemaList + ") ORDER BY OWNER, TABLE_NAME, COLUMN_NAME";
               } else
               {
         	       sql = "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, 'X' AS CHAR_USED, DATA_LENGTH " +
	                  "FROM   DBA_TAB_COLUMNS " +
	                  "WHERE  OWNER IN (" + schemaList + ") ORDER BY OWNER, TABLE_NAME, COLUMN_NAME";            	   
               }
            } else if (data.Mysql())
            {
         	   sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE " +
 	               "FROM   INFORMATION_SCHEMA.COLUMNS " +
 	               "WHERE  TABLE_SCHEMA IN (" + schemaList + ") ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME";
         	}

            if (sql.length() == 0)
         	   return;
            
            try
            {
	            SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql);        	
	
	            int i = 0;
	            while (s.next())
	            {
	            	schema = trim(s.rs.getString(1));
	            	table = trim(s.rs.getString(2));
	            	columnName = trim(s.rs.getString(3));
	            	key =  schema + "." + table + "." + columnName;
	                dataType = trim(s.rs.getString(4));
	                if (dataType != null)
		               dataType = dataType.toUpperCase();
	                dataTypeMap.put(key, new DataMap(dataType));
	                if (data.Oracle())
	                {
	                	charUsed = s.rs.getString(5);
	                	dataLength = s.rs.getString(6);
	                	if ((charUsed != null && charUsed.equalsIgnoreCase("C")) && 
	                			(dataType.equalsIgnoreCase("CHAR") || dataType.equalsIgnoreCase("VARCHAR2") || dataType.equalsIgnoreCase("VARCHAR")))
	                		columnLengthMap.put(key, dataLength);
	                	if (dataType.equalsIgnoreCase("BLOB") || dataType.equalsIgnoreCase("CLOB") || dataType.equalsIgnoreCase("NCLOB"))
	                	{
	                		sql2 = "SELECT MAX(DBMS_LOB.GETLENGTH(" + putQuote(columnName) + ")) FROM " + putQuote(schema) + "." + putQuote(table);
	                		result = data.queryFirstLongValue(sql2);
	                		if (result != -1.0)
	                		{
	                			if (dataType.equalsIgnoreCase("BLOB"))
	                			{
	                				lobsLengthMap.put(key, IBMExtractUtilities.getLobSize(result * 1.2));
	                			} else if (dataType.equalsIgnoreCase("CLOB"))
	                			{
	                				lobsLengthMap.put(key, IBMExtractUtilities.getLobSize(result * 2.0));
	                			} else if (dataType.equalsIgnoreCase("NCLOB"))
	                			{
	                				lobsLengthMap.put(key, IBMExtractUtilities.getLobSize(result * 4.0));
	                		    }
	                		}
	                	}
	                }
	                ++i;
	            }
	            s.close(methodName);
	            log(i + " data types cached in datatypemap");
            } catch (SQLException e)
            {
            	e.printStackTrace();
            }
            if (debug)
            {
            	IBMExtractUtilities.printHashMap(dataTypeMap);
            	IBMExtractUtilities.printHashMap(columnLengthMap);
            	IBMExtractUtilities.printHashMap(lobsLengthMap);
            }
        }
        
        private String getInitialValue(int numCols, String type)
        {
        	String modifier = "";
    		for (int i = 0; i < numCols; ++i)
    		{
    			modifier += (i == 0) ? type : ","+type;
    		}
        	return modifier;
        }
        
        private String getEndingValue(String dataTypes, String limitKey)
        {
        	String modifier = "", partValue;
        	String[] dataType = dataTypes.split(",");
        	String[] partKey = limitKey.split(",");
        	
        	if (dataType.length != partKey.length)
        		return limitKey;
        	
    		for (int i = 0; i < partKey.length; ++i)
    		{
                if (dataType[i].equalsIgnoreCase("DATE") || dataType[i].equalsIgnoreCase("TIMESTAMP"))
                {
                	if (data.Oracle())
                       partValue = "'" + formatDate(partKey[i]) + "'";
                	else                		
                       partValue = "'" + partKey[i] + "'";
                }
                else
                {
                   partValue = partKey[i];
                }
    			modifier += (i == 0) ? partValue : ","+partValue;
    		}
        	return modifier;
        }

        private void BuildPartitionColumnTypesMap()
        {
        	String methodName = "BuildPartitionColumnTypesMap";
        	tabColumnsMap = new HashMap();
        	ArrayList al = new ArrayList();
        	String schema, table;
        	boolean added = false;
        	String key = "", sql = "", prevSchema = "", prevTable = "";

        	if (data.Oracle())
        	{
        		sql = "SELECT OWNER, TABLE_NAME, DATA_TYPE " +
		            "FROM   DBA_TAB_COLUMNS " +
		            "WHERE  OWNER IN (" + schemaList + ") " + 
		            "ORDER BY OWNER, TABLE_NAME, COLUMN_ID";
        	} else if (data.zDB2())
        	{
        		sql = "SELECT TBCREATOR, TBNAME, COLTYPE " +
        				"FROM SYSIBM.SYSCOLUMNS " +
        				"WHERE TBCREATOR IN (" + schemaList + ") " +
        				"AND PARTKEY_COLSEQ > 0 " +
        				"ORDER BY TBCREATOR, TBNAME, PARTKEY_COLSEQ";
        	}
        	
        	if (sql.length() == 0)
        		return;
        	
        	try
        	{
        		SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql); 
	            int i = 0;
	        	while (s.next())
	        	{
	        		schema = trim(s.rs.getString(1));
	        		table = trim(s.rs.getString(2));
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			tabColumnsMap.put(key, al);
	        			al = new ArrayList();
	        			added = true;
	        		}
	        		key = schema + "." + table;
        		    al.add(new TabColumnsData(s.rs));
        		    added = false;
        			prevSchema = schema;
        			prevTable = table;
	        		++i;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		tabColumnsMap.put(key, al);	        		
	        	}
	        	s.close(methodName);
	            log(i + " values cached in tabColumnsMap");
        	} catch (SQLException e)
        	{
        		e.printStackTrace();
        	}        	
        }
        
        private String getPartitionColumnsTypes(String schemaName, String tabName)
        {
        	int i = 0;
        	String columnType = "", columnType2 = "", sql = "";
        	String key = removeQuote(schemaName) + "." + removeQuote(tabName);
        	if (tabColumnsMap.containsKey(key))
        	{
        		ArrayList al = (ArrayList) tabColumnsMap.get(key);
        		Iterator itr = al.iterator();
        		while (itr.hasNext())
        		{
        			TabColumnsData c = (TabColumnsData) itr.next();
	            	columnType2 = c.dataType;
	            	columnType += (i++ > 0) ? "," + columnType2 : columnType2;
        		}
        	}
            return columnType;
        }

        private void BuildPartitionColumnsMap()
        {
        	String methodName = "BuildPartitionColumnsMap";
        	partitionColumnsMap = new HashMap();
        	ArrayList al = new ArrayList();
        	String schema, table;
        	boolean added = false;
        	String key = "", sql = "", prevSchema = "", prevTable = "";

        	if (data.Oracle())
        	{
        		sql = "SELECT C.OWNER, C.NAME, C.COLUMN_NAME " +
		            "FROM   DBA_PART_KEY_COLUMNS C " +
		            "WHERE  C.OWNER IN (" + schemaList + ") " + 
		            "AND    C.OBJECT_TYPE = 'TABLE' " +
		            "ORDER BY C.OWNER, C.NAME, COLUMN_POSITION";
        	} else if (data.zDB2())
        	{
        		sql = "SELECT TBCREATOR, TBNAME, NAME " +
        				"FROM SYSIBM.SYSCOLUMNS " +
        				"WHERE TBCREATOR IN (" + schemaList + ") " +
        				"AND PARTKEY_COLSEQ > 0 " +
        				"ORDER BY TBCREATOR, TBNAME, PARTKEY_COLSEQ";
        	}
        	
        	if (sql.length() == 0)
        		return;
        	
        	try
        	{
        		SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql); 
	            int i = 0;
	        	while (s.next())
	        	{
	        		schema = trim(s.rs.getString(1));
	        		table = trim(s.rs.getString(2));
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			partitionColumnsMap.put(key, al);
	        			al = new ArrayList();
	        			added = true;
	        		}
	        		key = schema + "." + table;
        		    al.add(new TabColumnsData(s.rs, ""));
        		    added = false;
        			prevSchema = schema;
        			prevTable = table;
	        		++i;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		partitionColumnsMap.put(key, al);	        		
	        	}
	        	s.close(methodName);
	            log(i + " values cached in partitionColumnsMap");
        	} catch (SQLException e)
        	{
        		e.printStackTrace();
        	}        	        	
        }
        
        private String getPartitionColumns(String schemaName, String tabName)
        {
        	int i = 0;
        	String columnName = "", columnName2 = "", sql = "";
        	
        	String key = removeQuote(schemaName) + "." + removeQuote(tabName);

        	if (partitionColumnsMap.containsKey(key))
        	{
        		ArrayList al = (ArrayList) partitionColumnsMap.get(key);
        		Iterator itr = al.iterator();
        		while (itr.hasNext())
        		{
        			TabColumnsData c = (TabColumnsData) itr.next();
	            	columnName2 = getCaseName(getCustomColumnName(schemaName, tabName, c.columnName));
	                columnName += (i++ > 0) ? "," + putQuote(columnName2) : putQuote(columnName2);
        		}
        		
        	}
            return columnName;
        }
        
        private void BuildPartitionTypeMap()
        {
        	String methodName = "BuildPartitionTypeMap";
        	partitionTypeMap = new HashMap();
        	String schema, table;
        	String key, value, sql = "";

        	if (data.Oracle())
        	{
        	     sql = "SELECT OWNER, TABLE_NAME, PARTITIONING_TYPE FROM DBA_PART_TABLES " +
        			"WHERE OWNER IN ("+schemaList+") ORDER BY OWNER, TABLE_NAME";
        	} else if (data.zDB2())
        	{
        		sql = "SELECT CREATOR, NAME, PARTKEYCOLNUM FROM SYSIBM.SYSTABLES " +
        				"WHERE CREATOR IN ("+schemaList+") ORDER BY CREATOR, NAME, PARTKEYCOLNUM";
        	}
        	
        	if (sql.length() == 0)
        		return;
        	
        	try
        	{
        		SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql); 
	            int i = 0;
	            while (s.next())
	            {
	            	schema = trim(s.rs.getString(1));
	            	table = trim(s.rs.getString(2));
	            	key = schema + "." + table;
	            	value = trim(s.rs.getString(3));
	            	partitionTypeMap.put(key, value);
	            	++i;
	            }	            
	            s.close(methodName);
	            if (debug) log (i + " values cached in partitionTypeMap");
        	} catch (SQLException e)
        	{
        		e.printStackTrace();
        	}
        }
        
        private String PartitionType(String schemaName, String tabName)
        {
        	String key = removeQuote(schemaName) + "." + removeQuote(tabName), partType = "";
        	int partNum;
        	        	
        	if (partitionTypeMap.containsKey(key))
        	{
        		partType = (String) partitionTypeMap.get(key);
        		if (partType == null ||  partType.equalsIgnoreCase("null"))
        			partType = "";
            	if (data.zDB2())
            	{
            		try
            		{
            		   partNum = Integer.valueOf(partType);
            		} catch (Exception e)
            		{
            			partNum = 0;
            		}
            		if (partNum == 0)
            			partType = "";
            		else
            			partType = "RANGE";
            	}
        	}
            return partType;
        }
        
        private void BuildPartitionsMap()
        {
        	String methodName = "BuildPartitionsMap";
        	partitionsMap = new HashMap();
        	ArrayList al = new ArrayList();
        	String schema, table;
        	boolean added = false;
        	String key = "", sql = "", prevSchema = "", prevTable = "";

        	if (data.Oracle())
        	{
        		sql = "SELECT T.TABLE_OWNER, T.TABLE_NAME, T.PARTITION_NAME, T.HIGH_VALUE, T.HIGH_VALUE_LENGTH " +
                "FROM   DBA_TAB_PARTITIONS T " + 
                "WHERE  T.TABLE_OWNER IN (" + schemaList + ") " + 
                "ORDER BY T.TABLE_OWNER, T.TABLE_NAME, T.PARTITION_POSITION";
        	} else if (data.zDB2())
        	{
                sql = "SELECT T.CREATOR, T.NAME, P.PARTITION, P.LIMITKEY " +
		            "FROM SYSIBM.SYSTABLEPART P, SYSIBM.SYSTABLES T " +
		            "WHERE P.TSNAME = T.TSNAME " +
		            "AND T.CREATOR IN (" + schemaList + ") " +
		            "ORDER BY T.CREATOR, T.NAME, P.PARTITION";                     
        	}
        	
        	if (sql.length() == 0)
        		return;
        	
        	try
        	{
        		SQL s = new SQL(data.connection);
	            s.PrepareExecuteQuery(methodName, sql); 
	            int i = 0;
	        	while (s.next())
	        	{
	        		schema = trim(s.rs.getString(1));
	        		table = trim(s.rs.getString(2));
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			partitionsMap.put(key, al);
	        			al = new ArrayList();
	        			added = true;
	        		}
	        		key = schema + "." + table;
        		    al.add(new PartitionData(s.rs, data));
        		    added = false;
        			prevSchema = schema;
        			prevTable = table;
	        		++i;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		partitionsMap.put(key, al);	        		
	        	}
	        	s.close(methodName);
	            log(i + " values cached in partitionsMap");
        	} catch (SQLException e)
        	{
        		e.printStackTrace();
        	}
        }
        
        private String fixOraPartDate(String input)
        {
        	String regex = "\\s*to_date\\s*\\(\\s*\\'(.*?)\\s*\\'.*\\)";
    		String s;
    		Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
    		Matcher m = p.matcher(input);
    		if (m.matches())
    		{
    			if (m.group(1) != null)
    			   s = "'"+m.group(1).trim()+"'";
    			else
    			   s = input;
    		} else
    			s = input;
    		return s;
        }
        
        private String genOraPartitions(String schemaName, String tabName) throws SQLException
        {
           String partValue, partName, dataTypes;
           String partColNames, partType = PartitionType(schemaName, tabName);
           int colNums;
           StringBuffer buffer = new StringBuffer();
           
           if (partType == null || partType.length() == 0)
        	   return "";
           if (partType.equalsIgnoreCase("RANGE") && extractPartitions == false)
         	   return "-- Range Partition not extracted since extractPartitions=false in "+IBMExtractUtilities.getConfigFile() + linesep;
           if (partType.equalsIgnoreCase("HASH") && extractHashPartitions == false)
         	   return "-- Hash Partition not extracted since extractHashPartitions=false in "+IBMExtractUtilities.getConfigFile() + linesep;
           if (partType.equalsIgnoreCase("LIST"))
        	   return "-- List Partitions are not supported in DB2" + linesep;
           partColNames = getPartitionColumns(schemaName, tabName);
           dataTypes = getPartitionColumnsTypes(schemaName, tabName);
           colNums = partColNames.split(",").length; 
           int i = 0;
           String key = removeQuote(schemaName) + "." + removeQuote(tabName);
                 
           if (partitionsMap.containsKey(key))
           {
        	   ArrayList al = (ArrayList) partitionsMap.get(key);
        	   Iterator itr = al.iterator();
        	   while (itr.hasNext())
        	   {
        		    PartitionData p = (PartitionData) itr.next();
	   				partName = p.partitionName;
					partValue = fixOraPartDate(p.highValue);
					if (i > 0) 
					{ 
					   buffer.append(",");
					} else {
						if (partType.equalsIgnoreCase("RANGE"))
						{
					       buffer.append("PARTITION BY RANGE ("+partColNames+")" + linesep);
					       buffer.append("(" + linesep);
					       buffer.append("PARTITION " + partName + " STARTING ("+getInitialValue(colNums,"MINVALUE")+") INCLUSIVE ENDING ("+ getEndingValue(dataTypes, partValue) +") INCLUSIVE" + linesep);
						} else if (partType.equalsIgnoreCase("HASH"))
						{
					       buffer.append("DISTRIBUTE BY HASH ("+partColNames+")" + linesep);                    		
						}
					    ++i;
					    continue;
					}
					if (partType.equalsIgnoreCase("RANGE"))
					{
					    if (partValue.matches("(?i).*MAXVALUE.*")) 
					    {
					       buffer.append("PARTITION " + partName + " ENDING ("+getInitialValue(colNums,"MAXVALUE")+") INCLUSIVE" + linesep);	                	
					    }
					    else
					    {
					       buffer.append("PARTITION " + partName + " ENDING ("+ getEndingValue(dataTypes, partValue) +") INCLUSIVE" + linesep);	                	
					    }
					}
					i++;        		   
        	   }
           }
           if (partType.equalsIgnoreCase("RANGE"))
              buffer.append(")" + linesep);
           if (i == 0) 
           {
              return "";
           }
           else 
           {
              buffer.append("-- Please check the converted DDL for partitions"+linesep);
              return buffer.toString();
           }
        }
        
        private String genzDB2Partitions(String schemaName, String tabName) throws SQLException
        {
            int i, colNums;
            String partColNames, dataTypes, partType = PartitionType(schemaName, tabName);
            StringBuffer buffer = new StringBuffer();  
            String limitKey, partName;
            
            if (partType == null || partType.length() == 0)
         	   return "";
            if (partType.equalsIgnoreCase("RANGE") && extractPartitions == false)
         	   return "-- Partition not extracted since extractPartitions=false in "+IBMExtractUtilities.getConfigFile() + linesep;
            
            partColNames = getPartitionColumns(schemaName, tabName);
            dataTypes = getPartitionColumnsTypes(schemaName, tabName);
            colNums = partColNames.split(",").length; 

            buffer.append("PARTITION BY RANGE ("+partColNames+")" + linesep);
            buffer.append("(" + linesep);


            String key = removeQuote(schemaName) + "." + removeQuote(tabName);
            
            i = 0;
            if (partitionsMap.containsKey(key))
            {
         	   ArrayList al = (ArrayList) partitionsMap.get(key);
         	   Iterator itr = al.iterator();
         	   while (itr.hasNext())
         	   {
         		    PartitionData p = (PartitionData) itr.next();
                    if (i > 0) buffer.append(",");
                    partName = "PART_" + String.format("%05d", p.partNum);
                    limitKey = p.limitKey;
                    if (i == 0)
                    {
                        buffer.append("PARTITION " + partName + " STARTING ("+getInitialValue(colNums,"MINVALUE")+") " +
                        		"INCLUSIVE ENDING AT (" + getEndingValue(dataTypes, limitKey) + ") INCLUSIVE " + linesep);                	
                    } else
                    {
                    	if (limitKey.matches("(?i).*MAXVALUE.*"))
                    	{
    	                    buffer.append("PARTITION " + partName + " ENDING ("+getInitialValue(colNums,"MAXVALUE")+") INCLUSIVE" + linesep);	                	
                    	} else
                    	{
                            buffer.append("PARTITION " + partName + " ENDING AT (" + getEndingValue(dataTypes, limitKey) + ") INCLUSIVE " + linesep);                		
                    	}
                    }
                    i++;
         	   }
            }

            buffer.append(")" + linesep);
            if (i == 0) 
            {
                return "";
            }
            else 
            {
                buffer.append("-- Please check the converted DDL for partitions"+linesep);
                return buffer.toString();
            }
        }
 
        private void getSybaseUDTInfo()
        {
        	String methodName = "getSybaseUDTInfo";
        	String userType, baseType, length, precision, scale;
        	String sql = "select User_type = s.name, Storage_type = st.name, s.length, s.prec, s.scale " +
        		"from systypes s, systypes st " +
        		"where s.type = st.type " +
        		"and s.usertype > 99 " +
        		"and st.usertype < 100 " +
        		"and st.name not in ('sysname', 'longsysname', 'nchar', 'nvarchar')";
        	
        	SQL s = new SQL(data.connection);

            try
            {
              if (udtProp.size() > 0)
            	return;       
              
              s.PrepareExecuteQuery(methodName, sql);	

              while (s.next()) 
              {
            	  userType = s.rs.getString(1).toUpperCase();
            	  baseType = s.rs.getString(2).toUpperCase();
            	  length = s.rs.getString(3);
            	  precision = s.rs.getString(4);
            	  scale = s.rs.getString(5);
            	  if (baseType.equals("VARCHAR") || baseType.equals("CHAR") || baseType.equals("BINARY") || 
            			  baseType.equals("NCHAR") || baseType.equals("UNICHAR") || baseType.equals("DECIMAL") ||  
            			  baseType.equals("IMAGE") || baseType.equals("TEXT") || baseType.equals("NTEXT") ||
            			  baseType.equals("UNITEXT") || baseType.equals("NUMERIC") || baseType.equals("VARBINARY") ||
            			  baseType.equals("NVARCHAR") || baseType.equals("UNIVARCHAR") || baseType.equals("BINARY"))
            	  {
            		  if (precision != null)
            		  {
            			  if (scale != null)
            			  {
            				  if (scale.equalsIgnoreCase("0"))
            		             baseType = baseType+"("+precision+")";
            				  else
            					 baseType = baseType+"("+precision+","+scale+")";
            			  }
            			  else
            				 baseType = baseType+"("+precision+")";
            		  } else if (length != null)
            		  {
         				 baseType = baseType+"("+length+")";            			  
            		  }
            	  }
            	  udtProp.setProperty(userType, baseType);
              }
              s.close(methodName); 
            } catch (Exception e)
            {
            	log("Error in getting Sybase UDT Information ");
            	e.printStackTrace();
            }             
        }
        
        private String getSybDataType(String colType, int precision, int scale)
        {        	
        	String dataType = "", baseType;
        	if (colType == null)
        		return " <ERROR>";
        	else
        	{
        		dataType = colType.toUpperCase();
        		baseType = (String) udtProp.get(dataType);
        		if (baseType != null)
        		{
        			dataType = baseType;
        			colType = dataType.toLowerCase();
        		}
        	}
        	if (dataType.equals("NUMERIC") || dataType.equals("DECIMAL") || dataType.equals("DEC"))
        	{
        		if (precision > 31)
        			precision = 31;
        		if (scale > 31)
        			scale = 31;
        		return colType + "(" + precision + "," + scale + ")";
        	}
        	else if (dataType.equals("FLOAT") || dataType.equals("CHAR") || dataType.equals("CHARACTER") ||
        			dataType.equals("VARCHAR") || dataType.equals("CHAR VARYING") || dataType.equals("CHARACTER VARYING") ||
        			dataType.equals("NCHAR") || dataType.equals("NVARCHAR") || dataType.equals("BINARY") || dataType.equals("VARBINARY") ||
        			dataType.equals("UNICHAR") || dataType.equals("UNIVARCHAR"))
        	{
        		if (precision <= 0)
        			return colType;
        		else
        			return colType + "(" + precision + ")";
        	}
        	else
        	   return colType;
        }
        
        private void genSybaseTableScript(int id, long numRows) throws Exception
        {
        	String methodName = "genSybaseTableScript";
            int precision, scale, colDisplayWidth;
            String schema = removeQuote(t.srcSchName[id]), table = removeQuote(t.srcTableName[id]);
            String colType = "", colName = "", nullType = "", identity, deflt, defValue, compCol;
            StringBuffer buffer = new StringBuffer();  
            boolean nulls, isAutoIncr;
            String dstSchema = sybSchema(schema);
            ResultSetMetaData metadata = dd.resultsetMetadata;
            
            int colIndex = 1, colCount = metadata.getColumnCount();
            
            if (majorSourceDBVersion >= 15)
            	compCol = "c.computedcol";
            else
            	compCol = "c.cdefault";
            
        	String sql = "SELECT c.name cname, t.name type, cm.text deflt, c.length, c.prec, c.scale, " +
        			"convert(bit, (c.status & 8)) nulls, convert(bit, (c.status & 0x80)) iden " +
        			"FROM syscolumns c " +
        			"JOIN sysobjects o ON c.id = o.id " +
        			"LEFT JOIN systypes t ON c.type = t.type AND c.usertype = t.usertype " +
        			"LEFT JOIN syscomments cm " +
        			"ON cm.id = CASE WHEN c.cdefault = 0 THEN "+compCol+" ELSE c.cdefault END " +
        			"LEFT JOIN sysusers u ON u.uid = o.uid " +
        			"WHERE u.name = '"+schema+"' and o.name = '"+table+"' AND o.type = 'U' " +
        			"order by c.colid";
    	
        	db2DropWriter.write("--#SET :" + "DROP:" + "TABLE:" + schema + "." + table + linesep);           	
            db2DropWriter.write("DROP TABLE "+ schema + "." + table + linesep + "go" + linesep);
            buffer.setLength(0);
            buffer.append("--#SET :" + "TABLE" + ":" + schema + ":" + table + linesep);           	
            buffer.append("CREATE TABLE " + schema + "." + table + linesep);
            buffer.append("(" + linesep);
            
            SQL s = new SQL(data.connection);
            try
            {
	            s.PrepareExecuteQuery(methodName, sql);	
	            while (s.next()) 
	            {
	            	colName = s.rs.getString(1);
	            	colType = s.rs.getString(2);
	            	deflt =  s.rs.getString(3);
	            	colDisplayWidth = s.rs.getInt(4);
	            	precision = s.rs.getInt(5);
	            	scale = s.rs.getInt(6);
	            	nulls = s.rs.getBoolean(7);
	            	isAutoIncr = s.rs.getBoolean(8);
	                if (precision == 0 && colDisplayWidth > 0)
	              	   precision = colDisplayWidth;
	                nullType = (nulls) ? " NULL" : " NOT NULL";
	         	    identity = "";
	                if (majorSourceDBVersion >= 15)
	                {
		         	    if (colType == null && deflt == null)
		         	    	colType = metadata.getColumnTypeName(colIndex);
		         	    if (colType == null && deflt != null)
		         	    {
		         	    	colType = deflt;
		         	    	deflt = null;
		         	    }
	                } else
	                {
		         	    if (colType == null)
		         	    	colType = metadata.getColumnTypeName(colIndex);	                	
	                }
	         	    defValue = "";
	         	    if (deflt != null)
	         	    {
	         	    	deflt = deflt.replaceAll("\\r|\\n|\\r\\n"," ");
	         	    	deflt = deflt.trim();
	         	    	// create default chardflt as ' '
	         	    	if (deflt.toLowerCase().startsWith("create"))
	         	    	{
	         	    		int pos = deflt.toLowerCase().lastIndexOf("as");
	         	    		if (pos != -1)
	         	    		{
	         	    			defValue = " DEFAULT " + deflt.substring(pos+2).trim();
	         	    		}
	         	    	} else
	         	    	{
		         	    	defValue = " DEFAULT " + deflt;	         	    		
	         	    	}
	         	    }	         	    	
	                if (isAutoIncr)
	                {
	            	   identity = " IDENTITY ";
	                   getIdentityAttributes(t.srcSchName[id], t.srcTableName[id], colName);
	                   last_value = (last_value == -1) ? last_value = numRows + 1 : last_value + increment_value;
	                   db2SeqWriter.write("--#SET :SEQUENCE:"+dstSchema+":"+removeQuote(t.srcTableName[id]) + "_" + colName + linesep);
	                   db2SeqWriter.write("ALTER TABLE " + putQuote(dstSchema) + "." + putQuote(t.srcTableName[id]) + " ALTER COLUMN " + putQuote(colName) +
	                       " RESTART WITH " + last_value + linesep + ";" + linesep);
	                }    
	                // SKIN is not yet ready to process default, so defer it until it is implemented
	                defValue = "";
	                buffer.append(colName + " " + getSybDataType(colType, precision, scale) + identity + " " + nullType + defValue);
	                if (colIndex != colCount)
	                {
	                    buffer.append(","+linesep);
	                } else
	                    buffer.append(linesep);
	                if (colType.equalsIgnoreCase("text"))
	                {
	                	if (dataUnload)
	                	{
			                db2FixLobsWriter.write("--#SET :FIXLOBS:"+dstSchema+":"+removeQuote(t.srcTableName[id]) + "_" + colName + linesep);
			                db2FixLobsWriter.write("UPDATE " + putQuote(dstSchema) + "." + putQuote(t.srcTableName[id]) + linesep +
			                		   "SET \"ACS_TEXTPTR_" + colName + "\" =  x'000000' || GENERATE_UNIQUE()" + linesep + ";" + linesep);
	                	}
	                }
	                ++colIndex;
	            }
	            s.close(methodName); 
            } catch (Exception e)
            {
            	e.printStackTrace();
            }
            buffer.append(")" + linesep);
            buffer.append("go" + linesep);
            buffer.append(linesep);
    		db2TablesWriter.write(buffer.toString());
        }
        
        private void genLoadExceptionTableScript(int id)
        {
        	boolean isAutoIncr;
            StringBuffer buffer = new StringBuffer();  
            String exceptName = t.exceptSchemaName[id] + "." +  t.exceptTableName[id];
            buffer.append("--#SET :" + "TABLE" + ":" + removeQuote(t.exceptSchemaName[id]) + ":" + removeQuote(t.exceptTableName[id]) + linesep);           	
            buffer.append("CREATE TABLE " + exceptName + " AS (SELECT " + " * FROM " + getCaseName(t.dstSchemaName[id] + "." + getCaseName(t.dstTableName[id])) + ") DEFINITION ONLY" + linesep + ";" + linesep);
            buffer.append("--#SET :" + "ALTER_TABLE" + ":" + removeQuote(t.exceptSchemaName[id]) + ":" + removeQuote(t.exceptTableName[id]) + "." + putQuote("TSTCOL") + linesep);           	
            buffer.append("ALTER TABLE " + exceptName + " ADD COLUMN TSTCOL TIMESTAMP" + linesep + ";" + linesep);
            buffer.append("--#SET :" + "ALTER_TABLE" + ":" + removeQuote(t.exceptSchemaName[id]) + ":" + removeQuote(t.exceptTableName[id]) + "."  + putQuote("MSGCOL") + linesep);           	
            buffer.append("ALTER TABLE " + exceptName + " ADD COLUMN MSGCOL CLOB(32K)" + linesep + ";" + linesep);
            try
			{
				db2ExceptionTablesWriter.write(buffer.toString());
				if (ddlGen)
				{
		        	db2DropExpTablesWriter.write("--#SET :" + "DROP_TABLE:" + removeQuote(t.exceptSchemaName[id]) + ":" + removeQuote(t.exceptTableName[id]) + linesep);           	
		            db2DropExpTablesWriter.write("DROP TABLE "+ t.exceptSchemaName[id] + sqlsep + t.exceptTableName[id] + linesep + ";" + linesep);
				}
			} catch (IOException e)
			{
				e.printStackTrace();
			}

        }

        private void genTableScript(int id, long numRows) throws Exception
        {
            int autoIncrCount = 0;
            boolean isAutoIncr;
            String key, colName, colName2, nullType = "", identity = "", isGlobal = "", commitToken = "";
            StringBuffer buffer = new StringBuffer();  
            StringBuffer seqBuffer = new StringBuffer();
        	String tsName, targetDType;
        	ResultSetMetaData metadata = dd.resultsetMetadata;
            
            int ij, colIndex, colCount = metadata.getColumnCount();
            
            if (db2Skin())
            {
            	genSybaseTableScript(id, numRows);
            	return;
            }
            
        	db2DropWriter.write("--#SET :" + "DROP_TABLE:" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
            db2DropWriter.write("DROP TABLE "+ getCaseName(t.dstSchemaName[id]) + sqlsep + getCaseName(t.dstTableName[id]) + ";" + linesep);
            buffer.setLength(0);
            isGlobal = isTableTemporary(getCaseName(t.srcSchName[id]), getCaseName(t.srcTableName[id]));
            if (isGlobal.length() > 0)
               commitToken = TempOnCommitIndicator(getCaseName(t.srcSchName[id]), getCaseName(t.srcTableName[id]));
            buffer.append("--#SET :" + "TABLE" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);
            if (netezza)
            {
                buffer.append("CREATE " + isGlobal + " TABLE " + getCaseName(t.dstSchemaName[id]) + sqlsep + getCaseName(t.dstTableName[id]) + linesep);            	
            } else
            {
                buffer.append("CREATE " + isGlobal + " TABLE " + getCaseName(t.dstSchemaName[id]) + sqlsep + getCaseName(t.dstTableName[id]) + linesep);
            }
            buffer.append("(" + linesep);
            
            for (colIndex = 1; colIndex <= colCount; ++colIndex)
            {
               ij = colIndex - 1;
               colName2 = metadata.getColumnName(colIndex);
        	   colName = getCaseName(getCustomColumnName(t.srcSchName[id], t.srcTableName[id], colName2));
               colName = getTruncName(id, "COL", colName,30);            
               key = removeQuote(t.srcSchName[id]) + "." + removeQuote(t.srcTableName[id]) + "." + colName2;
               targetDType = ((DataMap) dataTypeMap.get(key)).targetDataType;
               nullType = (metadata.isNullable(colIndex) == ResultSetMetaData.columnNoNulls) ? " NOT NULL" : "";
               String nullCustomMapping = IBMExtractUtilities.CustomNullMapping(custNullMap, t.srcSchName[id], t.srcTableName[id], colName2);
               if (nullCustomMapping != null && nullCustomMapping.equalsIgnoreCase("NULL"))
                  nullType = "";
               else if (nullCustomMapping != null && nullCustomMapping.equalsIgnoreCase("NOT NULL"))
            	  nullType = "NOT NULL";
               
               isAutoIncr = metadata.isAutoIncrement(colIndex);
               identity = "";
               if (isAutoIncr)
               {
                   autoIncrCount++;
                   getIdentityAttributes(t.srcSchName[id], t.srcTableName[id], colName2);
                   last_value = (last_value == -1) ? last_value = numRows + 1 : last_value + increment_value;
                   if (last_value > 2147483647)
                   {
                	   targetDType = "BIGINT";
                   }
                   if (autoIncrCount == 1)
                   {
                	   if (!customMapping.equalsIgnoreCase("false") || netezza)
                	   {
                           identity = "";
                       	   db2DropSeqWriter.write("--#SET :" + "DROP:" + "SEQUENCE:" + removeQuote(getCaseName(t.dstSchemaName[id])) + "." + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
                           db2DropSeqWriter.write("DROP SEQUENCE " + removeQuote(getCaseName(t.dstSchemaName[id])) + sqlsep + removeQuote(getCaseName(t.dstTableName[id])) + ";" + linesep);
                           seqBuffer.append("--#SET :" + "SEQUENCE:" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
                           seqBuffer.append("CREATE SEQUENCE " + getCaseName(t.dstSchemaName[id]) + sqlsep + getCaseName(t.dstTableName[id]) + " AS BIGINT " + linesep);
                           seqBuffer.append("MINVALUE 1 MAXVALUE 9223372036854775807 START WITH "+last_value+" INCREMENT BY "+increment_value+" CACHE 1000 NO CYCLE ORDER" + linesep + ";" + linesep);
                	   } else
                          identity = " GENERATED BY DEFAULT AS IDENTITY (START WITH "+last_value+", INCREMENT BY "+increment_value+", CACHE 20)";
                   } 
                   else
                   {
                       identity = "";
                       String tmpStr = removeQuote(getCaseName(t.dstTableName[id]));
                       tmpStr = FixString(tmpStr);
                       String seqName = "SEQ"+(autoIncrCount-1)+"_"+tmpStr;
                       String trigName = "TRIG"+(autoIncrCount-1)+"_"+tmpStr;
                       trigName = getTruncName(0, "", trigName, 18); 
                       trigName = getCaseName(t.dstSchemaName[id]) + ".\"" + trigName + "\"";
                       seqName = getTruncName(0, "", seqName, 18); 
                       seqName = getCaseName(t.dstSchemaName[id]) + ".\"" + seqName + "\"";
                   	   db2DropSeqWriter.write("--#SET :" + "DROP:" + "SEQUENCE:" + removeQuote(getCaseName(t.dstSchemaName[id])) + "." + seqName + linesep);           	
                       db2DropSeqWriter.write("DROP SEQUENCE " + removeQuote(getCaseName(t.dstSchemaName[id])) + sqlsep + seqName + ";" + linesep);
                   	   db2DropSeqWriter.write("--#SET :" + "DROP:" + "TRIGGER:" + removeQuote(getCaseName(t.dstSchemaName[id])) + "." + trigName + linesep);           	
                       db2DropWriter.write("DROP TRIGGER " + removeQuote(getCaseName(t.dstSchemaName[id])) + sqlsep + trigName + ";" + linesep);
                       seqBuffer.append(linesep);
                       seqBuffer.append("--#SET :" + "SEQUENCE:" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + seqName + linesep);           	
                       seqBuffer.append("CREATE SEQUENCE " + getCaseName(t.dstSchemaName[id]) + sqlsep + putQuote(seqName) + " AS BIGINT " +linesep);
                       seqBuffer.append("MINVALUE 1 MAXVALUE 9223372036854775807 START WITH " + last_value + "  INCREMENT BY "+increment_value+" CACHE 20;" + linesep);
                       seqBuffer.append(linesep);
                       seqBuffer.append("CREATE TRIGGER " + removeQuote(getCaseName(t.dstSchemaName[id])) + sqlsep + trigName + linesep); 
                       seqBuffer.append("NO CASCADE BEFORE INSERT ON " + getCaseName(t.dstSchemaName[id]) + sqlsep + getCaseName(t.dstTableName[id]) + linesep);  
                       seqBuffer.append("REFERENCING NEW AS NEW FOR EACH ROW MODE DB2SQL" + linesep); 
                       seqBuffer.append("SET NEW."+putQuote(colName)+" = NEXT VALUE FOR "+seqName+ ";" + linesep);
                       seqBuffer.append(linesep);
                   }
               }               
               genDB2DefaultValues(t.dstSchemaName[id], t.srcSchName[id], t.srcTableName[id], colName2, nullType, targetDType);
               String dataMapping = IBMExtractUtilities.CustomDataTypeMap(custDatamap, t.srcSchName[id], t.srcTableName[id], colName2);
               if (dataMapping != null)
            	   targetDType = dataMapping;
               buffer.append("\""+colName+"\"" + " " + targetDType + " " + nullType + " " + identity);
               if (colIndex != colCount)
               {
                   buffer.append(","+linesep);
               } else
                   buffer.append(linesep);
            }            
            buffer.append(")" + linesep);
            if (data.zDB2())
            {   
            	if (!useBestPracticeTSNames)
            	{
            		if (custTSBP.size() > 0)
            		{
            			tsName = getCustomTableSpaces(t.dstSchemaName[id], t.dstTableName[id]);
            		} else
            		{
	                    tsName = getzDB2TableSpaces(t.srcSchName[id], t.srcTableName[id]);
            		}
	                if (!(tsName == null || tsName.length() == 0))
	                	if (!netezza)
	                       buffer.append(tsName + linesep);
            	}	                
                String part = genzDB2Partitions(t.srcSchName[id], t.srcTableName[id]);
                if (!(part == null || part.length() == 0))
                {
                	if (!netezza)
                       buffer.append(part + linesep);
                }
            }
            else if (data.Oracle())
            {
            	if (!useBestPracticeTSNames)
            	{
            		if (custTSBP.size() > 0)
            		{
            			tsName = getCustomTableSpaces(t.dstSchemaName[id], t.dstTableName[id]);
            		} else
            		{
            		    tsName = getOraTableSpaces(t.srcSchName[id], t.srcTableName[id]);
            		}
	                if (!(tsName == null || tsName.length() == 0))
	                {
	                	if (!netezza)
	                       buffer.append(tsName + linesep);
	                }
            	}
                String part = genOraPartitions(t.srcSchName[id], t.srcTableName[id]);
                if (!(part == null || part.length() == 0))
                {
                   buffer.append(part + linesep);
                }
            } else
            {
            	if (!useBestPracticeTSNames)
            	{
            		if (custTSBP.size() > 0)
            		{
            			tsName = getCustomTableSpaces(t.dstSchemaName[id], t.dstTableName[id]);
		                if (!(tsName == null || tsName.length() == 0))
		                {
		                	if (!netezza)
		                       buffer.append(tsName + linesep);
		                }
            		}
            	}            	
            }
            if (compressTable && !netezza)         
            {
            	if (isGlobal.equals(""))
                   buffer.append("COMPRESS YES" + linesep);
            	else
            	{
                	if (commitToken.length() > 0)
            	       buffer.append(commitToken + linesep);
            	}
            } else
            {
            	if (isGlobal.length() > 0 && commitToken.length() > 0)
            	{
            		if (netezza)
             	       buffer.append("-- NETEZZA DOES NOT SUPPORT GLOBAL TEMPORARY TABLE DEFINITION. THIS WILL FAIL." + linesep);
            		else
            			buffer.append(commitToken + linesep);
            	}
            }
            if (zdb2)
            {
                buffer.append("CCSID UNICODE" + linesep);
            }
            if (data.zDB2() && !netezza)
            {
            	if (dataCaptureChangesMap.containsKey(removeQuote(t.srcSchName[id]) + "." + removeQuote(t.srcTableName[id])))
            	{
            		buffer.append("DATA CAPTURE CHANGES" + linesep);
            	} else
            		buffer.append("DATA CAPTURE NONE" + linesep);            	
            }
            if (netezza)
            {
            	key = removeQuote(t.dstSchemaName[id]) + "." + removeQuote(t.dstTableName[id]);
            	tsName = (custTSBP.containsKey(key)) ? (String) custTSBP.get(key) : "DISTRIBUTE ON RANDOM";
            	buffer.append(tsName + linesep);
            }
            buffer.append(";" + linesep);
            buffer.append(linesep);
            if (data.Oracle() && db2_compatibility)
            {
               db2TablesWriter.write(buffer.toString());
            } else
            {
            	if (netezza)
            	{
            		db2TablesWriter.write(buffer.toString());
            	} else
            	{
	            	try
	            	{
	            	   String dml = ModifyTableAll(buffer.toString(),t.dstSchemaName[id], t.dstTableName[id]);
	                   db2TablesWriter.write(dml);
	            	} catch (Exception e)
	            	{
	            		e.printStackTrace();
	            		db2TablesWriter.write(buffer.toString());
	            	}            		
            	}
            }
            if (extractObjects)
               db2SeqWriter.write(seqBuffer.toString());
            db2TablesWriter.write(getComments(t.dstSchemaName[id], t.dstTableName[id], t.srcSchName[id], t.srcTableName[id]));
            if (!netezza)
               db2TablesWriter.write(getTableAttributes(t.dstSchemaName[id], t.dstTableName[id], t.srcSchName[id], t.srcTableName[id]));
        }
        
        private long countRows(int id) throws SQLException
        {
        	long rows = 0L;
        	String methodName = "countRows";
        	SQL s = new SQL(data.connection);
            s.PrepareExecuteQuery(methodName, t.countSrcSQL[id]);          

            if (s.next())
            {
               rows = Long.parseLong(s.rs.getString(1));
            }            
            s.close(methodName);
            return rows;
        }
        
        private String getInputFileName(int id)
        {
           return IBMExtractUtilities.getInputFileName(OUTPUT_DIR, loadDirectory, usePipe, id, t);
        }
        
        private StringBuffer genDB2LoadScript(int id, boolean pipe) throws Exception
        {
        	ResultSetMetaData metadata = dd.resultsetMetadata;
        	StringBuffer buffer = new StringBuffer();
        	
        	if (data.Oracle())
        	{
                String isTempTable = isTableTemporary(removeQuote(t.srcSchName[id]), removeQuote(t.srcTableName[id]));
                if (isTempTable.length() > 0)
                	return buffer;
        	}
        	
            int dupID = t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getDupID();
            String dupStr = (dupID == 0) ? "" : Integer.toString(dupID), dumpFileModifier = "";

            String tmpRemote = (remoteLoad) ? " CLIENT " : "", tmpDBName = dbName.toLowerCase();            
            String lobsinfile = "";
            String fil = IBMExtractUtilities.FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase()) + dupStr;
            File f  = new File(loadDirectory + "dump" + filesep + fil + ".txt");
            String dumpFile = f.getCanonicalPath();
            f  = new File(loadDirectory + "msg" + filesep + fil + ".txt");
            String msgFile = f.getCanonicalPath();
            f  = new File(loadDirectory + "data" + filesep + fil);
            String xmlDir = f.getCanonicalPath();
            String codePage = "";
            
            if (encoding.equalsIgnoreCase("utf-8"))
            {
               codePage = " CODEPAGE=1208 ";
            }
            int i, colCount = metadata.getColumnCount();
            buffer.setLength(0);
            buffer.append("--#SET :" + "RUNSTATS" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
            buffer.append("RUNSTATS ON TABLE "+getCaseName(t.dstSchemaName[id]) + "." + getCaseName(t.dstTableName[id])+" WITH DISTRIBUTION AND SAMPLED DETAILED INDEXES ALL SET PROFILE ONLY" + linesep + ";" + linesep);            	
            buffer.append("--#SET :" + "LOAD" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
            buffer.append("LOAD " + tmpRemote + " FROM "+ linesep + getInputFileName(id) + linesep);            	
            buffer.append("OF DEL " + linesep);
            if (bval.isBlob || bval.isClob)
            {
                lobsinfile = "LOBSINFILE";
            	buffer.append("LOBS FROM \"" + xmlDir + filesep + "\"" + linesep); 
            }
            if (bval.isXml)
            {
            	buffer.append("XML FROM \"" + xmlDir + filesep + "\"" + linesep); 
            }
            if (remoteLoad) 
            {
                buffer.append("-- For Remote LOAD, you have to copy or mount LOBS FROM or XML FROM directory to the " +
                     "remote DB2 server otherwise LOAD will fail." + linesep);
            }
            if (!remoteLoad)
            {
            	dumpFileModifier = " DUMPFILE=\"" + dumpFile + "\" ";
            }
            buffer.append("MODIFIED BY "+lobsinfile+" "+codePage+" COLDEL" + colsep + ((bval.isXml) ? " " : " ANYORDER ") + " USEDEFAULTS CHARDEL\"\" DELPRIORITYCHAR " + ((norowwarnings) ? " NOROWWARNINGS " : "") + ((bval.isXml) ? " XMLCHAR " : "") + dumpFileModifier + linesep);            	
            buffer.append("METHOD P (");
            for (i = 1; i <= colCount; ++i)
            {
               buffer.append(i);
               if (i != colCount)
               {
                   buffer.append(",");
               }
            }
            buffer.append(")" + linesep); 
            if (!saveCount.equals("0"))
            {
            	try
            	{
            	   int x = Integer.parseInt(saveCount);
            	   if (x >= 0)
                      buffer.append("SAVECOUNT " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}            	
            }
            if (!limitLoadRows.equalsIgnoreCase("ALL"))
            {
            	try
            	{
            	   int x = Integer.parseInt(limitLoadRows);
            	   if (x >= 0)
                      buffer.append("ROWCOUNT " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}
            }
            if (!warningCount.equalsIgnoreCase("0"))
            {
            	try
            	{
            	   int x = Integer.parseInt(warningCount);
            	   if (x >= 0)
                      buffer.append("WARNINGCOUNT " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}
            }
            buffer.append("MESSAGES " + putQuote(msgFile) + linesep);
            if (!(tempFilesPath == null || tempFilesPath.trim().length() == 0))
            	buffer.append("TEMPFILES PATH " + putQuote(tempFilesPath) + linesep);
            buffer.append(((loadReplace) ? "REPLACE" : "INSERT") + " INTO " + getCaseName(t.dstSchemaName[id]) + "." + getCaseName(t.dstTableName[id]) + linesep);
            buffer.append("(" + linesep);
            for (i = 1; i <= colCount; ++i)
            {
               String tmp = metadata.getColumnName(i);
               tmp = getCaseName(getCustomColumnName(t.srcSchName[id], t.srcTableName[id], tmp));
               tmp = getTruncName(0, "", tmp, 30);            	   
               buffer.append(putQuote(tmp));
               if (i != colCount)
               {
                   buffer.append("," + linesep);
               } else
               {
                   buffer.append(linesep);                   
               }
            }
            buffer.append(")" + linesep);
            if (loadException)
            {
            	if (!data.Access())
            	    buffer.append("FOR EXCEPTION " + t.exceptSchemaName[id] + "." + t.exceptTableName[id] + linesep);
            }
        	if (!bval.isXml)
        	{
        		if (loadstats && loadReplace)
        		{
                    buffer.append("STATISTICS USE PROFILE" + linesep);            			
        		} else
        		{
                    buffer.append("STATISTICS NO" + linesep);            			
        		}
	            buffer.append("NONRECOVERABLE " + linesep);
	            buffer.append("INDEXING MODE AUTOSELECT" + linesep);
            }
            buffer.append(";" + linesep);
            buffer.append(linesep);
        	return buffer;
        }

        private StringBuffer genNZLoadScript(int id, boolean flag) throws Exception
        {
        	StringBuffer buffer = new StringBuffer();

            int dupID = t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getDupID();
            String dupStr = (dupID == 0) ? "" : Integer.toString(dupID), dumpFileModifier = "";

            String fil = IBMExtractUtilities.FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase()) + dupStr;
            File f  = new File(loadDirectory + "nzctl" + filesep + fil + ".ctl");
            String nzctlFile = f.getCanonicalPath();
            f  = new File(loadDirectory + "nzlog");
            String nzLogdir = f.getCanonicalPath();
            f  = new File(loadDirectory + "nzbad" + filesep + fil + ".bad");
            String nzBadFile = f.getCanonicalPath();

            buffer.append("--#SET :" + "LOAD" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);
            // "INSERT into " + target_table + " SELECT * FROM EXTERNAL '" + load_pipe +"' USING (DELIMITER '|' REMOTESOURCE 'JDBC')";
            buffer.append("INSERT INTO " + getCaseName(t.dstSchemaName[id])+".."+getCaseName(t.dstTableName[id]) + linesep);            	
            buffer.append("SELECT * FROM EXTERNAL '" + removeQuote(getInputFileName(id)) + "'" + linesep);
            buffer.append("USING (" + linesep);
            buffer.append("          delimiter       '|'" + linesep);
            buffer.append("          escapeChar      '\\'" + linesep);
            buffer.append("          logDir          '" + nzLogdir + "'" + linesep);
            buffer.append("          ctrlChars       TRUE" + linesep);
            buffer.append("          nullValue       ''" + linesep);
            if (!warningCount.equalsIgnoreCase("0"))
            {
            	try
            	{
            	   int x = Integer.parseInt(warningCount);
            	   if (x >= 0)
                      buffer.append("          maxErrors       " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}
            }
            if (!limitLoadRows.equalsIgnoreCase("ALL"))
            {
            	try
            	{
            	   int x = Integer.parseInt(limitLoadRows);
            	   if (x >= 0)
                      buffer.append("          maxRows         " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}
            } else
                buffer.append("          maxRows         0" + linesep);
            buffer.append("          skipRows        0" + linesep);
            buffer.append("          socketBufSize   83886080" + linesep);
            buffer.append("          dateStyle       YMD" + linesep);
            buffer.append("          dateDelim       '-'" + linesep);
            buffer.append("          timeDelim       ':'" + linesep);
            buffer.append("          REMOTESOURCE    'JDBC'" + linesep);
            buffer.append(")" + linesep + ";" + linesep);            
            return buffer;
        }
        
        private void genNZLoadScript(int id) throws Exception
        {
        	StringBuffer buffer = new StringBuffer();
        	
            int dupID = t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getDupID();
            String dupStr = (dupID == 0) ? "" : Integer.toString(dupID), dumpFileModifier = "";

            String fil = IBMExtractUtilities.FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase()) + dupStr;
            File f  = new File(loadDirectory + "nzctl" + filesep + fil + ".ctl");
            String nzctlFile = f.getCanonicalPath();
            f  = new File(loadDirectory + "nzlog");
            String nzLogdir = f.getCanonicalPath();
            f  = new File(loadDirectory + "nzbad" + filesep + fil + ".bad");
            String nzBadFile = f.getCanonicalPath();
            
            buffer.setLength(0);
            buffer.append("--#SET :" + "LOAD" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
            buffer.append("DATAFILE " + getInputFileName(id) + linesep);            	
            buffer.append("{" + linesep);
            buffer.append("          database        "+removeQuote(t.dstSchemaName[id]) + linesep);
            buffer.append("          tablename       "+removeQuote(t.dstTableName[id]) + linesep);
            buffer.append("          delimiter       '|'" + linesep);
            buffer.append("          escapeChar      '\\\\'" + linesep);
            buffer.append("          logDir          " + putQuote(nzLogdir) + linesep);
            buffer.append("          badFile         " + putQuote(nzBadFile) + linesep);
            buffer.append("          ctrlChars       TRUE" + linesep);
            buffer.append("          nullValue       ''" + linesep);
            if (!warningCount.equalsIgnoreCase("0"))
            {
            	try
            	{
            	   int x = Integer.parseInt(warningCount);
            	   if (x >= 0)
                      buffer.append("          maxErrors       " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}
            }
            if (!limitLoadRows.equalsIgnoreCase("ALL"))
            {
            	try
            	{
            	   int x = Integer.parseInt(limitLoadRows);
            	   if (x >= 0)
                      buffer.append("          maxRows         " + x + linesep);
            	} catch (Exception v)
            	{
            		;
            	}
            } else
                buffer.append("          maxRows         0" + linesep);
            buffer.append("          skipRows        0" + linesep);
            buffer.append("          socketBufSize   83886080" + linesep);
            buffer.append("          dateStyle       YMD" + linesep);
            buffer.append("          dateDelim       '-'" + linesep);
            buffer.append("          timeDelim       ':'" + linesep);
            buffer.append("}" + linesep);            
            BufferedWriter nzctlWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(nzctlFile, false), sqlFileEncoding));
            nzctlWriter.write(buffer.toString());
            nzctlWriter.close();            
            db2LoadWriter.write(genNZLoadScript(id,true).toString());
            nzLoadScript.write("nzload -cf " + nzctlFile + linesep);
            buffer.setLength(0);
            buffer.append("--#SET :" + "STATS" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);
            buffer.append("GENERATE STATISTICS ON "+getCaseName(t.dstSchemaName[id])+".."+getCaseName(t.dstTableName[id])+ linesep + ";" + linesep); 
            db2RunstatWriter.write(buffer.toString());
        }
        
        private void genDB2LoadScript(int id) throws Exception
        {
        	StringBuffer buffer = genDB2LoadScript(id, false);
        	if (buffer.length() > 0)
        	{
	            db2LoadWriter.write(buffer.toString());
	            buffer.setLength(0);
	            buffer.append("RUNSTATS ON TABLE "+getCaseName(t.dstSchemaName[id])+"."+getCaseName(t.dstTableName[id])+ linesep); 
	            buffer.append("ON ALL COLUMNS WITH DISTRIBUTION"+ linesep); 
	            buffer.append("ON ALL COLUMNS AND DETAILED INDEXES ALL"+ linesep); 
	            buffer.append("ALLOW WRITE ACCESS ;"+ linesep);
	            buffer.append(linesep);
	            buffer.append(linesep);
	            db2RunstatWriter.write(buffer.toString());
	            db2LoadTerminateWriter.write(getDB2LoadTerminate(id));
	            db2CheckPendingWriter.write("SET INTEGRITY FOR "+getCaseName(t.dstSchemaName[id])+"."+getCaseName(t.dstTableName[id])+" IMMEDIATE CHECKED;" + linesep);
        	}
        }
        
        private StringBuffer getzDB2Load(int id, boolean jcl) throws Exception
        {
           String key, colType, nocopypend = (z.znocopypend) ? "NOCOPYPEND" : "";            
           StringBuffer buffer = new StringBuffer();  
           DataMap map;
           ResultSetMetaData metadata = dd.resultsetMetadata;
           
           int precision, colIndex, colCount = metadata.getColumnCount();
           
           
           buffer.setLength(0);
           if (jcl)
           {
               buffer.append("//*--#SET :" + "LOAD" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep + Constants.zCommentLine + linesep);
        	   buffer.append("//LOAD     EXEC DSNUPROC,SYSTEM="+z.db2SubSystemID+"," + linesep);
        	   buffer.append("//	     UID='" + getNameSeq("IDMTLOAD-") + "'," + linesep);
        	   buffer.append("//         UTPROC=''," + linesep);
      	       buffer.append("//         LIB='"+z.SDSNLOAD+"'" + linesep);
      	       buffer.append("//DSNUPROC.SYSREC   DD DSN=<INPUTDATASETNAME>," + linesep);
      	       buffer.append("//         DISP=OLD" + linesep);
    	       buffer.append("//DSNUPROC.SYSDISC  DD DSN=<DISCARDDATASETNAME>," + linesep);
    	       buffer.append("//         DISP=(NEW,CATLG,CATLG)," + linesep);
    	       buffer.append("//         UNIT=SYSDA,SPACE=(CYL,(10,1))" + linesep);
    	       buffer.append("//DSNUPROC.SYSUT1   DD DSN=<SYSUT1>," + linesep);
    	       buffer.append("//         DISP=(NEW,DELETE,CATLG)," + linesep);
    	       buffer.append("//         UNIT=SYSDA,SPACE=(CYL,(10,1))" + linesep);
    	       buffer.append("//DSNUPROC.SORTOUT  DD DSN=<SORTOUT>," + linesep);
    	       buffer.append("//         DISP=(NEW,DELETE,CATLG)," + linesep);
    	       buffer.append("//         UNIT=SYSDA,SPACE=(CYL,(10,1))" + linesep);
    	       buffer.append("//DSNUPROC.SYSERR   DD DSN=<ERRORDATASETNAME>," + linesep);
    	       buffer.append("//         DISP=(NEW,CATLG,CATLG)," + linesep);
    	       buffer.append("//         UNIT=SYSDA,SPACE=(CYL,(10,1))" + linesep);
    	       buffer.append("//DSNUPROC.SYSMAP   DD UNIT=SYSALLDA," + linesep);
    	       buffer.append("//         SPACE=(TRK,(1,1))" + linesep); 
    	       buffer.append("//DSNUPROC.SYSIN    DD *" + linesep + linesep);
           }           
           else
           {
               buffer.append("--#SET :" + "LOAD" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);
               buffer.append("CALL SYSPROC.DSNUTILS('"+getNameSeq("L")+"','NO'," + linesep + "'");        	   
           }
           buffer.append("LOAD DATA REPLACE LOG NO "+nocopypend+" FORMAT DELIMITED " + linesep);
           buffer.append("COLDEL X'"+getHexCode(colsep)+"' CHARDEL X'"+getHexCode("\"")+"' DECPT X'"+getHexCode(".")+"' " + linesep);
           buffer.append("ENFORCE NO " + linesep);
           buffer.append("UNICODE CCSID(1208,1208,0) " + linesep);
           buffer.append("INTO TABLE " + getCaseName(t.dstSchemaName[id]) + "." + getCaseName(t.dstTableName[id]) + linesep);
           buffer.append("(" + linesep);
           for (colIndex = 1; colIndex <= colCount; ++colIndex)
           {
        	  try
        	  {
                 precision = metadata.getPrecision(colIndex);
        	  }
        	  catch (Exception e)
              {
               	 precision = 2147483647;
               	 log("Error: Precision found more than 2GB limiting it to 2GB");                	
              }
              String colName, colName2, modifier;
              colType = metadata.getColumnTypeName(colIndex).toUpperCase();
              colName2 = metadata.getColumnName(colIndex);
              colName = getCaseName(getCustomColumnName(t.srcSchName[id], t.srcTableName[id], colName2));
              colName = getTruncName(0, "", colName,30);
              key = removeQuote(t.srcSchName[id]) + "." + removeQuote(t.srcTableName[id]) + "." + metadata.getColumnName(colIndex);
              map = (DataMap) dataTypeMap.get(key);              
              if (map.targetDataType.startsWith("CHAR"))
                 modifier = "CHAR";
              else if (map.targetDataType.startsWith("VARCHAR"))
                 modifier = "VARCHAR";
              else if (map.targetDataType.startsWith("BINARY"))
                 modifier = "BINARY";
              else if (map.targetDataType.startsWith("VARBINARY"))
              {
                 if (precision > 32000)
                     modifier = "VARCHAR BLOBF";
                 else
                    modifier = "VARBINARY";
              }
              else if (map.targetDataType.startsWith("XML"))
                 modifier = "VARCHAR BLOBF";
              else if (map.targetDataType.startsWith("GRAPHIC"))
                 modifier = "GRAPHIC";
              else if (map.targetDataType.startsWith("VARGRAPHIC"))
                 modifier = "VARGRAPHIC";
              else if (map.targetDataType.startsWith("DBCLOB"))
                 modifier = "VARCHAR DBCLOBF";
              else if (map.targetDataType.startsWith("CLOB"))
                 modifier = "VARCHAR CLOBF";
              else if (map.targetDataType.startsWith("BLOB"))
                 modifier = "VARCHAR BLOBF";
              else if (map.targetDataType.startsWith("INT"))
                 modifier = "INTEGER EXTERNAL";
              else if (map.targetDataType.startsWith("DOUBLE"))
                 modifier = "DOUBLE EXTERNAL";
              else if (map.targetDataType.startsWith("FLOAT"))
                 modifier = "FLOAT EXTERNAL";
              else if (map.targetDataType.startsWith("NUMERIC"))
                 modifier = "DECIMAL EXTERNAL";
              else if (map.targetDataType.equals("DATE"))
                 modifier = "DATE EXTERNAL";
              else if (map.targetDataType.equals("TIME"))
                 modifier = "TIME EXTERNAL";
              else if (map.targetDataType.equals("TIMESTAMP"))
                 modifier = "TIMESTAMP EXTERNAL";
              else if (map.targetDataType.startsWith("DECFLOAT"))
                 modifier = "DECFLOAT EXTERNAL";
              else
                 modifier = map.targetDataType;               
               
              buffer.append("\""+colName+"\" " + modifier);
              buffer.append((colIndex != colCount) ? "," + linesep : linesep);
              //log ("column name::" + metadata.getColumnName(i) + "::data type::" + metadata.getColumnTypeName(i) + 
              //"::java type::" + metadata.getColumnClassName(i));
           }
           buffer.append(")" + linesep); 
           if (jcl)
           {
               buffer.append("IDENTITYOVERRIDE" + linesep + "/*" + linesep + Constants.zCommentLine + linesep);        	   
           } else
           {
	           buffer.append("IDENTITYOVERRIDE', ?, 'LOAD', '<INPUTDATASETNAME>','',0,'<DISCARDDATASETNAME>','SYSDA',10, " + linesep);
	           buffer.append("'','',0,'','',0,'','',0,'','',0,'','',0,'<SYSUT1>','SYSDA',10,'<SORTOUT>','SYSDA',10,'','',0, " + linesep);
	           buffer.append("'<ERRORDATASETNAME>','SYSDA',10,'','',0)" + linesep);
	           buffer.append(";" + linesep);
	           buffer.append(linesep);
           }
           return buffer;
        }
        
        private StringBuffer getzDB2Check(int id)
        {
           StringBuffer buffer = new StringBuffer();  
           buffer.append(zConnectStr + linesep);           	
           buffer.append("--#SET :" + "CHECK_QUERY" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
           buffer.append("SELECT 'CALL SYSPROC.DSNUTILS(''"+getNameSeq("C")+"'',''NO'',''CHECK DATA TABLESPACE \"'||DBNAME||'\".\"'||TSNAME||'\" " + linesep); 
           buffer.append("SHRLEVEL CHANGE'',?, ''CHECK DATA'', '''','''',0,'''','''',0, " + linesep);
           buffer.append("'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0, " + linesep);
           buffer.append("''<ERRORDATASETNAME>'',''SYSDA'',10,'''','''',0);'" + linesep);
           buffer.append("FROM SYSIBM.SYSTABLES WHERE CREATOR = '"+removeQuote(getCaseName(t.dstSchemaName[id]))+"' " + linesep);
           buffer.append("AND NAME = '"+removeQuote(getCaseName(t.dstTableName[id]))+"'" + linesep);
           buffer.append("AND CHECKFLAG = 'C';" + linesep);
           buffer.append("TERMINATE;" + linesep);           	
           return buffer;
        }
        
        private String getzDB2RUNS(int id)
        {
           StringBuffer buffer = new StringBuffer();  
           buffer.append(zConnectStr + linesep);           	
           buffer.append("--#SET :" + "RUNSTATS" + ":" + removeQuote(getCaseName(t.dstSchemaName[id])) + ":" + removeQuote(getCaseName(t.dstTableName[id])) + linesep);           	
           buffer.append("SELECT 'CALL SYSPROC.DSNUTILS(''"+getNameSeq("S")+"'',''NO'',''RUNSTATS TABLESPACE \"'||DBNAME||'\".\"'||TSNAME||'\" " + linesep);
           buffer.append("TABLE("+getCaseName(t.dstSchemaName[id])+"."+getCaseName(t.dstTableName[id])+") COLUMN(ALL) INDEX(ALL) SHRLEVEL CHANGE''," + linesep); 
           buffer.append("?, ''RUNSTATS TABLESPACE'', '''','''',0,'''','''',0, " + linesep);
           buffer.append("'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0,'''','''',0, " + linesep);
           buffer.append("'''','''',0,'''','''',0);'" + linesep);
           buffer.append("FROM SYSIBM.SYSTABLES WHERE CREATOR = '"+removeQuote(getCaseName(t.dstSchemaName[id])) + "' " + linesep);
           buffer.append("AND NAME = '"+removeQuote(getCaseName(t.dstTableName[id]))+"';" + linesep); 
           buffer.append(linesep);
           buffer.append("TERMINATE;" + linesep);           	
           return buffer.toString();           
        }
        
        private void BuildTableAttributeMap()
        {
        	String methodName = "getTableAttributes";
        	tableAttributeMap = new HashMap();
        	tableAttributeMap2 = new HashMap();
        	columnAttribMap = new HashMap();
        	String schema, table, prevSchema = "", prevTable = "", attrib01, attrib02, attrib03;
        	String key, value, sql = "", sql2 = "";
        	boolean added = false;

        	if (data.Teradata())
         	{
         	    sql = "SELECT databasename, tablename, columnname FROM dbc.columns WHERE databasename IN (" + schemaList + ")  AND uppercaseflag = 'U'";
         	} else if (data.Oracle())
         	{
         		sql = "select owner, table_name, column_name, 1/(power(10,(data_scale-data_precision+1))), " +
         				"1/power(10,(data_scale-data_precision)) from dba_tab_columns " +
         				"where owner IN (" + schemaList + ") " +
         				"and data_type = 'NUMBER' and data_scale > data_precision";
         		sql2 = "select owner, table_name, column_name, data_scale from dba_tab_columns " +
         				"where owner IN (" + schemaList + ") " +
         				"and data_type = 'NUMBER' and data_scale < 0";
         	}
        	
        	if (sql.length() == 0)
        		return;
        	
        	try
        	{
        		SQL s = new SQL(data.connection);
	        	key = "";
	        	int i = 0;
				s.PrepareExecuteQuery(methodName, sql); 
	        	while (s.next())
	        	{
	        		schema = trim(s.rs.getString(1));
	        		table = trim(s.rs.getString(2));
	        		key = schema + "." + table;
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			tableAttributeMap.put(key, columnAttribMap);
	        			columnAttribMap = new HashMap();
	        			added = true;
	        		}
	        		if (data.Teradata())
	        		{
	        			attrib01 = trim(s.rs.getString(3));
	        			attrib01 = getCaseName(getCustomColumnName(schema, table, attrib01));
		        		value = "";
	        		    columnAttribMap.put(attrib01, value);
	        		} else if (data.Oracle())
	        		{
		        		attrib01 = getCaseName(trim(s.rs.getString(3)));
		        		attrib02 = trim(s.rs.getString(4));
		        		attrib03 = trim(s.rs.getString(5));
		        		value = attrib02+"~"+attrib03;
	        		    columnAttribMap.put(attrib01, value);
	        		}
        		    added = false;
	        		++i;
        			prevSchema = schema;
        			prevTable = table;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		tableAttributeMap.put(key, columnAttribMap);	        		
	        	}
	        	s.close(methodName);        		
	        	if (debug) log (i + " values cached in tableAttributeMap");
	        	columnAttribMap = new HashMap();
	        	key = "";
	        	i = 0;
				s.PrepareExecuteQuery(methodName, sql2); 
	        	while (s.next())
	        	{
	        		schema = trim(s.rs.getString(1));
	        		table = trim(s.rs.getString(2));
	        		key = schema + "." + table;
	        		if (i == 0)
	        		{
	        			prevSchema = schema;
	        			prevTable = table;
	        		}
	        		if (!(schema.equals(prevSchema) && table.equals(prevTable)))
	        		{
	        			tableAttributeMap2.put(key, columnAttribMap);
	        			columnAttribMap = new HashMap();
	        			added = true;
	        		}
	        		attrib01 = getCaseName(trim(s.rs.getString(3)));
	        		attrib02 = trim(s.rs.getString(4));
	        		value = attrib02;
        		    columnAttribMap.put(attrib01, value);
        		    added = false;
	        		++i;
        			prevSchema = schema;
        			prevTable = table;
	        	}
	        	if (!added && key.length() > 0)
	        	{
	        		tableAttributeMap2.put(key, columnAttribMap);	        		
	        	}
	        	s.close(methodName);        		
	        	if (debug) log (i + " values cached in tableAttributeMap2");
        	} catch (SQLException e)
        	{
        		e.printStackTrace();
        	}
        }
        
        private String getTableAttributes(String schema, String table, String srcSchema, String srcTable)
        {
        	int count = 0;
        	String key = srcSchema + "." + srcTable;
        	StringBuffer buffer = new StringBuffer();
        	String attrib01 = "", attrib02 = "", attrib03 = "";
        	String[] tmpVal;

        	if (tableAttributeMap.containsKey(key))
        	{
        		columnAttribMap = (HashMap) tableAttributeMap.get(key);
        		Iterator it = columnAttribMap.entrySet().iterator();
        		while (it.hasNext()) 
        		{
        	        Map.Entry pairs = (Map.Entry) it.next();
            		if (data.Teradata())
            		{
    	        		if (count > 0)
    	        			attrib01 += "~";	        		
        	            attrib01 += (String) pairs.getKey();
            		} else if (data.Oracle())
            		{
    	        		if (count > 0)
    	        		{
    	        			attrib01 += "~";
    	        			attrib02 += "~";
    	        			attrib03 += "~";
    	        		}
    	        		attrib01 += getCaseName((String) pairs.getKey());
            	        tmpVal = ((String) pairs.getValue()).split("~");            			
    	        		attrib02 += tmpVal[0];
    	        		attrib03 += tmpVal[1];
            		}            		
            		++count;
        	    }
        	}
			if (data.Teradata() && attrib01.length() > 0)
			{
				String setVariablePre = "", setVariablePost = "";
				buffer.append("-- This trigger is created to do an upper case on columns defined in teradata" + linesep);
				buffer.append("--#SET :TRIGGER:" + schema + ":" + table + linesep);
				buffer.append("CREATE TRIGGER " + schema + "." + putQuote("TRIGUPCASE_BEFORE_" + removeQuote(table)) + linesep);
				buffer.append("BEFORE INSERT ON " + schema + "." + table + linesep);
				buffer.append("REFERENCING NEW AS N FOR EACH ROW" + linesep);
				String[] cols = attrib01.split("~");
				for (int i = 0; i < cols.length; ++i)
				{
					if (i > 0)
						setVariablePre += ", ";
					setVariablePre = "N." + getCaseName(getCustomColumnName(schema, table, cols[i]));
				}
				for (int i = 0; i < cols.length; ++i)
				{
					if (i > 0)
						setVariablePost += ", ";
					setVariablePost = "UPPER(" + getCaseName(cols[i]) + ")";
				}
				buffer.append("SET (" + setVariablePre + ") = (" + setVariablePost + ")" + linesep + ";" + linesep);				
			} else if (data.Oracle() && attrib01.length() > 0)
			{
				String[] cols = attrib01.split("~");
				String[] mins = attrib02.split("~");
				String[] maxs = attrib03.split("~");
				for (int i = 0; i < cols.length; ++i)
				{
					buffer.append("-- This check constraint is created to take care of Oracle number where precision < scale" + linesep);				
					buffer.append("--#SET :CHECK:" + schema + ":" + table + linesep);
					buffer.append("ALTER TABLE " + schema + "." + table + linesep);
					buffer.append("ADD CONSTRAINT ORANUMCK_" + (i+1) + "_" + removeQuote(table) + linesep);
					buffer.append("CHECK (" + getCaseName(getCustomColumnName(schema, table, cols[i])) + " >= " + mins[i] + " AND " + 
							getCaseName(getCustomColumnName(schema, table, cols[i])) + " < " + maxs[i] + ")" + linesep);
					buffer.append(";" + linesep + linesep);
				}
			}

        	if (tableAttributeMap.containsKey(key))
        	{
	        	count = 0;
	        	attrib01 = attrib02 = "";

        		columnAttribMap = (HashMap) tableAttributeMap.get(key);
        		Iterator it = columnAttribMap.entrySet().iterator();
        		while (it.hasNext()) 
        		{
	        		if (count > 0)
	        		{
	        			attrib01 += "~";
	        			attrib02 += "~";
	        		}
        	        Map.Entry pairs = (Map.Entry) it.next();
	        		attrib01 += getCaseName((String)pairs.getKey());
	        		attrib02 += (String)pairs.getValue();
	        		++count;	        			
        		}	        	
				if (data.Oracle() && attrib01.length() > 0)
				{
					String setVariablePre = "", setVariablePost = "";
					buffer.append("-- This trigger is created to take care of Oracle number where scale < 0" + linesep);
					buffer.append("--#SET :TRIGGER:" + schema + ":" + table + linesep);
					buffer.append("CREATE TRIGGER " + schema + "." + putQuote("TRIGORASCALE_BEFORE_" + removeQuote(table)) + linesep);
					buffer.append("BEFORE INSERT ON " + schema + "." + table + linesep);
					buffer.append("REFERENCING NEW AS N FOR EACH ROW" + linesep);
					String[] cols = attrib01.split("~");
					String[] mins = attrib02.split("~");
					for (int i = 0; i < cols.length; ++i)
					{
						if (i > 0)
							setVariablePre += ", ";
						setVariablePre = "N." + getCaseName(getCustomColumnName(schema, table, cols[i]));
					}
					for (int i = 0; i < cols.length; ++i)
					{
						if (i > 0)
							setVariablePost += ", ";
						setVariablePost = "ROUND(" + getCaseName(getCustomColumnName(schema, table, cols[i])) + "," + mins[i] + ")";
					}
					buffer.append("SET (" + setVariablePre + ") = (" + setVariablePost + ")" + linesep + ";" + linesep);				
				}
        	}	        	
	        return buffer.toString();        	
        }
        
        private String getDB2LoadTerminate(int id) throws Exception
        {
           StringBuffer buffer = new StringBuffer();  
           if (Constants.win())
              buffer.append("LOAD FROM NUL: OF DEL TERMINATE INTO " + getCaseName(t.dstSchemaName[id])+"."+getCaseName(t.dstTableName[id]) + ";" + linesep);
           else
               buffer.append("LOAD FROM /dev/null OF DEL TERMINATE INTO " + getCaseName(t.dstSchemaName[id])+"."+getCaseName(t.dstTableName[id]) + ";" + linesep);        	   
           return buffer.toString();                                 
        }

        private void genzDB2LoadScript(int id, boolean jcl) throws Exception
        {
           StringBuffer buffer;
           String script, inputName, discName, errName, sysut1, sortout;
           String[] dsNames;
           ResultSetMetaData metadata = dd.resultsetMetadata;
           
           if (z.zOSDataSets[id] != null)
           {
              dsNames = z.zOSDataSets[id].split(",");
              buffer =  getzDB2Load(id, jcl);
              for (int i = 0; i < dsNames.length; ++i)
              {
                 inputName = dsNames[i].replace("'", "");
                 discName = inputName + ".DISC";
                 errName = inputName + ".LERR";
                 sysut1 =  inputName + ".UT1";
                 sortout =  inputName + ".OUT";
                 script = buffer.toString();
                 script = script.replaceFirst("<INPUTDATASETNAME>", inputName);
                 script = script.replaceFirst("<DISCARDDATASETNAME>", discName);
                 script = script.replaceFirst("<ERRORDATASETNAME>", errName);
                 script = script.replaceFirst("<SYSUT1>", sysut1);
                 script = script.replaceFirst("<SORTOUT>", sortout);
                 if (i > 0)
                 {
                    script = script.replaceFirst("DATA REPLACE", "DATA RESUME YES");                    
                 }
                 if (jcl)
                	 z.writezLoad(db2LoadWriter, script);
                 else
                	 db2LoadWriter.write(script);
              }
              script = getzDB2RUNS(id);
              if (jcl)
                 ;//z.writezLoad(script);
              else
            	 db2RunstatWriter.write(script);
              buffer = getzDB2Check(id);
              script = buffer.toString();
              errName = dsNames[0].replace("'", "") + ".CERR";
              script = script.replaceFirst("<ERRORDATASETNAME>", errName);       
              if (jcl)
            	  ;//z.writezLoad(script);
              else
            	  db2CheckPendingWriter.write(script);
           }
        }

        private String massageSmallDateTime(String str)
        {
        	Pattern p;
        	Matcher m;
        	int idx;
        	p = Pattern.compile("([0-9]{1,4}[-|/][0-9]{1,2}[-|/][0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\\.[0-9]{1,9}", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
    		m = p.matcher(str);
    		if (m.find())
    		{
    			str = m.group(1);
            	if ((idx = str.indexOf('-')) < 4) 
            	{
            		str = IBMExtractUtilities.pad(str, str.length()+4-idx, "0");
            	}
    		}
    		return str;
    	}

        private String massageTS(String str)
        {
        	int idx;
        	if (str.endsWith("00:00:00.0"))
        		return str.substring(0,str.length()-2);
        	if ((idx = str.indexOf('-')) < 5) 
        	{
        		return IBMExtractUtilities.pad(str, str.length()+4-idx, "0");
        	}
        	return str;
        }              

        private String massageDate(String str)
        {
        	int idx;
        	Pattern p;
        	Matcher m;
        	if (netezza || !db2_compatibility)
        	{
        	  p = Pattern.compile("([0-9]{1,4}[-|/][0-9]{1,2}[-|/][0-9]{1,2})");
    		  m = p.matcher(str);
    		  return m.find() ? m.group(1) : str;
        	} else
        	{
        		p = Pattern.compile("([0-9]{1,4}[-|/][0-9]{1,2}[-|/][0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\\.[0-9]{1,9}", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        		m = p.matcher(str);
        		if (m.find())
        		{
        			str = m.group(1);
                	if ((idx = str.indexOf('-')) < 4) 
                	{
                		str = IBMExtractUtilities.pad(str, str.length()+4-idx, "0");
                	}
        		} else
        		{
        			str = massageTS(str);
        		}
        		return str;
        	}
        }              

        private byte[] getQuoteFixedinBytes(byte[] input, int len)
        {
        	if (len == 0)
        		return null;
        	byte[] bytesWithQuote = new byte[len*2+2];
        	bytesWithQuote[0] = 34;
        	int k = 1;
        	for (int i = 0; i < len; ++i)
        	{
        		bytesWithQuote[k++] = input[i];
        		if (input[i] == 34)
        		{
        			bytesWithQuote[k++] = 34;
        		}
        	}
        	bytesWithQuote[k++] = 34;
        	byte[] finalBytes = new byte[k];
        	System.arraycopy(bytesWithQuote, 0, finalBytes, 0, k);
        	return finalBytes;
        }
        
        private byte[] getBinaryString(InputStream data) throws Exception
        {
           byte[] tmpBytes = null;
           int len;
           if (data != null)
           {
        	   
              len = data.read(binaryData);
              //log("Before binary=" + printBytes(binaryData, 0, len));
              if (len > 0)
              {
            	  tmpBytes = getQuoteFixedinBytes(binaryData, len);
                  
              }
           } else
        	   tmpBytes = null;
           return tmpBytes;
        }
        
        private String fixDoubleQuoteInString(String inputString)
        {
            if (inputString != null || inputString.length() == 0)
            {
            	inputString = (netezza) ? inputString.replace("|","\\|") : inputString.replace("\"","\"\"");
                if (trimTrailingSpaces)
                	if (!inputString.matches("^\\s+$"))
                       inputString = inputString.replaceAll("\\s+$", "");
                inputString = (netezza) ? inputString : "\"" + inputString + "\"";
            }
            return inputString;
        }
        
        private String getStringValue(ResultSet rs, int colIndex) throws SQLException
        {
        	String tmpStr;
        	try
        	{
        		tmpStr = rs.getString(colIndex);
        	} catch (Exception e)
        	{
        		tmpStr = rs.getObject(colIndex).toString();
        	}
        	return tmpStr;
        }
        
        private BinData getColumnValue(int id, long numRow, 
              ResultSet rs, ResultSetMetaData metaData, int colIndex, String targetDataType) throws Exception
        {        	
        	byte[] tmpBytes = null;
            String colType = "", javaType, tmpStr = "";
            boolean nullable;
            InputStream inputStreamData;
            
            Blob blob = null;
            int ij = colIndex - 1, blobLength = 0;
            byte[] lob = null;
            
            try
            {
                colType = metaData.getColumnTypeName(colIndex).toUpperCase();
                
                javaType = metaData.getColumnClassName(colIndex);
                
                nullable = (metaData.isNullable(colIndex) == ResultSetMetaData.columnNoNulls) ? false : true;
                // Fix for Oracle Stream closed problem
                if (!((data.Oracle() && (colType.equalsIgnoreCase("BLOB") || colType.equalsIgnoreCase("LONG") || colType.equalsIgnoreCase("LONG RAW"))) ||
                	  (data.Mssql() && (colType.equalsIgnoreCase("BINARY") || colType.equalsIgnoreCase("VARBINARY")))))
                {
                	if (data.Mysql() && colType.equalsIgnoreCase("TIME"))
                	{
                		try
                		{
                			tmpStr = getStringValue(rs, colIndex);
                		} catch (Exception e)
                		{
                		    tmpStr = "23:59:59";                			
                		}
                	} else
                		tmpStr = getStringValue(rs, colIndex);;
                }
                if (data.Oracle())
                {
                   if (colType.startsWith("TIMESTAMP"))
                   {
                	   if (convertOracleTimeStampWithTimeZone2Varchar.equalsIgnoreCase("true"))
                	   {
                		   tmpStr = rs.getString(colIndex);
                	   } else
                	   {
                     	  Timestamp tst = rs.getTimestamp(colIndex); 
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
                   }                   
                   else if (colType.equalsIgnoreCase("DATE"))
                   {
                	   tmpStr = getStringValue(rs, colIndex);
                	   if (tmpStr != null)
                	   {
                		   if (targetDataType.startsWith("DATE"))
                			  tmpStr = massageDate(tmpStr);
                		   else
                		      tmpStr = massageTS(tmpStr);
                	   }
                   }
                   else if (colType.equalsIgnoreCase("RAW"))
                   {
                	   if (javaType.equalsIgnoreCase("byte[]"))
                       {
                		   inputStreamData = rs.getBinaryStream(colIndex);
                           tmpBytes = getBinaryString(inputStreamData);
                           if (tmpBytes == null)
                       	    tmpStr = null;
                       } else
                    	   tmpStr = getStringValue(rs, colIndex);
                   }
                   else if (colType.equalsIgnoreCase("NUMBER"))
                   {
                	   // 7.17886093667943577140020741529475352374E-01
                	   int p, len;
                	   if (tmpStr != null && ((len = tmpStr.length()) > 42) && (p = tmpStr.toUpperCase().lastIndexOf('E')) > 0)
                	   {
                	       tmpStr = tmpStr.substring(0,(p-(len-42))) + tmpStr.substring(p);
                	   }
                   }
                   else if (colType.equals("BLOB") || colType.equals("CLOB") || colType.equals("NCLOB")|| colType.equals("DBCLOB") || colType.equals("SYS.XMLTYPE") || 
                			   colType.equalsIgnoreCase("XMLTYPE") ||  
                			   colType.equalsIgnoreCase("LONG") || colType.equalsIgnoreCase("LONG RAW"))
                   {
                		   tmpStr = "";
                   }
                   else
                   {
                	   tmpStr = getStringValue(rs, colIndex);               		   
                   }
                } else if (data.Postgres())
                {
                    if (colType.equalsIgnoreCase("bool"))
                    {
                    	tmpStr = getStringValue(rs, colIndex);
                        if (tmpStr != null)
                        {
                            if (tmpStr.equalsIgnoreCase("t") || tmpStr.equalsIgnoreCase("true") || 
                                tmpStr.equalsIgnoreCase("yes") || tmpStr.equalsIgnoreCase("1") ||
                                tmpStr.equalsIgnoreCase("y"))
                            {
                                tmpStr = "1";
                            }
                            else
                            {
                                tmpStr = "0";
                            }
                        }
                    }
                    else
                    {
                    	tmpStr = getStringValue(rs, colIndex);                            
                    }
                }
                else if (data.Sybase() || data.Mssql())
                {
                	 if (!(bval.isCLOB[ij] || bval.isBLOB[ij] || bval.isXML[ij]))
                	 {
	             	     if (colType.equalsIgnoreCase("BINARY") || colType.equalsIgnoreCase("VARBINARY"))
	            	     {
	             	    	inputStreamData = rs.getBinaryStream(colIndex);
	             	    	tmpBytes = getBinaryString(inputStreamData); 
	                        if (tmpBytes == null)
	                     	   tmpStr = null;
	            	     } else if (colType.equalsIgnoreCase("UNIQUEIDENTIFIER"))
	            	     {
	            	    	 tmpBytes = rs.getBytes(colIndex);
	            	    	 if (tmpBytes != null)
	            	    	 {
	                	    	 int len = tmpBytes.length;
	                	    	 if (len > 0)
	                             {
	                           	     tmpBytes = getQuoteFixedinBytes(tmpBytes, len);
	                             }
	            	    	 }
	            	     }
	             	     else
	            	     {
	             	    	tmpStr = getStringValue(rs, colIndex);  
	             	    	 }     
                	 }
                     if (colType.startsWith("SMALLDATETIME"))
                     {
                  	     Timestamp tst = rs.getTimestamp(colIndex); 
                  	     if (tst != null)
                  	     {
              	            tmpStr = tst.toString();
              	            if (tmpStr != null)
              	            {
              		           tmpStr = massageSmallDateTime(tmpStr);
              	            }
                  	     } else
                  		    tmpStr = null;
                     }                   
                }
                else if (data.Mysql())
                {
                	if (colType.equals("YEAR") && javaType.equals("java.sql.Date"))
                	{
                		java.sql.Date tmpDate = rs.getDate(colIndex);
                		if (tmpDate != null)
                		{
                		    SimpleDateFormat sdf =new SimpleDateFormat("yyyy");
                		    tmpStr = sdf.format(tmpDate);
                		    if (tmpStr.equals("0001") || tmpStr.equals("0000"))
                		    	tmpStr = "";
                		} else
                			tmpStr = null;
                	}
                	else if (colType.equals("TIME"))
                	{
            			try
            			{
            				tmpStr = getStringValue(rs, colIndex);
            			} catch (Exception e)
            			{
            				tmpStr = "23:59:59";
            			}
                	}
                	else if (colType.equals("DATE"))
                	{
                		if (tmpStr != null && tmpStr.equals("0001-01-01") && mysqlZeroDateToNull)
                		{
                			tmpStr = "";
                		}
                	}
                	else if (colType.equals("DATETIME"))
                	{
                		if (tmpStr != null && tmpStr.equals("0001-01-01 00:00:00.0") && mysqlZeroDateToNull)
                		{
                			tmpStr = "";
                		}
                	}
                	else
                	{
                		tmpStr = getStringValue(rs, colIndex);
                	}                    	
                }
                else if (data.Domino())
                {
                   Object field = null;
                   try
                   {
                      field = rs.getObject(colIndex);
                   } catch (SQLException qex)
                   {
                      if (qex.getErrorCode() == 23316)
                      {
                          field =  rs.getString(colIndex);   
                      } else
                      {
                         field = "(null)";
                      }
                   }
                   tmpStr = (field == null) ? null : field.toString();
                }
                else
                {
                    if (!(data.Access() && colType.equals("LONGBINARY")))
                    {
                        if (javaType.equalsIgnoreCase("byte[]"))
                        {
                           inputStreamData = rs.getBinaryStream(colIndex);
                           tmpBytes = getBinaryString(inputStreamData);
                           if (tmpBytes == null)
                        	   tmpStr = null;
                        } else
                        	tmpStr = getStringValue(rs, colIndex);
		            }
                }
       	        if (colType.equalsIgnoreCase("TIME") && mapTimeToTimestamp.equalsIgnoreCase("true"))
    	        {
    	    	    tmpStr = "1900-01-01-" + rs.getString(colIndex);
    	        }
                if (tmpStr == null) return new BinData(0, "".getBytes());
                if (!(bval.isCLOB[ij] || bval.isBLOB[ij] || bval.isXML[ij]))
                {
                	if (javaType != null)
                	{
	                    if (javaType.equalsIgnoreCase("java.lang.String"))
	                    {
	                    	tmpStr = fixDoubleQuoteInString(tmpStr);
	                    }
	                    else if (colType.equalsIgnoreCase("TIMESTAMPTZ") || colType.equalsIgnoreCase("TIMESTAMPLTZ"))
	                    {
	                        if (tmpStr != null)
	                        {
	                           tmpStr = convertTSTZ(tmpStr);
	                        }
	                    }
	                    else if (javaType.equalsIgnoreCase("java.sql.Time") && colType.equalsIgnoreCase("TIMETZ"))
	                    {
	                        if (tmpStr != null)
	                        {
	                            tmpStr = convertTimeTZ(tmpStr);
	                        }
	                    } else if (colType.equalsIgnoreCase("CLOB") || colType.equalsIgnoreCase("DBCLOB"))
	                    {
	                    	tmpStr = fixDoubleQuoteInString(tmpStr);
	                    }
                	}
                	if (tmpBytes == null)
                	{
                		byte[] bytes = tmpStr.getBytes(encoding);
                		return new BinData(bytes.length, bytes);
                	} else
                	{
                		return new BinData(tmpBytes.length, tmpBytes);
                	}
                }
            } catch (SQLException e)
            {
            	String err = e.getMessage();
                log(t.srcTableName[id]+" Row[" + numRow +"] Col[" + colIndex + "] Error:" + err);
                byte[] bytes = "SkipThisRow".getBytes(encoding);
                return new BinData(bytes.length, bytes);
            }
            return dd.extractLOBS(nullable, numRow, colType, id, colIndex, dataTypeMap);
        }
        
    	private String formatTZ(String tz)
    	{
    		int m = 2,n;
    		String s = tz.trim(), t, u, v;
    		if (!(s.charAt(0) == '-') || (s.charAt(0) == '+'))
    		{
    			s = "+" + s;
    		}
    		if (s.contains(":"))
    		{
    			m = s.lastIndexOf(':');
    		} else if (s.contains("."))
    		{
    			m = s.lastIndexOf('.');
    		}
    		t = s.substring(0,1);
    		u = s.substring(1, m);
    		v = s.substring(m+1);
    		m = Integer.valueOf(u);
    		n = Integer.valueOf(v);
    		tz = t + String.format("%02d", m) + String.format("%02d", n);
    		return tz;
    	}
        
        private String convertTSTZ(String ts)
        {        
            SimpleDateFormat sdf;
            String newts = "", tz = "", micro = "";
            int count, pos, pos2;
            Date date;
            Pattern p;
            Matcher m;
            
            try
            {
                newts = ts;
                pos = ts.lastIndexOf("+");
                if (pos == -1)
                {
                	p = Pattern.compile("-");
                	m = p.matcher(ts);
                	count = 0;
                	while (m.find())
                	{
                		count += 1;
                	}
                	if (count > 2)
                	{
                	   pos = ts.lastIndexOf("-");
                	} else
                	{
                    	p = Pattern.compile(":");
                    	m = p.matcher(ts);
                    	count = 0;
                    	while (m.find())
                    	{
                    		count += 1;
                    	}
                    	if (count > 2)
                    	{
                    	   pos = ts.lastIndexOf(" ")+1;
                    	} else if (count == 1)
                    	{
                    	   pos = ts.lastIndexOf(" ")+1;
                    	}
                    	if (count == 0)
                    	{
                        	p = Pattern.compile("\\.");
                        	m = p.matcher(ts);
                        	while (m.find())
                        	{
                        		count += 1;
                        	}
                        	if (count > 2)
                        	{
                        	   pos = ts.lastIndexOf(" ")+1;
                        	} else if (count == 1)
                        	{
                        	   pos = ts.lastIndexOf(" ")+1;
                        	}                		
                    	}
                	}
                }
                if (pos != -1)
                {
                    newts = ts.substring(0,pos);
                    newts = newts.trim();
                    tz = ts.substring(pos);
                    if (tz != null && tz.length() > 0)
                       tz = formatTZ(tz);
                } else
                    pos = ts.length();
                pos2 = newts.lastIndexOf('.'); 
                if (pos2 > 0)
                {
                    micro = newts.substring(pos2);
                    newts = newts.substring(0,pos2);
                    if (micro.length() > 4)
                       micro = "." + IBMExtractUtilities.padRight(micro.substring(1), 6, "0");
                    else
                       micro = "." + IBMExtractUtilities.padRight(micro.substring(1), 3, "0");
                }
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
                try
                {
                   date = sdf.parse(newts);
                } catch (ParseException e)
                {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss", Locale.US);
                    date = sdf.parse(newts);            	
                }
                if (tz != null && tz.length() > 0 && convertTimeStampWithTimeZone2UTCTime.equalsIgnoreCase("true"))
                {
    	            TimeZone z = TimeZone.getTimeZone("GMT"+tz);
    	            //System.out.println("Timezone = " + z.getDisplayName());
	                sdf.setTimeZone(z);
                }
                newts = sdf.format(date);
                newts = newts + micro;
                if (convertOracleTimeStampWithTimeZone2Varchar.equalsIgnoreCase("true"))
                	newts = newts + " " + tz.substring(0,3) + ":" + tz.substring(3);
                //System.out.println("old value " + ts + " new value " + newts + " TZ = " + tz);  
            } catch (Exception e)
            {
                newts = "";
                e.printStackTrace();
            }
            return newts;
        }
        
        private String convertTimeTZ(String ts)
        {        
            SimpleDateFormat sdf;
            String newts = "", tz = "", micro = "";
            int pos, pos2;
            Timestamp tst = null;
            
            try
            {
                newts = ts;
                pos = ts.length()-3;
                if (ts.charAt(pos) == '-' || ts.charAt(pos) == '+')
                {
                    newts = ts.substring(0,pos);
                    tz = ts.substring(pos);
                    if (tz.length() == 3) tz += "00";
                } else
                    pos = ts.length();
                pos2 =newts.lastIndexOf('.'); 
                if (pos2 > 0)
                {
                    micro = newts.substring(pos2);
                    newts = newts.substring(0,pos2);
                }
                newts = newts + tz;
                
                if (tz.equals(""))
                {
                    sdf = new SimpleDateFormat("HH:mm:ss", Locale.US);
                } else
                {
                    sdf = new SimpleDateFormat("HH:mm:ssZ", Locale.US);
                }
                Date date = sdf.parse(newts);
                tst = new Timestamp(date.getTime());
                newts = tst.toString();
                pos2 =newts.lastIndexOf('.'); 
                if (pos2 > 0)
                {
                    newts = newts.substring(0,pos2);
                }
                newts = newts.substring(11) + micro;
                //System.out.println("old value " + ts + " new value " + newts);  
            } catch (ParseException e)
            {
                newts = "";
                e.printStackTrace();
            }
            return newts;
        }
        
        private void flushData(int id)
        {
        	try
        	{
        		Object lock1 = new Object();
     		    synchronized (lock1)
    		    {
				    if (usePipe)
				    {
					   int bytesReturn;
					   if (Constants.win())
					   {
						   bytesReturn = Pipes.WriteFile(pipeHandles[id], fileBuffer.toByteArray(), fileBuffer.size());
						   if (bytesReturn == -1)
						   {
							  int errorNumber = Pipes.GetLastError();
							  log("Error Writing to pipe " + Pipes.FormatMessage(errorNumber));
						   }
					   } else
					   {
						   bytesReturn = fc[id].write(ByteBuffer.wrap(fileBuffer.toByteArray()));
						   if (bytesReturn == -1)
						   {
							  log("Error Writing to Unix pipe ");
						   }
					   }
				    } else
			    	      fp[id].write(fileBuffer.toByteArray());
    		    }
        	} catch (Exception e)
        	{
        		e.printStackTrace();
        	}
        }
        
        private void writeDataFile(int id, long recordCount, byte[] bytes) throws Exception
        {
           int rowSize = 0;
           long maxRows;
           double mult;
           String dsName;
           
           if (zos)
           {
        	    z.writeZData(id, recordCount, bytes);
           }
           else
           {
		       if (fileBuffer.size() < bytesBufferSize)
		       {
			      fileBuffer.write(bytes);
		       } else
		       {
			      flushData(id);
	    	      fileBuffer.reset();
	    	      fileBuffer.write(bytes);
		       }
           }
        }
        
        private void BuildBladeMemoryMap()
        {
        	if (dataUnload)
        	{
	            synchronized (empty)
			    {
	               if (tempTableMap == null)
	                  BuildTemporaryTableMap();				
			    }
        	}
    		try
        	{
	            synchronized (empty)
	 		    {
	                if (dataTypeMap == null)
	                   BuildDataTypeMap();			
	 		    }
        	} catch (Exception e)
        	{
        		e.printStackTrace();
        		System.exit(-1);
        	}        		
        	if (ddlGen || extractObjects)
        	{
        		try
	        	{
		            synchronized (empty)
					{
		                if (uniqueIndexAllMap == null)
		                   BuildIndexMapAllCount();
		                if (uniqueIndexNullMap == null)
			                BuildIndexMapNullCount();
		            }				
		            synchronized (empty)
					{
						if (dataCaptureChangesMap == null)
							BuildDataCaptureChangesMap();
					}
		        	synchronized (empty)
					{
						if (tableAttributeMap == null)
							BuildTableAttributeMap();
					}
		        	synchronized (empty)
					{
		            	if (partitionTypeMap == null)
		                   BuildPartitionTypeMap();				
					}
		            synchronized (empty)
				    {
		               if (tempTableMap == null)
		                  BuildTemporaryTableMap();				
				    }
		        	synchronized (empty)
					{
			            if (checkConstraintMap == null)
			            {
				           BuildCheckConstraintMap();									
			            }
			            if (checkColumnsMap == null)
			            {
					       BuildCheckColumns();
			            }
					}
		        	synchronized (empty)
					{
						if (uniqueConstraintMap == null)
						{
							BuildUniqueConstraintMap();
						}
					}
		        	synchronized (empty)
					{
		                if (defaultValuesMap == null)
		            	   BuildDefaultValuesMap();					
					}
		        	synchronized (empty)
					{
		                if (indexMap == null)
		                {
		                   BuildIndexMap();				
		                }
		                if (indexClusterMap == null)
		                {
		                	BuildIndexClusterMap();
		                }
					}
		        	synchronized (empty)
					{
		            	if (indexExpressionMap == null)
		                   BuildIndexExpressionmap();				
					}
		        	synchronized (empty)
					{
		            	if (functionalIndexColMap == null)
		                   BuildFunctionalIndexColumn();				
					}        	
					synchronized (empty)
					{
						if (xmlIndexMap == null)
						  BuildDB2XMLIndexPattern();				
					}
					synchronized (empty)
					{
						if (fkMap == null)
							BuildFKeysMap();
					}
					synchronized (empty)
					{
						if (partitionsMap == null)
							BuildPartitionsMap();
					}
					synchronized (empty)				
					{
						if (tabColumnsMap == null)
							BuildPartitionColumnTypesMap();
					}
					synchronized (empty)
					{
						if (partitionColumnsMap == null)
							BuildPartitionColumnsMap();
					}
	        	} catch (Exception e)
	        	{
	        		e.printStackTrace();
	        		System.exit(-1);
	        	}
        	}
        }
        
        private void nzJDBCLoad(int id, boolean start) throws Exception
        {
            if (t.multiTables[id] == 0)
            {
         	   StringBuffer buffer;
               t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).setInitialThreadID(id);
               buffer = genNZLoadScript(id, true);
     	       if (buffer.length() > 0)
     	       {
         	       Thread.sleep(2000);            	       
 				   RunNZLoad task = new RunNZLoad(dstdata, buffer);
 				   t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread = new Thread(task);
 				   if (start)
 				        t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.start();
 				   Thread.sleep(2000);
     	       }
            }        	
        }
        
        private void db2PipeLoad(int id, boolean start) throws Exception
        {
            if (t.multiTables[id] == 0)
            {
         	    StringBuffer buffer;
                t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).setInitialThreadID(id);
                buffer = genDB2LoadScript(id, true);
     	       if (buffer.length() > 0)
    	       {
        	       Thread.sleep(2000);            	       
 				   RunDB2Load task = new RunDB2Load(dstdata, loadWarningList, messageMap, fileMap, buffer);
 				   t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread = new Thread(task);
 				   if (start)
 				        t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.start();
 				   Thread.sleep(2000);
    	       }
            }        	
        }
        
        private void db2PipeLoad(int id, String fileName) throws Exception
        {
           if (t.multiTables[id] == 0)
           {
        	   StringBuffer buffer;
               t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).setInitialThreadID(id);
               db2PipeLoadWriter[id] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, false), sqlFileEncoding));
               db2PipeLoadWriter[id].write(connectStr);
               buffer = genDB2LoadScript(id, true);
               db2PipeLoadWriter[id].write(buffer.toString());
               db2PipeLoadWriter[id].write("TERMINATE;" + linesep);
    	       db2PipeLoadWriter[id].close();
    	       if (buffer.length() > 0)
    	       {
        	       Thread.sleep(2000);            	       
				   String executingScriptName = (new File(fileName)).getAbsolutePath();
				   RunDB2Script task = new RunDB2Script(null, dstDB2Home, executingScriptName, true);
				   t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread = new Thread(task);
				   t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.start();
				   Thread.sleep(2000);
    	       }
           }
        }
        
        private void db2SyncLoad(int id) throws Exception
        {
       	   String fil = IBMExtractUtilities.FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase());
    	   String fileName = OUTPUT_DIR + fil + "_syncload.sql";
           if (t.multiTables[id] == 0)
           {
        	   StringBuffer buffer;
               t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).setInitialThreadID(id);
               db2SyncLoadWriter[id] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, false), sqlFileEncoding));
       		   db2SyncLoadWriter[id].write(connectStr);
               buffer = genDB2LoadScript(id, true);
               db2SyncLoadWriter[id].write(buffer.toString());
               db2SyncLoadWriter[id].write("TERMINATE;" + linesep);
               db2SyncLoadWriter[id].close();
               if (buffer.length() > 0)
               {
        	       Thread.sleep(2000);            	       
				   String executingScriptName = (new File(fileName)).getAbsolutePath();
				   RunDB2Script task = new RunDB2Script(null, dstDB2Home, executingScriptName, true);
				   t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread = new Thread(task);
               }
           }        	
        }
        
        private long dumpData(int id, long numRows, long tableRows)
        {
        	int i = 0, colCount = dd.colCount();
        	boolean skip;
            BinData binData;
            String targetDataType = "";
            long last = System.currentTimeMillis();
            try
            {
                while (dd.rs.next()) 
                {
                    byteOutputStream.reset();
                    skip = false;
                    for(i = 1; i <= colCount; i++ ) 
                    {
                    	targetDataType = dd.getTargetDataType(id, i, dataTypeMap);
                    	binData = getColumnValue(id, numRows, dd.rs, dd.resultsetMetadata, i, targetDataType);
                        if (binData != null && binData.buffer.toString().equals("SkipThisRow"))
                        {
                            skip = true;
                            continue;
                        }
                        else
                        {
                        	if (binData != null)
                        	   byteOutputStream.write(binData.buffer);
                        	if (!netezza)
                        	   bytesUnloaded += binData.length;
                        }
                        if (i != colCount)
                        {
                        	byteOutputStream.write(colsep.getBytes(encoding));
                        }
                    }
                    if (!zos)
                    {
                    	byteOutputStream.write(linesep.getBytes(encoding));
                    }
                    numRows++;
                    if (!skip)
                    {                        	
                    	writeDataFile(id, tableRows, byteOutputStream.toByteArray());
                    	synchronized (byteOutputStream)
    					{
    						if (byteOutputStream != null)
    						{
    							bytesUnloaded += byteOutputStream.size();
    						}
    					}
                    }
                    if (batchSize != 0 && (numRows % batchSize == 0))
                    {
                        long now = System.currentTimeMillis();
                        DecimalFormat myFormatter = new DecimalFormat("###.###");
                        log(removeQuote(t.srcTableName[id]) + "["+number+"/"+REEL_NUM+"] " + batchSize + " rows unloaded in " + 
                        		myFormatter.format((now - last)/1000.0) + " sec"+getJobsStatus());
                        last = now;
                    }
                    if (!limitExtractRows.equalsIgnoreCase("ALL"))
                    {
                    	try
                    	{
                    	   int x = Integer.parseInt(limitExtractRows);
                    	   if (x >= 0 && numRows >= x)
                    		   break;
                    	} catch (Exception es)
                    	{
                    		;
                    	}
                    }
                }            	
            } catch (SQLException qex)
            {
                log("Exception unloading SQL=" + t.query[id] + " Row # (" + numRows + ") Col #(" + i + ") " + targetDataType + " Error code=" + qex.getErrorCode() + " Message=" + qex.getMessage());
                qex.printStackTrace();            	
            } catch (Exception ex)
            {
                log("Exception unloading SQL=" + t.query[id] + " Row # (" + numRows + ") Col #(" + i + ") " + targetDataType + " Message=" + ex.getMessage());
                ex.printStackTrace();            	
            }
            return numRows;
        }
        
        private long processTable(int id)
        {
        	String methodName = "processTable";
            long tableRows = 0;            
            
            BinData binData;
            String sql = t.query[id];
            boolean skip = false;
            int colCount = 0, numUniq = 1;

            if (sql.length() == 0) return 0L;
            
            if (data.Sybase())
               getSybaseUDTInfo();
            else if (data.Mysql() || data.Mssql())
            {
                String newSQL = IBMExtractUtilities.ReBuildSQLQuery(data, sql, t.srcSchName[id], t.srcTableName[id],
                		server, port, dbName, uid, pwd);
                if (!newSQL.equals(sql))
                {
                	log("Re-writing SQL to get decrypted data. New SQL=" + newSQL);
                	sql = newSQL;
                }
            }
            
            long numRows = 0;
            fileBuffer.reset();
            try
            {
                if (zdb2)
                {
                   if (dataUnload)
                   {
                      tableRows = countRows(id);
                   }
                }
                dd.getDataDumpResultSet(id, dataTypeMap, columnLengthMap, lobsLengthMap, sql);
                if (debug)
                {
                	log("After dd.getDataDumpResultSet");
                	IBMExtractUtilities.printHashMap(dataTypeMap);
                }
                bval = dd.bval;
                colCount = dd.colCount();
                if (ddlGen)
                {
                	synchronized (methodName)
					{
                    	BuildNotNullcolumnsMap(id, dd.resultsetMetadata);						
					}
                }
                long last = System.currentTimeMillis();
                if (syncLoad)
                {
                    if (netezza) 
                 	   nzJDBCLoad(id, false);
                    else
                       db2SyncLoad(id);
                }
                else if (usePipe)
                {
               	   String fil = IBMExtractUtilities.FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase());
            	   String fileName = OUTPUT_DIR + fil + "_pipeload.sql";
                   if (netezza) 
                	   nzJDBCLoad(id, true);
                   else
                      db2PipeLoad(id, fileName);
         	       if (Constants.win())
        	       {
        	    	   fileName = "\\\\.\\pipe\\" + fil + ".pipe";
            	       log("Waiting for "+(netezza ? "Netezza" : "DB2")+" Load to connect to the pipe " + fileName + "[" + pipeHandles[id] + "]");
            		   boolean connected = Pipes.ConnectNamedPipe(pipeHandles[id], 0);
            		   if (!connected)
            		   {
            			  int lastError = Pipes.GetLastError();
            			  if (lastError == Constants.ERROR_PIPE_CONNECTED)
            				 connected = true;
            			  else
            			  {
            				  if (lastError != 0)
            				      log((netezza ? "Netezza" : "DB2")+" Load failed to connect to the pipe " + fileName + "[" + pipeHandles[id] + "] Error : " + 
                					   Pipes.FormatMessage(lastError));
            				  else
            					  log((netezza ? "Netezza" : "DB2")+" Load Connected to the pipe " + fileName + "[" + pipeHandles[id] + "]");
            			  }
            		   }
            		   if (connected)
            		   {
            			  log((netezza ? "Netezza" : "DB2")+" Load Connected to the pipe " + fileName);
            		   }
        	       } else
        	       {
        	    	    FileOutputStream fos = null;
        				try
        				{
            				if (t.multiTables[id] == 0)
            				{
            					fileName = OUTPUT_DIR + "data" + filesep + fil + ".pipe";
        	            	    log("Waiting for "+(netezza ? "Netezza" : "DB2")+" Load thread[" + id + "] to connect to the pipe " + fileName);
            					fos = new FileOutputStream(new File(fileName));
            					log((netezza ? "Netezza" : "DB2")+" Load [" + id + "] Connected to the pipe " + fileName);
            					fc[id] = fos.getChannel();
            					} else
            				{
            					while (fc[id-1] == null)
            					{
            						log("Main "+(netezza ? "Netezza" : "DB2")+" Load thread["+id+"] not yet connected. Waiting ...");
            						Thread.sleep(2000);
            					}
            					fc[id] = fc[id-1]; // Use same file channel for parallel unload of the same table
            				}
        				} catch (Exception e)
        				{
        					e.printStackTrace();
        				}
        	       }
                }
          	    if (debug) IBMExtractUtilities.StartTimeKeeper("UNLOAD_"+t.srcSchName[id]+"."+t.srcTableName[id]+"_"+number);
                if (dataUnload || usePipe)
                {
                	dd.openLobWriters(id);
                	numRows = dumpData(id, numRows, tableRows);
                	dd.closeLobWriters();
                	// Flush unwritten fileBuffer to the disk
                	if (fileBuffer.size() > 0)
                	    flushData(id);
                	if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getCount() == 1)
                	{
                        if (zos)
                        {
                            z.closeZDataFile(id);
                        } else
                        {
                        	if (usePipe)
                        	{
	                        	if (Constants.win())
	                        	{
	            					Pipes.FlushFileBuffers(pipeHandles[id]);
	            				    Pipes.CloseHandle(pipeHandles[id]);
	            				    Pipes.DisconnectNamedPipe(pipeHandles[id]);
	                        	} else
	                        	{
	                        		if (fc[id] != null)
	                        		{
	            						try
	            						{
	            							fc[id].close();
	            						} catch (IOException e)
	            						{
	            							e.printStackTrace();
	            						}
	                        		}
	                        	}	
	                        	// Block till load is done so that other loads are not started simultaneously.
	                        	// We will however not block for a single table unloaded in multiple threads.
	                        	log("Blocking inner load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	                        	try
	            				{
	                        		if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread != null)
	                        			{
	                        			t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.join();
	                        			}
		                        	log("Released inner load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	            				} catch (InterruptedException e)
	            				{
	            					e.printStackTrace();
	            				}

                        	} else
                        	{
                        		if (fp[id] != null)
                        		{
            						try
            						{
            							fp[id].close();
            						} catch (IOException e)
            						{
            							e.printStackTrace();
            						}
                        		}                        		
                        	}
                        }                		
                	}
                }
                if (syncLoad)
                {
                	if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getCount() == 1)
                	{
	            	    log("Starting inner load thread[" + id + "] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	 				    t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.start();
	 				    Thread.sleep(2000);
	                	log("Blocking inner load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	                	try
	    				{
	                		if (t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread != null)
	                			t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).loadThread.join();
	                    	log("Released inner load thread["+id+"] for " + t.dstSchemaName[id]+"."+t.dstTableName[id]);
	    				} catch (InterruptedException e)
	    				{
	    					e.printStackTrace();
	    				}
                	}
                }
                if (data.Mysql())
                {
	                // Added following close for MySQL as MySQL does not like any thing open before commit.
	                if (dd.rs != null)
	                {
	                	try
	                	{
		                   dd.rs.close();	                	
	                	} catch (Exception fx)
	                	{
	                		fx.printStackTrace();
	                	}
	                }
	                dd.close();
                }
                if (!data.Informix())
                  data.connection.commit();
                if (!(data.Informix() || data.Mysql()))
                {
            	    data.connection.setAutoCommit(true);
                }
          	    if (debug) IBMExtractUtilities.StopTimeKeeper("UNLOAD_"+t.srcSchName[id]+"."+t.srcTableName[id]+"_"+number);
          	    if (debug) IBMExtractUtilities.StartTimeKeeper("DDL_"+t.srcSchName[id]+"."+t.srcTableName[id]+"_"+number);
                if (ddlGen && !usePipe) 
                {
                    if (t.multiTables[id] == 0)
                    {
                       if (!(data.Domino()))
                       {
                          genCheckConstraints(id);
                          genFKeys(id);
                          genTableKeys(id);
                          checkUniqueConstraint(id);
                       }
                       genTableScript(id, numRows);
                    }
                }
                if (dataUnload && !usePipe)
                {
                   if (t.multiTables[id] == 0)
                   {
                	   if (db2)
                	   {
                      	   if (loadException)
                    	   {
                    	      genLoadExceptionTableScript(id);
                    	   }
                           genDB2LoadScript(id);                		   
                	   } else if (zos)
                	   {
                           genzDB2LoadScript(id, z.generateJCLScripts);                		   
                	   } else if (netezza)
                	   {
                           genNZLoadScript(id);                		                   		   
                	   }
                   }
                }
                if (!(data.Domino() || data.Informix()))
            	    data.connection.setAutoCommit(false);
                numUniq++;
            } 
            catch (SQLException qex)
            {
                log("Exception processing SQL=" + sql + " Message=" + qex.getMessage());
                qex.printStackTrace();
                try
                {
                    data.connection.rollback();
                } catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
            catch (Exception ex)
            {
                log("Exception processing SQL=" + sql + " Message=" + ex.getMessage());
                ex.printStackTrace();
                try
                {
                    data.connection.rollback();
                } catch (SQLException e)
                {
                    e.printStackTrace();
                }
            }
            finally
            {
            	try
            	{
	                if (dd.rs != null)
	                    dd.rs.close();
	                dd.close();
            	} 
            	catch (Exception e)
                {
                    log("close() error " + e.getMessage());                	
                }
            }
            jobsStatus = (jobsCompleted++)+"/"+t.totalTables;
      	    if (debug) IBMExtractUtilities.StopTimeKeeper("DDL_"+t.srcSchName[id]+"."+t.srcTableName[id]+"_"+number);
            return numRows;
        }
    }

    private static void log(String msg)
    {
    	if (Constants.zos())
    	{
            System.out.println(Constants.timestampFormat.format(new Date()) + ":" + msg);    		
    	} else 
    	{
            System.out.println("[" + Constants.timestampFormat.format(new Date()) + "] " + msg);    		
    	}
    }

    public static void main(String[] args)
    {
        Constants.setDbTargetName();
        db2_compatibility = Constants.getDbTargetName().equalsIgnoreCase(Constants.db2luw_compatibility);
        sqlsep = Constants.sqlsep;
        netezza = Constants.netezza();
        zdb2 = Constants.zdb2();
        zos = Constants.zos();
        db2 = Constants.db2();
    	long start = System.currentTimeMillis(), end;
        if (args.length < 16)
        {
            System.out.println("usage: java -Xmx600m -DTARGET_DB=<targetdbname> -DOUTPUT_DIR=. ibm.GenerateExtract table_prop_file colsep " +
                               "dbSourceName threads server dbname portnum uid pwd ddlgen(true/false) dataunload(true/false) " +
                               "objects[true/false] pipe[true|false] sync[true|false] fetchSize [loadreplace(true/false)]");
            System.out.println("<targetdbname> can be db2luw or netezza");
            System.exit(-1);
        }
        IBMExtractUtilities.replaceStandardOutput(OUTPUT_DIR + filesep + "IBMDataMovementTool.log");
        IBMExtractUtilities.replaceStandardError(OUTPUT_DIR + filesep + "IBMDataMovementToolError.log");
        TABLES_PROP_FILE = args[0];
        colsep = args[1];
        threads = Integer.parseInt(args[3]);
        server = args[4];
        dbName = args[5];
        port = Integer.parseInt(args[6]);
        uid = args[7];
        pwd = args[8]; 
        pwd = IBMExtractUtilities.Decrypt(pwd);
        ddlGen = Boolean.valueOf(args[9]).booleanValue();
        dataUnload = Boolean.valueOf(args[10]).booleanValue();
        extractObjects = Boolean.valueOf(args[11]).booleanValue();
        usePipe = Boolean.valueOf(args[12]).booleanValue();
        syncLoad = Boolean.valueOf(args[13]).booleanValue();
        fetchSize = Integer.parseInt(args[14]);
        IBMExtractUtilities.fetchSize = fetchSize;
        loadReplace = Boolean.valueOf(args[15]).booleanValue();
        String version = GenerateExtract.class.getPackage().getImplementationVersion();
        if (version != null)
        	log("Version " + version);
        log("OS Type:" + System.getProperty("os.name"));
        log("Java Version:" + System.getProperty("java.version") + ": " + System.getProperty("java.vm.version") + ": " 
        		+ System.getProperty("java.vm.name") + ": " + System.getProperty("java.vm.vendor") + ": " + System.getProperty("sun.arch.data.model") + " bit");
        log("Default encoding " + System.getProperty("file.encoding"));
        log("TABLES_PROP_FILE:" + TABLES_PROP_FILE);
        log("DATAMAP_PROP_FILE:" + IBMExtractUtilities.DATAMAP_PROP_FILE);
        log("colsep:" + colsep);
        log("dbSourceName:" + args[2]);
        log("dbTargetName:" + Constants.getDbTargetName());
        log("threads:" + threads);
        log("server:" + server);
        log("dbName:" + dbName);
        log("port:" + port);
        log("uid:" + uid);
        log("ddlGen:" + ddlGen);
        log("dataUnload:" + dataUnload);
        if (extractObjects && netezza)
        {
        	extractObjects = false;
            log("extractObjects:false (Overide)");
        } else
        	log("extractObjects:" + extractObjects);
        log("usePipe:" + usePipe);
        log("syncLoad:" + syncLoad);
        log("loadReplace:" + loadReplace);

        GenerateExtract pg = new GenerateExtract(args[2]);
        if (mainData.Access() || mainData.Mysql()) 
        {
            if (mainData.Access())
            {
               ddlGen = false;
               log("Using odbc-jdbc bridge so ddl generation is set to false. You will only get data from access database.");
            }
            if (fetchSize != 0)
               log("Warning: For source database=" + args[2] + ", you should consider setting " +
                  "fetchsize=0 to be able to fetch large tables. Otherwise, you may run into " +
                  "outofmemory errors.");
        }
        log("fetchSize:" + fetchSize);
        log("Timezone = " + System.getProperty("user.timezone") + " Offset=" + IBMExtractUtilities.getTimeZoneOffset());
        log(IBMExtractUtilities.getDBVersion(args[2], server, port, dbName, uid, pwd));
        pg.run();
        end = System.currentTimeMillis();
        log("====  Total time: " + IBMExtractUtilities.getSize(bytesUnloaded, " unloaded in ") + IBMExtractUtilities.getElapsedTime(start));
        if (bytesUnloaded > 0)
        {
            log("====  Unload rate: " + IBMExtractUtilities.getExtractionRate(bytesUnloaded, end-start));
            log("====  Average Network Speed: " + IBMExtractUtilities.getAvgNetworkSpeed(bytesUnloaded, end-start));
            log("====  Average Network Latency: " + ping.toString());
        }
        ping.shutdown();
    }
}
