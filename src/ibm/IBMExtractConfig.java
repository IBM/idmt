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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.ibm.jzos.ZUtil;

public class IBMExtractConfig 
{
    private static String sep = Constants.win() ? ";" : ":";
    private static String filesep = System.getProperty("file.separator");    
    private static String linesep = System.getProperty("line.separator");
    private String currHome = IBMExtractUtilities.getHomeDir(), javaHome = null;
	static String INPUT_DIR = null;
	
	private String srcJDBC = "", dstJDBC = "", appJAR = "";
	private String srcVendor = Constants.oracle;
	private String srcDB2Home = "", dstDB2Home = "";
	private String srcDB2Instance = "", dstDB2Instance = "", dstDB2FixPack = "";
	private String srcDB2Release = "", dstDBRelease = "";
	private String srcServer = "localhost", dstServer = "localhost";
	private String srcUid = "", dstUid = "";
	private String srcPwd = "", dstPwd = "";
	private int srcPort = 0, dstPort = 0, numThreads = 5, numWorkDir = 1;
	private String srcSchName = "", dstSchName = "", selectSchemaName = "";
	private String extractDDL = "true", extractData = "true", extractObjects = "true", rowcount, rowexceptcount, fixcheck, zLoadScript, DB2Installed = "true";
	private String regenerateTriggers = "false", dbclob = "false", graphic = "false", trimTrailingSpaces = "false", debug = "false"; 
	private String remoteLoad = "false", compressTable = "false", compressIndex = "false", encoding = "UTF-8", sqlFileEncoding = "UTF-8";	
	private String customMapping = "false", mysqlZeroDateToNull = "false", clusteredIndexes = "false", norowwarnings = "false";
	private String extractPartitions = "true"; // Default true for Oracle and false for others
	private String extractHashPartitions = "false"; 
	private String oracleNumberMapping = "false", exceptSchemaSuffix = "_EXP", exceptTableSuffix = "";
 	private String retainConstraintsName = "false", useBestPracticeTSNames = "true";
	public String Message = "", limitExtractRows = "ALL", limitLoadRows = "ALL";
	public String geninput, genddl, unload, meet, usePipe, syncLoad, autoFixScript, datacheck;
	public String zdb2tableseries = "Q", zHLQName = System.getProperty("user.name").toUpperCase(), znocopypend = "true", zoveralloc = "1.3636", zsecondary = "0", storclas = "none";
	public String extentSizeinBytes = "36000", shell = null, loadException = "true";
	public boolean isTargetInstalled = true, db2Compatibility = false;
	public String caseSensitiveTabColName = null, mapTimeToTimestamp = null;
	private String sybaseConvClass = "", deleteLoadFile = "false";
	private String teradataConnStringExtraParam = "", instanceName = "", mtkHome, sybaseUDTConversionToBaseType = null;
	private String varcharLimit = "4096", numRowsToCheck = "100", batchSizeDisplay = "0", loadDirectory;
	private BufferedWriter genInputWriter, unloadWriter, rowCountWriter, dataCheckWriter, genWriter, checkRemovalWriter;	
	private Properties propParams, propJDBC;
	private String outputDirectory;
	private String fetchSize = "100", validObjects = "true";
	private String mapCharToVarchar, loadStats = "false";
	public boolean paramPropFound = true;
	private BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
	private String srcDBName = "", dstDBName = "", tempFilesPath = "";
	private String commitCount = "100", saveCount = "0", warningCount = "0";
	private String jobCard1 = "//DB2IDMT JOB (DB2IDMT),'DB2 IDMT',CLASS=A,";
	private String jobCard2 = "//     MSGCLASS=A,NOTIFY=&SYSUID";
	private String db2SubSystemID = "DB2P";
	private String SDSNLOAD = "DB2.SDSNLOAD";
	private String RUNLIBLOAD = "DB2.RUNLIB.LOAD";
	private String DSNTEPXX = "DSNTEP91"; 
	private String generateJCLScripts = "true";
	private String convertOracleTimeStampWithTimeZone2Varchar = "false";
	private String convertTimeStampWithTimeZone2UTCTime = "false";
	private String additionalURLProperties = "null";
	
	/**
	 * @return the jobCard1
	 */
	public String getJobCard1()
	{
		return jobCard1;
	}

	/**
	 * @return the jobCard2
	 */
	public String getJobCard2()
	{
		return jobCard2;
	}

	/**
	 * @return the db2SubSystemID
	 */
	public String getDb2SubSystemID()
	{
		return db2SubSystemID;
	}

	/**
	 * @param jobCard1 the jobCard1 to set
	 */
	public void setJobCard1(String jobCard1)
	{
		this.jobCard1 = jobCard1;
	}

	/**
	 * @param jobCard2 the jobCard2 to set
	 */
	public void setJobCard2(String jobCard2)
	{
		this.jobCard2 = jobCard2;
	}

	/**
	 * @param db2SubSystemID the db2SubSystemID to set
	 */
	public void setDb2SubSystemID(String db2SubSystemID)
	{
		this.db2SubSystemID = db2SubSystemID;
	}
	
	/**
	 * @return the commitCount
	 */
	public String getCommitCount()
	{
		commitCount = IBMExtractUtilities.getDefaultString(commitCount, "100");
		return commitCount;
	}

	/**
	 * @param commitCount the commitCount to set
	 */
	public void setCommitCount(String commitCount)
	{
		this.commitCount = commitCount;
	}

	/**
	 * @return the exceptTableSuffix
	 */
	public String getExceptTableSuffix()
	{
		return IBMExtractUtilities.getDefaultString(exceptTableSuffix, "", true);
	}

	/**
	 * @param exceptTableSuffix the exceptTableSuffix to set
	 */
	public void setExceptTableSuffix(String exceptTableSuffix)
	{
		this.exceptTableSuffix = exceptTableSuffix;
	}

	/**
	 * @return the generateJCLScripts
	 */
	public String getGenerateJCLScripts()
	{
		return generateJCLScripts;
	}

	/**
	 * @param generateJCLScripts the generateJCLScripts to set
	 */
	public void setGenerateJCLScripts(String generateJCLScripts)
	{
		this.generateJCLScripts = generateJCLScripts;
	}

	/**
	 * @return the exceptSchemaSuffix
	 */
	public String getExceptSchemaSuffix()
	{
		return IBMExtractUtilities.getDefaultString(exceptSchemaSuffix, "_EXP", true);
	}

	/**
	 * @param exceptSchemaSuffix the exceptSchemaSuffix to set
	 */
	public void setExceptSchemaSuffix(String exceptSchemaSuffix)
	{
		this.exceptSchemaSuffix = exceptSchemaSuffix;
	}

	/**
	 * @return the db2Installed
	 */
	public String getDB2Installed()
	{
		if (DB2Installed == null || DB2Installed.equals("null") || DB2Installed.length() == 0)
		   return "true";
		else
		   return DB2Installed;
	}

	/**
	 * @return the sDSNLOAD
	 */
	public String getSDSNLOAD()
	{
		return SDSNLOAD;
	}

	/**
	 * @return the rUNLIBLOAD
	 */
	public String getRUNLIBLOAD()
	{
		return RUNLIBLOAD;
	}

	/**
	 * @return the dSNTEPXX
	 */
	public String getDSNTEPXX()
	{
		return DSNTEPXX;
	}


	/**
	 * @param sdsnload the sDSNLOAD to set
	 */
	public void setSDSNLOAD(String sdsnload)
	{
		SDSNLOAD = sdsnload;
	}

	/**
	 * @param runlibload the rUNLIBLOAD to set
	 */
	public void setRUNLIBLOAD(String runlibload)
	{
		RUNLIBLOAD = runlibload;
	}

	/**
	 * @param dsntepxx the dSNTEPXX to set
	 */
	public void setDSNTEPXX(String dsntepxx)
	{
		DSNTEPXX = dsntepxx;
	}

	/**
	 * @param db2Installed the db2Installed to set
	 */
	public void setDB2Installed(String db2Installed)
	{
		if (DB2Installed == null || DB2Installed.equals("null") || DB2Installed.length() == 0)
		   this.DB2Installed = "true";
		else
		   this.DB2Installed = db2Installed;			
	}

	/**
	 * @return the clusteredIndexes
	 */
	public String getClusteredInexes()
	{
		if (clusteredIndexes == null || clusteredIndexes.equalsIgnoreCase("null"))
			clusteredIndexes = "false";
		return clusteredIndexes;
	}

	/**
	 * @param clusteredIndexes the clusteredIndexes to set
	 */
	public void setClusteredInexes(String clusteredIndexes)
	{
		this.clusteredIndexes = clusteredIndexes;
	}

	/**
	 * @return the loadDirectory
	 */
	public String getLoadDirectory()
	{
		if ((loadDirectory == null || loadDirectory.length() == 0 || loadDirectory.equalsIgnoreCase("null")) && 
				(outputDirectory != null && outputDirectory.length() > 0))
		{
			loadDirectory = "";
			return loadDirectory;
		}
        if (!(loadDirectory.endsWith("\\") || loadDirectory.endsWith("/")))
    	   loadDirectory += filesep;
		return loadDirectory;
	}

	/**
	 * @param loadDirectory the loadDirectory to set
	 */
	public void setLoadDirectory(String loadDirectory)
	{
		if (loadDirectory == null || loadDirectory.length() == 0)
		   this.loadDirectory = outputDirectory;
		else
		   this.loadDirectory = loadDirectory;
	}

    /**
	 * @return the saveCount
	 */
	public String getSaveCount()
	{
		return IBMExtractUtilities.getDefaultString(saveCount, "0");
	}

	/**
	 * @return the warningCount
	 */
	public String getWarningCount()
	{
		return IBMExtractUtilities.getDefaultString(warningCount, "0");
	}

	/**
	 * @param warningCount the warningCount to set
	 */
	public void setWarningCount(String warningCount)
	{
		this.warningCount = warningCount;
	}

	/**
	 * @param saveCount the saveCount to set
	 */
	public void setSaveCount(String saveCount)
	{
		this.saveCount = saveCount;
	}

	/**
	 * @return the batchSizeDisplay
	 */
	public String getBatchSizeDisplay()
	{
		batchSizeDisplay = IBMExtractUtilities.getDefaultString(batchSizeDisplay,"0");
		int rows;
		try
		{
			rows = Integer.parseInt(batchSizeDisplay);
		} catch (Exception e)
		{
			rows = 0;
		}
		if (rows <= 0)
		   batchSizeDisplay = "0";
		return batchSizeDisplay;
	}

	/**
	 * @param batchSizeDisplay the batchSizeDisplay to set
	 */
	public void setBatchSizeDisplay(String batchSizeDisplay)
	{
		if (batchSizeDisplay == null || batchSizeDisplay.length() == 0)
			this.batchSizeDisplay = "0";
		else
		{
			int rows;
			try
			{
				rows = Integer.parseInt(batchSizeDisplay);
			} catch (Exception e)
			{
				rows = 0;
			}
			if (rows <= 0)
			   this.batchSizeDisplay = "0";
			else
		       this.batchSizeDisplay = batchSizeDisplay;			
		}
	}

	/**
	 * @return the numRowsToCheck
	 */
	public String getNumRowsToCheck()
	{
		return IBMExtractUtilities.getDefaultString(numRowsToCheck, "100");
	}

	/**
	 * @param numRowsToCheck the numRowsToCheck to set
	 */
	public void setNumRowsToCheck(String numRowsToCheck)
	{
		this.numRowsToCheck = numRowsToCheck;
	}

	/**
	 * @return the debug
	 */
	public String getDebug()
	{
		return (debug == null || debug.equalsIgnoreCase("null")) ? "false" : debug;
	}

	/**
	 * @return the tempFilesPath
	 */
	public String getTempFilesPath()
	{
		tempFilesPath = IBMExtractUtilities.getDefaultString(tempFilesPath, "");
		return tempFilesPath;
	}

	/**
	 * @param tempFilesPath the tempFilesPath to set
	 */
	public void setTempFilespPath(String tempFilesPath)
	{
		this.tempFilesPath = tempFilesPath;
	}

	/**
	 * @param debug the debug to set
	 */
	public void setDebug(String debug)
	{
		this.debug = debug;
	}

	/**
	 * @return the varcharLimit
	 */
	public String getVarcharLimit()
	{
		return (varcharLimit == null || varcharLimit.equalsIgnoreCase("null")) ? "10000" : varcharLimit;
	}

	/**
	 * @param varcharLimit the varcharLimit to set
	 */
	public void setVarcharLimit(
			String varcharLimit)
	{
		this.varcharLimit = varcharLimit;
	}

	/**
	 * @return the mapTimeToTimestamp
	 */
	public String getMapTimeToTimestamp()
	{
		if (mapTimeToTimestamp == null || mapTimeToTimestamp.equalsIgnoreCase("null"))
		{
			if (srcVendor != null && srcVendor.equalsIgnoreCase("sybase") && db2Compatibility)
			    mapTimeToTimestamp = "true";
		}
		return mapTimeToTimestamp;
	}

	/**
	 * @param mapTimeToTimestamp the mapTimeToTimestamp to set
	 */
	public void setMapTimeToTimestamp(String mapTimeToTimestamp)
	{
		if (mapTimeToTimestamp == null || mapTimeToTimestamp.equalsIgnoreCase("null"))
		{
			if (srcVendor != null && srcVendor.equalsIgnoreCase("sybase") && db2Compatibility)
				mapTimeToTimestamp = "true";
		}
		this.mapTimeToTimestamp = mapTimeToTimestamp;
	}

	/**
	 * @return the sybaseUDTConversionToBaseType
	 */
	public String getSybaseUDTConversionToBaseType()
	{
		if (sybaseUDTConversionToBaseType == null || sybaseUDTConversionToBaseType.equalsIgnoreCase("null"))
		{
			if (srcVendor != null && srcVendor.equalsIgnoreCase("sybase") && db2Compatibility)
				sybaseUDTConversionToBaseType = "false";
		}
		return sybaseUDTConversionToBaseType;
	}

	/**
	 * @param sybaseUDTConversionToBaseType the sybaseUDTConversionToBaseType to set
	 */
	public void setSybaseUDTConversionToBaseType(
			String sybaseUDTConversionToBaseType)
	{
		if (sybaseUDTConversionToBaseType == null || sybaseUDTConversionToBaseType.equalsIgnoreCase("null"))
		{
			if (srcVendor != null && srcVendor.equalsIgnoreCase("sybase") && db2Compatibility)
				sybaseUDTConversionToBaseType = "false";
		}
		this.sybaseUDTConversionToBaseType = sybaseUDTConversionToBaseType;
	}

	/**
	 * @return the mtkHome
	 */
	public String getMtkHome()
	{
		if (mtkHome == null || mtkHome.length() == 0)
		{
			if (Constants.win())
				return "C:\\MTK";
			else 
				return "/opt/mtk";
		}
		return mtkHome;
	}

	/**
	 * @param mtkHome the mtkHome to set
	 */
	public void setMtkHome(String mtkHome)
	{
		this.mtkHome = mtkHome;
	}

	/**
	 * @return the instanceName
	 */
	public String getInstanceName()
	{
		if (instanceName == null || instanceName.equalsIgnoreCase("null") ||
				instanceName.length() == 0)
		   return "";
		else
			return instanceName;
	}

	/**
	 * @param instanceName the instanceName to set
	 */
	public void setInstanceName(String instanceName)
	{
		this.instanceName = instanceName;
	}

	/**
	 * @return the sqlFileEncoding
	 */
	public String getSqlFileEncoding()
	{
		if (sqlFileEncoding == null || sqlFileEncoding.length() == 0 || sqlFileEncoding.equalsIgnoreCase("null"))
			sqlFileEncoding = "UTF-8";
		return sqlFileEncoding;
	}

	/**
	 * @param sqlFileEncoding the sqlFileEncoding to set
	 */
	public void setSqlFileEncoding(String sqlFileEncoding)
	{
		this.sqlFileEncoding = sqlFileEncoding;
	}

	/**
	 * @return the teradataConnStringExtraParam
	 */
	public String getTeradataConnStringExtraParam()
	{
		if (teradataConnStringExtraParam == null || teradataConnStringExtraParam.equals("null") || teradataConnStringExtraParam.length() == 0)
			//return "CHARSET=UTF8,TYPE=FASTEXPORT,TMODE=ANSI";
			return "CHARSET=UTF8";
		else
			return teradataConnStringExtraParam;
	}

	/**
	 * @param teradataConnStringExtraParam the teradataConnStringExtraParam to set
	 */
	public void setTeradataConnStringExtraParam(String teradataConnStringExtraParam)
	{
		this.teradataConnStringExtraParam = teradataConnStringExtraParam;
	}

	/**
	 * @return the mysqlZeroDateToNull
	 */
	public String getMysqlZeroDateToNull()
	{
		return mysqlZeroDateToNull;
	}

	/**
	 * @param mysqlZeroDateToNull the mysqlZeroDateToNull to set
	 */
	public void setMysqlZeroDateToNull(String mysqlZeroDateToNull)
	{
		this.mysqlZeroDateToNull = mysqlZeroDateToNull;
	}

	/**
	 * @return the loadStats
	 */
	public String getLoadStats()
	{
		if (loadStats == null || loadStats.length() == 0 || loadStats.equals("null"))
		   return "false";
		else
		   return loadStats;
	}

	/**
	 * @return the norowwarnings
	 */
	public String getNorowwarnings()
	{
		return IBMExtractUtilities.getDefaultString(norowwarnings, "false");
	}

	/**
	 * @param norowwarnings the norowwarnings to set
	 */
	public void setNorowwarnings(String norowwarnings)
	{
		this.norowwarnings = norowwarnings;
	}

	/**
	 * @param loadStats the loadStats to set
	 */
	public void setLoadStats(String loadStats)
	{
		this.loadStats = loadStats;
	}

	/**
	 * @return the syncLoad
	 */
	public String getSyncLoad()
	{
		if (syncLoad == null || syncLoad.length() == 0 || syncLoad.equals("null"))
		   syncLoad = "false";
		return syncLoad;
	}

	/**
	 * @param syncLoad the syncLoad to set
	 */
	public void setSyncLoad(String syncLoad)
	{
		if (syncLoad == null || syncLoad.length() == 0 || syncLoad.equals("null"))
		   this.syncLoad = "false";
		else
		   this.syncLoad = syncLoad;
	}

	/**
	 * @return the deleteLoadFile
	 */
	public String getDeleteLoadFile()
	{
		return deleteLoadFile;
	}

	/**
	 * @param deleteLoadFile the deleteLoadFile to set
	 */
	public void setDeleteLoadFile(String deleteLoadFile)
	{
		this.deleteLoadFile = deleteLoadFile;
	}

	/**
	 * @return the selectSchemaName
	 */
	public String getSelectSchemaName()
	{
		return selectSchemaName;
	}

	/**
	 * @param selectSchemaName the selectSchemaName to set
	 */
	public void setSelectSchemaName(String selectSchemaName)
	{
		this.selectSchemaName = selectSchemaName;
	}

	/**
	 * @return the extractObjects
	 */
	public String getExtractObjects()
	{
		return extractObjects;
	}

	/**
	 * @param extractObjects the extractObjects to set
	 */
	public void setExtractObjects(String extractObjects)
	{
		this.extractObjects = extractObjects;
	}

	/**
	 * @return the customMapping
	 */
	public String getCustomMapping()
	{
		return customMapping;
	}
 
	/**
	 * @param customMapping the customMapping to set
	 */
	public void setCustomMapping(String customMapping)
	{
		this.customMapping = customMapping;
	}

	/**
	 * @return the caseSensitiveTabColName
	 */
	public String getCaseSensitiveTabColName()
	{
        if (caseSensitiveTabColName == null || caseSensitiveTabColName.length() == 0 || caseSensitiveTabColName.equalsIgnoreCase("null"))
        {        	
        	if (srcVendor != null && srcVendor.equalsIgnoreCase("sybase") && db2Compatibility)
	            caseSensitiveTabColName = "true";
        	else
        		caseSensitiveTabColName = "false";
        }
		return caseSensitiveTabColName;
	}

	/**
	 * @param caseSensitiveTabColName the caseSensitiveTabColName to set
	 */
	public void setCaseSensitiveTabColName(String caseSensitiveTabColName)
	{
        if (caseSensitiveTabColName == null || caseSensitiveTabColName.length() == 0 || caseSensitiveTabColName.equalsIgnoreCase("null"))
        {        	
        	if (srcVendor != null && srcVendor.equalsIgnoreCase("sybase") && db2Compatibility)
	            caseSensitiveTabColName = "true";
        }		
		this.caseSensitiveTabColName = caseSensitiveTabColName;
	}

	/**
	 * @return the mapCharToVarchar
	 */
	public String getMapCharToVarchar()
	{
		return (mapCharToVarchar == null || mapCharToVarchar.equals("null")) ? "false" : mapCharToVarchar;
	}

	/**
	 * @param mapCharToVarchar the mapCharToVarchar to set
	 */
	public void setMapCharToVarchar(String mapCharToVarchar)
	{
		this.mapCharToVarchar = mapCharToVarchar;
	}

	/**
	 * @return the dstDB2FixPack
	 */
	public String getDstDB2FixPack()
	{
		if (dstDB2FixPack == null || dstDB2FixPack.length() == 0 || dstDB2FixPack.equals("null"))
			dstDB2FixPack = "1";				
		return dstDB2FixPack;
	}

	/**
	 * @param dstDB2FixPack the dstDB2FixPack to set
	 */
	public void setDstDB2FixPack(String dstDB2FixPack)
	{
		this.dstDB2FixPack = dstDB2FixPack;
	}

	/**
	 * @return the sybaseConvClass
	 */
	public String getSybaseConvClass()
	{
		String convClass = 	"com.sybase.jdbc3.utils.TruncationConverter";
		sybaseConvClass = IBMExtractUtilities.fixSybaseConverterClassName(convClass);
		return sybaseConvClass;
	}


	/**
	 * @return the zHLQName
	 */
	public String getZHLQName()
	{
		return zHLQName;
	}

	/**
	 * @param name the zHLQName to set
	 */
	public void setZHLQName(String name)
	{
		zHLQName = name;
	}

	/**
	 * @return the shell
	 */
	public String getShell()
	{
		if (shell == null || shell.equalsIgnoreCase("null"))
		{			
		    shell = IBMExtractUtilities.getShell();
		}
		return shell;
	}

	/**
	 * @param shell the shell to set
	 */
	public void setShell(String shell)
	{
		if (shell == null || shell.equalsIgnoreCase("null"))
		{
			if (Constants.win())
				this.shell = "cmd";
			else
				this.shell = "/bin/bash";
		} else
		   this.shell = shell;
	}

	/**
	 * @return the usePipe
	 */
	public String getUsePipe()
	{
		if (usePipe == null || usePipe.equalsIgnoreCase("null"))
			usePipe = "false";
		return usePipe;
	}

	/**
	 * @param usePipe the usePipe to set
	 */
	public void setUsePipe(String usePipe)
	{
		if (usePipe == null || usePipe.equalsIgnoreCase("null"))
			usePipe = "false";
		this.usePipe = usePipe;
	}

	/**
	 * @return the extentSizeinBytes
	 */
	public String getExtentSizeinBytes()
	{
		return (extentSizeinBytes == null || extentSizeinBytes.equalsIgnoreCase("null")) ? "36000" : extentSizeinBytes;
	}

	/**
	 * @param extentSizeinBytes the extentSizeinBytes to set
	 */
	public void setExtentSizeinBytes(String extentSizeinBytes)
	{
		this.extentSizeinBytes = extentSizeinBytes;
	}

	/**
	 * @return the validObjects
	 */
	public String getValidObjects()
	{
		return validObjects;
	}

	/**
	 * @param validObjects the validObjects to set
	 */
	public void setValidObjects(String validObjects)
	{
		this.validObjects = validObjects;
	}

	/**
	 * @return the oracleNumberMapping
	 */
	public String getOracleNumberMapping()
	{
		return oracleNumberMapping;
	}

	/**
	 * @param oracleNumberMapping the oracleNumberMapping to set
	 */
	public void setOracleNumberMapping(String oracleNumberMapping)
	{
		oracleNumberMapping = oracleNumberMapping;
	}

	/**
	 * @return the numWorkDir
	 */
	public int getNumJVM()
	{
		return numWorkDir;
	}

	/**
	 * @param numWorkDir the numWorkDir to set
	 */
	public void setNumJVM(String numWorkDir)
	{
		try
		{
		   this.numWorkDir = Integer.valueOf(numWorkDir);
		} catch (Exception e)
		{
			this.numWorkDir = 1;
		}
	}

	/**
	 * @return the numThreads
	 */
	public int getNumThreads()
	{		
		return numThreads;
	}

	/**
	 * @param numThreads the numThreads to set
	 */
	public void setNumThreads(String numThreads)
	{
		try
		{
		   this.numThreads = Integer.valueOf(numThreads);
		} catch (Exception e)
		{
			this.numThreads = 5;
		}
	}

	/**
	 * @return the encoding
	 */
	public String getEncoding()
	{
		if (encoding == null || encoding.length() == 0 || encoding.equalsIgnoreCase("null"))
			encoding = "UTF-8";
		return encoding;
	}

	/**
	 * @param encoding the encoding to set
	 */
	public void setEncoding(String encoding)
	{
		this.encoding = encoding;
	}

	/**
	 * @return the zdb2tableseries
	 */
	public String getZdb2tableseries()
	{
		return zdb2tableseries;
	}

	/**
	 * @return the znocopypend
	 */
	public String getZnocopypend()
	{
		return znocopypend;
	}

	/**
	 * @return the zoveralloc
	 */
	public String getZoveralloc()
	{
		return zoveralloc;
	}

	/**
	 * @return the zsecondary
	 */
	public String getZsecondary()
	{
		return zsecondary;
	}

	/**
	 * @param zdb2tableseries the zdb2tableseries to set
	 */
	public void setZdb2tableseries(String zdb2tableseries)
	{
		this.zdb2tableseries = zdb2tableseries;
	}

	/**
	 * @param znocopypend the znocopypend to set
	 */
	public void setZnocopypend(String znocopypend)
	{
		this.znocopypend = znocopypend;
	}

	/**
	 * @param zoveralloc the zoveralloc to set
	 */
	public void setZoveralloc(String zoveralloc)
	{
		this.zoveralloc = zoveralloc;
	}

	/**
	 * @param zsecondary the zsecondary to set
	 */
	public void setZsecondary(String zsecondary)
	{
		this.zsecondary = zsecondary;
	}

	/**
	 * @return the limitExtractRows
	 */
	public String getLimitExtractRows()
	{
		return limitExtractRows;
	}

	/**
	 * @return the limitLoadRows
	 */
	public String getLimitLoadRows()
	{
		return limitLoadRows;
	}

	/**
	 * @param limitExtractRows the limitExtractRows to set
	 */
	public void setLimitExtractRows(String limitExtractRows)
	{
		this.limitExtractRows = limitExtractRows;
	}

	/**
	 * @param limitLoadRows the limitLoadRows to set
	 */
	public void setLimitLoadRows(String limitLoadRows)
	{
		this.limitLoadRows = limitLoadRows;
	}

	/**
	 * @return the useBestPracticeTSNames
	 */
	public String getUseBestPracticeTSNames()
	{
		return useBestPracticeTSNames;
	}

	/**
	 * @param useBestPracticeTSNames the useBestPracticeTSNames to set
	 */
	public void setUseBestPracticeTSNames(String useBestPracticeTSNames)
	{
		this.useBestPracticeTSNames = useBestPracticeTSNames;
	}

	/**
	 * @return the retainConstraintsName
	 */
	public String getRetainConstraintsName()
	{
		return retainConstraintsName;
	}

	/**
	 * @param retainConstraintsName the retainConstraintsName to set
	 */
	public void setRetainConstraintsName(String retainConstraintsName)
	{
		this.retainConstraintsName = retainConstraintsName;
	}

	/**
	 * @return the extractPartitions
	 */
	public String getExtractPartitions()
	{
		return extractPartitions;
	}

	public String getConvertOracleTimeStampWithTimeZone2Varchar()
	{
		return convertOracleTimeStampWithTimeZone2Varchar;
	}

	public void setConvertOracleTimeStampWithTimeZone2Varchar(
			String convertOracleTimeStampWithTimeZone2Varchar)
	{
		this.convertOracleTimeStampWithTimeZone2Varchar = convertOracleTimeStampWithTimeZone2Varchar;
	}

	public String getConvertTimeStampWithTimeZone2UTCTime()
	{
		return convertTimeStampWithTimeZone2UTCTime;
	}

	public void setConvertTimeStampWithTimeZone2UTCTime(
			String convertTimeStampWithTimeZone2UTCTime)
	{
		this.convertTimeStampWithTimeZone2UTCTime = convertTimeStampWithTimeZone2UTCTime;
	}

	/**
	 * @return the extractHashPartitions
	 */
	public String getExtractHashPartitions()
	{
		return extractHashPartitions;
	}

	/**
	 * @param extractHashPartitions the extractHashPartitions to set
	 */
	public void setExtractHashPartitions(String extractHashPartitions)
	{
		this.extractHashPartitions = extractHashPartitions;
	}

	/**
	 * @param extractPartitions the extractPartitions to set
	 */
	public void setExtractPartitions(String extractPartitions)
	{
		this.extractPartitions = extractPartitions;
	}

	/**
	 * @return the srcDB2Release
	 */
	public String getSrcDB2Release()
	{
		return srcDB2Release;
	}

	/**
	 * @return the dstDBRelease
	 */
	public String getDstDBRelease()
	{
		return dstDBRelease;
	}

	/**
	 * @param srcDB2Release the srcDB2Release to set
	 */
	public void setSrcDB2Release(String srcDB2Release)
	{
		this.srcDB2Release = srcDB2Release;
	}

	/**
	 * @param dstDBRelease the dstDBRelease to set
	 */
	public void setDstDBRelease(String dstDBRelease)
	{
		this.dstDBRelease = dstDBRelease;
	}

	public IBMExtractConfig()
	{
		outputDirectory = getLastConfigDirectory();
        INPUT_DIR = System.getProperty("INPUT_DIR");
        if (INPUT_DIR == null)
            INPUT_DIR = ".";
        if (!INPUT_DIR.equalsIgnoreCase(".")) 
        {
            if (! (INPUT_DIR.endsWith("\\") || INPUT_DIR.endsWith("/")))
              INPUT_DIR += "/";
            //INPUT_DIR += "input";
        }
        //log("INPUT Directory = " + INPUT_DIR);
        new File(INPUT_DIR).mkdirs();
        if (Constants.win()) 
        {
           geninput = "geninput.cmd";
           genddl = "genddl.cmd";
           unload = "unload.cmd";
           rowcount = "rowcount.cmd";
           rowexceptcount = "rowexceptcount.cmd";
           fixcheck = "fixcheck.cmd";
           zLoadScript = "zloadscript.cmd";
           autoFixScript = "AutoFix.cmd";
           datacheck = "DataCheck.cmd";
        } else
        {
            geninput = "geninput";
            genddl = "genddl";
            unload = "unload";
            rowcount = "rowcount"; 
            rowexceptcount = "rowexceptcount";
            fixcheck = "fixcheck";
            zLoadScript = "zloadscript";
            autoFixScript = "AutoFix";
            datacheck = "datacheck";
        }          
	}
	
	/**
	 * @return the dbclob
	 */
	public String getDbclob()
	{
		return (dbclob == null || dbclob.equalsIgnoreCase("null")) ? "false" : dbclob;
	}

	/**
	 * @return the graphic
	 */
	public String getGraphic()
	{
		return (graphic == null || graphic.equalsIgnoreCase("null")) ? "false" : graphic;
	}

	/**
	 * @return the trimTrailingSpaces
	 */
	public String getTrimTrailingSpaces()
	{
		return trimTrailingSpaces;
	}

	/**
	 * @param dbclob the dbclob to set
	 */
	public void setDbclob(String dbclob)
	{
		this.dbclob = dbclob;
	}

	/**
	 * @param graphic the graphic to set
	 */
	public void setGraphic(String graphic)
	{
		this.graphic = graphic;
	}

	/**
	 * @param trimTrailingSpaces the trimTrailingSpaces to set
	 */
	public void setTrimTrailingSpaces(String trimTrailingSpaces)
	{
		this.trimTrailingSpaces = trimTrailingSpaces;
	}

	/**
	 * @return the regenerateTriggers
	 */
	public String getRegenerateTriggers()
	{
		return regenerateTriggers;
	}

	/**
	 * @param regenerateTriggers the regenerateTriggers to set
	 */
	public void setRegenerateTriggers(String regenerateTriggers)
	{
		this.regenerateTriggers = regenerateTriggers;
	}

	public String getSrcDB2Instance() {
		return srcDB2Instance;
	}

	public String getDstDB2Instance() {
		if ((dstDB2Instance == null || dstDB2Instance.length() == 0) && isTargetInstalled)
		{
			dstDB2Instance = IBMExtractUtilities.getEnvironmentName("DB2INSTANCE");
		}
		return dstDB2Instance;
	}

	public String getSrcDB2Home() {
		return srcDB2Home;
	}
	
	public String getDstDB2Home() {
		if (dstDB2Home == null || dstDB2Home.length() ==0)
		{
			dstDB2Home = IBMExtractUtilities.getDB2PathName();
		}
		return dstDB2Home;
	}
	
	/**
	 * @return the extractDDL
	 */
	public String getExtractDDL() {
		return extractDDL;
	}

	public String getFetchSize() {
		return fetchSize;
	}

	/**
	 * @param extractDDL the extractDDL to set
	 */
	public void setExtractDDL(String extractDDL) {
		this.extractDDL = extractDDL;
	}

	/**
	 * @return the extractDDL
	 */
	public String getExtractData() {
		return extractData;
	}

	/**
	 * @param extractDDL the extractDDL to set
	 */
	public void setExtractData(String extractData) {
		this.extractData = extractData;
	}

	/**
	 * @return the srcJDBC
	 */
	public String getSrcJDBC() {
		return srcJDBC;
	}

	/**
	 * @return the dstJDBC
	 */
	public String getDstJDBC() {
		return dstJDBC;
	}

	/**
	 * @return the srcVendor
	 */
	public String getSrcVendor() {
		return srcVendor;
	}

	public void setDstVendor(String vendor) 
	{
		Constants.setDbTargetName(vendor);
	}

	public String getDstVendor() {
		return Constants.getDbTargetName();
	}

	/**
	 * @return the srcDBName
	 */
	public String getSrcDBName() {
		return srcDBName;
	}

	/**
	 * @return the dstDBName
	 */
	public String getDstDBName() {
		return dstDBName;
	}

	/**
	 * @return the srcServer
	 */
	public String getSrcServer() {
		return srcServer;
	}

	public String getSrcServerName() {
		if (srcVendor != null && (srcVendor.equalsIgnoreCase("informix") ||
				srcVendor.equalsIgnoreCase("mssql")))
		{
			String serverString;
			if (getInstanceName().length() > 0)
				return srcServer + "," + getInstanceName();
			else
				return srcServer;
		}
		return srcServer;
	}

	/**
	 * @return the dstServer
	 */
	public String getDstServer() {
		return dstServer;
	}

	/**
	 * @return the srcUid
	 */
	public String getSrcUid() {
		return srcUid;
	}

	/**
	 * @return the dstUid
	 */
	public String getDstUid() {
		return dstUid;
	}

	/**
	 * @return the srcPwd
	 */
	public String getSrcPwd() {
		return srcPwd;
	}

	/**
	 * @return the dstPwd
	 */
	public String getDstPwd() {
		return dstPwd;
	}

	/**
	 * @return the srcPort
	 */
	public int getSrcPort() {
		return srcPort;
	}

	/**
	 * @return the dstPort
	 */
	public int getDstPort() {
		return dstPort;
	}

	/**
	 * @return the srcSchName
	 */
	public String getSrcSchName() {
		return srcSchName;
	}

	public String getDstSchName() 
	{
		if (dstSchName == null || dstSchName.length() == 0)
		{
			dstSchName = srcSchName;
		}
		String[] tmp1 = srcSchName.split(":");
		String[] tmp2 = dstSchName.split(":");
    	if (tmp1.length != tmp2.length)
    	{
    		log("# of dstSchemaName does not match with # of srcSchemaName in "+IBMExtractUtilities.getConfigFile()+". Resetting dstSchemaName to srcSchname");
			dstSchName = srcSchName;
    	}
		return dstSchName;
	}

	/**
	 * @return the javaHome
	 */
	public String getJavaHome() {
		if (javaHome == null || javaHome.length() == 0 || javaHome.equalsIgnoreCase("null"))
		{
			if (isTargetInstalled)
			    return  IBMExtractUtilities.db2JavaPath();
			else
				return System.getProperty("java.home");
		}
		else
			return javaHome;
	}

	public void setJavaHome(String javaHome) {
		this.javaHome = javaHome;
	}

	/**
	 * @return the remoteLoad
	 */
	public String getRemoteLoad()
	{
		return remoteLoad;
	}

	/**
	 * @return the compressTable
	 */
	public String getCompressTable()
	{
		return compressTable;
	}

	/**
	 * @return the compressIndex
	 */
	public String getCompressIndex()
	{
		return compressIndex;
	}

	/**
	 * @param remoteLoad the remoteLoad to set
	 */
	public void setRemoteLoad(String remoteLoad)
	{
		this.remoteLoad = remoteLoad;
	}

	/**
	 * @param compressTable the compressTable to set
	 */
	public void setCompressTable(String compressTable)
	{
		this.compressTable = compressTable;
	}

	/**
	 * @param compressIndex the compressIndex to set
	 */
	public void setCompressIndex(String compressIndex)
	{
		this.compressIndex = compressIndex;
	}

	/**
	 * @return the currHome
	 */
	public String getCurrHome() {
		return currHome;
	}

	/**
	 * @return the appJAR
	 */
	public String getAppJAR() {
		return appJAR;
	}

	/**
	 * @return the db_compatibility
	 */
	public String getOutputDirectory() 
	{
		return outputDirectory;
	}

	public boolean isOutputDirectorySameAsLoadDirectory()
	{
		String output = getOutputDirectory();
		String load = getLoadDirectory();
		if (load.length() == 0)
			return true;
		if (load.endsWith(filesep))
			load = load.substring(0,load.length()-1);
		if (load.equals(output))
			return true;
		else
			return false;
	}
	
	public String getDSwitch()
	{
		String idmtConfigFile = "-DAppHome="+IBMExtractUtilities.FormatENVString(IBMExtractUtilities.getHomeDir()) + " -DIDMTConfigFile=" + IBMExtractUtilities.FormatENVString(IBMExtractUtilities.getConfigFile());
		if (Constants.win())
		    return idmtConfigFile + " -Djava.library.path="+IBMExtractUtilities.FormatENVString(currHome);
		else
			return idmtConfigFile;
	}
	
	public boolean isDataExtracted()
	{
		String scriptName = "";
		boolean bool = false;
		if (outputDirectory == null || outputDirectory.equals(""))
		{
			bool = false;
		} else
		{
		    scriptName = outputDirectory + System.getProperty("file.separator") + 
	           getDB2RutimeShellScriptName();
		    if (IBMExtractUtilities.FileExists(scriptName))
		    	bool = true;
		}
		return bool;
	}
	
	public String getDB2RutimeShellScriptName()
	{
		String scriptName = "";
		if (extractDDL.equals("true") && extractData.equals("true"))
			scriptName = Constants.genScript;
		else if (!extractDDL.equals("true") && extractData.equals("true"))
			scriptName = Constants.loadScript;
		else if (extractDDL.equals("true") && !extractData.equals("true"))
			scriptName = Constants.ddlScript;
		return scriptName;
	}
	
	public String getMeetScriptName()
	{
		if (Constants.win())
		{
			meet = (srcVendor.equalsIgnoreCase("mssql")) ? "GenInputForMTK.cmd" : "GenInputForMEET.cmd";
		} else
		{
            meet = (srcVendor.equalsIgnoreCase("mssql")) ? "GenInputForMTK" : "GenInputForMEET";
		}
		return meet;				
	}
	
	public String getAutoFixScriptName()
	{
		return autoFixScript;
	}
	
	public void setFetchSize(String fetchSize)
	{
		this.fetchSize = fetchSize;
	}
	
	public void setOutputDirectory(String outputDirectory) 
	{
		this.outputDirectory = outputDirectory;
		writeLastUsedConfigFile();
	}

	/**
	 * @param srcJDBC the srcJDBC to set
	 */
	public void setSrcJDBC(String srcJDBC) 
	{
		this.srcJDBC = srcJDBC;
	}

	/**
	 * @param dstJDBC the dstJDBC to set
	 */
	public void setDstJDBC(String dstJDBC) 
	{
		this.dstJDBC = dstJDBC;
	}

	/**
	 * @param srcVendor the srcVendor to set
	 */
	public void setSrcVendor(String srcVendor) 
	{
		this.srcVendor = srcVendor;
	}

	/**
	 * @param srcDBName the srcDBName to set
	 */
	public void setSrcDBName(String srcDBName) 
	{
		this.srcDBName = srcDBName;
	}

	/**
	 * @param dstDBName the dstDBName to set
	 */
	public void setDstDBName(String dstDBName) 
	{
		this.dstDBName = dstDBName;
	}

	/**
	 * @param srcServer the srcServer to set
	 */
	public void setSrcServer(String srcServer) 
	{
		this.srcServer = srcServer;
	}

	/**
	 * @param dstServer the dstServer to set
	 */
	public void setDstServer(String dstServer) 
	{
		this.dstServer = dstServer;
	}

	/**
	 * @param srcUid the srcUid to set
	 */
	public void setSrcUid(String srcUid) 
	{
		this.srcUid = srcUid;
	}

	/**
	 * @param dstUid the dstUid to set
	 */
	public void setDstUid(String dstUid) 
	{
		this.dstUid = dstUid;
	}

	/**
	 * @param srcPwd the srcPwd to set
	 */
	public void setSrcPwd(String srcPwd) 
	{
		this.srcPwd = srcPwd;
	}

	/**
	 * @param dstPwd the dstPwd to set
	 */
	public void setDstPwd(String dstPwd) 
	{
		this.dstPwd = dstPwd;
	}

	/**
	 * @param srcPort the srcPort to set
	 */
	public void setSrcPort(String srcPort) 
	{
		this.srcPort = Integer.parseInt(srcPort);
	}

	/**
	 * @param dstPort the dstPort to set
	 */
	public void setDstPort(String dstPort) 
	{
		try
		{
		    this.dstPort = Integer.parseInt(dstPort);
		} catch (Exception e)
		{
			;
		}
	}

	public void setSrcSchName(String[] srcSchName) 
	{
		String str = "";
		for (int i = 0; i < srcSchName.length; ++i)
		{
			if (i > 0)
				str += ":";
			str += srcSchName[i];
		}
		this.srcSchName = str;
	}

	public void setSrcSchName(String srcSchName) 
	{
		this.srcSchName = srcSchName;
	}

	public void setDstSchName(String[] dstSchName) 
	{
		String str = "";
		for (int i = 0; i < dstSchName.length; ++i)
		{
			if (i > 0)
				str += ":";
			str += dstSchName[i];
		}
		this.dstSchName = str;
	}

	public void setDstSchName(String dstSchemaName) 
	{
		this.dstSchName = dstSchemaName;
	}

	/**
	 * @param currHome the currHome to set
	 */
	public void setCurrHome(String currHome) 
	{
		this.currHome = currHome;
	}

	/**
	 * @param appJAR the appJAR to set
	 */
	public void setAppJAR(String appJAR) 
	{
		this.appJAR = appJAR;
	}
	
	public void setSrcDB2Home(String srcDB2Home) 
	{
		this.srcDB2Home = srcDB2Home;
	}

	public void setDstDB2Home(String dstDB2Home) 
	{
		this.dstDB2Home = dstDB2Home;
	}

	public void setSrcDB2Instance(String srcDB2Instance) 
	{
		this.srcDB2Instance = srcDB2Instance;
	}

	public void setDstDB2Instance(String dstDB2Instance) 
	{
		this.dstDB2Instance = dstDB2Instance;
	}

	public String writeAutoFixScript(String outputDir, String inputFileName, String outputFileName) throws IOException
	{
		String tmpFileName;
		if (outputDir == null || outputDir.trim().length() == 0)
		{
		   new File(outputDirectory).mkdirs();
		   tmpFileName = outputDirectory + filesep + autoFixScript;
		} else
		{
			new File(outputDir).mkdirs();
			tmpFileName = outputDir + filesep + autoFixScript;			
		}
        StringBuffer buffer = new StringBuffer(); 
        String memArg = IBMExtractUtilities.getVMMemoryParam();
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: You can run this script to fix SQL script for DB2."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Fix Script for DB2" + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+linesep+linesep);
            buffer.append("SET INPUTFILENAME="+IBMExtractUtilities.putQuote(inputFileName,sep)+linesep);
            buffer.append("SET OUTPUTFILENAME="+IBMExtractUtilities.putQuote(outputFileName,sep)+linesep);
            buffer.append("SET EXTRAPARAM=-MODIFIER=#IDMT -DONT_MODIFY_COMM -SQLPLUS_MODE -FP="+getDstDB2FixPack()+linesep+linesep);

            buffer.append("\"%JAVA_HOME%\\bin\\java\" "+memArg+" -cp %CLASSPATH% ibm.TinyMig %INPUTFILENAME% %OUTPUTFILENAME% %EXTRAPARAM%"+linesep);
    		genWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# You can run this script to fix SQL script for DB2."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Fix Script for DB2" + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+linesep+linesep);
            buffer.append("INPUTFILENAME="+IBMExtractUtilities.putQuote(inputFileName,sep)+linesep);
            buffer.append("OUTPUTFILENAME="+IBMExtractUtilities.putQuote(outputFileName,sep)+linesep);
            buffer.append("EXTRAPARAM=-MODIFIER=#IDMT -DONT_MODIFY_COMM -SQLPLUS_MODE -FP="+getDstDB2FixPack()+linesep+linesep);

            buffer.append("$JAVA_HOME/bin/java "+memArg+" -cp \"$CLASSPATH\" ibm.TinyMig $INPUTFILENAME $OUTPUTFILENAME $EXTRAPARAM"+linesep);        	
            Runtime rt= Runtime.getRuntime();
            genWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        genWriter.write(buffer.toString());
        genWriter.close();
        log("Command File " + tmpFileName + " created.");	
        return tmpFileName;
	}
	
	public void writeMeetScript(String outputDirectory, String dbSource) throws IOException
	{
		String tool = (dbSource.equalsIgnoreCase("mssql")) ? "MTK" : "MEET";
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + getMeetScriptName();
        StringBuffer buffer = new StringBuffer(); 
        String memArg = IBMExtractUtilities.getVMMemoryParam();
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: You can run this script to generate an input file for "+tool+"."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Generate input for "+tool+" Tool" + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);

            buffer.append("SET SRCSCHEMA="+IBMExtractUtilities.FormatENVString(getSelectSchemaName())+linesep);
            buffer.append("SET SOURCENAME="+IBMExtractUtilities.FormatENVString(dbSource)+linesep);
            buffer.append("SET SERVER="+IBMExtractUtilities.FormatENVString(srcServer)+linesep);
            buffer.append("SET DATABASE="+IBMExtractUtilities.FormatENVString(srcDBName)+linesep);
            buffer.append("SET PORT="+srcPort+linesep);
            buffer.append("SET DBUID="+IBMExtractUtilities.FormatENVString(srcUid)+linesep);
            buffer.append("REM Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("SET DBPWD="+IBMExtractUtilities.Encrypt(srcPwd)+linesep);
            buffer.append("SET VALIDOBJECTS="+validObjects+linesep+linesep);
            if (srcVendor.equalsIgnoreCase("mssql"))
                buffer.append("SET MTKHOME="+getMtkHome()+linesep+linesep);

            if (srcVendor.equalsIgnoreCase("mssql"))
                buffer.append("\"%JAVA_HOME%\\bin\\java\" "+memArg+" -DOUTPUT_DIR="+IBMExtractUtilities.FormatENVString(outputDirectory)+" -cp %CLASSPATH% ibm.GenerateMeet %SOURCENAME% %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD% %SRCSCHEMA% %VALIDOBJECTS% %MTKHOME%"+linesep);
            else
               buffer.append("\"%JAVA_HOME%\\bin\\java\" "+memArg+" -DOUTPUT_DIR="+IBMExtractUtilities.FormatENVString(outputDirectory)+" -cp %CLASSPATH% ibm.GenerateMeet %SOURCENAME% %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD% %SRCSCHEMA% %VALIDOBJECTS%"+linesep);
    		genWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# You can run this script to generate an input file for "+tool+"."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Generate input for "+tool+" Tool" + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);

            buffer.append("SRCSCHEMA="+IBMExtractUtilities.FormatENVString(getSelectSchemaName())+linesep);
            buffer.append("SOURCENAME="+IBMExtractUtilities.FormatENVString(dbSource)+linesep);
            buffer.append("SERVER="+IBMExtractUtilities.FormatENVString(srcServer)+linesep);
            buffer.append("DATABASE="+IBMExtractUtilities.FormatENVString(srcDBName)+linesep);
            buffer.append("PORT="+srcPort+linesep);
            buffer.append("DBUID="+IBMExtractUtilities.FormatENVString(srcUid)+linesep);
            buffer.append("### Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("DBPWD="+IBMExtractUtilities.Encrypt(srcPwd)+linesep);
            buffer.append("VALIDOBJECTS="+validObjects+linesep+linesep);
            if (srcVendor.equalsIgnoreCase("mssql"))
                buffer.append("MTKHOME="+getMtkHome()+linesep+linesep);

            if (srcVendor.equalsIgnoreCase("mssql"))
                buffer.append("$JAVA_HOME/bin/java "+memArg+" -DOUTPUT_DIR="+IBMExtractUtilities.FormatENVString(outputDirectory)+" -cp \"$CLASSPATH\" ibm.GenerateMeet $SOURCENAME $SERVER $DATABASE $PORT \"$DBUID\" $DBPWD $SRCSCHEMA $VALIDOBJECTS $MTKHOME"+linesep);        	
            else
                buffer.append("$JAVA_HOME/bin/java "+memArg+" -DOUTPUT_DIR="+IBMExtractUtilities.FormatENVString(outputDirectory)+" -cp \"$CLASSPATH\" ibm.GenerateMeet $SOURCENAME $SERVER $DATABASE $PORT \"$DBUID\" $DBPWD $SRCSCHEMA $VALIDOBJECTS"+linesep);        	
            Runtime rt= Runtime.getRuntime();
            genWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        genWriter.write(buffer.toString());
        genWriter.close();
        log("Command File " + tmpFileName + " created.");
	}
		
	public void writeGeninput() throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + geninput;
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: You can run this script to overwrite tables script, which is an input to the unload."+linesep);
            buffer.append(":: Normally, you will not require to run this script since tables file is already created."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);

            buffer.append("SET DBVENDOR="+IBMExtractUtilities.FormatENVString(srcVendor)+linesep);
            buffer.append("SET DSTSCHEMA="+IBMExtractUtilities.FormatENVString(getDstSchName())+linesep);
            buffer.append("SET SRCSCHEMA="+IBMExtractUtilities.FormatENVString(srcSchName)+linesep);
            buffer.append("SET SELECTEDSCHEMA="+IBMExtractUtilities.FormatENVString(getSelectSchemaName())+linesep);
            buffer.append("SET SERVER="+IBMExtractUtilities.FormatENVString(getSrcServerName())+linesep);
            buffer.append("SET DATABASE="+IBMExtractUtilities.FormatENVString(srcDBName)+linesep);
            buffer.append("SET PORT="+srcPort+linesep);
            buffer.append("SET DBUID="+IBMExtractUtilities.FormatENVString(srcUid)+linesep);
            buffer.append("REM Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("SET DBPWD="+IBMExtractUtilities.Encrypt(srcPwd)+linesep);
            buffer.append("SET COMPATIBILITY="+String.valueOf(db2Compatibility)+linesep);
            
            buffer.append("SET CASESENSITIVITY="+IBMExtractUtilities.FormatENVString(getCaseSensitiveTabColName())+linesep+linesep);

            buffer.append("\"%JAVA_HOME%\\bin\\java\" -DINPUT_DIR=. "+getDSwitch()+" -cp %CLASSPATH% ibm.GenInput %DBVENDOR% %DSTSCHEMA% %SRCSCHEMA% %SELECTEDSCHEMA% %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD% %COMPATIBILITY% %CASESENSITIVITY%"+linesep);
    		genInputWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# You can run this script to overwrite tables script, which is an input to the unload."+linesep);
            buffer.append("# Normally, you will not require to run this script since tables file is already created."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);

            buffer.append("DBVENDOR="+IBMExtractUtilities.FormatENVString(srcVendor)+linesep);
            buffer.append("DSTSCHEMA="+IBMExtractUtilities.FormatENVString(getDstSchName())+linesep);
            buffer.append("SRCSCHEMA="+IBMExtractUtilities.FormatENVString(srcSchName)+linesep);
            buffer.append("SELECTEDSCHEMA="+IBMExtractUtilities.FormatENVString(getSelectSchemaName())+linesep);
            buffer.append("SERVER="+IBMExtractUtilities.FormatENVString(getSrcServerName())+linesep);
            buffer.append("DATABASE="+IBMExtractUtilities.FormatENVString(srcDBName)+linesep);
            buffer.append("PORT="+srcPort+linesep);
            buffer.append("DBUID="+IBMExtractUtilities.FormatENVString(srcUid)+linesep);
            buffer.append("### Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("DBPWD="+IBMExtractUtilities.Encrypt(srcPwd)+linesep);
            buffer.append("COMPATIBILITY="+String.valueOf(db2Compatibility)+linesep+linesep);
            buffer.append("CASESENSITIVITY="+IBMExtractUtilities.FormatENVString(getCaseSensitiveTabColName())+linesep);

            buffer.append("$JAVA_HOME/bin/java -DINPUT_DIR=. "+getDSwitch()+" -cp \"$CLASSPATH\" ibm.GenInput $DBVENDOR $DSTSCHEMA $SRCSCHEMA $SELECTEDSCHEMA \"$SERVER\" $DATABASE $PORT \"$DBUID\" $DBPWD $COMPATIBILITY $CASESENSITIVITY"+linesep);        	
            Runtime rt= Runtime.getRuntime();
    		genInputWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        genInputWriter.write(buffer.toString());
        genInputWriter.close();
        log("Command File " + tmpFileName + " created.");
	}
	
	public void writeUnload(String outputDirectory, String fileName, int initiatorID) throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + fileName;
        StringBuffer buffer = new StringBuffer(); 
        String tableFileName = outputDirectory; 
        String memArg = IBMExtractUtilities.getVMMemoryParam();
        buffer.setLength(0);
        if (srcVendor.equals("mysql"))
           fetchSize = "0";
        tableFileName = outputDirectory + filesep + srcDBName + ".tables"; 
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: This script is the heart of the IBM Data Movement Tool."+linesep);
            buffer.append(":: This script can be run from GUI or command line."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            
            if (srcJDBC.equals(dstJDBC))
                buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("SET TABLES="+IBMExtractUtilities.FormatENVString(tableFileName)+linesep);	
            buffer.append("SET COLSEP="+(Constants.netezza() ? "\"|\"" : "~")+linesep);
            buffer.append("SET DBVENDOR="+IBMExtractUtilities.FormatENVString(srcVendor)+linesep);
            buffer.append("SET NUM_THREADS="+numThreads+linesep);
            buffer.append("SET SERVER="+IBMExtractUtilities.FormatENVString(getSrcServerName())+linesep);
            buffer.append("SET DATABASE="+IBMExtractUtilities.FormatENVString(srcDBName)+linesep);
            buffer.append("SET PORT="+srcPort+linesep);
            buffer.append("SET DBUID="+IBMExtractUtilities.FormatENVString(srcUid)+linesep);
            buffer.append("REM Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("SET DBPWD="+IBMExtractUtilities.Encrypt(srcPwd)+linesep);
            buffer.append("SET GENDDL="+extractDDL + linesep);
            buffer.append("SET UNLOAD="+extractData + linesep);
            buffer.append("SET OBJECTS="+extractObjects + linesep);
            buffer.append("SET USEPIPE="+usePipe + linesep);
            buffer.append("SET SYNCLOAD="+syncLoad + linesep);
            buffer.append("SET FETCHSIZE="+fetchSize + linesep);
            buffer.append("SET LOADREPLACE=true" + linesep);

            buffer.append("\"%JAVA_HOME%\\bin\\java\" "+memArg+" -DTARGET_DB="+getDstVendor()+" -DOUTPUT_DIR=. "+getDSwitch()+ " -DREEL_NUM="+initiatorID + " -cp %CLASSPATH% ibm.GenerateExtract %TABLES% %COLSEP% %DBVENDOR% %NUM_THREADS% %SERVER% %DATABASE% %PORT% %DBUID% %DBPWD% %GENDDL% %UNLOAD% %OBJECTS% %USEPIPE% %SYNCLOAD% %FETCHSIZE% %LOADREPLACE%" + linesep);
    		unloadWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep);
            buffer.append("# This script is the heart of the IBM Data Movement Tool."+linesep);
            buffer.append("# This script can be run from GUI or command line."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            
            if (srcJDBC.equals(dstJDBC))
            {
                if (Constants.db2())
                    buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR, sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
                else
                	buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR, sep)+":$JZOS_HOME/ibmjzos.jar:"+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            } else
            {
                if (Constants.db2())
                   buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);
                else
                   buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+":$JZOS_HOME/ibmjzos.jar:"+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);
            }

            buffer.append("TABLES="+tableFileName+linesep);
            buffer.append("COLSEP="+(Constants.netezza() ? "\\|" : "\\~")+linesep);
            buffer.append("DBVENDOR="+IBMExtractUtilities.FormatENVString(srcVendor)+linesep);
            buffer.append("NUM_THREADS="+numThreads+linesep);
            buffer.append("SERVER="+IBMExtractUtilities.FormatENVString(getSrcServerName())+linesep);
            buffer.append("DATABASE="+IBMExtractUtilities.FormatENVString(srcDBName)+linesep);
            buffer.append("PORT="+srcPort+linesep);
            buffer.append("DBUID="+IBMExtractUtilities.FormatENVString(srcUid)+linesep);
            buffer.append("### Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("DBPWD="+IBMExtractUtilities.Encrypt(srcPwd)+linesep);
            buffer.append("GENDDL="+extractDDL + linesep);
            buffer.append("UNLOAD="+extractData + linesep);
            buffer.append("OBJECTS="+extractObjects + linesep);
            buffer.append("USEPIPE="+usePipe + linesep);
            buffer.append("SYNCLOAD="+syncLoad + linesep);
            buffer.append("FETCHSIZE="+fetchSize + linesep);
            buffer.append("LOADREPLACE=true" + linesep);

            buffer.append("$JAVA_HOME/bin/java "+memArg+" -DTARGET_DB="+getDstVendor()+" -DOUTPUT_DIR=. "+getDSwitch()+ " -DREEL_NUM="+initiatorID + " -cp \"$CLASSPATH\" ibm.GenerateExtract $TABLES $COLSEP $DBVENDOR $NUM_THREADS \"$SERVER\" $DATABASE $PORT \"$DBUID\" $DBPWD $GENDDL $UNLOAD $OBJECTS $USEPIPE $SYNCLOAD $FETCHSIZE $LOADREPLACE" + linesep);
            Runtime rt= Runtime.getRuntime();
    		unloadWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        unloadWriter.write(buffer.toString());
        unloadWriter.close();
        log("Command file " + tmpFileName + " created.");
	}
	
	public void writezLoad(String outputDirectory) throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + zLoadScript;
        StringBuffer buffer = new StringBuffer();  
        String tableFileName = outputDirectory + filesep + srcDBName + ".tables"; 

        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: Run this script to load data in z/OS DB2."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("SET TABLES="+tableFileName+linesep);
            buffer.append("SET COLSEP=~"+linesep);
            buffer.append("SET NUM_THREADS="+numThreads+linesep);
            buffer.append("SET COMMIT_COUNT="+getCommitCount()+linesep);
            buffer.append("SET LOADREPLACE=true"+linesep);

            buffer.append("\"%JAVA_HOME%\\bin\\java\" -DOUTPUT_DIR=\"" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + "\" " + getDSwitch() + " -cp %CLASSPATH% ibm.DeployObjects " + Constants.dropfkeys +linesep+linesep);
            buffer.append("\"%JAVA_HOME%\\bin\\java\" -DOUTPUT_DIR=\"" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + "\" " + getDSwitch() + " -cp %CLASSPATH% ibm.zOSLoader %TABLES% %COLSEP% %NUM_THREADS% %COMMIT_COUNT% %LOADREPLACE%"+linesep);
            buffer.append("\"%JAVA_HOME%\\bin\\java\" -DOUTPUT_DIR=\"" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + "\" " + getDSwitch() + " -cp %CLASSPATH% ibm.DeployObjects " + Constants.fkeys +linesep+linesep);
    		checkRemovalWriter = new BufferedWriter(new FileWriter(tmpFileName, false));        	
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# Run this script to run SET INTEGRITY COMMANDS to remove CHECK Pending status."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            if (srcJDBC.equals(dstJDBC))
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("TABLES="+tableFileName+linesep);
            buffer.append("COLSEP=\\~"+linesep);
            buffer.append("NUM_THREADS="+numThreads+linesep);
            buffer.append("COMMIT_COUNT="+getCommitCount()+linesep);
            buffer.append("LOADREPLACE=true"+IBMExtractUtilities.FormatENVString(dstDBName)+linesep);

            buffer.append("$JAVA_HOME/bin/java -DOUTPUT_DIR=\"" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + "\" " + getDSwitch() + " -cp \"$CLASSPATH\" ibm.DeployObjects " + Constants.dropfkeys +linesep+linesep);        	
            buffer.append("$JAVA_HOME/bin/java -DOUTPUT_DIR=\"" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + "\" " + getDSwitch() + " -cp \"$CLASSPATH\" ibm.zOSLoader $TABLES $COLSEP $NUM_THREADS $COMMIT_COUNT $LOADREPLACE"+linesep);        	
            buffer.append("$JAVA_HOME/bin/java -DOUTPUT_DIR=\"" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + "\" " + getDSwitch() + " -cp \"$CLASSPATH\" ibm.DeployObjects " + Constants.fkeys +linesep+linesep);        	
            Runtime rt= Runtime.getRuntime();
            checkRemovalWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }		
        checkRemovalWriter.write(buffer.toString());
        checkRemovalWriter.close();
        log("Command file " + tmpFileName + " created.");
	}
	
	public void writeFixCheck(String outputDirectory) throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + fixcheck;
        StringBuffer buffer = new StringBuffer();  
        String tableFileName = outputDirectory + filesep + srcDBName + ".tables"; 

        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: Run this script to run SET INTEGRITY COMMANDS to remove CHECK Pending status."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("SET CASESENSITIVE="+getCaseSensitiveTabColName()+linesep);
            buffer.append("SET TABLES="+IBMExtractUtilities.FormatENVString(tableFileName)+linesep);
            buffer.append("SET DST_VENDOR="+getDstVendor()+linesep);
            buffer.append("SET DST_SERVER="+IBMExtractUtilities.FormatENVString(dstServer)+linesep);
            buffer.append("SET DST_DATABASE="+IBMExtractUtilities.FormatENVString(dstDBName)+linesep);
            buffer.append("SET DST_PORT="+dstPort+linesep);
            buffer.append("SET DST_UID="+IBMExtractUtilities.FormatENVString(dstUid)+linesep);
            buffer.append("REM Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("SET DST_PWD="+IBMExtractUtilities.Encrypt(dstPwd)+linesep+linesep);

            buffer.append("\"%JAVA_HOME%\\bin\\java\" -DOUTPUT_DIR=" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + " " + getDSwitch() + " -cp %CLASSPATH% ibm.FixCheck %TABLES% %CASESENSITIVE% %DST_VENDOR% %DST_SERVER% %DST_DATABASE% %DST_PORT% %DST_UID% %DST_PWD%"+linesep);
    		checkRemovalWriter = new BufferedWriter(new FileWriter(tmpFileName, false));        	
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# Run this script to run SET INTEGRITY COMMANDS to remove CHECK Pending status."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            if (srcJDBC.equals(dstJDBC))
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("CASESENSITIVE="+getCaseSensitiveTabColName()+linesep);
            buffer.append("TABLES="+IBMExtractUtilities.FormatENVString(tableFileName)+linesep);
            buffer.append("DST_VENDOR="+IBMExtractUtilities.FormatENVString(getDstVendor())+linesep);
            buffer.append("DST_SERVER="+IBMExtractUtilities.FormatENVString(dstServer)+linesep);
            buffer.append("DST_DATABASE="+IBMExtractUtilities.FormatENVString(dstDBName)+linesep);
            buffer.append("DST_PORT="+dstPort+linesep);
            buffer.append("DST_UID="+IBMExtractUtilities.escapeUnixChar(IBMExtractUtilities.FormatENVString(dstUid))+linesep);
            buffer.append("### Password was encrypted. You can replace this with clear text password, if required."+linesep);
            buffer.append("DST_PWD="+IBMExtractUtilities.Encrypt(dstPwd)+linesep+linesep);

            buffer.append("$JAVA_HOME/bin/java -DOUTPUT_DIR=" + IBMExtractUtilities.FormatENVString(getOutputDirectory()) + " " + getDSwitch() + " -cp \"$CLASSPATH\" ibm.FixCheck $TABLES $CASESENSITIVE $DST_VENDOR $DST_SERVER $DST_DATABASE $DST_PORT \"$DST_UID\" $DST_PWD"+linesep);        	
            Runtime rt= Runtime.getRuntime();
            checkRemovalWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }		
        checkRemovalWriter.write(buffer.toString());
        checkRemovalWriter.close();
        log("Command file " + tmpFileName + " created.");
	}
	
	public void writeRowCount(String outputDirectory) throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + rowcount;
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: Run this script to count rows from source and target DB server to compare."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            if (srcJDBC.equals(dstJDBC))
                buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("SET TABLES="+srcDBName+".tables"+linesep);
            buffer.append("\"%JAVA_HOME%\\bin\\java\" -Xmx500m -DOUTPUT_DIR=logs "+getDSwitch()+ " -cp %CLASSPATH% ibm.Count %TABLES%"+linesep);
    		rowCountWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# Run this script to count rows from source and target DB server to compare."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            if (srcJDBC.equals(dstJDBC))
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("TABLES="+srcDBName+".tables"+linesep);
            buffer.append("$JAVA_HOME/bin/java -Xmx500m -DOUTPUT_DIR=logs "+getDSwitch()+ " -cp \"$CLASSPATH\" ibm.Count $TABLES"+linesep);        	
            Runtime rt= Runtime.getRuntime();
    		rowCountWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        rowCountWriter.write(buffer.toString());
        rowCountWriter.close();
        log("Command file " + tmpFileName + " created.");
	}

	public void writeExceptRowCount(String outputDirectory) throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + rowexceptcount;
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: Run this script to count rows from exception tables."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("SET TABLES="+srcDBName+".tables"+linesep);
            buffer.append("\"%JAVA_HOME%\\bin\\java\" -Xmx500m -DOUTPUT_DIR=logs "+getDSwitch()+ " -cp %CLASSPATH% ibm.CountExcept %TABLES%"+linesep);
    		rowCountWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# Run this script to count rows from exception tables."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("TABLES="+srcDBName+".tables"+linesep);
            buffer.append("$JAVA_HOME/bin/java -Xmx500m -DOUTPUT_DIR=logs "+getDSwitch()+ " -cp \"$CLASSPATH\" ibm.CountExcept $TABLES"+linesep);        	
            Runtime rt= Runtime.getRuntime();
    		rowCountWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        rowCountWriter.write(buffer.toString());
        rowCountWriter.close();
        log("Command file " + tmpFileName + " created.");
	}

    public void writeDataCheck(String outputDirectory) throws IOException
	{
		new File(outputDirectory).mkdirs();
		String tmpFileName = outputDirectory + filesep + datacheck;
        StringBuffer buffer = new StringBuffer();  
        buffer.setLength(0);
        if (Constants.win())
        {
            buffer.append(":: Copyright(r) IBM Corporation"+linesep);
            buffer.append("::"+linesep+linesep);
            buffer.append(":: Run this script to check data from source and target DB server to compare."+linesep);
            buffer.append("::"+linesep+linesep);

            buffer.append("@echo off"+linesep);
            buffer.append("cls"+linesep+linesep);

            buffer.append("ECHO Executed by: %USERNAME% Machine: %COMPUTERNAME% On %OS% %DATE% %TIME%"+linesep); 
            buffer.append("ECHO."+linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("ECHO -------------------------------------------------------------------"+linesep);
            buffer.append("ECHO."+linesep+linesep);
            
            
            buffer.append("SET JAVA_HOME="+getJavaHome()+linesep);
            if (srcJDBC.equals(dstJDBC))
                buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("SET CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("SET TABLES="+srcDBName+".tables"+linesep);
            buffer.append("\"%JAVA_HOME%\\bin\\java\" -Xmx500m -DOUTPUT_DIR=logs "+getDSwitch()+ " -cp %CLASSPATH% ibm.DataCheck %TABLES% "+ getNumRowsToCheck() + linesep);
            dataCheckWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
        } else
        {
            buffer.append("#!"+getShell()+linesep+linesep);
            buffer.append("# Copyright(r) IBM Corporation"+linesep);
            buffer.append("#"+linesep+linesep);
            buffer.append("# Run this script to check data from source and target DB server to compare."+linesep);
            buffer.append("#"+linesep+linesep);

            buffer.append("echo -------------------------------------------------------------------"+linesep);
            buffer.append("echo Migration from "+srcVendor+" to " + getDstVendor() + linesep);
            buffer.append("echo -------------------------------------------------------------------"+linesep);
            
            buffer.append("JAVA_HOME="+getJavaHome()+linesep);
            if (srcJDBC.equals(dstJDBC))
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+linesep+linesep);
            else
                buffer.append("CLASSPATH="+IBMExtractUtilities.putQuote(appJAR,sep)+sep+IBMExtractUtilities.putQuote(srcJDBC,sep)+sep+IBMExtractUtilities.putQuote(dstJDBC,sep)+linesep+linesep);

            buffer.append("TABLES="+srcDBName+".tables"+linesep);
            buffer.append("$JAVA_HOME/bin/java -Xmx500m -DOUTPUT_DIR=logs "+getDSwitch()+ " -cp \"$CLASSPATH\" ibm.DataCheck $TABLES " + getNumRowsToCheck() + linesep);        	
            Runtime rt= Runtime.getRuntime();
            dataCheckWriter = new BufferedWriter(new FileWriter(tmpFileName, false));
            rt.exec("chmod 755 "+ tmpFileName);
        }
        dataCheckWriter.write(buffer.toString());
        dataCheckWriter.close();
        log("Command file " + tmpFileName + " created.");
	}

	public void getParamValues()
	{
		String tmp;
		appJAR = currHome + File.separator + "IBMDataMovementTool.jar";
        if (paramPropFound)
        {
            javaHome = (String) propParams.get("javaHome"); 
        	tmp = (String) propParams.get("numThreads");
        	if (tmp != null && tmp.length() > 0 && !tmp.equalsIgnoreCase("null"))
               numThreads = Integer.parseInt(tmp);
        	tmp = (String) propParams.get("numWorkDir");
        	if (tmp != null && tmp.length() > 0 && !tmp.equalsIgnoreCase("null"))
               numWorkDir = Integer.parseInt(tmp);
            srcJDBC = (String) propParams.get("srcJDBC");
            dstJDBC = (String) propParams.get("dstJDBC");
	        srcVendor = (String) propParams.get("srcVendor"); 
	        srcServer = (String) propParams.get("srcServer"); 
	        srcPort = Integer.parseInt((String) propParams.get("srcPort")); 
	        srcDBName = (String) propParams.get("srcDBName"); 
	        srcSchName = (String) propParams.get("srcSchName"); 
	        srcUid = (String) propParams.get("srcUid"); 
	        srcPwd = (String) propParams.get("srcPwd");
	        srcPwd = IBMExtractUtilities.Decrypt(srcPwd);
	        Constants.setDbTargetName((String) propParams.get("dstVendor"));
	        db2Compatibility = Constants.db2Compatibility();
	        dstServer = (String) propParams.get("dstServer"); 
	        dstPort = Integer.parseInt((String) propParams.get("dstPort")); 
	        dstDBName = (String) propParams.get("dstDBName"); 
	        dstSchName = (String) propParams.get("dstSchName"); 
	        if (dstSchName == null || dstSchName.length() == 0 || dstSchName.equals("null"))
	        	dstSchName = srcSchName;
	        selectSchemaName = (String) propParams.get("selectSchemaName");
	        if (selectSchemaName == null || selectSchemaName.length() == 0 || selectSchemaName.equalsIgnoreCase("null"))
	        	selectSchemaName = srcSchName;
	        dstUid = (String) propParams.get("dstUid"); 
	        dstPwd = (String) propParams.get("dstPwd"); 
	        dstPwd = IBMExtractUtilities.Decrypt(dstPwd);
	        extractDDL = (String) propParams.get("extractDDL");
	        extractData = (String) propParams.get("extractData");
	        extractObjects = (String) propParams.get("extractObjects");
	        if (extractObjects == null || extractObjects.length() == 0 || extractObjects.equals("null"))
	        	extractObjects = "true";
	        srcDB2Home = (String) propParams.get("srcDB2Home");
	        dstDB2Home = (String) propParams.get("dstDB2Home");
	        srcDB2Instance = (String) propParams.get("srcDB2Instance");
	        dstDB2Instance = (String) propParams.get("dstDB2Instance");
	        dstDB2FixPack = (String) propParams.getProperty("dstDB2FixPack");
	        srcDB2Release =  (String) propParams.get("srcDB2Release");
	        dstDBRelease =  (String) propParams.get("dstDBRelease");
	        regenerateTriggers = (String) propParams.get("regenerateTriggers");
	        trimTrailingSpaces = (String) propParams.get("trimTrailingSpaces");
	        DB2Installed = (String) propParams.get("db2Installed");
	        dbclob = (String) propParams.get("dbclob");
	        graphic = (String) propParams.get("graphic");
	        remoteLoad = (String) propParams.get("remoteLoad");
	        loadException = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("loadException"),"true");
	        exceptSchemaSuffix = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("exceptSchemaSuffix"),"_EXP", true);
	        exceptTableSuffix = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("exceptTableSuffix"),"", true);
	        norowwarnings = (String) propParams.getProperty("norowwarnings");
	        compressTable = (String) propParams.get("compressTable");
	        compressIndex = (String) propParams.get("compressIndex");
	        debug = (String) propParams.get("debug");
	        extractPartitions = (String) propParams.get("extractPartitions");
	        mysqlZeroDateToNull = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("mysqlZeroDateToNull"), "false");
	        deleteLoadFile = (String) propParams.get("deleteLoadFile");
	        extractHashPartitions = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("extractHashPartitions"), "false");
	        encoding = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("encoding"), "UTF-8");
	        mapCharToVarchar = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("mapCharToVarchar"), "false");
	        caseSensitiveTabColName = (String) propParams.getProperty("caseSensitiveTabColName");
	        mapTimeToTimestamp = (String) propParams.getProperty("mapTimeToTimestamp");
	        sqlFileEncoding = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("sqlFileEncoding"), "UTF-8");
	        loadStats = (String) propParams.getProperty("loadStats");
	        clusteredIndexes = (String) propParams.getProperty("clusteredIndexes");
	        
	        customMapping = (String) propParams.getProperty("customMapping");
	        if (customMapping == null || (!(customMapping.equalsIgnoreCase("null") || customMapping.equalsIgnoreCase("PE")
	        		|| customMapping.equalsIgnoreCase("CE"))))
	        	customMapping = "false";
	        retainConstraintsName = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("retainConstraintsName"), "false");
	        useBestPracticeTSNames = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("useBestPracticeTSNames"), "true");
	        limitExtractRows = IBMExtractUtilities.getDefaultString((String) propParams.get("limitExtractRows"), "ALL");
	        limitLoadRows = IBMExtractUtilities.getDefaultString((String) propParams.get("limitLoadRows"), "ALL");
	        oracleNumberMapping = IBMExtractUtilities.getDefaultString((String) propParams.get("oracleNumberMapping"), "false");
	        zdb2tableseries = IBMExtractUtilities.getDefaultString((String) propParams.get("zdb2tableseries"), "Q");
	        sybaseConvClass = IBMExtractUtilities.getDefaultString((String) propParams.get("sybaseConvClass"), getSybaseConvClass());
	        sybaseUDTConversionToBaseType = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("sybaseUDTConversionToBaseType"), getSybaseUDTConversionToBaseType());
	        if (Constants.zos())
	        {
		        zHLQName = IBMExtractUtilities.getDefaultString((String) propParams.get("zHLQName"), System.getProperty("user.name").toUpperCase());
		        znocopypend = IBMExtractUtilities.getDefaultString((String) propParams.get("znocopypend"), "true");
		        zoveralloc = IBMExtractUtilities.getDefaultString((String) propParams.get("zoveralloc"), "1.3636");
		        zsecondary = IBMExtractUtilities.getDefaultString((String) propParams.get("zsecondary"), "0");
		        storclas = IBMExtractUtilities.getDefaultString((String) propParams.get("storclas"), "none");
			    jobCard1 = IBMExtractUtilities.getDefaultString((String) propParams.get("jobCard1"), "//DB2CREAT JOB (DB2CREAT),'DB2 CREATE',CLASS=A,");
				jobCard2 = IBMExtractUtilities.getDefaultString((String) propParams.get("jobCard2"), "//     MSGCLASS=A,NOTIFY=&SYSUID");
				db2SubSystemID = IBMExtractUtilities.getDefaultString((String) propParams.get("db2SubSystemID"), "DB2P");
				SDSNLOAD = IBMExtractUtilities.getDefaultString((String) propParams.get("SDSNLOAD"), "DB2.SDSNLOAD");
				RUNLIBLOAD = IBMExtractUtilities.getDefaultString((String) propParams.get("RUNLIBLOAD"), "DB2.RUNLIB.LOAD");
				DSNTEPXX = IBMExtractUtilities.getDefaultString((String) propParams.get("DSNTEPXX"), "DSNTEP91");	
				generateJCLScripts = IBMExtractUtilities.getDefaultString((String) propParams.get("generateJCLScripts"), "true");
	        }
	        validObjects = IBMExtractUtilities.getDefaultString((String) propParams.get("validObjects"), "true");
	        extentSizeinBytes = IBMExtractUtilities.getDefaultString((String) propParams.get("extentSizeinBytes"), "36000");
	        commitCount = IBMExtractUtilities.getDefaultString((String) propParams.get("commitCount"), "100");
	        saveCount = IBMExtractUtilities.getDefaultString((String) propParams.get("saveCount"), "0");
	        warningCount = IBMExtractUtilities.getDefaultString((String) propParams.get("warningCount"), "0");
	        varcharLimit = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("varcharLimit"), "4096");
	        numRowsToCheck = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("numRowsToCheck"), "100"); 
	        batchSizeDisplay = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("batchSizeDisplay"),"0");
	        additionalURLProperties = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("additionalURLProperties"),"null");
	        loadDirectory = (String) propParams.getProperty("loadDirectory");
	        tempFilesPath = (String) propParams.getProperty("tempFilesPath");
	        batchSizeDisplay = getBatchSizeDisplay();
	        usePipe = IBMExtractUtilities.getDefaultString((String) propParams.get("usepipe"), "false");
	        syncLoad = IBMExtractUtilities.getDefaultString((String) propParams.getProperty("syncLoad"), "false");
	        caseSensitiveTabColName = getCaseSensitiveTabColName();
	        mapTimeToTimestamp = getMapTimeToTimestamp();
	        if (srcVendor.equalsIgnoreCase("teradata"))
	           teradataConnStringExtraParam = (String) propParams.getProperty("teradataConnStringExtraParam");
	        if (srcVendor.equalsIgnoreCase("informix"))
	        	instanceName = (String) propParams.getProperty("instanceName");
	        if (srcVendor.equalsIgnoreCase("mssql"))
	        	mtkHome = (String) propParams.getProperty("mtkHome");
	        if (clusteredIndexes == null || clusteredIndexes.length() == 0 || clusteredIndexes.equalsIgnoreCase("null"))
	        {
	        	if (!customMapping.equalsIgnoreCase("false"))
	        		clusteredIndexes = "true";
	        	else
	        		clusteredIndexes = "false";
	        }
        } else
        {
	        Constants.setDbTargetName();
        }
	}
	
	public boolean pingJDBCDrivers(String jdbcList)
	{
		if (jdbcList != null && jdbcList.length() == 0)
			return false;
		
		boolean jdbcFound = true;
		String name;
		
		String[] names = jdbcList.split(sep);
		for (int i = 0; i < names.length; ++i)
		{
			name = names[i];
			if (IBMExtractUtilities.FileExists(name))
			{
				System.out.println("JAR file '" + name + "' found");
			} else
			{
				System.out.println("JAR file '" + name + "' not found.");
				jdbcFound = false;
			}			
		}
		Message = "JDBC Driver JAR files not found.";
		return jdbcFound;
	}
	
	public String getJDBCList(String vendor)
	{
		String[] tmp, tmp2;
		String jdbcList = "", listJDBC;
		listJDBC = (String) propJDBC.get(vendor.toLowerCase());		
		tmp2 = listJDBC.split("\\|\\|");		
		for (int j = 0; j < tmp2.length; ++j)
		{
			tmp = tmp2[j].split(":");
			if (j > 0)
			{
				jdbcList += " OR \n";
			}			
			for (int i = 0; i < tmp.length; ++i)
			{
				if (i < (tmp.length - 1))
				{
					jdbcList += tmp[i] + "\n";
				} else
				{
					jdbcList += tmp[i];
				}
			}
		}
        return jdbcList;
	}
		
	private String getLastConfigDirectory()
	{
		Properties lastKnownConfig = new Properties();
		String value = "", userDir = IBMExtractUtilities.getHomeDir();
		String file = userDir + filesep + Constants.LAST_USED_CONFIG_FILE;
		if (IBMExtractUtilities.FileExists(file))
		{
	        try
			{
	        	FileInputStream inputStream = new FileInputStream(file);
	        	lastKnownConfig.load(inputStream);
	        	inputStream.close();
	        	value = (String) lastKnownConfig.getProperty("LastUsedConfigFileDirectory");
	        	if (value == null || value.length() == 0)
					value = System.getProperty("OUTPUT_DIR") == null ? userDir + filesep + "migr" : System.getProperty("OUTPUT_DIR");
			} catch (Exception e)
			{
				value = System.getProperty("OUTPUT_DIR") == null ? userDir + filesep + "migr" : System.getProperty("OUTPUT_DIR");
			}			
		} else
		{
			value = System.getProperty("OUTPUT_DIR") == null ? userDir + filesep + "migr" : System.getProperty("OUTPUT_DIR");			
		}	
		System.setProperty("OUTPUT_DIR", value);
		new File(value).mkdirs();
		System.setProperty("IDMTConfigFile", value + filesep + Constants.IDMT_CONFIG_FILE);
		return value;
	}
	
	public void loadIDMTConfigFile()
	{
		String fileName;
		FileInputStream finStream;

		paramPropFound = true;
    	String propFile = IBMExtractUtilities.getConfigFile();
    	try 
    	{
    		finStream = new FileInputStream(propFile);
    		propParams.load(finStream); 
    		finStream.close();
    		if (propParams.get("javaHome") == null)
    		{
    			paramPropFound = false;
    		}
    	} catch (Exception e)
    	{
    		try
			{
    			fileName = IBMExtractUtilities.getHomeDir() + filesep + Constants.OLD_IDMT_CONFIG_FILE;
    			finStream = new FileInputStream(fileName);
				propParams.load(finStream);
    			finStream.close();
    			IBMExtractUtilities.deleteFile(fileName);
	    		if (propParams.get("javaHome") == null)
	    		{
	    			paramPropFound = false;
	    		}    		
			} catch (Exception e1)
			{
				paramPropFound = false;
			} 
    	}
	}
	
	public void loadConfigFile()
	{
		FileInputStream finStream;
		InputStream istream;
        propParams = new Properties();
        propJDBC = new Properties();
        
        try
        {
        	istream = ClassLoader.getSystemResourceAsStream(Constants.JDBC_PROP_FILE);
            if (istream == null)
            {
            	try 
            	{
            		finStream = new FileInputStream(Constants.JDBC_PROP_FILE);
            		propJDBC.load(finStream);  
            		finStream.close();
            	} catch (Exception e)
            	{
                    log("exception loading properties: " + e);
                    System.exit(-1);
            	}
            } else
            {
            	propJDBC.load(istream);
            }	
        }
        catch (IOException ex)
        {
            log("exception loading properties: " + ex);
            System.exit(-1);
        }
        loadIDMTConfigFile();
    }
	
	private void writeLastUsedConfigFile()
	{
		try
		{
    		String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			BufferedWriter propWriter = new BufferedWriter(new FileWriter(IBMExtractUtilities.lastUsedPropFile(), false));
			propWriter.write("#### This file was generated on : " + 
		    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
			if (versionInfo != null)
			   propWriter.write("#### IBM Data Movement Tool Version : " + versionInfo + linesep + "####" + linesep);
            propWriter.write("LastUsedConfigFileDirectory=" + IBMExtractUtilities.FormatCMDString(outputDirectory)  + linesep);
            propWriter.close();
            new File(outputDirectory).mkdirs();
            System.setProperty("OUTPUT_DIR", outputDirectory);
    		System.setProperty("IDMTConfigFile", outputDirectory + filesep + Constants.IDMT_CONFIG_FILE);
		} catch (Exception e)
		{
            e.printStackTrace();
            System.exit(-1);
		}
	}
	
	public void writeConfigFile()
	{
		String configFile = IBMExtractUtilities.getConfigFile();
        try
        {
    		String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			BufferedWriter propWriter = new BufferedWriter(new FileWriter(configFile, false));
			propWriter.write("#### This file was generated on : " + 
		    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
			if (versionInfo != null)
			   propWriter.write("#### IBM Data Movement Tool Version : " + versionInfo + linesep + "####" + linesep);
            propWriter.write("appJAR=" + IBMExtractUtilities.FormatCMDString(appJAR) + linesep);
            propWriter.write("javaHome=" + IBMExtractUtilities.FormatCMDString(getJavaHome()) + linesep);            
            propWriter.write("srcVendor=" + srcVendor + linesep); 
            propWriter.write("dstVendor=" + getDstVendor() + linesep); 
            propWriter.write("srcServer=" + IBMExtractUtilities.FormatCMDString(srcServer) + linesep); 
            propWriter.write("dstServer=" + IBMExtractUtilities.FormatCMDString(dstServer) + linesep); 
            propWriter.write("srcDBName=" + IBMExtractUtilities.FormatCMDString(srcDBName) + linesep); 
            propWriter.write("dstDBName=" + IBMExtractUtilities.FormatCMDString(dstDBName) + linesep); 
            propWriter.write("srcUid=" + IBMExtractUtilities.FormatCMDString(srcUid) + linesep); 
            propWriter.write("dstUid=" + IBMExtractUtilities.FormatCMDString(dstUid) + linesep); 
            propWriter.write("srcPwd=" + IBMExtractUtilities.Encrypt(srcPwd) + linesep);
            propWriter.write("dstPwd=" + IBMExtractUtilities.Encrypt(dstPwd) + linesep);
            propWriter.write("srcPort=" + srcPort + linesep); 
            propWriter.write("dstPort=" + dstPort + linesep); 
		    propWriter.write("srcJDBC=" + IBMExtractUtilities.FormatCMDString(srcJDBC) + linesep);
            propWriter.write("dstJDBC="+ IBMExtractUtilities.FormatCMDString(dstJDBC) + linesep);
            propWriter.write("srcSchName=" + IBMExtractUtilities.FormatCMDString(srcSchName) + linesep); 
            propWriter.write("dstSchName=" + IBMExtractUtilities.FormatCMDString(getDstSchName()) + linesep);            
            propWriter.write("selectSchemaName=" + IBMExtractUtilities.FormatCMDString(selectSchemaName) + linesep);
		    propWriter.write("srcDB2Home=" + IBMExtractUtilities.FormatCMDString(srcDB2Home) + linesep);
   		    propWriter.write("dstDB2Home=" + IBMExtractUtilities.FormatCMDString(getDstDB2Home()) + linesep);
   		    propWriter.write("srcDB2Instance=" + IBMExtractUtilities.FormatCMDString(srcDB2Instance) + linesep);
		    propWriter.write("dstDB2Instance=" + IBMExtractUtilities.FormatCMDString(getDstDB2Instance()) + linesep);
            propWriter.write("dstDB2FixPack=" + IBMExtractUtilities.FormatCMDString(getDstDB2FixPack()) + linesep);
		    propWriter.write("dstDBRelease=" + IBMExtractUtilities.FormatCMDString(dstDBRelease) + linesep);
            propWriter.write("loadDirectory="+IBMExtractUtilities.FormatCMDString(getLoadDirectory())+linesep);
            propWriter.write("tempFilesPath="+IBMExtractUtilities.FormatCMDString(getTempFilesPath())+linesep);
            propWriter.write("extractDDL=" + extractDDL + linesep); 
            propWriter.write("extractData=" + extractData + linesep); 
            propWriter.write("extractObjects=" + extractObjects + linesep); 
            propWriter.write("usepipe=" + getUsePipe() + linesep);
            propWriter.write("syncLoad="+ getSyncLoad()+linesep);
            propWriter.write("numThreads=" + numThreads + linesep);
            propWriter.write("numWorkDir=" + numWorkDir + linesep);
            propWriter.write("debug=" + debug + linesep);
            propWriter.write("RetainColName=true" + linesep);
            propWriter.write("encoding=" + encoding + linesep);
            propWriter.write("sqlFileEncoding=" + sqlFileEncoding + linesep);
            propWriter.write("graphic=" + graphic + linesep);
            propWriter.write("loadStats=" + getLoadStats() + linesep);
            propWriter.write("customMapping=" + customMapping + linesep);
            propWriter.write("db2Installed="+getDB2Installed() + linesep);
            propWriter.write("clusteredIndexes="+getClusteredInexes() + linesep);
            propWriter.write("dbclob=" + dbclob + linesep);
            propWriter.write("trimTrailingSpaces=" + IBMExtractUtilities.getDefaultString(trimTrailingSpaces, "false") + linesep);
            propWriter.write("regenerateTriggers=" + IBMExtractUtilities.getDefaultString(regenerateTriggers,"false") + linesep);
            propWriter.write("remoteLoad=" + IBMExtractUtilities.getDefaultString(remoteLoad,"false") + linesep);
            propWriter.write("loadException=" + IBMExtractUtilities.getDefaultString(loadException,"true") + linesep);
            propWriter.write("exceptSchemaSuffix=" + IBMExtractUtilities.getDefaultString(exceptSchemaSuffix,"_EXP", true) + linesep);
            propWriter.write("exceptTableSuffix=" + IBMExtractUtilities.getDefaultString(exceptTableSuffix,"", true) + linesep);
            propWriter.write("norowwarnings=" + getNorowwarnings() + linesep);
            propWriter.write("compressTable=" + compressTable + linesep);
            propWriter.write("compressIndex=" + compressIndex + linesep);
            propWriter.write("extractPartitions=" + extractPartitions + linesep);
			   if (srcVendor.equalsIgnoreCase("mysql"))
               propWriter.write("mysqlZeroDateToNull=" + mysqlZeroDateToNull + linesep);
            propWriter.write("deleteLoadFile=" + deleteLoadFile + linesep);
            propWriter.write("extractHashPartitions=" + IBMExtractUtilities.getDefaultString(extractHashPartitions,"false") + linesep);
            propWriter.write("retainConstraintsName=" + IBMExtractUtilities.getDefaultString(retainConstraintsName,"false") + linesep);
            propWriter.write("useBestPracticeTSNames=" + IBMExtractUtilities.getDefaultString(useBestPracticeTSNames,"false") + linesep);
            propWriter.write("limitExtractRows=" + IBMExtractUtilities.getDefaultString(limitExtractRows,"ALL") + linesep);
            propWriter.write("limitLoadRows=" + IBMExtractUtilities.getDefaultString(limitLoadRows,"ALL") + linesep);
            propWriter.write("oracleNumberMapping=" + IBMExtractUtilities.getDefaultString(oracleNumberMapping,"false") + linesep);
            propWriter.write("validObjects=" + IBMExtractUtilities.getDefaultString(validObjects,"false") + linesep);
            propWriter.write("extentSizeinBytes=" + IBMExtractUtilities.getDefaultString(extentSizeinBytes,"36000") + linesep);
            propWriter.write("commitCount=" + IBMExtractUtilities.getDefaultString(commitCount,"100") + linesep);
            propWriter.write("saveCount=" + IBMExtractUtilities.getDefaultString(saveCount,"0") + linesep);
            propWriter.write("warningCount=" + IBMExtractUtilities.getDefaultString(warningCount,"0") + linesep);
            propWriter.write("varcharLimit="+IBMExtractUtilities.getDefaultString(varcharLimit,"4096")+linesep);
            propWriter.write("numRowsToCheck="+numRowsToCheck+linesep);
            propWriter.write("batchSizeDisplay="+getBatchSizeDisplay()+linesep);
            propWriter.write("mapCharToVarchar=" + getMapCharToVarchar() + linesep);
            propWriter.write("caseSensitiveTabColName=" + getCaseSensitiveTabColName() + linesep);
            propWriter.write("mapTimeToTimestamp=" + IBMExtractUtilities.getDefaultString(mapTimeToTimestamp, "false") + linesep);
            propWriter.write("additionalURLProperties=" + IBMExtractUtilities.getDefaultString(additionalURLProperties, "null") + linesep);
            
            if (Constants.zdb2())
            {
            	propWriter.write("zHLQName="+ zHLQName + linesep);
            	propWriter.write("zdb2tableseries="+ zdb2tableseries + linesep);
            	propWriter.write("znocopypend=" + znocopypend + linesep);
            	propWriter.write("zoveralloc=" + zoveralloc + linesep);
            	propWriter.write("zsecondary=" + zsecondary + linesep);
				propWriter.write("storclas=" + storclas + linesep);
				propWriter.write("jobCard1=" + jobCard1 + linesep);
				propWriter.write("jobCard2=" + jobCard2 + linesep);
				propWriter.write("db2SubSystemID=" + db2SubSystemID + linesep);
				propWriter.write("SDSNLOAD=" + SDSNLOAD + linesep);
				propWriter.write("RUNLIBLOAD=" + RUNLIBLOAD + linesep);
				propWriter.write("DSNTEPXX=" + DSNTEPXX + linesep);
				propWriter.write("generateJCLScripts="+generateJCLScripts+linesep);
            }            
            if (srcVendor.equalsIgnoreCase("sybase"))
            {
            	propWriter.write("sybaseConvClass=" + getSybaseConvClass() + linesep);
            	propWriter.write("sybaseUDTConversionToBaseType=" + getSybaseUDTConversionToBaseType() + linesep);
            } else if (srcVendor.equalsIgnoreCase("teradata"))
            {
            	propWriter.write("teradataConnStringExtraParam="+getTeradataConnStringExtraParam()+linesep);
            } else if (srcVendor.equalsIgnoreCase("informix"))
            {
            	propWriter.write("instanceName="+getInstanceName()+linesep);
            } else if (srcVendor.equalsIgnoreCase("mssql"))
            {
            	propWriter.write("instanceName="+getInstanceName()+linesep);
            	propWriter.write("mtkHome="+IBMExtractUtilities.FormatCMDString(getMtkHome()) + linesep);
            } else if (srcVendor.equalsIgnoreCase(Constants.oracle))
            {
            	propWriter.write("convertOracleTimeStampWithTimeZone2Varchar="+getConvertOracleTimeStampWithTimeZone2Varchar()+linesep);            	
            	propWriter.write("convertTimeStampWithTimeZone2UTCTime="+getConvertTimeStampWithTimeZone2UTCTime()+linesep);            	
            }
        	propWriter.write(linesep+linesep+"#### Comments ##### " + linesep);
			propWriter.write("#debug=[false|true]. If true, debug messages will be printed. " + linesep);
			propWriter.write("#db2Installed=[true|false]. If false, tool will not ask for DB2 connection information. " + linesep);
			propWriter.write("#RetainColName=[true|false]. If true, tool will not truncate column name to 30 chars. " + linesep);
			propWriter.write("#mapCharToVarchar=[true|false]. If true, map CHAR to VARCHAR" + linesep);
			propWriter.write("#encoding=Specify a valid encoding. e.g. [CP1252|US-ASCII|ISO-8859-1|UTF-8|UTF-16BE|UTF-16LE|UTF-16|SJIS]" + linesep);
			propWriter.write("#sqlFileEncoding=UTF-8 The default is UTF-8. This encoding is for the SQL scripts generated. Specify valid encoding" + linesep);
			propWriter.write("#graphic=[false|true]. Treat NVARCHAR, NCHAR and NTEXT column as VARCHAR, CHAR and CLOB if false."+ linesep);
			propWriter.write("#loadstats=[false|true]. Create STATISTICS option in LOAD Script if true." + linesep);
			propWriter.write("#customMapping=[false|[PE|CE]]. Do not use this unless you are doing FileNet migration. The default is false and use either PE or CE for FileNet" + linesep);
			propWriter.write("#db2_compatibility=[true|false]. If true, use Oracle compatibility features in DB2. Use it when migrating from Oracle to DB2 V9.7 onwards" + linesep);
			propWriter.write("#dbclob=[false|true]. If false, CLOBS are retained as it is. If true, CLOB is converted to DBCLOB in DB2." + linesep);
			propWriter.write("#trimTrailingSpaces=[true|false]. If true, it will trim trailing whitespaces from CHAR and VARCHAR columns." + linesep);
			propWriter.write("#remoteLoad=[true|false]. If true, LOAD can be used from a client. But, CLOBS, BLOBS and XML still need to be on DB2 server as that is the LOAD requirement" + linesep);
			propWriter.write("#loadException=[true|false]. If true, load exception tables will be created. A load exception table is a consolidated report of all of the rows that violated unique index rules, range constraints, and security policies during a load operation" + linesep);
			propWriter.write("#exceptSchemaSuffix=_EXP. Suffix that you want to add to distinguish the exception table schema name" + linesep);
			propWriter.write("#exceptTableSuffix= Suffix that you want to add in the table name to distinguish it from the original table name" + linesep);
			propWriter.write("#norowwarnings=[true|false]. If true, NOROWWARNINGS modifier is added to the LOAD statement so that row warnings are *not* recorded in the message file." + linesep);
			propWriter.write("#compressTable=[true|false]. If true, table will be created with COMPRESS YES in CREATE TABLE script" + linesep);
			propWriter.write("#compressIndex=[true|false]. If true, index will be created with COMPRESS YES in CREATE INDEX script for DB2 9.7 onwards" + linesep);
			propWriter.write("#clusteredIndexes=[true|false]. If true, clustered indexes will be carried over from source database to DB2."+linesep);
			propWriter.write("#extractPartitions=[true|false]. If true, Oracle table partitions information will be mapped in DB2" + linesep);
			propWriter.write("#deleteLoadFile=[true|false]. If true, data file will be deleted after load has completed successfully" + linesep);
			propWriter.write("#extractHashPartitions=[false|true]. If true, Oracle Hash Partitions will be mapped in DB2" + linesep);
			propWriter.write("#retainConstraintsName=[false|true]. If true, constraints name are used from source database as it is" + linesep);
			propWriter.write("#useBestPracticeTSNames=[true|false]. If false, extract Oracle tablespace names and use it in DB2." + linesep);
			propWriter.write("#limitExtractRows=ALL. If ALL, extract all rows otherwise specify a value > 0." + linesep);
			propWriter.write("#limitLoadRows=ALL. If ALL, load all rows in DB2 otherwise specify a value > 0." + linesep);
			propWriter.write("#oracleNumberMapping=[false|true]. If set to true, map NUMBER(0) -> DOUBLE, NUMBER(1-5) -> SMALLINT, NUMBER(5-9) -> INTEGER, NUMBER(10-18) -> BIGINT, NUMBER(19-21) -> FLOAT/DECFLOAT(16), NUMBER(>31) -> DOUBLE/DECFLOAT(16)" + linesep);
			propWriter.write("#oracleNumberMapping=<dataType>. If not set to [true|false], it can be set to a data type and all Oracle NUMBER columns will be mapped to this data type. e.g. oracleNumberMapping=DECIMAL(18,6) or any other data type of your choice" + linesep);
			propWriter.write("#validObjects=[true|false]. If set to true, extract only VALID objects from Oracle database for creating MEET input file" + linesep);
			propWriter.write("#extentSizeinBytes=36000. Set this to the number of bytes to buffer data before it is written to the disk for data files." + linesep);
			propWriter.write("#varcharLimit=4096. Set this to a value where you want varchar(>n) to be converted to DB2 DBCLOB or CLOB." + linesep);
			propWriter.write("#numRowsToCheck=100. Number of rows to check when running DataCheck program to check lengths of BLOBS at source and target." + linesep);
			propWriter.write("#batchSizeDisplay=0. Progress of the data extraction to display elapsed time to unload 'n' number of rows. To turn this off, specify 0" + linesep);
			propWriter.write("#loadDirectory= The blank value defaults to the output directory. Specify the directory name of the server on which you will mount the directory. This is useful when you unload data on source server and then FTP or mount disks to the target DB2 server. This param is used in LOAD statement to specify target server load mount point for the data." + linesep);
			propWriter.write("#tempFilesPath= The blank value does not add TEMPFILES PATH to the LOAD statement. Specify the full directory name which will be used by the LOAD to store temporary file." + linesep);
			propWriter.write("#usepipe=[false|true]. If set to true, extract happens to a pipe. Make sure tables are created at target database before using this option.  You can only use either usePipe or syncLoad but not both." + linesep);
			propWriter.write("#syncLoad=[false|true]. If set to true, extract and load happens simultaneously. Make sure tables are created at target database before using this option. You can only use either usePipe or syncLoad but not both." + linesep);
			propWriter.write("#caseSensitiveTabColName=[false|true]. If set to true, table and column names case sensitivity as found in source database will be brought in DB2 as it is. If set to false, the table and column names will be converted to UPPERCASE." + linesep);
			propWriter.write("#mapTimeToTimestamp=[false|true]. If set to true, treats source database time data type to DB2 Timestamp and adds 1990-01-01 to the date part." + linesep);
			propWriter.write("#numWorkDir=<a number> Divide the work in different work directory to minimize time window. Use this for data unload and load purposes" + linesep);
			propWriter.write("#additionalURLProperties=<string> Additional JDBC url properties that you want added to the connection url. The default value is 'null' which means to ignore." + linesep);
			if (Constants.zdb2()) 
			{
				if  (Constants.zos())
				{
					propWriter.write("#generateJCLScripts=[true|false]. If true, the JCL scripts will be generated so that they can be run from z/OS and not USS" + linesep);
					propWriter.write("#zHLQName=<HLQ>. If left blank, it will be set to the HLQ of logged in user" + linesep);
					propWriter.write("#zdb2tableseries=Q. The first letter of the dataset series name for not to overwrite dataset "
									+ "for other databases migration. This is only for migration to zDB2" + linesep);
					propWriter.write("#znocopypend=[true|false]. If true, use NOCOPYPEND option in LOAD for zDB2" + linesep);
					propWriter.write("#zoveralloc=value. The overAlloc variable specifies by how much we want to oversize our file allocation "
									+ "requests.  A value of 1 would mean don't oversize at all.  In an environment with tons of free storage, this might "
									+ "actually work.  In a realistic environment, 15/11 (1.3636) seems to be a good guess.  But, I guess it could be "
									+ "good for tuning to give others the ability to customize this.  I would recommend starting at 1.3636 (15/11) and lowering the "
									+ "value until you get file write errors, and then bumping it back up a little. " + linesep);
					propWriter.write("#zsecondary=value. Allocate fixed secondary extent. Starting with secondary set to 0 and "
									+ "increase it slowly until file errors occur and then bring it back down" + linesep);
					propWriter.write("#storclas=none. Specify none if do not want to use storclas. Length can be from 1 to 8 only" + linesep);					
					propWriter.write("#jobcard1=<Value>. Specify your definition of the job card 1 and it will be used as the first line of the JCL. Only first word will be changed for different jobs." + linesep);					
					propWriter.write("#jobcard2=<Value>. Specify your definition of the job card 2 and it will be used as the first line of the JCL." + linesep);					
					propWriter.write("#db2SubSystemID=<Value>. Specify ID of the DB2 subsystem." + linesep);
					propWriter.write("#SDSNLOAD=<Value>. Specify your name of the SDSN LOAD" + linesep);
					propWriter.write("#RUNLIBLOAD=<Value>. Specify your name of the RUNLIB LOAD" + linesep);
					propWriter.write("#DSNTEPXX=<Value>. Specify your name of the DSNTEPXX" + linesep);
				} else
				   propWriter.write("#commitCount=100. # of rows in multi-row inserts in DB2 on z/OS" + linesep);				
			}
			if (Constants.db2())
			{
			    propWriter.write("#saveCount=0. A number that will add a SAVECOUNT clause in the generated DB2 LOAD statement. The default value is zero, meaning that no consistency points will be established, unless necessary" + linesep);				
			    propWriter.write("#             An improper value of saveCount may impact LOAD performance." + linesep);				
			}
			propWriter.write("#warningCount=0. A number if > 0 will stop the LOAD after that many warnings." + linesep);				
			if (srcVendor.equalsIgnoreCase("mysql"))
				   propWriter.write("#mysqlZeroDateToNull=[false|true]. If set to true, mysql date of 0000-00-00 will be treated as NULL." + linesep);
			else if (srcVendor.equals("sybase"))
            {
				propWriter.write("#sybaseUDTConversionToBaseType=[true|false] If set to true, it will convert Sybase user defined data types to base data type." + linesep);
            	propWriter.write("#sybaseConvClass=com.sybase.jdbc3.utils.TruncationConverter. This is the Sybase class in jconn3.jar. If you are using a later version of JDBC driver, you might need to change this for the path name as per JDBC jar file." + linesep);
            } else if (srcVendor.equalsIgnoreCase("teradata"))
            {
            	propWriter.write("#teradataConnStringExtraParam=paramValues Specify extra JDBC connection params (e.g. CHARSET=UTF8,TYPE=FASTEXPORT,TMODE=ANSI) for teradata. See JDBC doc for details." + linesep);
            } else if (srcVendor.equalsIgnoreCase("informix"))
            {
            	
            	propWriter.write("#instanceName=Name Specify Informix server name when using Informix JDBC driver. Keep this value blank if using DB2 JCC driver for Informix. This is DBSERVERNAME in your onconfig file." + linesep);
            } else if (srcVendor.equalsIgnoreCase("mssql"))
            {            	
            	propWriter.write("#instanceName=Name Specify Named SQL Server Instance name here. Keep this blank if connecting to a default SQL Server Instance." + linesep);
            	propWriter.write("#mtkHome=Value Specify mtkHome value so that scripts for converting SQL Server Procedures using MTK are created properly." + linesep);
            } else if (srcVendor.equalsIgnoreCase(Constants.oracle))
            {
    			propWriter.write("#convertOracleTimeStampWithTimeZone2Varchar=[true|false]. If true, Oracle data type of TIMESTAMP WITH TIMEZOE or LOCAL TIMEZONE is converted to VARCHAR(60)" + linesep);            	
    			propWriter.write("#convertTimeStampWithTimeZone2UTCTime=[true|false]. If true, Oracle data type of TIMESTAMP WITH TIMEZOE is converted to UTC Time" + linesep);            	
            }
            
            if (srcVendor.equalsIgnoreCase("mysql"))
            	propWriter.write("#mapTextToLongVarchar=[false|true]. If set to true, it will map TEXT, MEDIUMTEXT and LONGTEXT in MySQL to LONG_VARCHAR(65535)." + linesep);            	
            
            propWriter.close();
            log("All input parameters are saved in '"+configFile+"' file.");
            propParams.load(new FileInputStream(configFile));            		
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            System.exit(-1);
        }
	}
	
	private int getSrcVendorNum()
	{
		if (srcVendor.equals("") || srcVendor.equals(Constants.oracle))
		   return 1;
		else if (srcVendor.equals(Constants.mssql))
		   return 2;
		else if (srcVendor.equals(Constants.sybase))
		   return 3;
		else if (srcVendor.equals(Constants.access))
		   return 4;
		else if (srcVendor.equals(Constants.mysql))
		   return 5;
		else if (srcVendor.equals(Constants.postgres))
		   return 6;
		else if (srcVendor.equals(Constants.zdb2))
		   return 7;
		else if (srcVendor.equals(Constants.db2luw))
		   return 8;
		else if (srcVendor.equals(Constants.teradata))
		   return 9;
		else if (srcVendor.equals(Constants.informix))
		   return 10;
		else if (srcVendor.equals(Constants.netezza))
		   return 11;
		else
		   return 0;
	}

	private int getDstVendorNum()
	{
		String vend = Constants.getDbTargetName();
		if (vend.equals("") || vend.equals(Constants.db2luw_compatibility))
		   return 1;
		else if (vend.equals(Constants.db2luw))
		   return 2;
		else if (vend.equals(Constants.netezza))
		   return 3;
		else
		   return 0;
	}
	
	public int getDefaultSrcVendorPort(String vendor)
	{
		String defPort = "0";
		if (vendor.equals(Constants.oracle))
			defPort = Constants.oraclePort;
		else if (vendor.equals(Constants.mssql))
			defPort = Constants.mssqlPort;
		else if (vendor.equals(Constants.sybase))
			defPort = Constants.sybasePort;
		else if (vendor.equals(Constants.mysql))
			defPort = Constants.mysqlPort;
		else if (vendor.equals(Constants.access))
			defPort = Constants.accessPort;
		else if (vendor.equals(Constants.postgres))
			defPort = Constants.postgresPort;
		else if (vendor.equals(Constants.zdb2))
			defPort = Constants.zdb2Port;
		else if (vendor.equals(Constants.db2luw))
			defPort = Constants.db2luwPort;
		else if (vendor.equals(Constants.teradata))
			defPort = Constants.teradataPort;
		else if (vendor.equals(Constants.informix))
			defPort = Constants.informixPort;
		else if (vendor.equals(Constants.netezza))
			defPort = Constants.netezzaPort;
		return Integer.valueOf(defPort);
	}
	
	public int getDefaultDstVendorPort(String vendor)
	{
		String defPort = "0";
		if (vendor.equals(Constants.db2luw_compatibility))
			defPort = Constants.db2luwPort;
		else if (vendor.equals(Constants.db2luw))
			defPort = Constants.db2luwPort;
		else if (vendor.equals(Constants.netezza))
			defPort = Constants.netezzaPort;
		else if (vendor.equals(Constants.zdb2))
			defPort = Constants.zdb2Port;
		return Integer.valueOf(defPort);
	}

	public String getSrcVendor(int num)
	{
		String vendor = "";
		switch (num)
		{
		   case 1 : vendor = Constants.oracle; break;
		   case 2 : vendor = Constants.mssql; break;
		   case 3 : vendor = Constants.sybase; break;
		   case 4 : vendor = Constants.access; break;
		   case 5 : vendor = Constants.mysql; break;
		   case 6 : vendor = Constants.postgres; break;
		   case 7 : vendor = Constants.zdb2; break;
		   case 8 : vendor = Constants.idb2; break;
		   case 9 : vendor = Constants.db2luw; break;
		   case 10 : vendor = Constants.teradata; break;
		   case 11 : vendor = Constants.informix; break;
		   case 12 : vendor = Constants.netezza; break;
		   default : vendor = "";
		}
		return vendor;
	}

	public String getDstVendor(int num)
	{
		String vendor = "";
		switch (num)
		{
		   case 1 : vendor = Constants.db2luw_compatibility; break;
		   case 2 : vendor = Constants.db2luw; break;
		   case 3 : vendor = Constants.netezza; break;
		   case 4 : vendor = Constants.zdb2; break;
		   default : vendor = "";
		}
		return vendor;
	}

	/**
	 * @return the loadException
	 */
	public String getLoadException()
	{
		return IBMExtractUtilities.getDefaultString(loadException, "true");
	}

	/**
	 * @param loadException the loadException to set
	 */
	public void setLoadException(String loadException)
	{
		this.loadException = loadException;
	}

	private void getDstVendorConsoleInput()
	{
		String vend;
		if (Constants.zos())
			vend = Constants.zdb2;
		else
		{
			int num = 1;
			String inputa;
			do
			{
				int defNum = getDstVendorNum();
				num = 1;
				System.out.println("DB2 LUW with compatibility  : 1 ");
				System.out.println("DB2 LUW                     : 2 ");
				System.out.println("Netezza                     : 3 ");
				System.out.println("DB2 on z/OS                 : 4 ");
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
			} while (!(num >= 1 && num <= 4));
			db2Compatibility = ((num == 1) ? true : false);			
			vend = getDstVendor(num);
		}
		if (dstPort == 0)
			dstPort = getDefaultDstVendorPort(vend);
		System.setProperty("TARGET_DB", vend);
		Constants.setDbTargetName();
	}

	public void setDB2Compatibility(boolean db2Compatibility)
	{
		this.db2Compatibility = db2Compatibility;
	}
	
	public boolean getDB2Compatibility()
	{
		return db2Compatibility;
	}
	
	private void getSrcVendorConsoleInput()
	{
		int num = 1;
		String inputa;
		do
		{
			int defNum = getSrcVendorNum();
			num = 1;
			System.out.println("Oracle                  : 1 ");
			System.out.println("MS SQL Server           : 2 ");
			System.out.println("Sybase                  : 3 ");
			System.out.println("MS Access Database      : 4 ");
			System.out.println("MySQL                   : 5 ");
			System.out.println("PostgreSQL              : 6 ");
			System.out.println("DB2 z/OS                : 7 ");
			System.out.println("DB2 iSeries             : 8 ");
			System.out.println("DB2 LUW                 : 9 ");
			System.out.println("Teradata                : 10 ");
			System.out.println("Informix                : 11 ");
			System.out.println("Netezza                 : 12 ");
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
		} while (!(num >= 1 && num <= 12));
		
		srcVendor = getSrcVendor(num);
		if (srcPort == 0)
			srcPort = getDefaultSrcVendorPort(srcVendor);
		
		if (srcVendor.equals(Constants.oracle))
		{
			if (db2Compatibility)
			{
				num = (regenerateTriggers.equals("true")) ? 1 : 2;
				num = getYesNoQuitConsoleInput(num, 
						"Split Oracle multiple action triggers (Yes) : 1 ", 
						"Do not split                                : 2 ",
						"Enter regenerateTriggers");		
				switch (num)
				{
				   case 1 : regenerateTriggers = "true"; break;
				   case 2 : regenerateTriggers = "false"; break;
				}				
			}
		} else if (srcVendor.equals(Constants.informix))	
		{
			instanceName = getStringConsoleInput(instanceName, "Enter Informix server name (Leave blank if using JCC)");
		}
	}
	
	private String getStringConsoleInput(String token, String prompt, String validValues)
	{
		int num = 1;
		String inputa, returnValue = "";
				
		do
		{
			if (!IBMExtractUtilities.isValidValue(token, validValues))
			   System.out.println("Invalid value="+token+". Valid values are: " + validValues);
			if (token == null || token.length() == 0)
			   System.out.print(prompt);
			else
			{
				if (IBMExtractUtilities.isValidValue(token, validValues))
				{
					System.out.print(prompt + " (Default="+token+") : ");
				} else
				    System.out.print(prompt);
			}			   
			try
			{
				inputa = stdin.readLine();
				if (inputa.equals(""))
				{
					num = -1;							
				} 
				else if (inputa.equals("1"))
				{
					num = 1;							
				}
				else
				{
					token = inputa.toUpperCase();
					if (IBMExtractUtilities.isValidValue(token, validValues))
					{
						System.out.print("You entered '" + token + "' Press 1 to re-enter or hit enter to continue : ");
					}
				}
				returnValue = token;
			}
			catch(Exception e)
			{
				num = -1;
			}
		} while (num == 1);		
		return returnValue;
	}
	
	public String getValidSchemaList(String selectedList, String schemaList)
	{
		boolean done, match;
		do
		{
			String tok = (selectedList == null || selectedList.length() == 0) ? schemaList : selectedList;
			selectSchemaName = getStringConsoleInput(tok, "Select your schema names (separator is :)");
			String[] tmp1 = srcSchName.split(":");
			String[] tmp2 = selectSchemaName.split(":");
			if (tmp2.length > tmp1.length)
			{
				System.out.println("You have more entries in your schema list than available. Re-enter.");
				done = false;
			} else
			{
				done = true;
				for (int i = 0; i < tmp2.length; ++i)
				{
					match = false;
					for (int j = 0; j < tmp1.length; ++j)
					{
						if (tmp2[i].equals(tmp1[j]))
						{
							match = true;
							break;
						}
					}
					if (!match)
					{
						System.out.println("You have an invalid schema name in your list. Re-enter.");						
						done = false;
						break;
					}
				}				
			}
		} while (!done);
		return selectSchemaName;
	}
	
	public String getStringConsoleInput(String token, String prompt)
	{
		int num = 1;
		String inputa, returnValue = "";
		
		do
		{
			if (token == null || token.equals(""))
			   System.out.print(prompt + " : ");
			else
			   System.out.print(prompt + " (Default="+token+") : ");
			try
			{
				inputa = stdin.readLine();
				if (inputa.equals(""))
				{
					num = -1;		
					returnValue = token;
				} else
				{
					System.out.print("You entered '" + inputa + "' Press 1 to re-enter or hit enter to continue : ");
					returnValue = inputa;
					inputa = stdin.readLine();
					if (inputa.equals(""))
					{
						num = -1;
					}					
				}
			}
			catch(Exception e)
			{
				num = -1;
			}
		} while (num == 1);		
		return returnValue;
	}

	private String getIntOrALLConsoleInput(String token, String prompt)
	{
		int num = 1;
		String inputa, returnValue = "";
		
		do
		{
			if (token == null || token.length() == 0)
			{
			   token = "ALL";
			}
			System.out.print(prompt + " (Default="+token+") : ");
			try
			{
				inputa = stdin.readLine();
				if (inputa.equals(""))
				{
					num = -1;		
					returnValue = token;
				} 
				else if (inputa.equalsIgnoreCase("ALL"))
				{
					num = -1;		
					returnValue = "ALL";
				} 
				else
				{
					int x = Integer.parseInt(inputa);
					num = (x < 0) ? 1 : -1;
					returnValue = inputa;					
				}
			}
			catch(Exception e)
			{
				num = 1;
			}
		} while (num == 1);		
		return returnValue;
	}

	public int getIntConsoleInput(int token, String prompt)
	{
		
		int num = 1;
		String inputa;
		int returnValue = 0;
		
		do
		{
			num = -1;
			System.out.print(prompt + " (Default="+token+") : ");
			try
			{
				inputa = stdin.readLine();
				if (inputa.equals(""))
				{
					num = returnValue = token;
				} else
				{
					num = returnValue = (inputa.equals("")) ? token : Integer.parseInt(inputa);
				}
			}
			catch(Exception e)
			{
				num = -1;
			}
		} while (num == -1);
		return returnValue;
	}
	
	public int getYesNoQuitConsoleInput(int defToken, String line1, String line2, String prompt)
	{
		int num = 1;
		String inputa;

		do
		{
			num = 1;
			System.out.println(line1);
			System.out.println(line2);
		    System.out.print(prompt + " (Default="+defToken+") : ");
			try
			{
				inputa = stdin.readLine();
				num = (inputa.equals("")) ? defToken : Integer.parseInt(inputa);
			}
			catch(Exception e)
			{
				num = -1;
			}
		} while (!(num >= 1 && num <= 2));
		return num;
	}
	
	public int getYesNoOtherConsoleInput(int defToken, String line1, String line2, String line3, String prompt)
	{
		int num = 1;
		String inputa;

		do
		{
			num = 1;
			System.out.println(line1);
			System.out.println(line2);
			System.out.println(line3);
		    System.out.print(prompt + " (Default="+defToken+") : ");
			try
			{
				inputa = stdin.readLine();
				num = (inputa.equals("")) ? defToken : Integer.parseInt(inputa);
			}
			catch(Exception e)
			{
				num = -1;
			}
		} while (!(num >= 1 && num <= 3));
		return num;
	}

	public String getAdditionalURLProperties()
	{
		return (additionalURLProperties == null || additionalURLProperties.length() == 0) ? "null" : additionalURLProperties;
	}

	public void setAdditionalURLProperties(String additionalURLProperties)
	{
		this.additionalURLProperties = additionalURLProperties;
	}

	private void createListofTables()
	{
		System.setProperty("OUTPUT_DIR", outputDirectory);
		IBMExtractUtilities.CreateTableScript(srcVendor, dstSchName, srcSchName, selectSchemaName,
				srcServer, srcPort, srcDBName, srcUid, srcPwd, Boolean.valueOf(db2Compatibility), caseSensitiveTabColName);
		if (IBMExtractUtilities.Message.equals(""))
		{
			System.out.println("You can remove tables that you do not want to migrate by editing "+srcDBName+".tables file"); 
		}		
	}
	
	public void collectInput()
	{
		int num = 1, bytesToWrite;
		
		outputDirectory = getStringConsoleInput(outputDirectory, "Specify output directory");
		setOutputDirectory(outputDirectory);
		if (!Constants.zos())
		{
			System.out.println(" ******* Target database name: ***** ");		
			getDstVendorConsoleInput();		
			System.out.println("Your Target database is '" + getDstVendor() + "'");
		}


		if (Constants.zos())
		{
			System.out.print("Checking if ibmjzos.jar available ... ");
			if (IBMExtractUtilities.FileExists(System.getProperty("java.home") + "/lib/ext/ibmjzos.jar"))
			{
				System.out.println("found");
			} else
			{
				System.out.println("not found. Exiting ...");
				System.out.println("Please ask your system administrator to install ibmjzos");
				System.out.println("The information about jzos is at http://www-03.ibm.com/systems/z/os/zos/tools/java/products/jzos/overview.html");
				System.exit(-1);
			}
		    System.out.println(" *** Collecting z/OS specific information now. If not sure, use defaults *** ");
		    zHLQName = getStringConsoleInput(zHLQName, "Enter HLQ underneath datasets will be created in z/OS");
		    zdb2tableseries = getStringConsoleInput(zdb2tableseries, "Name to be put after TBLDATA");
		    znocopypend = getStringConsoleInput(znocopypend, "Enter the value for NOCOPYPEND for DB2 LOAD");
		    zoveralloc = getStringConsoleInput(zoveralloc, "Enter the value of over allocation param");
		    zsecondary = getStringConsoleInput(zsecondary, "Enter number of secondary cylinders");
		    storclas = getStringConsoleInput(storclas, "Enter the store class");
		    jobCard1 = getStringConsoleInput(jobCard1, "Enter the job card 1 description");
			jobCard2 = getStringConsoleInput(jobCard2, "Enter the job card 2 description");
			db2SubSystemID = getStringConsoleInput(db2SubSystemID, "Enter the DB2 Sub-System ID");
			SDSNLOAD = getStringConsoleInput(SDSNLOAD, "Enter the SDSNLOAD");
			RUNLIBLOAD = getStringConsoleInput(RUNLIBLOAD, "Enter the RUNLIB.LOAD");
			DSNTEPXX = getStringConsoleInput(DSNTEPXX, "Enter the DSNTEPXX");	
			num = (generateJCLScripts.equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Generate JCL        : 1 ", 
					"Do not generate JCL : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : generateJCLScripts = "true"; break;
			   case 2 : generateJCLScripts = "false"; break;
			}
		}		
		
		loadDirectory = outputDirectory;
		loadDirectory = getStringConsoleInput(loadDirectory, "Specify LOAD directory");
		loadDirectory = getLoadDirectory();
		num = (debug.equals("true")) ? 1 : 2;
		num = getYesNoQuitConsoleInput(num, 
				"Debug (Yes)        : 1 ", 
				"Debug (No)         : 2 ",
				"Enter a number");		
		switch (num)
		{
		   case 1 : debug = "true"; break;
		   case 2 : debug = "false"; break;
		}

		if (Constants.db2())
		{
			num = (getNorowwarnings().equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Include NOROWWARNINGS in LOAD    : 1 ", 
					"Do not add NOROWWARNINGS in LOAD : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : norowwarnings = "true"; break;
			   case 2 : norowwarnings = "false"; break;
			}
			tempFilesPath = getStringConsoleInput(tempFilesPath, "Specify TEMPFILES Path for LOAD");
			tempFilesPath = getTempFilesPath();
		} else
			norowwarnings = "false";
		
		if (Constants.db2())
		{
			num = (remoteLoad.equals("false")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"IS TARGET DB2 LOCAL    : 1 ", 
					"IS TARGET DB2 REMOTE   : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : remoteLoad = "false"; break;
			   case 2 : remoteLoad = "true"; break;
			}
		} else
			remoteLoad = "false";

		if (Constants.db2())
		{
			num = (loadException.equals("false")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"DUMP BAD DATA IN EXCEPTION TABLES (Yes)    : 1 ", 
					"DUMP BAD DATA IN EXCEPTION TABLES (No)     : 2 ", 
					"Enter a number");		
			switch (num)
			{
			   case 1 : loadException = "true"; break;
			   case 2 : loadException = "false"; break;
			}
			if (num == 1)
			{
			    exceptSchemaSuffix = getIntOrALLConsoleInput(exceptSchemaSuffix, "Enter exception schema suffix. ");
			    exceptTableSuffix = getIntOrALLConsoleInput(exceptTableSuffix, "Enter exception table suffix. ");
			}
		} else 
		{
			loadException = "false";
		}

		num = (extractDDL.equals("true")) ? 1 : 2;
		num = getYesNoQuitConsoleInput(num, 
				"Extract DDL (Yes)        : 1 ", 
				"Extract DDL (No)         : 2 ",
				"Enter a number");		
		switch (num)
		{
		   case 1 : extractDDL = "true"; break;
		   case 2 : extractDDL = "false"; break;
		}
		  
		num = (extractData.equals("true")) ? 1 : 2;
		num = getYesNoQuitConsoleInput(num, 
				"Extract Data (Yes)        : 1 ", 
				"Extract Data (No)         : 2 ",
				"Enter a number");		
		switch (num)
		{
		   case 1 : extractData = "true"; break;
		   case 2 : extractData = "false"; break;
		}
		
		num = (extractObjects.equals("true")) ? 1 : 2;		
		num = getYesNoQuitConsoleInput(num, 
				"Extract Objects (Yes)        : 1 ", 
				"Extract Objects (No)         : 2 ",
				"Enter a number");		
		switch (num)
		{
		   case 1 : extractObjects = "true"; break;
		   case 2 : extractObjects = "false"; break;
		}
		
		if (extractData.equals("true"))
		{
			limitExtractRows = getIntOrALLConsoleInput(limitExtractRows, "Enter # of rows limit to extract. ");
			limitLoadRows = getIntOrALLConsoleInput(limitLoadRows, "Enter # of rows limit to load data. ");
			if (!Constants.netezza())
		        numRowsToCheck = getIntOrALLConsoleInput(numRowsToCheck, "Number of rows to check for length comparison for DataCheck program");
		    int rows;
		    try
		    {
		       rows = Integer.parseInt(batchSizeDisplay);
		    } catch (Exception e)
		    {
		    	rows = 0;
		    }
		    batchSizeDisplay = "" + getIntConsoleInput(rows, "# of rows extracted for which elapsed time is displayed");
		    numWorkDir = getIntConsoleInput(numWorkDir, "Number of work directories to extract data");
		    if (numWorkDir < 1)
		    {
		    	numWorkDir = 1;
		    	System.out.println("Invalid input. Setting numWorkDir="+numWorkDir);
		    }

		    numThreads = getIntConsoleInput(numThreads, "Number of threads to extract data");
		    if (numThreads < 1)
		    {
		    	numThreads = 5;
		    	System.out.println("Invalid input. Setting threads="+numThreads);
		    }		    					
		}
		
		if (!Constants.netezza())
		{
			num = (getClusteredInexes().equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Migrate clustered indexes in DB2 (Yes)  : 1 ", 
					"Migrate clustered indexes in DB2 (No)   : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : clusteredIndexes = "true"; break;
			   case 2 : clusteredIndexes = "false"; break;
			}
	
			num = (compressTable.equals("false")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Compress Table in DB2 (No)     : 1 ", 
					"Compress Table in DB2 (YES)    : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : compressTable = "false"; break;
			   case 2 : compressTable = "true"; break;
			}
	
			num = (compressIndex.equals("false")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Compress Index in DB2 (No)     : 1 ", 
					"Compress Index in DB2 (YES)    : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : compressIndex = "false"; break;
			   case 2 : compressIndex = "true"; break;
			}
		}

		System.out.println(" ******* Source database information: ***** ");
		
		getSrcVendorConsoleInput();
		
		num = (caseSensitiveTabColName == null || getCaseSensitiveTabColName().equals("true")) ? 2 : 1;
		num = getYesNoQuitConsoleInput(num, 
				"Convert tab/col names to uppercase    : 1 ", 
				"Retain tab/col names case sensitivity : 2 ",
				"Enter a number");		
		switch (num)
		{
		   case 1 : caseSensitiveTabColName = "false"; break;
		   case 2 : caseSensitiveTabColName = "true"; break;
		}
		
		num = (mapTimeToTimestamp != null && getMapTimeToTimestamp().equals("false")) ? 1 : 2;
		num = getYesNoQuitConsoleInput(num, 
				"Convert time to timestamp (No)  : 1 ", 
				"Convert time to timestamp (Yes) : 2 ",
				"Enter a number");		
		switch (num)
		{
		   case 1 : mapTimeToTimestamp = "false"; break;
		   case 2 : mapTimeToTimestamp = "true"; break;
		}
		
		do
		{
			encoding = getStringConsoleInput(encoding, "Enter encoding. ");
		} while (!IBMExtractUtilities.CheckValidEncoding(encoding));
	    
		if (Constants.zos())
		{
			sqlFileEncoding = ZUtil.getDefaultPlatformEncoding();
		}
		do
		{
			sqlFileEncoding = getStringConsoleInput(sqlFileEncoding, "Enter SQL Scripts file encoding. ");
		} while (!IBMExtractUtilities.CheckValidEncoding(sqlFileEncoding));

	    try
	    {
	       bytesToWrite = Integer.valueOf(extentSizeinBytes);
	    } catch (Exception e)
	    {
	    	bytesToWrite = 36000;
	    }
        bytesToWrite = getIntConsoleInput(bytesToWrite, "Number of data bytes to write in buffer before flushing to disk");
        extentSizeinBytes = String.valueOf(bytesToWrite);
	    

        if (!Constants.netezza())
        {
		    int varcharsize = 4096;
		    try
		    {
		    	varcharsize = Integer.valueOf(varcharLimit);
		    } catch (Exception e)
		    {
		    	varcharsize = 4096;
		    }
		    varcharsize = getIntConsoleInput(varcharsize, "VARCHAR size beyond which you want VARCHAR to go as CLOB");
		    varcharLimit = String.valueOf(varcharsize);
        }

		if (Constants.db2())
		{

			/*************
			num = (deleteLoadFile.equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Delete data file after load (Yes) : 1 ", 
					"Delete data file after load (No)  : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : deleteLoadFile = "true"; break;
			   case 2 : deleteLoadFile = "false"; break;
			}			
			**************/
	
			num = (getLoadStats().equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Generate Statistics during DB2 LOAD (Yes) : 1 ", 
					"Generate Statistics during DB2 LOAD (No)  : 2 ",
					"Enter a number");		
			switch (num)
			{
			   case 1 : loadStats = "true"; break;
			   case 2 : loadStats = "false"; break;
			}			
		}

		num = (extractHashPartitions.equals("true")) ? 1 : 2;	    	
		if (srcVendor.equals(Constants.oracle))
		{
			if (!Constants.netezza())
			{
				num = (extractPartitions.equals("true")) ? 1 : 2;
				num = getYesNoQuitConsoleInput(num, 
						"Map Oracle Range Partition to DB2 (Yes) : 1 ", 
						"Map Oracle Range Partition to DB2 (No)  : 2 ",
						"Enter a number");		
				switch (num)
				{
				   case 1 : extractPartitions = "true"; break;
				   case 2 : extractPartitions = "false"; break;
				}			
				num = (extractHashPartitions.equals("true")) ? 1 : 2;
				num = getYesNoQuitConsoleInput(num, 
						"Map Oracle Hash Partition to DB2 (Yes) : 1 ", 
						"Map Oracle Hash Partition to DB2 (No)  : 2 ",
						"Enter a number");		
				switch (num)
				{
				   case 1 : extractHashPartitions = "true"; break;
				   case 2 : extractHashPartitions = "false"; break;
				}
				num = (convertOracleTimeStampWithTimeZone2Varchar.equalsIgnoreCase("true")) ? 1 : 2;
				num = getYesNoQuitConsoleInput(num, 
						"Convert Oracle Timestamp with Timezone to VARCHAR (Yes) : 1 ", 
						"Convert Oracle Timestamp with Timezone to VARCHAR (No)  : 2 ",
						"Enter a number");		
				switch (num)
				{
				   case 1 : convertOracleTimeStampWithTimeZone2Varchar = "true"; break;
				   case 2 : convertOracleTimeStampWithTimeZone2Varchar = "false"; break;
				}
				num = (convertTimeStampWithTimeZone2UTCTime.equalsIgnoreCase("true")) ? 1 : 2;
				num = getYesNoQuitConsoleInput(num, 
						"Convert Oracle Timestamp with Timezone to UTC (Yes) : 1 ", 
						"Convert Oracle Timestamp with Timezone to UTC (No)  : 2 ",
						"Enter a number");		
				switch (num)
				{
				   case 1 : convertTimeStampWithTimeZone2UTCTime = "true"; break;
				   case 2 : convertTimeStampWithTimeZone2UTCTime = "false"; break;
				}
			}
		} else if (srcVendor.equals(Constants.sybase))
		{
			if (!Constants.netezza())
			{
				sybaseConvClass = getStringConsoleInput(sybaseConvClass, "Sybase class name=");
				num = (sybaseUDTConversionToBaseType != null && sybaseUDTConversionToBaseType.equals("true")) ? 1 : 2;
				num = getYesNoQuitConsoleInput(num, 
						"Convert Sybase UDT to base type (Yes) : 1 ", 
						"Convert Sybase UDT to base type (No)  : 2 ",
						"Enter a number");		
				switch (num)
				{
				   case 1 : sybaseUDTConversionToBaseType = "true"; break;
				   case 2 : sybaseUDTConversionToBaseType = "false"; break;
				}			
			}
		} else if (srcVendor.equalsIgnoreCase(Constants.mysql))
		{
			num = (mysqlZeroDateToNull.equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Treat MySQL Data 0000-00-00 as NULL (Yes) : 1 ", 
					"Treat MySQL Data 0000-00-00 as NULL (No)  : 2 ", 
					"Enter a number");		
			switch (num)
			{
			   case 1 : mysqlZeroDateToNull = "true"; break;
			   case 2 : mysqlZeroDateToNull = "false"; break;
			}			
		} else if (srcVendor.equalsIgnoreCase("teradata"))
		{
		    teradataConnStringExtraParam = getStringConsoleInput(getTeradataConnStringExtraParam(),"Enter extra JDBC Param values");
		}
		
		if (srcVendor.equals(Constants.access))
		{
			System.out.println("We will only extract data from access database. Turning DDL off: ");
			extractDDL = "false";
			extractObjects = "false";
		    srcServer = getStringConsoleInput(srcServer,"Enter full path for MS Acccess database file name");
		    srcDBName = "access";
		    srcPort = 0;
		    srcUid = "null";
		    srcPwd = "null";
		    srcSchName = "ADMIN";
		    fetchSize = "0";
		}
		else if (srcVendor.equals(Constants.mysql))
		{
			srcServer = getStringConsoleInput(srcServer,"Enter source database Host Name or IP Address");
			fetchSize = "0";
		}
		else
		{
		    srcServer = getStringConsoleInput(srcServer,"Enter source database Host Name or IP Address");
		}

		if (!srcVendor.equals(Constants.access))
		{		
		    srcPort = getIntConsoleInput(srcPort, "Enter "+srcServer+"'s port number");
		    if (srcVendor.equals(Constants.oracle))
		       srcDBName = getStringConsoleInput(srcDBName, "Enter Oracle Service Name or Instance Name");
		    else
		       srcDBName = getStringConsoleInput(srcDBName, "Enter source Database name");
		    srcUid = getStringConsoleInput(srcUid, "Enter User ID of source database");
		    srcPwd = getStringConsoleInput(srcPwd, "Enter source database Passsword");	
		    srcJDBC = getJDBCLocations(srcVendor, srcJDBC);
			AddJarsToClasspath(srcJDBC);    
			System.out.println("Now we will try to connect to the "+srcVendor+" to extract the schema names.");
			if (getYesNoQuitConsoleInput(1, 
					"Do you want to continue (Yes) : 1 ", 
					"Quit program                  : 2 ",
					"Enter a number") == 2)
				return;
			boolean remote;
			if (srcVendor.equalsIgnoreCase(Constants.db2luw))
				remote = (remoteLoad.equalsIgnoreCase("true")) ? true : false;
			else
				remote = true;
			String extraParam = (srcDBName.equals(Constants.sybase)) ? getSybaseConvClass() : "";
			if (srcVendor.equalsIgnoreCase("informix"))
			{
				if (instanceName != null && instanceName.trim().length() > 0)
				   srcServer = srcServer + "," + instanceName;
			}
			if (IBMExtractUtilities.TestConnection(remote, false, srcVendor, srcServer, srcPort, srcDBName, srcUid, srcPwd, extraParam))
			{
				srcDB2Home = IBMExtractUtilities.DB2Path;
				srcDB2Instance = IBMExtractUtilities.InstanceName;
				srcSchName = IBMExtractUtilities.GetSchemaList(srcVendor, srcServer, srcPort, srcDBName, srcUid, srcPwd);
				dstSchName = srcSchName;
				if (IBMExtractUtilities.Message.equals(""))
				{
					System.out.println(srcVendor + "'s schema List extracted=" + srcSchName);
					if (selectSchemaName != null && selectSchemaName.length() > 0)
						System.out.println("Your selected schema List =" + selectSchemaName);						
					int choice = getYesNoOtherConsoleInput(1, 
							"Do you want to use '"+srcUid+"' as schema : 1 ", 
							"Or the extracted List                   : 2 ",
							"Or your selected list                   : 3 ",
							"Enter a number");
					if (choice == 1)
					{
						if (srcVendor.equalsIgnoreCase("oracle") || srcVendor.equalsIgnoreCase("db2luw") ||
								srcVendor.equalsIgnoreCase("zdb2"))
						   selectSchemaName = srcUid.toUpperCase();
						else
						   selectSchemaName = srcUid;	
					}
					else if (choice == 2)
						selectSchemaName = srcSchName;
					else
					{
						selectSchemaName = getValidSchemaList(selectSchemaName, srcSchName);
					}
					createListofTables();
				}
				else
				{
					System.out.println(IBMExtractUtilities.Message);
					System.out.println("Fix the issue. Exiting ... ");	
					System.exit(-1);
				}
			} else
			{
				System.out.println(IBMExtractUtilities.Message);
				System.out.println("Fix the issue. Exiting ... ");	
				System.exit(-1);
			}						
		} else
		{
			createListofTables();
		}
		
		System.out.println(" ******* Target database information: ***** ");

		isTargetInstalled = true;
		if (Constants.db2())
		{
			num = (DB2Installed.equals("true")) ? 1 : 2;
			num = getYesNoQuitConsoleInput(num, 
					"Do you have DB2 installed (Yes) : 1 ", 
					"Do you have DB2 installed (No)  : 2 ", 
					"Enter a number");		
			switch (num)
			{
			   case 1 : DB2Installed = "true"; break;
			   case 2 : DB2Installed = "false"; break;
			}	
			if (DB2Installed.equalsIgnoreCase("true"))
			{
				if (!IBMExtractUtilities.isDB2Installed(Boolean.valueOf(remoteLoad)))
				{
					System.out.println("Sorry. I did not detect DB2.");
					if (Constants.win())
					{
						System.out.println("This may be due to the fact that you are running this program from a regular Windows command prompt.");
						System.out.println("If you have DB2 installed, launch DB2 command prompt and then run this application again.");
					} else
					{
						System.out.println("This may be due to the fact that either there is no DB2 or you are not sourcing db2profile file in your profile.");
						System.out.println("You should have DBADM authority to do this migration");
					}
					isTargetInstalled = false;
				}
			}
			else
			{
				isTargetInstalled = false;
			}
		}
		
		if (Constants.db2())  
		{
			if(DB2Installed.equalsIgnoreCase("true"))
			{
			dstServer = getStringConsoleInput(dstServer,"Enter db2 database Host Name or IP Address");
			dstPort = getIntConsoleInput(dstPort, "Enter db2 Port Number");
			dstDBName = getStringConsoleInput(dstDBName, "Enter db2 Database name");
		    dstUid = getStringConsoleInput(dstUid, "Enter db2 database User ID");
		    dstPwd = getStringConsoleInput(dstPwd, "Enter db2 database Passsword");	
			}
		}
		else if (Constants.zdb2())
		{
		    System.out.println(" *** Collecting z/OS DB2 connection information. *** ");
			dstServer = getStringConsoleInput(dstServer,"Enter z/OS db2 database Host Name or IP Address");
			dstPort = getIntConsoleInput(dstPort, "Enter z/OS db2 Port Number");
			dstDBName = getStringConsoleInput(dstDBName, "Enter z/OS db2 LOCATION name");
		    dstUid = getStringConsoleInput(dstUid, "Enter z/OS db2 database User ID");
		    dstPwd = getStringConsoleInput(dstPwd, "Enter z/OS db2 database Passsword");		
		} else if (Constants.netezza())
		{
			dstDBName = "SYSTEM";
			dstServer = getStringConsoleInput(dstServer,"Enter Neteeza database Host Name or IP Address");
			dstPort = getIntConsoleInput(dstPort, "Enter Neteeza Port Number");
			dstDBName = getStringConsoleInput(dstDBName, "Enter Neteeza Database name");
		    dstUid = getStringConsoleInput(dstUid, "Enter Neteeza database User ID");
		    dstPwd = getStringConsoleInput(dstPwd, "Enter Neteeza database Passsword");				
		} else
		{
	        System.out.println("Oops. Something went wrong. Are you trying to use the tool for z/OS DB2 but not running it from the USS on z/OS. Please check.");
	        System.exit(-1);
			
		}
		
		boolean remote = (remoteLoad.equalsIgnoreCase("true")) ? true : false;
		if (Constants.db2() || Constants.zdb2())
		{
			if (isTargetInstalled)
			{
			    if (srcVendor.equalsIgnoreCase(Constants.sybase) && db2Compatibility)
			    {
			    	dstJDBC = getJDBCLocations(Constants.ants, dstJDBC);
			    } else
			    	dstJDBC = getJDBCLocations(getDstVendor(), dstJDBC);
			    AddJarsToClasspath(dstJDBC);
				if (IBMExtractUtilities.TestConnection(remote, db2Compatibility, getDstVendor(), dstServer, dstPort, dstDBName, dstUid, dstPwd, ""))
				{
					dstDB2Home = IBMExtractUtilities.DB2Path;
					dstDB2Instance = IBMExtractUtilities.InstanceName;
					dstDB2FixPack = IBMExtractUtilities.fixPack;
					dstDBRelease = IBMExtractUtilities.ReleaseLevel;
				} else
					System.out.println(IBMExtractUtilities.Message);
			} else
			{
				dstDB2Home = getStringConsoleInput(dstDB2Home, "Enter db2 Home directory Name");	
				dstDB2Instance = getStringConsoleInput(dstDB2Instance, "Enter db2 instance name");	
				dstDBRelease = getStringConsoleInput(dstDBRelease, "Enter db2 version # (e.g. 9.7)");	
				dstDB2FixPack = getStringConsoleInput(dstDB2FixPack, "Enter db2 Fix Pack number");	
			}			
		} else
		{
	    	dstJDBC = getJDBCLocations(getDstVendor(), dstJDBC);
		    AddJarsToClasspath(dstJDBC);
			if (IBMExtractUtilities.TestConnection(remote, db2Compatibility, getDstVendor(), dstServer, dstPort, dstDBName, dstUid, dstPwd, ""))
			{
				dstDB2Home = IBMExtractUtilities.DB2Path;
				dstDB2Instance = IBMExtractUtilities.InstanceName;
				dstDB2FixPack = IBMExtractUtilities.fixPack;
				dstDBRelease = IBMExtractUtilities.ReleaseLevel;
			} else
				System.out.println(IBMExtractUtilities.Message);			
		}
		
		try
    	{
			writeConfigFile();
			if (getNumJVM() > 1)
	    	{
				String outputDir = getOutputDirectory();
				boolean optimize = true;
				String tableScriptFile = outputDir + IBMExtractUtilities.filesep + getSrcDBName()+".tables";
				GenerateParallelScripts.GenParallelScripts(outputDir, getSrcVendor(), tableScriptFile, getNumJVM(), 
	    				getNumThreads(), getSrcServerName(), getSrcDBName(), getSrcPort(), getSrcUid(), getSrcPwd(), optimize);
	    	} 
			if (getNumJVM() > 1)
	    	{
	    		for (int i = 0; i < getNumJVM(); ++i)
	    		{
	    			String dirName = getOutputDirectory() + IBMExtractUtilities.filesep + "work" +
					    IBMExtractUtilities.pad((i+1), 2, "0");
		    		writeUnload(dirName, unload, i+1);
		    		writeRowCount(dirName);
		    		if (Constants.db2())
		    		{
			    		if (loadException.equalsIgnoreCase("true"))
			    			writeExceptRowCount(dirName);
			    		writeFixCheck(dirName);
			    		writeDataCheck(dirName);
		    		} else if (Constants.zdb2())
		    		{
		    			if (!Constants.zos())
		    			{
		    				writezLoad(dirName);
		    			}
		    		}
	    		}
	    	} else
	    	{
			    writeGeninput();
	    	    writeUnload(outputDirectory, unload,1);
	    	    writeRowCount(outputDirectory);
	    		if (Constants.db2())
	    		{
		    	    if (loadException.equalsIgnoreCase("true"))
		    	    	writeExceptRowCount(outputDirectory);
		    		writeFixCheck(outputDirectory);
		    		writeDataCheck(outputDirectory);
	    		} else if (Constants.zdb2())
	    		{
	    			if (!Constants.zos())
	    			{
	    				writezLoad(outputDirectory);
	    			}
	    		}
	 	        System.out.println("Input Data collection is complete now.");
	  	        System.out.println();
	 	        System.out.println("Change directory to :" + outputDirectory);
	    	    if (Constants.win())
	    	    {
	    	        System.out.println("Run unload.cmd command to extract data from " + srcVendor);
	     	        System.out.println("After completion of 'unload' command, run ");
	     	        System.out.println("if only DDL was selected              : " + Constants.ddlScript);
	     	        System.out.println("if only Data was selected             : " + Constants.loadScript);
	     	        System.out.println("if both DDL and Data were was selected: " + Constants.genScript);
	      	        System.out.println();
	     	        System.out.println("Instructions for using pipe or syncload between " + srcVendor + " and " + Constants.getDbTargetName());
	      	        System.out.println();
	      	        System.out.println("Open unload.cmd in an editor.");
	      	        System.out.println("Set GENDDL=TRUE, UNLOAD=FALSE, OBJECTS=TRUE, USEPIPE=FALSE, SYNCLOAD=FALSE and save the file");
	      	        System.out.println("Run unload.cmd to extract and run "+Constants.ddlScript+" to create all obejcts");
	      	        System.out.println("Open unload.cmd in an editor.");
	      	        System.out.println("Set GENDDL=FALSE, UNLOAD=TRUE, OBJECTS=FALSE, USEPIPE=TRUE, SYNCLOAD=FALSE and save the file");
	      	        System.out.println("Run unload.cmd It will extract and deploy data simultaneously");
	    	    }
	    	    else if (Constants.zos())
	    	    {
	    	        System.out.println("Run ./unload to extract data from " + srcVendor);
	    	        System.out.println("After completion of 'unload' command, run ");
	     	        System.out.println("if only DDL was selected              : ./" + Constants.ddlScript);
	     	        System.out.println("if only Data was selected             : ./" + Constants.loadScript);
	     	        System.out.println("if both DDL and Data were was selected: ./" + Constants.genScript);
	     	        System.out.println();
	      	        System.out.println("Run ./unload to extract and ./" + Constants.genScript + " to deply");
	    	    }
	    	    else
	    	    {
	    	        System.out.println("Run ./unload to extract data from " + srcVendor);
	    	        System.out.println("After completion of 'unload' command, run ");
	     	        System.out.println("if only DDL was selected              : ./" + Constants.ddlScript);
	     	        System.out.println("if only Data was selected             : ./" + Constants.loadScript);
	     	        System.out.println("if both DDL and Data were was selected: ./" + Constants.genScript);
	     	        System.out.println();
	     	        System.out.println("Instructions for using pipe or syncLoad between " + srcVendor + " and " + Constants.getDbTargetName());
	      	        System.out.println();
	      	        System.out.println("Open unload.cmd in an editor.");
	      	        System.out.println("Set GENDDL=TRUE, UNLOAD=FALSE, OBJECTS=TRUE, USEPIPE=FALSE, SYNCLOAD=FALSE and save the file");
	      	        System.out.println("Run ./unload to extract and run ./" + Constants.ddlScript + " to create all obejcts");
	      	        System.out.println("Open unload.cmd in an editor.");
	      	        System.out.println("Set GENDDL=FALSE, UNLOAD=TRUE, OBJECTS=FALSE, USEPIPE=TRUE, SYNCLOAD=FALSE and save the file");
	      	        System.out.println("Run ./unload It will extract and deploy data simultaneously");
	    	    }
      	        System.out.println();
      	        System.out.println("*** Thank you for using IBM Data Movement Tool. **** ");    	    
      	        System.out.println();
	    	}
    	} catch (Exception e)
    	{
    		e.printStackTrace();
    	}
	}
	
	public void AddJarsToClasspath(String jarList)
	{
		String[] tmp = jarList.split(sep);
		for (int i = 0; i < tmp.length; ++i)
		{
			File f = new File(tmp[i]);
			try
			{
				IBMExtractUtilities.AddFile(f);
			} catch (IOException e1)
			{
				e1.printStackTrace();
			}
		}		
	}
	
	public String getJDBCLocations(String vendor, String JDBCDrivers)
	{
		int num;
		String name, inputa, jdbcLocations = "", location;
		String targetJDBCHome = "";
		
		if (vendor.equalsIgnoreCase(Constants.db2luw) || vendor.equalsIgnoreCase(Constants.ants)
				|| vendor.equalsIgnoreCase(Constants.netezza))
		   targetJDBCHome = IBMExtractUtilities.targetJDBCHome(vendor);
		
	    System.out.println("Verifying locations of "+vendor+"'s JDBC drivers");
	    
	    if (pingJDBCDrivers(JDBCDrivers))
	    	return JDBCDrivers;
	    else
	    {
			String jdbcNames = (String) propJDBC.get(vendor.toLowerCase());
			String[] tmp2 = jdbcNames.split("\\|\\|");
			System.out.println("Enter a number to select which JDBC driver(s) to include");
			do
			{
				num = 1;
				for (int j = 0; j < tmp2.length; ++j)
				{
					System.out.println(tmp2[j] + " :  " + (j+1));
				}
				System.out.print("Enter a number (Default 1) : ");
				try
				{
					inputa = stdin.readLine();
					num = (inputa.equals("")) ? 1 : Integer.parseInt(inputa);
				}
				catch(Exception e)
				{
					num = -1;
				}
			} while (!(num >= 1 && num <= tmp2.length));
			
			String[] tmp = tmp2[num-1].split(":");
			for (int i = 0; i < tmp.length; ++i)
			{
		        name = tmp[i];
				do
				{
					num = 1;
					System.out.println("Do you want to include "+name+" (Yes) : 1 ");
					System.out.println("No                                    : 2 ");
					System.out.print("Enter a number (Default 1) : ");
					try
					{
						inputa = stdin.readLine();
						num = (inputa.equals("")) ? 1 : Integer.parseInt(inputa);
					}
					catch(Exception e)
					{
						num = -1;
					}
				} while (!(num >= 1 && num <= 2));		
				location = "";
				if (num == 1)
				{
					do
					{
						num = 1;
						location = getStringConsoleInput(targetJDBCHome, "Provide location of '" + name + "'");
						location = IBMExtractUtilities.getAbsolutePath(location + filesep + name);
					    num = pingJDBCDrivers(location) ? -1 : 1;
					} while (num == 1);
				}
				if (location.length() > 0)
				{
					if (i > 0)
						jdbcLocations += sep;
					jdbcLocations += location;
				}
			}
	    }
	    return jdbcLocations;
	}
	
	private static void log(String msg)
    {
        System.out.println("[" + Constants.timestampFormat.format(new Date()) + "] " + msg);
    }
    
    public static void main(String[] args)
    {    	
    	IBMExtractConfig cfg = new IBMExtractConfig();
    	cfg.loadConfigFile();
    	cfg.getParamValues();
    	cfg.collectInput();    	
    }
}
