package ibm;

import java.text.SimpleDateFormat;

public class Constants
{
	final static String filesep = System.getProperty("file.separator");
	final static String linesep = System.getProperty("line.separator");
	final static String OUTPUT_DIR = System.getProperty("OUTPUT_DIR") == null ? "." + filesep : System.getProperty("OUTPUT_DIR") + filesep;
	private final static String osType = (System.getProperty("os.name").toUpperCase().startsWith("WIN")) ? "WIN" : 
    	(System.getProperty("os.name").startsWith("z/OS")) ? "z/OS" : "OTHER";		
	final static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
	final static String EncrySeed = "00210040002300240025";
	final static String URL_PROP_FILE = "url.properties", DRIVER_PROP_FILE = "driver.properties";
	final static String DB2SKIN = "ants";
	final static String CUSTMAP_PROP_FILE = "CustomDataMapping.properties";    
	final static String CUSTNULL_PROP_FILE = "CustomNullMapping.properties"; 
	final static String CUSTCOLNAME_PROP_FILE = "CustomColNameMapping.properties"; 
	final static String CUSTOM_COLUMN_DISTRIBUTION = "CustomColumnDistribution.properties";
	final static String JDBC_PROP_FILE = "jdbcdriver.properties";
	final static String CUSTTSBP_PROP_FILE = "CustomTableSpaceBufferPoolMapping.properties";
	final static String SCHEMA_EXCLUDE_FILE = "SchemaExcludeList.properties";
	final static String DEPLOY_FILES_FILE = "DeployFiles.properties";	
	final static String DEPLOYED_OBJECT_FILE = "DeployedObjects.properties";
	final static String MAP_COMPATIBILITY_FILES = "compatibilityFiles.properties";
	final static String WHATISNEW = "whatisnew.txt";
	final static String LAST_USED_CONFIG_FILE = "LastUsedConfig.properties";
	final static String DATAMAP_PROP_FILE = "datamap.properties";
	final static String DATAMAPNZ_PROP_FILE = "datamapnz.properties";
	final static String IDMT_CONFIG_FILE = "IDMTConfig.properties";
	final static String OLD_IDMT_CONFIG_FILE = "IBMExtract.properties";
	final static int parameterMarkersLimit = 30000;
	final static byte   dbquote = (byte) 0x22;
	final static byte   escape = (byte) 0x5C;
	final static byte   LF = (byte) 0x0A;
	final static byte   CR = (byte) 0x0D;
	final static byte   NUL = (byte) 0x00;
	final static String zOSLoadPDSName = "IDMTLOAD";
	final static String zCommentLine = "//**************************************************************";
	
	
	final static int netezzaMaxlength = 60000;
	final static int readLobBuffer =  32000; // 1024000;
	final static int readFileBuffer = 2097152;
	
	final static String netezza = "netezza";
	final static String db2luw_compatibility = "db2luw_compatibility";
	final static String db2luw = "db2luw";
	final static String zdb2 = "zdb2";
	final static String informix = "informix";
	final static String oracle = "oracle";
	final static String postgres = "postgres";
	final static String mysql = "mysql";
	final static String mssql = "mssql";
	final static String access = "access";
	final static String hxtt = "hxtt";
	final static String idb2 = "idb2";
	final static String domino = "domino";
	final static String teradata = "teradata";
	final static String ants = "ants";
	final static String sybase = "sybase";

	final static String netezzaPort = "5480";
	final static String db2luwPort = "50000";
	final static String zdb2Port = "546";
	final static String informixPort = "1526";
	final static String oraclePort = "1521";
	final static String postgresPort = "5432";
	final static String mysqlPort = "3306";
	final static String mssqlPort = "1433";
	final static String accessPort = "0";
	final static String hxttPort = "0";
	final static String idb2Port = "0";
	final static String dominoPort = "0";
	final static String teradataPort = "1025";
	final static String antsPort = "50000";
	final static String sybasePort = "4100";

	final static int ERROR_PIPE_CONNECTED = 535;
	final static int ERROR_BROKEN_PIPE = 109;

	public static String prefix, sqlsep;
	
	public static String fixlobs, load, tempTables, exceptiontables, excepttabcount, nzLoadScript, runstats, tabstatus, tabcount, loadterminate, checkpending, dropobjects;
	public static String views, tables, fkeys, droptables, dropexceptiontables, pkeys, dropfkeys, indexes, check, sequences, dropsequences, synonyms, dropsynonyms, mviews;
	public static String roleprivs, objprivs, logins, groups, udf, tsbp, droptsbp, defalt, truncname, checkremoval, dropscripts, genScript, loadScript, ddlScript;
	
	private static String dbTargetName = "";
	

	public static String getDbTargetName()
	{
		return dbTargetName;
	}

	private static void setFileName()
	{
		prefix = netezza() ? "nz" : "db2";
		sqlsep = netezza() ? ".." : ".";
		if (netezza())
		{
			fixlobs         = "nzfixlobs.sql";
			load            = "nzload.sql";
			exceptiontables = "nzexceptiontables.sql";
			excepttabcount  = "nzexcepttabcount.sql";
			dropexceptiontables = "nzdropexceptiontables";
		    runstats        = "nzrunstats.sql";
		    tabstatus       = "nztabstatus.sql";
		    tabcount        = "nztabcount.sql";
		    loadterminate   = "nzloadterminate.sql";
		    checkpending    = "nzcheckpending.sql";
		    dropobjects     = "nzdropobjects.sql";
		    views           = "nzviews.nz";
		    tables          = "nztables.sql";
		    fkeys           = "nzfkeys.sql";
		    droptables      = "nzdroptables.sql";
		    pkeys           = "nzpkeys.sql";
		    dropfkeys       = "nzdropfkeys.sql";
		    indexes         = "nzindexes.sql";
		    check           = "nzcheck.sql";
		    sequences       = "nzsequences.sql";
		    dropsequences   = "nzdropsequences.sql";
		    synonyms        = "nzsynonyms.sql";
		    dropsynonyms    = "nzdropsynonyms.sql";
		    mviews          = "nzmviews.nz";
		    roleprivs       = "nzroleprivs.nz";
		    objprivs        = "nzobjprivs.nz";
		    logins          = "nzlogins.nz";
		    groups          = "nzgroups.nz";
		    udf             = "nzudf.sql";
		    tsbp            = "nzdatabases.sql";
		    droptsbp        = "nzdropdatabases.sql";
		    defalt          = "nzdefault.sql";
		    truncname       = "nztruncname.txt"; 
		    if (osType.equalsIgnoreCase("Win"))
		    {
		       checkremoval    = "nzcheckremoval.cmd";
		       dropscripts     = "nzdropobjects.cmd";
		       genScript       = "nzgen.cmd";
		       loadScript      = "nzload.cmd";
		       ddlScript       = "nzddl.cmd";
		       nzLoadScript    = "runnzload.cmd";
		    } 
		    else
		    {
			   checkremoval    = "nzcheckremoval.sh";
			   dropscripts     = "nzdropobjects.sh";
		       genScript       = "nzgen.sh";
		       loadScript      = "nzload.sh";
		       ddlScript       = "nzddl.sh";
		       nzLoadScript    = "runnzload.sh";
		    }
		} else
		{
			fixlobs         = "db2fixlobs.sql";
			load            = "db2load.sql";
			exceptiontables = "db2exceptiontables.sql";
			dropexceptiontables = "db2dropexceptiontables.sql";
			excepttabcount  = "db2excepttabcount.sql";
		    runstats        = "db2runstats.sql";
		    tabstatus       = "db2tabstatus.sql";
		    tabcount        = "db2tabcount.sql";
		    loadterminate   = "db2loadterminate.sql";
		    checkpending    = "db2checkpending.sql";
		    dropobjects     = "db2dropobjects.sql";
		    tempTables      = "db2temptables.sql";
		    views           = "db2views.db2";
		    tables          = "db2tables.sql";
		    fkeys           = "db2fkeys.sql";
		    droptables      = "db2droptables.sql";
		    pkeys           = "db2pkeys.sql";
		    dropfkeys       = "db2dropfkeys.sql";
		    indexes         = "db2indexes.sql";
		    check           = "db2check.sql";
		    sequences       = "db2sequences.sql";
		    dropsequences   = "db2dropsequences.sql";
		    synonyms        = "db2synonyms.sql";
		    dropsynonyms    = "db2dropsynonyms.sql";
		    mviews          = "db2mviews.db2";
		    roleprivs       = "db2roleprivs.db2";
		    objprivs        = "db2objprivs.db2";
		    logins          = "db2logins.db2";
		    groups          = "db2groups.db2";
		    udf             = "db2udf.sql";
		    tsbp            = "db2tsbp.sql";
		    droptsbp        = "db2droptsbp.sql";
		    defalt          = "db2default.sql";
		    truncname       = "db2truncname.txt";
		    if (osType.equalsIgnoreCase("Win"))
		    {
			   checkremoval    = "db2checkRemoval.cmd";
			   dropscripts     = "db2dropobjects.cmd";
		       genScript       = "db2gen.cmd";
		       loadScript      = "db2load.cmd";
		       ddlScript       = "db2ddl.cmd";
		    }
		    else
		    {
			   checkremoval    = "db2checkRemoval.sh";
			   dropscripts     = "db2dropobjects.sh";
		       genScript       = "db2gen.sh";
		       loadScript      = "db2load.sh";
		       ddlScript       = "db2ddl.sh";
		    }
		}
	}
	
	public static boolean win()
	{
		return osType.equalsIgnoreCase("Win");
	}
	
	public static boolean zos()
	{
		return osType.equalsIgnoreCase("z/OS");
	}

	public static void setDbTargetName(String dstVendor)
	{
		System.setProperty("TARGET_DB", dstVendor);
		Constants.dbTargetName = dstVendor;
		setFileName();
	}
	
	public static void setDbTargetName()
	{
		if (zos())
			Constants.dbTargetName = "zdb2";
		else
		{
			String dstVendor = (String) System.getProperty("TARGET_DB");
			if (dstVendor == null || dstVendor.length() == 0)
			    Constants.dbTargetName = db2luw;
			else
			{
				Constants.dbTargetName = dstVendor;				
			}
		}
		setFileName();
	}

	public static boolean db2()
    {
		return dbTargetName.equalsIgnoreCase(db2luw) || dbTargetName.equalsIgnoreCase(db2luw_compatibility);
    }
    
	public static boolean zdb2()
    {
    	return dbTargetName.equalsIgnoreCase(zdb2);
    }
    
	public static boolean netezza()
    {
    	return dbTargetName.equalsIgnoreCase(netezza);
    }
	
	public static boolean db2Compatibility()
	{
		return dbTargetName.equalsIgnoreCase(db2luw_compatibility);
	}
}
