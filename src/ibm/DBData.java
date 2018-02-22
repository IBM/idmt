package ibm;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBData
{
	private static Properties propURL = null, propDrivers = null;
	public String Message = "", releaseLevel, productName;
	public int majorSourceDBVersion = -1, minorSourceDBVersion = -1;
	
	private PrintStream ps = null;
	public Connection connection = null; 
	private String server;
	private int port;
	private String dbName;
	private String userid;
	private String pwd;
	private String extraParam;
	private int threadNum;
	private boolean autoCommit = false;
	public boolean isOpen = false;
	public String dbSourceName, dbTargetName;
	
	
	public DBData(String dbSourceName, String server, int port, String dbName, String userid, String pwd, String extraParam, int threadNum)
	{
		this.dbSourceName = dbSourceName;
		this.server = server;
		this.port = port;
		this.dbName = dbName;
		this.userid = userid;
		this.pwd = pwd;
		this.extraParam = extraParam;
		this.threadNum = threadNum;
		
        if (Domino())
            dbName = server;
        
        if (!(CheckSupportedVendor()))
        {
            IBMExtractUtilities.log("Invalid dbSourceName supplied '" + dbSourceName + "'");
            System.exit(-1);
        }
	}
	
	public String getUserID()
	{
		return userid;
	}
	
	public String getReleaseLevel()
	{
		return majorSourceDBVersion + "." + minorSourceDBVersion;
	}
	
    private String getURL(String dbSourceName, String server, int port, String dbName)
    {
        String url = "";
		InputStream istream;
		
		server = IBMExtractUtilities.removeQuote(server);
        try
		{
			if (propURL == null)
			{		
		        propURL = new Properties();
		        istream = ClassLoader.getSystemResourceAsStream(Constants.URL_PROP_FILE);
		        if (istream == null)
		        {
		        	FileInputStream finStream = new FileInputStream(Constants.URL_PROP_FILE);
					propURL.load(finStream);
					finStream.close();
		        } else
		        {
		            propURL.load(istream);
		            istream.close();
		        }
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
        
        if (Oracle())
        {
        	if (dbName.toLowerCase().startsWith("jdbc:oracle"))
        	{
        	   url = dbName;	
        	} else
               url = (String) propURL.get(dbSourceName) + server + ":" + port + ":" + dbName;
        } else if (Mssql())
        {
            String[] strs = server.split(",");
    		if (strs.length == 2)
    		{
               url = (String) propURL.get(dbSourceName) + strs[0] + ":" + port + ";instanceName="+strs[1]+";database=" + dbName;
    	    } else
    		{
               url = (String) propURL.get(dbSourceName) + server + ":" + port + ";database=" + dbName;
    		}
        }
        else if (Access())
        {
        	if (server.matches("(?i).*\\.mdb"))
        	    url = (String) propURL.get(dbSourceName) + server;
        	else
        		url = "odbc:jdbc:" + server;
        } else if (Hxtt() || Domino())
        {
            url = (String) propURL.get(dbSourceName) + server;
        } else if (Mysql()) 
        {
        	// Please note that sessionVariables=sql_mode='ANSI_QUOTES' is added to allow use of reserved words in table names etc.
        	// useUnbufferedInput=true&useReadAheadInput=false should be tried to improve executeQuery perf for v large MySQL tables.
        	url = (String) propURL.get(dbSourceName) + server + ":" + port + "?zeroDateTimeBehavior=round&sessionVariables=sql_mode='ANSI_QUOTES'&nullCatalogMeansCurrent=false";
        }
        else if (zDB2()) 
        {
           url = (String) propURL.get(dbSourceName) + server + ":" + port + "/" + dbName + ":retrieveMessagesFromServerOnGetMessage=true;emulateParameterMetaDataForZCalls=1;";                           
        }
        else if (iDB2()) 
        {
           url = (String) propURL.get(dbSourceName) + server + ":" + port + "/" + dbName + ";date format=iso;extended metadata=true;";                           
        }
        else if (Sybase()) 
        {
        	// Use this url if there is sybase connection problem.
            //url = (String) propURL.get(dbSourceName) + server + ":" + port + "/" + dbName + "?charset=" + System.getProperty("file.encoding");
            url = (String) propURL.get(dbSourceName) + server + ":" + port + "/" + dbName;
        }
        else if (Teradata())
        {
        	if (port == 1025)
               url = (String) propURL.get(dbSourceName) + server + "/DATABASE=" + dbName;
        	else
        	   url = (String) propURL.get(dbSourceName) + server + "/DATABASE=" + dbName + ",DBS_PORT=" + port;	 
        	String extraParam = IBMExtractUtilities.getIDMTProperty("teradataConnStringExtraParam");
        	if (extraParam != null && extraParam.length() > 0)
        		url = url + "," + extraParam;
        }
        else if (Informix())
        {
        	boolean ifx = ifxjdbc();
        	if (ifx)
        	{
        		String[] strs = server.split(",");
        		if (strs.length == 2)
        		{
        		   url = (String) propURL.get(dbSourceName) + strs[0] + ":" + port + "/" + dbName + ":INFORMIXSERVER=" + strs[1] + ";DELIMIDENT=y;";
        	    } else
        		{
         		   url = (String) propURL.get(dbSourceName) + server + ":" + port + "/" + dbName + ":INFORMIXSERVER=" + dbName + ";DELIMIDENT=y;";;        			
        		}
        	} else
        	{
        		String[] strs = server.split(",");
        		if (strs.length == 2)
        		{
        		    url = (String) propURL.get("db2luw") + strs[0] + ":" + port + "/" + dbName + ":retrieveMessagesFromServerOnGetMessage=true;DELIMIDENT=true;";
        		} else
        		{
        		    url = (String) propURL.get("db2luw") + server + ":" + port + "/" + dbName + ":retrieveMessagesFromServerOnGetMessage=true;DELIMIDENT=true;";        			
        		}
        	}
        }
        else if (SKIN())
        {
        	url = (String) propURL.get(dbSourceName) + server + ":" + port + "/master";
        }
        else 
        {
            url = (String) propURL.get(dbSourceName) + server + ":" + port + "/" + dbName;                
        }
        String additionalURL = IBMExtractUtilities.getDefaultString(IBMExtractUtilities.getIDMTProperty("additionalURLProperties"), "null") ;
        if (!additionalURL.equalsIgnoreCase("null"))
        {
        	if (url.endsWith(";"))
        	{
        		url = url.substring(0, url.length()-1);
        	}
        	if (additionalURL.startsWith(";"))
        	{
        		additionalURL = additionalURL.substring(1);
        	}
        	if (additionalURL.endsWith(";"))
        	{
        		additionalURL = additionalURL.substring(0, additionalURL.length()-1);
        	}
        	url = url + ";" + additionalURL + ";";
        }
        return url;
    }
    
    private String fixSybaseDriverName(String sybDriverName)
    {
    	String driverName = sybDriverName;
    	String classpath = IBMExtractUtilities.getClasspath();
    	boolean found3 = (classpath == null || classpath.trim().length() == 0) ? 
    			false : classpath.matches(".*jconn3.jar.*");
    	if (found3)
    	{
    		driverName = "com.sybase.jdbc3.jdbc.SybDriver";
    		IBMExtractUtilities.log(ps,"Sybase Driver Name " + driverName); 
    	}
    	boolean found4 = (classpath == null || classpath.trim().length() == 0) ? 
    			false : classpath.matches(".*jconn4.jar.*");
    	if (found4)
    	{
    		driverName = "com.sybase.jdbc4.jdbc.SybDriver";
    		IBMExtractUtilities.log(ps,"Sybase Driver Name " + driverName); 
    	}
    	if (found3 && found4)
    	{
    		IBMExtractUtilities.log(ps,"It appears that you have jconn3.jar and jconn4.jar in your classpath. Please use one of them.");
    	}
    	return driverName;
    }
    
    private String fixAntsClassName(String antsClassName)
    {
    	String convClassName = antsClassName;
    	String classpath = IBMExtractUtilities.getClasspath();
    	boolean found3 = (classpath == null || classpath.trim().length() == 0) ? 
    			false : classpath.matches(".*antsjconn3.jar.*");
    	if (found3)
    	{
    		convClassName = "com.ants.sybase.db2.jdbc3.jdbc.AntsSybDB2Driver";
    	}
    	boolean found2 = (classpath == null || classpath.trim().length() == 0) ? 
    			false : classpath.matches(".*antsjconn2.jar.*");
    	if (found2)
    	{
    		convClassName = "com.ants.sybase.db2.jdbc2.jdbc.AntsSybDB2Driver";
    	}
    	return convClassName;    	
    }
    
    private boolean ifxjdbc()
    {
    	String classpath = IBMExtractUtilities.getClasspath();
    	return classpath.matches(".*ifxjdbc.jar.*");
    }
    
    private String getDriverName(String dbSourceName)
    {
    	String driverName;
		InputStream istream;
		
		boolean ifx = ifxjdbc();
    	if (!ifx && dbSourceName.equalsIgnoreCase("informix"))
    		dbSourceName = "db2luw";
        try
		{
			if (propDrivers == null)
			{
		        propDrivers = new Properties();
	            istream = ClassLoader.getSystemResourceAsStream(Constants.DRIVER_PROP_FILE);
	            if (istream == null)
	            {
	            	FileInputStream finStream = new FileInputStream(Constants.DRIVER_PROP_FILE);
	                propDrivers.load(finStream);
	                finStream.close();
	            } else
	            {
	                propDrivers.load(istream);
	            	istream.close();
	            }
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}			
    	driverName = (String) propDrivers.getProperty(dbSourceName.toLowerCase());
		if (dbSourceName.equalsIgnoreCase("sybase"))
		{
			driverName = fixSybaseDriverName(driverName);
		}
    	return driverName;
    }
    
	public Connection getConnection()
	{
		if (connection != null)
			return connection;
		
		String driverName = getDriverName(dbSourceName);
		Properties login = new Properties();
		
		if (SKIN())
		{
        	driverName = getDriverName(Constants.DB2SKIN);
        	driverName = fixAntsClassName(driverName);
		}
        try
		{
			Class.forName(driverName).newInstance();
		} catch (Exception e)
		{
			e.printStackTrace();
			Message = "Driver "+driverName+" could not be loaded. See console's output.";
			return null;
		} 
		IBMExtractUtilities.log(ps, "Driver " + driverName + " loaded");

		try
		{
	        String url = getURL(dbSourceName, server, port, dbName);
	        if (Domino())
	           connection = DriverManager.getConnection(url);
	        else
	        {
	           String pass = (pwd == null) ? "" : pwd;
	           pass = (pass.matches("^\\s*$")) ? "" : pass;
	           login.put("user", userid);
	           login.put("password", pass);
	           if (Sybase())
	           {                   
					if (!(extraParam == null || extraParam.length() == 0))
					{
			        	 login.setProperty("CHARSET_CONVERTER_CLASS", extraParam);						
					}
	           } else if (Mssql())
	           {
	               login.setProperty("sendStringParametersAsUnicode","true");
	               login.setProperty("selectMethod","cursor");
	           } else if (DB2())
	           {
	        	   if (threadNum == 0)
	                  login.put("clientProgramName", "IDMTX");
	        	   else
	        	      login.put("clientProgramName", "IDMT" + threadNum);
	           } else if (SKIN())
	           {
	        	   login.put("DB2_DATABASE",dbName);
	           }

	           connection = DriverManager.getConnection(url, login);
	           if (!Informix())
	               connection.setAutoCommit(autoCommit);
	           
	           DatabaseMetaData md = connection.getMetaData(); 
	           
	           try 
	           {
	        	   productName = md.getDatabaseProductName();
	        	   IBMExtractUtilities.log(ps,"Database Product Name :" + productName); 
	           } catch (Exception e) 
	           {
	        	   productName = dbSourceName;
	           }
	           try {IBMExtractUtilities.log(ps,"Database Product Version :" + md.getDatabaseProductVersion()); } catch (Exception e) {}
	           try {IBMExtractUtilities.log(ps,"JDBC driver " + md.getDriverName() + " Version = " + md.getDriverVersion()); } catch (Exception e) {}
	           
	           String versionJDBC = md.getDriverVersion();
	           String versionDBMS = md.getDatabaseProductVersion();
	           if (Oracle())
	           {
		           try 
		           {
		           	  majorSourceDBVersion = md.getDatabaseMajorVersion(); 
		           	  minorSourceDBVersion = md.getDatabaseMinorVersion();
		           } catch (Exception e) 
		           {
		           	  String maj, min;
		           	  int pos2, pos = versionJDBC.indexOf(".");
		           	  if (pos > 0)
		           	  {
		           		 maj = versionJDBC.substring(0,pos);
		           	     pos2 = versionJDBC.indexOf(".", pos+1);
		           	     min = versionJDBC.substring(pos+1,pos2);
		           	     try
		           	     {
		           	     	majorSourceDBVersion = Integer.parseInt(maj);
		           	     	minorSourceDBVersion = Integer.parseInt(min);
		           	     } catch (Exception ex)
		           	     {
		           	    	IBMExtractUtilities.log(ps,"Could not determine database version.");
		           	     }
		           	   } else
		           	   {
		           		IBMExtractUtilities.log(ps,"Could not determine database version information.");
		           	   }
		           }
	           } else if (Sybase())
	           {
		           try
		           {
			           	majorSourceDBVersion = md.getDatabaseMajorVersion(); 
			           	minorSourceDBVersion = md.getDatabaseMinorVersion();
		           } catch (Exception e)
		           {
		           		String maj;
		           		int pos = versionDBMS.indexOf("/"), pos2;
		               	if (pos > 0)
		               	{
		               	    pos2 = versionDBMS.indexOf("/", pos+1);
		               		maj = versionDBMS.substring(pos+1,pos2);
		               		String[] toks = maj.split("\\.");
		               	    try
		               	    {
		               	    	majorSourceDBVersion = Integer.parseInt(toks[0]);
		               	    	minorSourceDBVersion = Integer.parseInt(toks[1]);
		               	    } catch (Exception ex)
		               	    {
		               	    	IBMExtractUtilities.log(ps,"Could not determine database version.");
		               	    }
		               	} else
		               	{
		               		IBMExtractUtilities.log(ps,"Could not determine database version information.");
		               	}
		           }
	            } else if (Mssql())
	            {
	            	versionJDBC = IBMExtractUtilities.executeSQL(connection, "select convert(varchar(100), SERVERPROPERTY('ProductVersion'))", "", false);
	            	String maj, min;
	            	int pos2, pos = versionJDBC.indexOf(".");
	            	if (pos > 0)
	            	{
	            		maj = versionJDBC.substring(0,pos);
	            	    pos2 = versionJDBC.indexOf(".", pos+1);
	            	    min = versionJDBC.substring(pos+1,pos2);
	            	    try
	            	    {
	            	    	majorSourceDBVersion = Integer.parseInt(maj);
	            	    	minorSourceDBVersion = Integer.parseInt(min);
	            	    } catch (Exception ex)
	            	    {
	            	    	IBMExtractUtilities.log(ps,"Could not determine database version.");
	            	    }
	            	} else
	            	{
	            		IBMExtractUtilities.log(ps,"Could not determine database version information.");
	            	}
	            }
	            else
	            {
	            	try
	            	{
	            		if (!Access())
	            		{
			           	   majorSourceDBVersion = md.getDatabaseMajorVersion(); 
			           	   minorSourceDBVersion = md.getDatabaseMinorVersion();
	            		}
	            	} catch (Exception e)
	            	{
	            		e.printStackTrace();
	            	}
	            }
	           	IBMExtractUtilities.log(ps,"Database Major Version :" + majorSourceDBVersion); 
	           	IBMExtractUtilities.log(ps,"Database Minor Version :" + minorSourceDBVersion);
	        }
	        isOpen = true;
		} catch (SQLException e)
        {
        	if (e.getErrorCode() == -4499)
        	{
        		if (e.getMessage().contains("An attempt was made to access a database"))
        			Message = "Connection succeeded but database not found";
        		else if(e.getMessage().contains("Error opening socket"))
        			Message = "Error opening socket to DB2 Server. Check if DB2 instance is up. If yes, check check port number.";
        		else
        			Message = "Error connecting to DB2 server";
        		IBMExtractUtilities.log(ps,"Error Message : " + e.getMessage());
        	} else if (e.getErrorCode() == -4214)
        	{
        		Message = "Connection succeeded but userid/password is incorrect.";
        	}
        	else if (Access())
        	{
        		if (e.getMessage().startsWith("[Microsoft][ODBC Driver Manager] Data source name not found"))
        		{
        			IBMExtractUtilities.log(ps,"It appears that you do not have " + System.getProperty("sun.arch.data.model") + " bit MDAC driver installed on this system for ACCESS database.");
        			IBMExtractUtilities.log(ps,"Or Override javaHome property in "+IBMExtractUtilities.getConfigFile()+" file with 32 bit Java and run again.");
        			IBMExtractUtilities.log(ps,"Or Change JAVA_HOME in the script to the 32 bit Java and try again from the command line.");
        		} else
        		{
        			IBMExtractUtilities.log(ps,"Error for : " + driverName + " for " + dbSourceName + " Error Message :" + e.getMessage());
                    e.printStackTrace();
        		}
        	}
        	else
        	{
        		if (e.getMessage().contains("User ID or Password invalid"))
        			Message = "Error connecting. User ID or Password invalid";
        		else if (e.getMessage().contains("The Network Adapter could not establish the connection"))
        			Message = "The Network Adapter could not establish the connection";
        		else if (e.getMessage().contains("The TCP/IP connection to the host  has failed"))
        			Message = "The TCP/IP connection to the host  has failed";
        		else if (e.getMessage().contains("Login failed for user"))
        			Message = "Login failed for user";
        		else
        			Message = dbSourceName + " JDBC connection problem. Please see console's output.";
        		IBMExtractUtilities.log(ps,"Error connecting : " + driverName + " for " + dbSourceName + " Error Message :" + e.getMessage());               
        	}
        	IBMExtractUtilities.log(ps,Message);
            e.printStackTrace();
            connection = null;
        }
		return connection;
	}
	
	public void close()
	{
		commit();
		if (connection != null)
		{
			try
			{
			   connection.close();
			   isOpen = false;
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public void commit()
	{
		if (connection != null)
		{
			try
			{
				if (!autoCommit)
				   connection.commit();
			} catch (SQLException e)
			{
				e.printStackTrace();
			}			
		}
	}
	
    public String queryNZFirstRow(String schemaName, String sql)
    {
    	String buffer = "";
    	ResultSet rs;

        PreparedStatement queryStatement = null;

        try
        {
        	if (connection == null)
        	{
        		setDBName(schemaName);
        		connection = getConnection();
        	}
    		if (schemaName.equalsIgnoreCase(getDBName()))
        	{
    			queryStatement = connection.prepareStatement(sql);	
        	} else
        	{
        		queryStatement = changeDatabase(schemaName).prepareStatement(sql);
        		setDBName(schemaName);
        	}

	        queryStatement = connection.prepareStatement(sql);
	        rs = queryStatement.executeQuery();
		    if (rs.next()) 
		    {
		    	buffer = IBMExtractUtilities.trim(rs.getString(1));
		    }
		    if (rs != null)
		        rs.close(); 
		    if (queryStatement != null)
		    	queryStatement.close();
	    }
        catch (Exception e)
	    {
	    	IBMExtractUtilities.log("Schema = " + schemaName + " Error in executing sql = " + sql);
	    	e.printStackTrace();
	    }
        return buffer;
    }

    public double queryFirstLongValue(String sql)
    {
    	double value = -1.0;
    	ResultSet rs;

        PreparedStatement queryStatement = null;

        try
        {
	        queryStatement = connection.prepareStatement(sql);
	        rs = queryStatement.executeQuery();
		    if (rs.next()) 
		    {
		    	value = rs.getDouble(1);
		    }
		    if (rs != null)
		        rs.close(); 
		    if (queryStatement != null)
		    	queryStatement.close();
	    }
        catch (Exception e)
	    {
        	value = -1L;
	    	IBMExtractUtilities.log("Error in executing sql = " + sql);
	    	e.printStackTrace();
	    }
        return value;
    }

    public float version()
    {
    	return Float.valueOf(majorSourceDBVersion + "." + minorSourceDBVersion);
    }
    
    public String queryFirstRow(String schemaName, String sql)
    {
    	String buffer = "";
    	ResultSet rs;

        PreparedStatement queryStatement = null;

        try
        {
	        queryStatement = connection.prepareStatement(sql);
	        rs = queryStatement.executeQuery();
		    if (rs.next()) 
		    {
		    	buffer = IBMExtractUtilities.trim(rs.getString(1));
		    }
		    if (rs != null)
		        rs.close(); 
		    if (queryStatement != null)
		    	queryStatement.close();
	    }
        catch (Exception e)
	    {
	    	IBMExtractUtilities.log("Schema = " + schemaName + " Error in executing sql = " + sql);
	    	e.printStackTrace();
	    }
        return buffer;
    }

    public String executeNZSQL(String schemaName, String tableName, String sql)
    {
    	String buffer = "";

        Statement statement = null;

        try
        {
        	if (connection == null)
        	{
        		setDBName(schemaName);
        		connection = getConnection();
        	}
    		if (schemaName.equalsIgnoreCase(getDBName()))
        	{
    			statement = connection.createStatement();	
        	} else
        	{
        		statement = changeDatabase(schemaName).createStatement();
        		setDBName(schemaName);
        	}

	        statement.execute(sql);
		    if (statement != null)
		    	statement.close();
	    }
        catch (Exception e)
	    {
        	buffer = e.getMessage();
        	if (buffer.contains("count of bad input rows"))
        	   IBMExtractUtilities.log("Bad Data in  " + schemaName + "." + tableName + " Data Load skipped...");
        	else
        	{
	    	   IBMExtractUtilities.log("Schema = " + schemaName + " Error in executing sql = " + sql);
	    	   e.printStackTrace();
        	}
	    }
        return buffer;
    }

	public void setPrintStream(PrintStream ps)
	{
		this.ps = ps;
	}
	
	public void SetConnection2(Connection conn)
	{
		this.connection = conn;
	}
	
	public void setAutoCommit(Boolean value)
	{
		this.autoCommit = value;
	}
	
	public String getDBName()
	{
		return dbName;
	}
	
	public void setDBName(String dbName)
	{
		this.dbName = dbName;
	}

	public Connection changeDatabase(String dbName)
	{
	    close();
	    connection = null;
		this.dbName = dbName;
		connection = getConnection();
		return connection;
	}
	
	public String getDBSourceName()
	{
		return dbSourceName;
	}
	
    public boolean CheckSupportedVendor()
    {
        return (SKIN() || Postgres() || Oracle() || DB2() || Mysql() || Sybase() || Mssql() || zDB2() || Access() || Hxtt() || 
        		Domino()  || Netezza() || iDB2() || Teradata() || Informix()) ? true : false;
    }
    public boolean Netezza()
    {
    	return dbSourceName.equalsIgnoreCase(Constants.netezza);
    }
	public boolean Oracle()
	{
		return dbSourceName.equalsIgnoreCase(Constants.oracle);
	}	
	public boolean Postgres()
	{
		return dbSourceName.equalsIgnoreCase(Constants.postgres);
	}
	public boolean Mysql()
	{
		return dbSourceName.equalsIgnoreCase(Constants.mysql);
	}
	public boolean Mssql()
	{
		return dbSourceName.equalsIgnoreCase(Constants.mssql);
	}
	public boolean zDB2()
	{
		return dbSourceName.equalsIgnoreCase(Constants.zdb2);
	}
	public boolean DB2()
	{
		return dbSourceName.equalsIgnoreCase(Constants.db2luw) || dbSourceName.equalsIgnoreCase(Constants.db2luw_compatibility);
	}
	public boolean Access()
	{
		return dbSourceName.equalsIgnoreCase(Constants.access);
	}
	public boolean Hxtt()
	{
		return dbSourceName.equalsIgnoreCase(Constants.hxtt);
	}
	public boolean Sybase()
	{
		return dbSourceName.equalsIgnoreCase(Constants.sybase);
	}
	public boolean Domino()
	{
		return dbSourceName.equalsIgnoreCase(Constants.domino);
	}
	public boolean iDB2()
	{
		return dbSourceName.equalsIgnoreCase(Constants.idb2);
	}
	public boolean Teradata()
	{
		return dbSourceName.equalsIgnoreCase(Constants.teradata);
	}
	public boolean Informix()
	{
		return dbSourceName.equalsIgnoreCase(Constants.informix);
	}
	public boolean SKIN()
	{
		return dbSourceName.equalsIgnoreCase(Constants.ants);
	}
}
