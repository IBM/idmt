/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;
 
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.JarURLConnection;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IBMExtractUtilities
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
	private static Properties mapDeployFiles = null, mapExcludeList = null, deployedObjectsList = new Properties();
	private static Properties mapCompatibilityFiles = null;
	private static Properties custDatamap = null;
    private static final Class[] parameters = new Class[]{URL.class};
    private static Hashtable timeKeeper = new Hashtable();
    private static HashMap partitionMap = null;
    private static ArrayList mviewMap = null;
    public static String DATAMAP_PROP_FILE = Constants.DATAMAP_PROP_FILE;
    public static String DATAMAPNZ_PROP_FILE = Constants.DATAMAPNZ_PROP_FILE;
    public static String linesep = System.getProperty("line.separator");
	public static String filesep = System.getProperty("file.separator");
	public static String SQLCode = "", Message = "", DB2Path = "", InstanceName = "", param01 = "", param02 = "", param03 = "";
	public static String param04 = "";
	public static String FailedLine = "";
	public static String ReleaseLevel = "";
	public static String Encoding = "UTF-8";
	public static boolean DataExtracted = false;
	public static boolean DeployCompleted = false;
	public static boolean ScriptExecutionCompleted = false;
	public static boolean DB2Compatibility = false;
	public static String fixPack = "1";
	public static boolean db2ScriptCompleted = false, db2sysadm = true;
	public static String sqlTerminator = "@";
	public static String bashShellName = null;	
	public static ArrayList tableList = new ArrayList();
	public static boolean debug = false;
	public static int fetchSize = 100;
	
	public static byte[] TrimBOM(byte[] bytesArray)
	{
		byte[] newArray = null;
		if (bytesArray != null)
		{
			if (bytesArray[0] == -2 && bytesArray[1] == -1)
			{
				int len = bytesArray.length;
				newArray = new byte[len-2];
				for (int i = 2, j = 0; i < len; ++i, ++j)
				{
					newArray[j] = bytesArray[i];
				}
			}
		}
		return newArray;
	}
	
	public static String lastUsedPropFile()
	{
    	return getHomeDir() + filesep + Constants.LAST_USED_CONFIG_FILE;
	}
	
	public static String getAppHome()
	{
		return (String) System.getProperty("AppHome");
	}
	
	public static String getConfigFile()
	{
		return (String) System.getProperty("IDMTConfigFile");
	}

	public static String FormatCMDString(String inputStr)
	{
		if (Constants.win())
		{
			if (inputStr == null || inputStr.equals(""))
				return inputStr;
			else
			    return inputStr.replaceAll("\\\\", "\\\\\\\\");
		} else
			return inputStr;
	}	

	public static String FormatENVString(String inputStr)
	{
		if (inputStr == null) return inputStr;
		return inputStr.contains(" ") ? putQuote(inputStr) : inputStr;
	}
	
    public static String getInputFileName(String outputDirectory, String loadDirectory, boolean usePipe, int id, TableList t)
    {
       int dupID = t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getDupID();
       String dupStr = (dupID == 0) ? "" : Integer.toString(dupID);

       loadDirectory = (outputDirectory != null && outputDirectory.length() > 0 && loadDirectory != null && loadDirectory.length() == 0) ? outputDirectory : loadDirectory;
       String name = FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase()) + dupStr;	
       String inputFile = "";
       
       if (usePipe)
       {
    	   if (Constants.win())
    	      inputFile = "\\\\.\\pipe\\"+name;
    	   else
    		  inputFile = outputDirectory + "data" + filesep + name + ".pipe";
       }
       else
       {
           name = name.replace("\"", "");
           File f  = new File(loadDirectory + "data" + filesep + name + ".txt");
           try
			{
				inputFile = f.getCanonicalPath();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
       }           
       return putQuote(inputFile);
    }
    
    public static String getLobsDirectoryName(String outputDirectory, String loadDirectory, boolean usePipe, int id, TableList t)
    {
    	String lobsDirectory = "";
    	int dupID = t.tabInfo.get(t.dstSchemaName[id]+"."+t.dstTableName[id]).getDupID();
        String dupStr = (dupID == 0) ? "" : Integer.toString(dupID);
    	String fil = IBMExtractUtilities.FixSpecialChars(removeQuote(t.dstSchemaName[id]).toLowerCase()+"_"+removeQuote(t.dstTableName[id]).toLowerCase()) + dupStr;
        File f = new File(loadDirectory + "data" + filesep + fil);
        try
		{
        	lobsDirectory = f.getCanonicalPath();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return lobsDirectory;
    }
    

    public static void blog(PrintStream ps, String msg, int n)
    {
    	if (ps != null)
    	{
			if (n == 0)
			{
	            ps.print(msg);
			} else
			{
	            ps.println(msg);
			}
    	}
        log(msg, n);
    }


    public static void log(PrintStream ps, String msg, int n)
    {
    	if (ps == null)
    	{
    		log(msg, n);
    	} else
    	{
			if (n == 0)
	            ps.print(msg);
	 		else
	            ps.println(msg);
    	}
    }

    public static void log(String msg, int n)
    {
		if (n == 0)
            System.out.print(msg);
 		else
            System.out.println(msg);    			
    }

    public static void log(String msg)
    {
    	log(msg, true);
    }

    public static void log(String msg, boolean line)
    {
    	if (Constants.zos())
    	{
    		if (line)
               System.out.println(timestampFormat.format(new Date()) + ":" + msg);
    		else
               System.out.print(timestampFormat.format(new Date()) + ":" + msg);    			
    	} else 
    	{
    		if (line)
               System.out.println("[" + timestampFormat.format(new Date()) + "] " + msg);
    		else
               System.out.print("[" + timestampFormat.format(new Date()) + "] " + msg);
    	}
    }
    
    public static void log(PrintStream ps, String msg, boolean line)
    {
    	if (ps == null)
    		log(msg, line);
    	else
    	{
	    	if (Constants.zos())
	    	{
	    		if (line)
	               ps.println(timestampFormat.format(new Date()) + ":" + msg);
	    		else
	               ps.print(timestampFormat.format(new Date()) + ":" + msg);    			
	    	} else 
	    	{
	    		if (line)
	               ps.println("[" + timestampFormat.format(new Date()) + "] " + msg);
	    		else
	               ps.print("[" + timestampFormat.format(new Date()) + "] " + msg);
	    	}
    	}
    }
    
    public static void log(PrintStream ps, String msg)
    {
    	if (ps == null)
    		log(msg);
    	else
    	{
	    	if (Constants.zos())
	    	{
	            ps.println(timestampFormat.format(new Date()) + ":" + msg);
	    	} else 
	    	{
	    		ps.println("[" + timestampFormat.format(new Date()) + "] " + msg);
	    	}
    	}
    }
    
	public static void StartTimeKeeper(String key)
	{
		synchronized (timeKeeper)
		{
			if (!timeKeeper.containsKey(key))
			{
			    StopWatch stopwatch = new StopWatch();
		        timeKeeper.put(key, stopwatch);
		        stopwatch.start();			
			}			
		}
	}
	
	public static void StopTimeKeeper(String key)
	{
		synchronized (timeKeeper)
		{
			if (timeKeeper.containsKey(key))
			{
				StopWatch stopwatch = (StopWatch) timeKeeper.get(key);
				stopwatch.stop();
			}			
		}
	}
	
	public static void RunTimeReport()
	{
		synchronized (timeKeeper)
		{
			String format = "%1$-30s:%2$20s";
			Enumeration e = timeKeeper.keys();
			log(String.format(format, new String[]{"Component", "Elapsed Time"}));
			while (e.hasMoreElements())
			{
				String key = (String) e.nextElement();			
				StopWatch s = (StopWatch) timeKeeper.get(key);
				log(String.format(format, new String[]{key, s.toString()}));
			}
			timeKeeper.clear();			
		}
	}

    public static String abbrName(String name)
    {
        if (!(name == null || name.length() == 0))
        {
           name = "" + name.charAt(0) + name.charAt(name.length()/2) + name.charAt(name.length()-1);
        }
        return name;
    }

    public static String truncString(String name, int len)
    {
        if (!(name == null || name.length() == 0))
        {
            if (name.length() > len)
            {
                name = name.substring(0,len);
            }
        }
        return name;
    }

    public static String GetSingleQuotedString(String srcString, String sep)
    {
    	String tmp2 = "";
    	String[] tmp = srcString.split(sep);
    	for (int i = 0; i < tmp.length; ++i)
    	{
    		if (i > 0)
    			tmp2 += ",";
    		tmp2 += "'" + tmp[i] + "'";
    	}
    	return tmp2;
    }
    
    
    public static String padRight(String s, int n, String t) 
    {
        return s == null ? null : String.format("%1$-" + n + "s", s).replaceAll(" ", t);  
    }

    public static String padRight(String s, int n) 
    {
        return s == null ? null : String.format("%1$-" + n + "s", s);  
    }

    public static String padLeft(String s, int n, String t) 
    {
        //return s == null ? null : String.format("%1$#" + n + "s", s).replaceAll(" ", t);
    	if (s == null)
    		return null;
    	if (s != null && s.length() < n)
    	{
    		for (int i = s.length(); i < n; i++)
    		{
    			s = t + s;
    		}
    	}
    	return s;    	
    }
    
    public static String escapeSingleQuote(String s)
    {
    	return (s == null) ? s : s.replaceAll("\\'", "\\\\'");
    }

    public static String padLeft(String s, int n) 
    {
    	return padLeft(s, n, " ");
        //return s == null ? null : String.format("%1$#" + n + "s", s);  
    }

    public static String pad(Object str, int padlen, String pad)
    {
	    String padding = new String();
	    int len = Math.abs(padlen) - str.toString().length();
	    if (len < 1)
	      return str.toString();
	    for (int i = 0 ; i < len ; ++i)
	      padding = padding + pad;
	      
	    return (padlen > 0 ? padding + str : str + padding);
    }

    public static String removeQuote(String name)
    {
    	if (name == null || name.length() == 0) return name;
    	int len = name.length();
    	if (name.charAt(0) == '"' && len > 2)
    		return name.substring(1, len-1);
    	else
    		return name;
    }

    public static String putQuote(String name)
    {
        if (name == null || name.length() == 0) return name;
        if (name.charAt(0) == '"') return name;
        return "\""+name+"\"";
    }
    
    public static void copyFile(File in, File out) throws IOException
	{
		FileChannel inChannel = new FileInputStream(in).getChannel();
		FileChannel outChannel = new FileOutputStream(out).getChannel();
		try
		{
			inChannel.transferTo(0, inChannel.size(), outChannel);
		} catch (IOException e)
		{
			throw e;
		} finally
		{
			if (inChannel != null)
				inChannel.close();
			if (outChannel != null)
				outChannel.close();
		}
	}

	public static String FileContents(String fp) throws IOException
	{
		BufferedReader in = new BufferedReader(new FileReader(fp));
		String line;
		StringBuffer buffer = new StringBuffer();

		while ((line = in.readLine()) != null)
		{
			if ((line.startsWith("DB21034E") || line.startsWith("valid Command Line Processor command")
		        || line.equals("")))
				continue;
			else
			   buffer.append(line+"\n");
		}

		in.close();
		return buffer.toString();
	}

    public static String getStringName(String name, String suffix)
    {
    	if (name == null || name.length() == 0)
    		return name;
    	if (name.charAt(name.length()-1) == '"')
    		return name.substring(0,name.length()-1) + suffix + "\"";
    	else
    		return name + suffix;
    }
    
    public static String escapeUnixChar(String strToEscape)
    {
    	StringCharacterIterator charIter = new StringCharacterIterator(strToEscape);
    	StringBuilder buf = new StringBuilder();
    	char ch = charIter.current();
    	while (ch != charIter.DONE)
    	{
    		// ;&()|*?[]~{}><!^"'\$
    		if (ch == ';') buf.append("\\;");
    		else if (ch == '&') buf.append("\\&");
    		else if (ch == '(') buf.append("\\(");
    		else if (ch == ')') buf.append("\\)");
    		else if (ch == '|') buf.append("\\|");
    		else if (ch == '*') buf.append("\\*");
    		else if (ch == '?') buf.append("\\?");
    		else if (ch == '[') buf.append("\\[");
    		else if (ch == ']') buf.append("\\]");
    		else if (ch == '~') buf.append("\\~");
    		else if (ch == '{') buf.append("\\{");
    		else if (ch == '}') buf.append("\\}");
    		else if (ch == '>') buf.append("\\>");
    		else if (ch == '<') buf.append("\\<");
    		else if (ch == '^') buf.append("\\^");
    		else if (ch == '"') buf.append("\\\"");
    		else if (ch == '$') buf.append("\\$");
    		else buf.append(ch);
    		ch = charIter.next();
    	}
    	return buf.toString();
    }

    public static String FixSpecialChars(String name)
    {    	
    	char[] specialChars = {'/','\\',':','>','<','"','*','?','|'};
    	char[] replaceChars = {'_','-','Z','G','L','D','S','Q','P'};
    	for (int i = 0; i < specialChars.length; ++i)
    	{
    		int pos = name.indexOf(specialChars[i]); 
    		if (pos > 0)
    		  name = name.replace(specialChars[i], replaceChars[i]);
    	}
    	return name;
    }
    
	private static String convertToHex(byte[] data)
	{
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.length; i++)
		{
			int halfbyte = (data[i] >> 4) & 0x0F;
			int two_halfs = 0;
			do
			{
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) ('0' + halfbyte));
				else
					buf.append((char) ('a' + (halfbyte - 10)));
				halfbyte = data[i] & 0x0F;
			} while (two_halfs++ < 1);
		}
		return buf.toString();
	}

	public static String MD5(String text)
	{
		try
		{
			java.security.MessageDigest md;
			md = java.security.MessageDigest.getInstance("MD5");
			byte[] md5hash = new byte[32];
			md.update(text.getBytes("iso-8859-1"), 0, text.length());
			md5hash = md.digest();
			return convertToHex(md5hash);
		} catch (Exception e) {}
		return null;
	}
   
	public static String putQuote(String strToConvert, String sep)
	{
		String targetString = "";
		String[] tmp = strToConvert.split(sep);
		for (int i = 0; i < tmp.length; ++i)
		{
			tmp[i] = tmp[i].contains(" ") ? "\"" + tmp[i] + "\"" : tmp[i];
			if (i > 0)
				targetString += sep;
			targetString += tmp[i];
		}
		return targetString;
	}

    public static String getClasspath() 
    {
    	ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        StringBuffer classpath = new StringBuffer();
        URL[] urls = ((URLClassLoader)classLoader).getURLs();
        for(int i = 0; i < urls.length; i++) 
        {
    	  if (i > 0)
    		  classpath.append(":");
    	  classpath.append(urls[i]);
        }
    	return classpath.toString();
    }

    public static void AddURL(URL u) throws IOException {
		
    	URLClassLoader sysloader = (URLClassLoader)ClassLoader.getSystemClassLoader();
    	Class sysclass = URLClassLoader.class;
     
    	try {
    		Method method = sysclass.getDeclaredMethod("addURL",parameters);
    		method.setAccessible(true);
    		method.invoke(sysloader,new Object[]{ u });
    	} catch (Throwable t) {
    		t.printStackTrace();
    		throw new IOException("Error, could not add URL to system classloader");
    	}    		
    }
    
    public static void AddFile(File f)throws IOException 
    {
    	AddURL(f.toURL());
    }
    
    public static String getAbsolutePath(String fileName)
    {
    	File f = new File(removeQuote(fileName));
    	if (f != null)
    	   fileName = f.getAbsolutePath();
    	return fileName;
    }
    
    public static boolean FileExists(String fileName)
    {
    	if (fileName == null) return false;
    	File f = new File(removeQuote(fileName));
    	return f.exists();
    }
    
    public static void getStringChunks(ResultSet rs, int colIndex, StringBuffer buf)
    {
        char[] buffer = new char[1024*1000];
        int charRead = 0;
        try
		{
            Reader input = rs.getCharacterStream(colIndex);
            if (input != null)
            {
				while ((charRead = input.read(buffer)) != -1)
				{
					buf.append(buffer, 0, charRead);
				}
            }
		} catch (Exception e)
		{
			e.printStackTrace();
		}
    }
    
    public static String fixSybaseConverterClassName(String converterClassName)
    {
    	String convClassName = converterClassName;
    	String classpath = getClasspath();
    	boolean found3 = (classpath == null || classpath.trim().length() == 0) ? 
    			false : classpath.matches(".*jconn3.jar.*");
    	if (found3)
    	{
    		convClassName = "com.sybase.jdbc3.utils.TruncationConverter";
    	}
    	boolean found4 = (classpath == null || classpath.trim().length() == 0) ? 
    			false : classpath.matches(".*jconn4.jar.*");
    	if (found4)
    	{
    		convClassName = "com.sybase.jdbc4.utils.TruncationConverter";
    	}
    	return convClassName;    	
    }
    
    public static String readJarFile(String aboutFile)
    {
    	try
    	{
	   		String line;
	   		InputStream istream;
	   		BufferedReader breader = null;
	    	File f = new File(aboutFile);
	    	StringBuffer buffer = new StringBuffer();
	    	if (!f.exists())
	    	{
	    		istream = ClassLoader.getSystemResourceAsStream(aboutFile);
	    		breader = new BufferedReader(new InputStreamReader(istream));
	    	} else
	    	{
	    		breader = new BufferedReader(new FileReader(f));
	    	}	
			while ((line = breader.readLine()) != null)
			{
				buffer.append(line + linesep);
			}
			breader.close();
			return buffer.toString();
    	} catch (Exception e) 
		{
			e.printStackTrace();
		}
    	return "";
    }
    
    public static String db2ScriptName(String outputDir, boolean ddlGen, boolean dataUnload)
    {    	
    	String scriptName = "", ext = "";
        if (Constants.win())
        {
        	ext = ".cmd";
        } else
        {
        	ext = ".sh";
        }
        if (ddlGen && dataUnload)
        {
        	scriptName = outputDir + filesep + Constants.prefix + "gen" + ext;
        } else if (ddlGen && !dataUnload)
        {
        	scriptName = outputDir + filesep + Constants.prefix + "ddl" + ext;
        }
        else
        {
        	scriptName = outputDir + filesep + Constants.prefix + "load" + ext;
        }        	
    	return scriptName;
    }
    
    private static String getDB2PathName(DBData data)
    {
    	String methodName = "getDB2PathName";
    	SQL s = new SQL(data.connection);
        String result = "";
        try
        {
	    	String sql = "select reg_var_value from sysibmadm.reg_variables where reg_var_name = 'DB2PATH'";
	    	s.PrepareExecuteQuery(methodName, sql);
			while (s.rs.next()) 
			{
				result = s.rs.getString(1);				
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	e.printStackTrace();
        	String userHome = System.getProperty("user.home");
			if (IBMExtractUtilities.FileExists(userHome + "/sqllib"))
			{
				result = userHome + "/sqllib/java";
			} else
				result = "";
        }
        log("DB2 PATH is " + result);
		return result;    	
    }
    
    public static String removeParen(String buffer)
    {
    	String matchStr = "";
		Pattern p;
		Matcher m;
		p = Pattern.compile("\\s*\\((.*)\\)\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		m = p.matcher(buffer);
		while (m.find()) 
		{
			matchStr = m.group(m.groupCount());
		}    	
		return (matchStr.length() == 0) ? buffer : matchStr;
    }

    public static int getTokenPosition(String regPattern, String buffer)
    {
    	int pos = -1;
		Pattern p;
		Matcher m;
		p = Pattern.compile(regPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		m = p.matcher(buffer);
		pos = -1;
		while (m.find()) 
		{
			pos = m.start(m.groupCount());
		}    	
		return pos;
    }
    
    public static String getTableNamesiSeriesDB2(String sql)
    {
    	StringTokenizer st;
    	int idx, pos, wherePos, tokenCount;
    	StringBuffer tableNames = new StringBuffer();
    	StringBuffer buffer = new StringBuffer(IBMExtractUtilities.removeParen(sql));
		String token;		
		String tabString = "";
		boolean tableCollected = false, correlCollected = false;
		
		wherePos = IBMExtractUtilities.getTokenPosition(".*\\s+(WHERE)\\s+.*", buffer.toString());
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
		pos = IBMExtractUtilities.getTokenPosition(".*\\s+(FROM)\\s+.*", buffer.toString());
		if (pos > 0)
		{
			if (wherePos > 0)
				tabString = buffer.substring(pos+5, wherePos);
			else
				tabString = buffer.substring(pos+5);
		}
		st = new StringTokenizer(tabString, "\t\n\r ,", true);
		idx = 0;
		tokenCount = st.countTokens();
		String[] words = new String [tokenCount];
	    while (st.hasMoreTokens())
	    {
	    	token = st.nextToken();
	    	if (!(token.equals("\n") || token.equals("\r") 
	    		|| token.equals("\t") || token.equals(" ")))
	    	{
	    	   words[idx] = token; 
	    	   idx++;
	    	}
	    }
	    tokenCount = idx;
	    for (idx=0; idx < tokenCount; idx++)
	    {
	    	token = words[idx];
	    	if (token.equalsIgnoreCase("JOIN") || token.equalsIgnoreCase(","))
	    	{
	    		if (!correlCollected)
	    			tableNames.append(":");
	    		tableCollected = correlCollected = false;
	    	}
	    	else if (token.equalsIgnoreCase("AS"))
	    	{
	    		;
	    	}
	    	else
	    	{
	    		if (!tableCollected || !correlCollected)
	    		{
			    	pos = token.indexOf('.');
			    	if (pos > 0)
			    	{
			    		if (tableNames.length() > 0)
			    			tableNames.append(";");
			    		tableNames.append(token.substring(0,pos) + ":" + token.substring(pos+1));	
			    		tableCollected = true;
			    	} else
			    	{
			    		if (!tableCollected)
			    		{
				    		if (tableNames.length() > 0)
				    			tableNames.append(";");
				    		tableNames.append(":" + token);
				    		tableCollected = true;
			    		} else
			    		{
			    			tableNames.append(":" + token);
				    		correlCollected = true;	    			
			    		}
			    	}
	    		}
	    	}
	    }
	    return tableNames.toString();
    }
    
    private static void getNZParams(DBData data)
    {
    	SQL s = new SQL(data.connection);    	
    	String sql;
    	String methodName = "getNZParams";
    	param01 = "";
    	param02 = "";
    	param03 = "";
    	param04 = "";
    	    	    
    	try
        {
    		sql = "select system_state from _v_system_info";
            s.PrepareExecuteQuery(methodName, sql);
            while (s.next()) 
	        {
            	param01 = trim(s.rs.getString(1));
	        }
            s.close(methodName);
    		sql = "select count(*) from _v_dslice";
            s.PrepareExecuteQuery(methodName, sql);
            while (s.next()) 
	        {
            	param02 = trim(s.rs.getString(1));
	        }
            s.close(methodName);
    		sql = "select count(*) from _v_spu";
            s.PrepareExecuteQuery(methodName, sql);
            while (s.next()) 
	        {
            	param03 = trim(s.rs.getString(1));
	        }
            s.close(methodName);
    		sql = "select count(*) from _v_spu where HW_STATETEXT = 'online'";
            s.PrepareExecuteQuery(methodName, sql);
            while (s.next()) 
	        {
            	param04 = trim(s.rs.getString(1));
	        }
            s.close(methodName);
            if (param01.length() > 0)
            	param01 = "System State = " + param01;
            if (param02.length() > 0)
            	param02 = "# of data slice = " + param02;
            if (param03.length() > 0)
            	param03 = "# of SPUs = " + param03;
            if (param04.length() > 0)
            	param04 = "# of SPUs online = " + param04;
	    } catch (Exception e)
	    {
	    	e.printStackTrace();
	    }     	
    }
    
    private static void getCompatibilityParams(DBData data)
    {
    	String methodName = "getCompatibilityParams";
    	SQL s = new SQL(data.connection);    	
    	param01 = "";
    	param02 = "";
    	param03 = "";
    	param04 = "";
    	
        try
        {
	    	String sql = "select a.value, b.value, c.value, d.value " +
	    			"from sysibmadm.dbcfg a, " +
	    			"sysibmadm.dbcfg b, " +
	    			"sysibmadm.dbcfg c, " +
	    			"sysibmadm.dbcfg d " +
	    			"where " +
	    			"a.name = 'varchar2_compat' " +
	    			"and b.name = 'date_compat' " +
	    			"and c.name = 'number_compat'" +
			        "and d.name = 'decflt_rounding'";
	    	s.PrepareExecuteQuery(methodName, sql);
			while (s.next()) 
			{
				param01 = s.rs.getString(1);
				param02 = s.rs.getString(2);  
			    param03 = s.rs.getString(3);
			    param04 = s.rs.getString(4);
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	param01 = "";
        	param02 = "";
        	param03 = "";
        	param04 = "";
        	e.printStackTrace();
        }
        log("DB2 Compatibility params varchar2_compat="+param01+" date_compat="+param02+" number_compat="+param03 + " param04=" + param04);  	
    }

    private static String getDB2ReleaseLevel(DBData data)
    {
    	String methodName = "getDB2ReleaseLevel";
    	SQL s = new SQL(data.connection);    	
        String result = "";
        try
        {
	    	String sql = "select prod_release from sysibmadm.env_prod_info fetch first row only";
	    	s.PrepareExecuteQuery(methodName, sql);
			while (s.next()) 
			{
			   result = s.rs.getString(1);				
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	result = "";
        }
		return result;    	
    }

    public static int executeUpdate(Connection mainConn, String sql)
    {
    	int sqlCount = 0;

        PreparedStatement statment = null;

        try
        {
        	statment = mainConn.prepareStatement(sql);
        	sqlCount = statment.executeUpdate();
	    }
        catch (SQLException ex)
        {
        	if (ex.getErrorCode() == -3603)
        	{
        		return -3603;
        	} else if (ex.getErrorCode() == -3600)
        	{
        		return -3600;
        	} else if (ex.getErrorCode() == -601)
        	{
        		return 0;
        	} else if (ex.getErrorCode() == -3608)
        	{
        		return -3608;
        	}
        	else
        	{
    	    	log("Error in executeUpdate sql="+sql);
    	    	ex.printStackTrace();        		
        	}
        }
        catch (Exception e)
	    {
	    	log("Error is executing sql="+sql);
	    	e.printStackTrace();
	    } finally
	    {
		    if (statment != null)
				try {statment.close(); } catch (SQLException e) { e.printStackTrace();	}	    	
	    }
        return sqlCount;
    }
    
    public static String executeSQL(Connection mainConn, String sql, String sqlTerminator, boolean terminator)
    {
    	int count = 0;
    	StringBuffer sb = new StringBuffer();

        PreparedStatement queryStatement = null;
        ResultSet Reader = null;
        String value, tok = (terminator) ? linesep + sqlTerminator + linesep : "~";

        try
        {
	        queryStatement = mainConn.prepareStatement(sql);
	        Reader = queryStatement.executeQuery();
		    while (Reader.next()) 
		    {
		    	if (count > 0)
		    		sb.append(tok);
		    	value = trim(Reader.getString(1));
		    	sb.append(value);
		    	count++;
		    }
		    if (count == 1)
		    	if (terminator)
		    	   sb.append(linesep + sqlTerminator + linesep);
		    if (Reader != null)
		        Reader.close(); 
		    if (queryStatement != null)
		    	queryStatement.close();
	    }
        catch (Exception e)
	    {
	    	log("Error is executing sql="+sql);
	    	e.printStackTrace();
	    }
        return sb.toString();
    }
    

    public static boolean CheckValidJavaHome(String javaHomeDir)
    {
		String java = javaHomeDir + filesep + "bin" + filesep + "java" + (Constants.win() ? ".exe" : ""); 
		return FileExists(java);
    }
    
    public static boolean CheckValidEncoding(String encoding)
    {
    	String tmpStr = "test";
    	try
		{
			tmpStr.getBytes(encoding);
		} catch (UnsupportedEncodingException e)
		{
			return false;
		}
    	return true;
    }
    
    public static boolean CheckValidInteger(String number)
    {
    	try
		{
			Integer.valueOf(number);
		} catch (Exception e)
		{
			return false;
		}
    	return true;
    }
    
    public static String executeSPSQL(String Vendor, String dbName, Connection conn, String sql, int colIndex)
    {
    	int count = 0;
    	StringBuffer sb = new StringBuffer();

        CallableStatement cstmt = null;
        PreparedStatement statement = null;
        ResultSet Reader = null;

        try
        {
        	if (Vendor.equalsIgnoreCase("sybase"))
        	{
	            statement = conn.prepareStatement("USE " + dbName);
	            statement.executeUpdate();
        	}
            cstmt = conn.prepareCall(sql);
            cstmt.executeQuery();
            Reader = cstmt.getResultSet();
		    while (Reader.next()) 
		    {
		    	sb.append(Reader.getString(colIndex));
		    	count++;
		    }
		    if (Reader != null)
		        Reader.close(); 
		    if (cstmt != null)
		    	cstmt.close();
		    if (statement != null)
		    	statement.close();
	    }
        catch (Exception e)
	    {
	    	log("Error is executing sql="+sql);
	    	e.printStackTrace();
	    }
        return sb.toString();
    }
    
    public static boolean booleanSQL(Connection mainConn, String sql)
    {
    	boolean ok = false;
    	int count = 0;

        PreparedStatement queryStatement;
        ResultSet Reader = null;        

        try
        {
           queryStatement = mainConn.prepareStatement(sql);
           Reader = queryStatement.executeQuery();
        while (Reader.next()) 
        {
        	count++;
        }
        if (count > 0)
        	ok = true;
        if (Reader != null)
            Reader.close(); 
        if (queryStatement != null)
        	queryStatement.close();
        } catch (SQLException ex)
        {
        	if (ex.getErrorCode() == -668)
        		log("Table used in sql " + sql + " is is check pending mode. Run SET INTEGRITY command against this table or drop and re-create.");
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        }
        return ok;
    }
    
    private static boolean checkSysAdmAuth(DBData data, String userName)
    {
    	String methodName = "checkSysAdmAuth";
    	SQL s = new SQL(data.connection);    	
        String result = "";
        try
        {
	    	String sql = "select case d_group when 'Y' then 'true' when 'N' then 'false' else 'false' end from table (sysproc.auth_list_authorities_for_authid('"+userName.toUpperCase()+"','U')) AS X where authority = 'SYSADM'";
	    	s.PrepareExecuteQuery(methodName, sql);
			while (s.next()) 
			{
				result = trim(s.rs.getString(1));				
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	result = "true"; 
        }
		return Boolean.valueOf(result);    	    	
    }
    
    private static String getInstanceName(DBData data)
    {
    	String methodName = "getInstanceName";
    	SQL s = new SQL(data.connection);    	
        String result = "";
        try
        {
	    	String sql = "select inst_name from sysibmadm.env_inst_info fetch first row only";
	    	s.PrepareExecuteQuery(methodName, sql);
			while (s.next()) 
			{
				result = s.rs.getString(1);				
			}
			s.close(methodName);
        } catch (Exception e)
        {
        	result = "";
        	e.printStackTrace();
        }
        log("DB2 instance name is " + result);
		return result;    	
    }

    public static void CloseConnection(Connection mainConn)
    {
    	try
		{
    		mainConn.commit();
			mainConn.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
    }
    
    public static String removeLineChar(String input)
    {
    	String patternStr = "(?m)$^|[\\r\\n|\\r|\\n]+";
    	String replaceStr = " ";
    	Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(input);
        replaceStr = matcher.replaceAll(" ");
        return replaceStr;
    }
    
    public static String getHomeDir()
    {
    	String appHome = System.getProperty("AppHome");
    	String s = (appHome == null) ? System.getProperty("user.dir") : appHome;
    	File f = new File(s);
    	try
		{
			return f.getCanonicalPath();
		} catch (IOException e)
		{
			return s;
		}
    }
    
    public static String getDBVersion(String Vendor, String server, int port, String dbName, String userid, String pwd)
	{
    	String methodName = "getDBVersion";
		String sql = null;
    	StringBuffer sb = new StringBuffer();
		Connection mainConn = null;
		DBData data = new DBData(Vendor, server, port, dbName, userid, pwd, "", 0);
		data.setAutoCommit(true);
		mainConn = data.getConnection();
    	SQL s = new SQL(data.connection);    	
		
		if (mainConn == null)
			return "Error";
		
		if (data.Oracle())
		{
			sql = "SELECT * FROM V$VERSION";
		} else if (data.DB2())
		{
			sql = "SELECT service_level||'(FP'||fixpack_num||')' FROM TABLE (sysproc.env_get_inst_info()) as x";
		}
		else if (data.Sybase() || data.Mssql())
		{
			sql = "SELECT @@VERSION";
		}
		else if (data.Mysql() || data.Postgres())
		{
			sql = "SELECT VERSION()";
		}
		else if (data.Teradata())
		{
			sql = "select infodata from dbc.dbcinfo where infokey = 'VERSION'";
		}
		else if (data.zDB2())
		{
			sql = "";
		}
		else if (data.Informix())
		{
			sql = "SELECT DBINFO('version', 'major')||'.'||DBINFO('version', 'minor') FROM systables WHERE tabid = 1";
		}
		
        if (sql == null || sql.length() == 0)
        	return "Unknown Version for " + data.dbSourceName;
		        
        try
        {
            try
            {
    	    	s.PrepareExecuteQuery(methodName, sql);
            	while (s.next())
            	{
            		sb.append("Version = " + s.rs.getString(1));
            	}
            	if (data.Teradata())
            	{
            		String amps = mainConn.nativeSQL("{fn teradata_amp_count()}");
            		sb.append(" Teradata AMP count = " + amps);
            	}
            	s.close(methodName);
            }
            catch (SQLException ex)
            {
            	sb.append("Version = " + data.version());
            }
            mainConn.close();
        } catch (SQLException e)
        {
        	log("Error for : " + data.dbSourceName + " Error Message :" + e.getMessage());
            System.exit(-1);
        }		
		return sb.toString();
	}
    
    public static Connection OpenConnection(String Vendor, String server, int port, String dbName, String userid, String pwd)
	{
		Connection mainConn = null;		
		Message = "";
		
		DBData data = new DBData(Vendor, server, port, dbName, userid, pwd, "", 0);
		mainConn = data.getConnection();
		Message = data.Message;

		if (mainConn == null)
			return null;
				
        if (data.DB2())
        {
     	   DB2Path = getDB2PathName();
     	   InstanceName = getInstanceName(data);
     	   getCompatibilityParams(data);
        }
		return mainConn;
	}
    
    public static boolean isValidValue(String value, String validValues)
	{
		if (validValues == null) return true;
		String[] valids = validValues.split(",");
		for (int i = 0; i < valids.length; ++i)
		{
			if (value.equalsIgnoreCase(valids[i]))
				return true;
		}
		return false;		
	}
	
    public static boolean DeleteDir(File dir) 
    {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i < children.length; i++) {
                boolean success = DeleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    public static String getSQLMessage(DBData data, String code, String message)
    {
    	StringBuffer sb = new StringBuffer();
    	String sqlCode = "SQL" + Math.abs(Integer.valueOf(code));
    	String sql = "VALUES (SYSPROC.SQLERRM('"+sqlCode+"','"+message+"',';','en_US',1))";
    	ResultSet rs = null;
    	try
		{
    		PreparedStatement partStatement = data.connection.prepareStatement(sql);
			rs = partStatement.executeQuery();
			while (rs.next()) {
				sb.append(rs.getString(1));				
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			
			data.commit();
			
			sb.append(linesep+linesep); 
			sql = "VALUES (SYSPROC.SQLERRM('"+sqlCode+"','','','en_US',0))";
			partStatement = data.connection.prepareStatement(sql);
			rs = partStatement.executeQuery();
			while (rs.next()) {
				sb.append(rs.getString(1));			
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
		} catch (SQLException e)
		{
			log("SYSPROC.SQLERRM Error:" + e.getMessage());
			return message;
		}
		return sb.toString();
    }
    
    public static boolean CheckExistsSQLErrorCode(int errorCode)
    {
    	if (Constants.netezza())
    	{
    		return errorCode == -1;
    	} else
    	{
    	    return (errorCode == -601 || errorCode == -612 || errorCode == 605 || errorCode == -624 || errorCode == 2714 || errorCode == 17242 ||
					errorCode == 1913 || errorCode == 17262 || errorCode == 2004);
    	}
    }
    
    public static boolean CheckNotExistsSQLErrorCode(int errorCode)
    {
    	return (errorCode == -204 || errorCode == 3701);
    }

    public static void DeployObject(DBData data, String outputDirectory, String type, String schema, String objectName, String skin, String sql)
	{		
    	DeployObject(data, outputDirectory, type, schema, objectName, skin, sql, false);
	}
    /**
     * Whatever changes are made to this procedure, same changes are to be made in the RunDeployObjects.deployObject and in DeployObjects.deployObject
     * @param conn
     * @param outputDirectory
     * @param type
     * @param schema
     * @param objectName
     * @param skin
     * @param sql
     */
    public static void DeployObject(DBData data, String outputDirectory, String type, String schema, String objectName, String skin, String sql, boolean tempTableDeploy)
	{		
    	String sqlerrmc = "";
    	int lineNumber = -1;
    	if (data.connection == null)
    	{
    		Message = "Connection does not exist.";
    		return;
    	}
		Message = SQLCode = FailedLine = ""; 
		Statement statement = null;
		try
		{
			if (skin != null && skin.length() > 0)
			{
			    statement = data.connection.createStatement();
		    	try
		    	{
				   statement.execute("use " + skin);
		    	} catch (Exception e)
		    	{
		    		statement.execute("use master");
		    		statement.execute("create database " + skin);
					statement.execute("use " + skin);
		    	}
			} else
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
				Message = e.getMessage();
				if (Message.matches("(?sim).*already\\s+exists.*"))
				{
				   SQLCode = "0";
				   Message = "";
				} 
				else
				{
				   SQLCode = "" + e.getErrorCode();					   
				   FailedLine = "-1";
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
				if (CheckExistsSQLErrorCode(e.getErrorCode()))
				{
					SQLCode = "0";
				} else
				{
					SQLCode = "" + e.getErrorCode();
					FailedLine = "" + lineNumber;
					if ((skin != null && skin.length() > 0))
					{
						Message = e.getMessage();
					} else
					{
						Message = getSQLMessage(data, SQLCode, sqlerrmc);									
					}
				}
			}
		}
		if (!tempTableDeploy)
		{
			String key = type + ":" + schema + ":" + objectName;
			LoadDeployedObjects(outputDirectory);
			if (SQLCode.equals("") || SQLCode.equals("0"))
			{
				deployedObjectsList.setProperty(key, "1");
			} else
			{
				deployedObjectsList.setProperty(key, "0");			
			}
			SaveDeployedObjects(outputDirectory);
		}
	}

    public static String CheckOracleRequisites(Connection connection, String userID, int majorSourceDBVersion)
    {
    	String sql = "", role = "";
    	String message = "";
    	if (!(userID == null || userID.length() == 0))
    		userID = userID.toUpperCase(); 
		try
		{
			if (majorSourceDBVersion != -1 && majorSourceDBVersion > 8)
     	       ((oracle.jdbc.driver.OracleConnection)connection).setSessionTimeZone(java.util.TimeZone.getDefault().getID());
			try
			{
				sql = "SELECT GRANTED_ROLE FROM DBA_ROLE_PRIVS WHERE GRANTEE = '" + userID + "' AND GRANTED_ROLE = 'DBA'";
				PreparedStatement partStatement = connection.prepareStatement(sql);
				ResultSet rs = partStatement.executeQuery();
				role = "";
				while (rs.next()) 
				{
					if (role.equals(""))
				      role = rs.getString(1);
					else
					  role = ":" + role + rs.getString(1);
				}
				if (!role.startsWith("DBA"))
				{
					//message = "DBA ROLE is not available to user '" + userID + "'. Please consult your Oracle DBA.";						
					message = "";
				}
				if (rs != null)
					rs.close();
				if (partStatement != null)
					partStatement.close();
				log(message);
				return message;
			} catch (SQLException e2)
			{
				if (e2.getErrorCode() == 942)
				{
					message = "DBA / SELECT_CATALOG_ROLE not available to user '" + userID + "'. Please consult your Oracle DBA.";
					log(message);
					return message;
				}
			}
		} catch (SQLException e)
		{
			try
			{
				if (majorSourceDBVersion != -1 && majorSourceDBVersion >= 8)
	     	       ((oracle.jdbc.driver.OracleConnection)connection).setSessionTimeZone(getTimeZoneOffset());
			} catch (Exception ed)
			{
	        	sql = "ALTER SESSION SET TIME_ZONE='"+getTimeZoneOffset() + "'";
	        	log("Serious Error Ora-1804: Unable to set timezone for Oracle. Trying " + sql);
				PreparedStatement statement;
				try
				{
					statement = connection.prepareStatement(sql);
					int rc = statement.executeUpdate();
					if (statement != null)
					   statement.close();
				} catch (SQLException e1)
				{					
					log("Alternate method did not work. It looks that the timezone file on Oracle server are not compatible with current version of Oracle.");
					log("Try \"select count(*) from v$timezone_names\" at your SQL*Plus and if count is 0, contact Oracle support to get a fix.");
					log("You can also look doc ID: 414590.1 and 417893.1 for a solution in metalink.oracle.com");
					log("Continuing with the movement of data and you might notice Oracle -01866 error for the tables having TIMESTAMP column");
					message = "Serious Timezone problem in Oracle database. See console log.";
				}				
			}			
		}
		return message;
    }
    
    public static String CheckTeradataRequisites(Connection connection, String userID)
    {
    	int count = 0;
    	PreparedStatement partStatement = null;
    	ResultSet rs = null;
    	Hashtable accessHash = new Hashtable();
    	String userName = "", tableName = "", accessRight = "";
    	String sql = "";
    	String message = "";
    	try
    	{
			sql = "Select RMM.Grantee As UserName " +
					", RMM.RoleName , ARR.AccessRight , ARR.Databasename, ARR.Tablename " +
					"From DBC.RoleMembers RMM Join DBC.AllRoleRights ARR ON RMM.RoleName = ARR.RoleName " +
					"Where ARR.Databasename = 'DBC' and RMM.Grantee = '"+userID+"' " +
					"Union Select ALR.Username , Null (Char(30)) , ALR.Accessright , ALR.Databasename , ALR.Tablename " +
					"From DBC.AllRights ALR Where ALR.Databasename = 'DBC' and alr.username = '"+userID+"' and tablename = 'ALL'" +
					"Order By 4,5,1,3,2";    		
			partStatement = connection.prepareStatement(sql);
			rs = partStatement.executeQuery();
			while (rs.next()) 
			{
	    		accessRight = trim(rs.getString(3));
	    		tableName = trim(rs.getString(5));
	    		if (accessRight != null)
	    		{
	    			if (accessRight.equalsIgnoreCase("R") || accessRight.equalsIgnoreCase("CD") || 
	    					accessRight.equalsIgnoreCase("CT"))
	    			{
		                log("Teradata Access for "+userID+" to DBC."+tableName+" is " + accessRight);	  
		                count++;
	    			}
	    		}
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			if (count > 0)
			{
				return Message;
			}
    	} catch (SQLException e)
    	{
    		e.printStackTrace();
			message = "Problem in determining access to DBC database for user '" + userID + "'. Please consult your Teradata DBA.";
			log(sql);
			log(message);
			return message;
    	}
		try
		{
			count = 0;
			accessHash.put("TVM", "N");
			accessHash.put("IDCOL", "N");
			sql = "SELECT username, tablename FROM DBC.allrights " +
					"where databasename = 'DBC' and tablename in ('TVM', 'IdCol') " +
					"and username in ('"+userID+"','PUBLIC') " +
					"group by username, tablename";
			partStatement = connection.prepareStatement(sql);
			rs = partStatement.executeQuery();
			while (rs.next()) 
			{
				userName = trim(rs.getString(1));
				tableName = trim(rs.getString(2)).toUpperCase();
				String n = (String) accessHash.get(tableName);
				n = "Y";
			}
	        Enumeration<String> keys = accessHash.keys();
	        while (keys.hasMoreElements()) 
	        {
	            String key = keys.nextElement();
	            String value = (String) accessHash.get(key);
	            if (value.equalsIgnoreCase("Y"))
	                log("Teradata Access to : DBC." + key + " Available");
	            else
	            {
	            	log("*Insufficient auths*. Ask your teradata DBA to grant SELECT on DBC." + key + " to '" + userID + "'");
	            	count++;
	            }
	        }
			if (count == 0)
			{
				message = "Select privilege to some DBC tables for '" + userID + "' not available. Please consult your Teradata DBA.";						
				message = "";
			}
			if (rs != null)
				rs.close();
			if (partStatement != null)
				partStatement.close();
			log(message);
			return message;
		} catch (SQLException e2)
		{
    		e2.printStackTrace();
			message = "Problem in determining access to DBC database for user '" + userID + "'. Please consult your Teradata DBA.";
			log(sql);
			log(message);
			return message;
		}
    }

    public static String trim(String name)
    {
    	if (name != null)
    		name = name.trim();
    	else
    		name = "";
    	return name;
    }
    
    private static void getDatabaseEncoding(String Vendor, Connection conn)
    {
        String name, result = "";
        String sql = "";
        
        if (Vendor.equalsIgnoreCase("sybase"))
        	sql = "{call sp_configure Languages}";
        
        if (sql.equals(""))
        	return;
        
        try
        {
	    	ResultSet rs;
	    	conn.setAutoCommit(true);
			CallableStatement callStatment = conn.prepareCall(sql);
			rs = callStatment.executeQuery();
			while (rs.next()) 
			{
				name = trim(rs.getString(1));
				if (name.equalsIgnoreCase("default character set id"))
				{
			       result = trim(rs.getString(4));
			       break;
				}
			}
			if (rs != null)
				rs.close();
			if (callStatment != null)
				callStatment.close();
			conn.commit();
			conn.setAutoCommit(false);
        } catch (Exception e)
        {
        	result = "";
        }
        Encoding = (result.equals("1")) ? "ISO-8859-1" : "UTF-8";        			    	
    }
    
    public static boolean TestConnection(boolean remote, boolean compatibilityMode, String Vendor, String server, int port, String dbName, String userid, String pwd, String extraParam)
	{
		Encoding = "UTF-8";
		Message = "";
    	DBData data = new DBData(Vendor, server, port, dbName, userid, pwd, extraParam, 0);
		Connection mainConn = data.getConnection();
		Message = data.Message;
		
		if (mainConn == null)
			return false;
		
        if (data.DB2())
        {
     	   DB2Path = getDB2PathName();
     	   ReleaseLevel = getDB2ReleaseLevel(data);
     	   float releaseLevel;
     	   try
           {
              releaseLevel = Float.parseFloat(ReleaseLevel);
           } catch (Exception e1)
           {
              releaseLevel = -1.0F;
           }
           if (releaseLevel >= 9.5)
           {
     	       fixPack = executeSQL(mainConn, "select fixpack_num from sysibmadm.env_inst_info fetch first row only", "", false);
     	       db2sysadm = checkSysAdmAuth(data, userid);
           } 
           else
           {
         	   fixPack = "1";
         	   db2sysadm = true;
           }
     	   if (ReleaseLevel.length() > 0)
     	   {
         	   InstanceName = getInstanceName(data);	
         	   getCompatibilityParams(data);
     	       DB2Compatibility = isDB2CompatiblitySet(data);
     	   }
     	   if (!data.Informix())
               data.commit();
           data.close();
            
           String sysadmMsg = (db2sysadm) ? "" : " Warning: User '" + userid + "' does not seem to be sysadm. You will face problems in deployment";
           if (compatibilityMode)
           {
     	       if (DB2Compatibility)
     	       {
     	           Message = "Connection to " + ((remote) ? "remote " : "local ") + data.dbSourceName + " server succeeded." + sysadmMsg ;            	    	   
     	           return true;
     	       } else
     	       {
     	    	   Message = "DB2_COMPATIBILITY_VECTOR is not set on "+((remote) ? "remote " : "local ")+"DB2 Server." + sysadmMsg;
     	    	   return false;
     	       }
           } 
           else
           {
 	           Message = "Connection to " + ((remote) ? "remote " : "local ") + data.dbSourceName + " server succeeded." + sysadmMsg;            	    	   
 	           return true;                	   
           }
        } else if (data.Oracle())
        {
     	    Message = CheckOracleRequisites(mainConn,userid,data.majorSourceDBVersion);
     	    if (!data.Informix())
               data.commit();
            data.close();
     	    if (Message.equals(""))
     	       return true;
     	    else 
     		   return false;
        }
        else if (data.Teradata())
        {
     	   Message = CheckTeradataRequisites(mainConn, userid);
     	   if (!data.Informix())
               data.commit();
            data.close();
     	   if (Message.equals(""))
     	      return true;
     	   else 
     		  return false;
        }
        else if (data.Sybase())
        {
     	   getDatabaseEncoding(data.dbSourceName, mainConn);
        }
        else if (data.Netezza())
        {
        	getNZParams(data);
        	ReleaseLevel = data.getReleaseLevel();
        	DB2Compatibility = false;
        	InstanceName = data.productName;
        }
        Message = "Connection to " + ((remote) ? "remote " : "local ") + data.dbSourceName + " succeeded.";
        log(Message);
 	    if (!data.Informix())
           data.commit();
        data.close();
		return true;
	}
	
    public static String getIDMTProperty(String propertyName)
    {
    	Properties propParams = new Properties();
		InputStream istream;	
		String value = null;
        try
		{
        	String propFile = Constants.IDMT_CONFIG_FILE;
        	propParams = new Properties();
            istream = ClassLoader.getSystemResourceAsStream(propFile);
            if (istream == null)
            {
            	FileInputStream finStream = new FileInputStream(propFile);
            	propParams.load(finStream);
            	finStream.close();
            } else
            {
            	propParams.load(istream);
            	istream.close();
            }
            value = (String) propParams.getProperty(propertyName);
		} catch (Exception e)
		{
			e.printStackTrace();
		} 
		return value;
    }
    
	private static void LoadExcludeList()
	{
		InputStream istream;		
        try
		{
			if (mapExcludeList == null)
			{
				mapExcludeList = new Properties();
	            istream = ClassLoader.getSystemResourceAsStream(Constants.SCHEMA_EXCLUDE_FILE);
	            if (istream == null)
	            {
	            	FileInputStream finStream = new FileInputStream(Constants.SCHEMA_EXCLUDE_FILE);
	            	mapExcludeList.load(finStream);
	            	finStream.close();
	            } else
	            {
	            	mapExcludeList.load(istream);
	            	istream.close();
	            }
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private static void LoadDeployedObjects(String outputDirectory)
	{	
		String file = outputDirectory + "/savedobjects/"+Constants.DEPLOYED_OBJECT_FILE;
		if (FileExists(file))
		{
	        try
			{
	        	FileInputStream inputStream = new FileInputStream(file);
	        	deployedObjectsList.load(inputStream);
	        	inputStream.close();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private static void SaveDeployedObjects(String outputDirectory)
	{
		try
		{
			if (!FileExists(outputDirectory + "/savedobjects"))
			{
				new File(outputDirectory + "/savedobjects").mkdir();				
			}
			FileOutputStream ostream = new FileOutputStream(outputDirectory + "/savedobjects/"+Constants.DEPLOYED_OBJECT_FILE);
			deployedObjectsList.store(ostream, "-- Deployed Objects in DB2");
			ostream.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static void LoadCompatibilityFiles()
	{
		InputStream istream;		
        try
		{
			if (mapCompatibilityFiles == null)
			{
				mapCompatibilityFiles = new Properties();
	            istream = ClassLoader.getSystemResourceAsStream(Constants.MAP_COMPATIBILITY_FILES);
	            if (istream == null)
	            {
	            	FileInputStream finStream = new FileInputStream(Constants.MAP_COMPATIBILITY_FILES);
	            	mapCompatibilityFiles.load(finStream);
	            	finStream.close();
	            } else
	            {
	            	mapCompatibilityFiles.load(istream);
	            	istream.close();
	            }
	            log("Compatibility file loaded: '" + Constants.MAP_COMPATIBILITY_FILES + "'");	        
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}		
	}
	
	private static void LoadDeployFiles()
	{
		InputStream istream;		
        try
		{
			if (mapDeployFiles == null)
			{
				mapDeployFiles = new Properties();
	            istream = ClassLoader.getSystemResourceAsStream(Constants.DEPLOY_FILES_FILE);
	            if (istream == null)
	            {
	            	FileInputStream finStream = new FileInputStream(Constants.DEPLOY_FILES_FILE);
	            	mapDeployFiles.load(finStream);
	            	finStream.close();
	            } else
	            {
	            	mapDeployFiles.load(istream);
	            	istream.close();
	            }
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static String getCompatibilityFiles(String Vendor)
	{
		String fileList = "";
		LoadCompatibilityFiles();
		
		fileList = (String) mapCompatibilityFiles.getProperty(Vendor.toLowerCase());
	    return fileList;		
	}
	
	public static String getDeployFiles(String Vendor)
	{
		String fileList = "";
		LoadDeployFiles();
		
		if (Constants.netezza())
		   fileList = (String) mapDeployFiles.getProperty("dstnetezza");
		else
		   fileList = (String) mapDeployFiles.getProperty(Vendor.toLowerCase());
	    return fileList;		
	}

	public static String getExcludeList(String Vendor)
	{
		String[] strArray = null;
		String schemaList = "";
		LoadExcludeList();
		
		schemaList = (String) mapExcludeList.getProperty(Vendor.toLowerCase());
		if (schemaList != null)
			strArray = schemaList.split("~");
		schemaList = "";
		for (int i = 0; i < strArray.length; ++i)
		{
			if (i > 0)
				schemaList += ",";
			schemaList += "'" + strArray[i] + "'";
		}
	    return schemaList;		
	}
	
	private static boolean Excluded(String Vendor, String schemaName)
	{	
		String[] strArray = null;
		String schemaList = null;
		if (schemaName == null || schemaName.equals(""))
			return false;
		
		LoadExcludeList();
		
		schemaList = (String) mapExcludeList.getProperty(Vendor.toLowerCase());
		if (schemaList != null)
			strArray = schemaList.split("~");
		for (int i = 0; i < strArray.length; ++i)
		{
			if (strArray[i].endsWith("%"))
			{
				String str = strArray[i].substring(0,strArray[i].lastIndexOf('%'));
				if (schemaName.startsWith(str))
					return false;				
			} else
			{
				if (strArray[i].equalsIgnoreCase(schemaName))
					return false;
			}
		}
	    return true;
	}
	
    public static String GetSchemaList(String Vendor, String server, int port, String dbName, String userid, String pwd)
    {
    	Connection mainConn = null;
        ResultSet         Reader;
        DatabaseMetaData  dbMetaData;
        PreparedStatement statement = null;
        String tableSchema = "";
    	String schemaNames = "";
		int jdbcVersion = -1;

		DBData data = new DBData(Vendor, server, port, dbName, userid, pwd, "", 0);
		mainConn = data.getConnection();
		Message = data.Message;
		
		if (mainConn == null)
			return "";
		
		try
		{
            dbMetaData = mainConn.getMetaData();    
            String jdbc = dbMetaData.getDriverVersion();
            if (jdbc != null && Character.isDigit(jdbc.charAt(0)))
            	jdbcVersion = Character.getNumericValue(jdbc.charAt(0));
            if (data.Mysql())
            {
                Reader = dbMetaData.getCatalogs(); 
            } else if (data.Mssql() && jdbcVersion < 3)
            {
            	String sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";
            	statement = mainConn.prepareStatement(sql);
            	Reader = statement.executeQuery();
            } else if (data.Informix())
            {
            	String sql = "select distinct owner from systables union select distinct owner from sysprocedures";
            	statement = mainConn.prepareStatement(sql);
            	Reader = statement.executeQuery();
            } else if (data.Oracle())
            {
            	String sql = "select distinct owner from dba_objects order by owner";
            	statement = mainConn.prepareStatement(sql);
            	Reader = statement.executeQuery();            	
            }
            else
            {
                Reader = dbMetaData.getSchemas();             	
            }
            int i = 0;
            while (Reader.next()) 
            {
            	tableSchema = trim(Reader.getString(1));
            	if (Excluded(data.dbSourceName, tableSchema))
            	{
            		if (i > 0)
            		   schemaNames += ":";
            		schemaNames += tableSchema;
            		++i;
            	}
            }  
            if (!data.Informix())
               mainConn.commit();
            if (Reader != null)
               Reader.close();
            if (statement != null)
              statement.close();
            mainConn.close();
        }
		catch (SQLException ex)
		{
			if (ex.getErrorCode() == -4220)
			{
		        ex.printStackTrace();
				log("The error of Unsupported ccsid, encoding, or locale is a known problem.");
				log("This error occurs mostly due to incorrect JVM on your machine which does not have proper international language support.");
				log("Follow these steps:");
				log("1. Close this tool.");
				log("2. For Windows: ");
				log("   Add this entry in your IBMDataMovementTool.cmd file before java statement");
				log("   SET PATH=\"C:\\Program Files\\IBM\\SQLLIB\\java\\jdk\\jre\\bin\\\";%PATH%");
				log("3. For Unix: ");
				log("   Add this entry in your IBMDataMovementTool.sh file before java statement");
				log("   For 64 bit:");
				log("   PATH=~/sqllib/java/jdk64/jre/bin:$PATH");
				log("   For 62 bit:");
				log("   PATH=~/sqllib/java/jdk/jre/bin:$PATH");
				log("4. Re-run the tool. Tool will now use IBM provided JRE that comes with DB2");
			} else
			{
		           ex.printStackTrace();				
			}
		}
		catch (Exception e)
        {
           log(" Error Message :" + e.getMessage());
           e.printStackTrace();
        }		       
    	return schemaNames;
    }
    
    private static boolean isOracleIOTOverFlow(Connection conn, String schemaName, String tableName)
    {
    	boolean notOverFlow = true;
    	String sql = "SELECT 1 FROM DBA_TABLES WHERE OWNER = '" + schemaName + "' " +
    			"AND TABLE_NAME = '" + tableName + "' AND IOT_TYPE = 'IOT_OVERFLOW'";
    	try
		{
    		PreparedStatement statement = conn.prepareStatement(sql);
    		ResultSet rs = statement.executeQuery();
			if (rs.next()) 
			{
				notOverFlow = false;			
			}
			if (rs != null)
				rs.close();
			if (statement != null)
				statement.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		return notOverFlow;
    }
    
    private static void BuildMViewMap(Connection conn, String schemaList)
    {
    	mviewMap = new ArrayList();
    	String key, sql = "SELECT OWNER, MVIEW_NAME FROM DBA_MVIEWS WHERE OWNER IN (" + schemaList + ")";
    	
    	try
		{
    		PreparedStatement statement = conn.prepareStatement(sql);
    		ResultSet rs = statement.executeQuery();
			while (rs.next()) 
			{
				key = trim(rs.getString(1)) + "." + trim(rs.getString(2));
				if (!mviewMap.contains(key))
				{
					mviewMap.add(key);
				}
			}
			if (rs != null)
				rs.close();
			if (statement != null)
				statement.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
    }

    private static void BuildPartitionMap(Connection conn, String schemaList)
    {
    	String part;
    	partitionMap = new HashMap();
    	String key, value, sql = "SELECT TABLE_OWNER, TABLE_NAME, PARTITION_NAME FROM DBA_TAB_PARTITIONS WHERE TABLE_OWNER IN (" + schemaList + ")";
    	try
		{
    		PreparedStatement statement = conn.prepareStatement(sql);
    		ResultSet rs = statement.executeQuery();
			while (rs.next()) 
			{
				key = trim(rs.getString(1)) + "." + trim(rs.getString(2));
				value = trim(rs.getString(3));
				if (partitionMap.containsKey(key))
				{
					part = (String) partitionMap.get(key) + ":" + value;
					partitionMap.put(key, part);
				} else
					partitionMap.put(key, value);
			}
			if (rs != null)
				rs.close();
			if (statement != null)
				statement.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
    }
    
    private static String getOracleTableString(Connection conn, String schemaList,
    		String db2SchemaName, String dstTableName, String schemaName, String tableName)
    {
    	String key = schemaName+"."+tableName;
    	String[] partNames;
    	if (partitionMap == null)
    		BuildPartitionMap(conn, schemaList);
    	
    	StringBuffer sb = new StringBuffer();
    	
    	if (partitionMap.containsKey(key))
    	{
    		partNames = ((String) partitionMap.get(key)).split(":");
    		for (int i = 0; i < partNames.length; ++i)
    		{
			    sb.append("\""+db2SchemaName+"\".\""+dstTableName+"\":SELECT * FROM \"" + schemaName+"\".\""+tableName+"\" PARTITION("+partNames[i]+")" + linesep);
    		}
    	} else
    	{
			sb.append("\""+db2SchemaName+"\".\""+dstTableName+"\":SELECT * FROM \"" + schemaName+"\".\""+tableName+"\"" + linesep);    		
    	}
    	return sb.toString();
    }
    
    public static void deleteFile(String fileName)
    {
        File file = new File(fileName);
        if (!file.delete())
     	   log("Unable to delete the file " + file.getName());    	
    }
    
    public static void deleteFiles( String directory, final String extension ) 
    {
    	FilenameFilter filter = new FilenameFilter() 
		{ 
			public boolean accept(File dir, String name) 
			{ 
				return name.endsWith(extension); 
			} 
		}; 
        File dir = new File(directory);

        String[] list = dir.list(filter);
        File file;
        if (list.length == 0) return;
        for (int i = 0; i < list.length; i++) 
        {
           file = new File(directory, list[i]);
           if (!file.delete())
        	   log("Unable to delete the file " + file.getName());
        }
    }

    public static String loadReport(String outputDirectory, String extension, String baseName)
    {
    	return null;
    }
    
    public static String combinePipeLogs(String outputDirectory, String extension, String baseName)
    {
    	byte[] buffer = new byte[12000];
    	SequenceInputStream sis = new SequenceInputStream(new ListPipeLogs(outputDirectory, extension));
    	int len;
    	String logFileName;
    	
    	logFileName = filesep + baseName + ".log";
    	
    	try
		{
			DataOutputStream fp = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputDirectory + logFileName)));
			while ((len = sis.read(buffer)) > 0)
			{
				fp.write(buffer, 0, len);
			}
			fp.close();
    		logFileName = new File(outputDirectory + logFileName).getCanonicalPath();
		} 
    	catch (FileNotFoundException e1)
    	{
    		e1.printStackTrace();
    	}
    	catch (IOException e)
		{
			e.printStackTrace();
		}
    	return logFileName;
    }
    
    public static String getCaseName(String caseSensitiveTabColName, String name)
    {
         return caseSensitiveTabColName.equalsIgnoreCase("true") ? name : name.toUpperCase();
    }

    public static String exceptTableName(String caseSensitiveTabColName, String schemaName, String tableName, String exceptSchemaSuffix, String exceptTableSuffix)
    {
    	return getCaseName(caseSensitiveTabColName, putQuote(removeQuote(schemaName) + exceptSchemaSuffix) + "." + putQuote(tableName + exceptTableSuffix));
    }
    

    private static int RunSetIntegrity(PrintStream ps, String caseSensitiveTabColName, Connection mainConn, String tableList, String exceptSchemaSuffix, String exceptTableSuffix)
    {
    	String schemaName, tableName, setIntegrityCommand;
    	String sql = "select t1.TABSCHEMA,t1.TABNAME,t1.STATUS from syscat.tables t1, (VALUES "+tableList+ " " +
				" ) AS T(TSCHEMA,TNAME) " +
				" where t1.tabschema = t.tschema " +
				" and t1.tabname = t.tname " +
				" and t1.status = 'C'";

		int count = 0, sqlCode, sqlCODE;
		
		PreparedStatement queryStatement;
		ResultSet Reader = null;
		
		try
		{
		    queryStatement = mainConn.prepareStatement(sql);
		    Reader = queryStatement.executeQuery();
		    while (Reader.next()) 
		    {
		    	schemaName = trim(Reader.getString(1));
		    	tableName = trim(Reader.getString(2));
		    	setIntegrityCommand = "SET INTEGRITY FOR " + putQuote(schemaName) + "." +
		    	      putQuote(tableName)  + " IMMEDIATE CHECKED";    
		    	sqlCode = executeUpdate(mainConn, setIntegrityCommand);
		    	if (sqlCode == 0)
		    	{
			    	log(ps, "Command : '" + setIntegrityCommand + "' Successful");
		    	}
		    	else if (sqlCode == -3600)
		    	{
			    	log(ps, "Command : '" + setIntegrityCommand + "' was run but table is not in check pending");		    		
		    	}
		    	else if (sqlCode == -3608)
		    	{
			    	log(ps, "Command : '" + setIntegrityCommand + "' was run but this table has a parent that is still in check pending");		    		
		    	}
		    	else if (sqlCode == -3603)
		    	{
		    		String exceptName = exceptTableName(caseSensitiveTabColName, schemaName, tableName, exceptSchemaSuffix, exceptTableSuffix);
			    	log(ps, "Command : " + setIntegrityCommand + " failed for -3603");		    		
			    	String exceptTable = "CREATE TABLE " + exceptName + " AS (SELECT " +
			    			" * FROM " + putQuote(schemaName) + "." + putQuote(tableName) + ") DEFINITION ONLY";
			    	sqlCODE = executeUpdate(mainConn, exceptTable);
			    	log(ps, "Exception table : " + exceptName + " created. (RC="+sqlCODE+")");
			    	setIntegrityCommand = "SET INTEGRITY FOR " + putQuote(schemaName) + "." +
			    	    putQuote(tableName)  + " IMMEDIATE CHECKED FOR EXCEPTION IN " + putQuote(schemaName) + "." + putQuote(tableName) +
		    	             " USE " + exceptName;    
			    	sqlCODE = executeUpdate(mainConn, setIntegrityCommand);
			    	log(ps, "Check table " + exceptName + " for the rejected data. (RC="+sqlCODE+")");
		    	}
		    	count++;
		    }
		    if (Reader != null)
		        Reader.close(); 
		    if (queryStatement != null)
		    	queryStatement.close();
		} catch (Exception e)
		{
			log(ps, "Error is executing sql="+sql);
			e.printStackTrace(ps);
		}
		return count;
    }
    
    private static void checktargetNetezzaTableStatus(PrintStream ps, String caseSensitiveTabColName, String tableFile, String dbTargetName, String dstServer, int dstPort, 
    		String dstDBName, String dstUID, String dstPWD)
    {
    	BufferedReader inputFileReader;
    	boolean missingTables = false;
    	String OUTPUT_DIR = null, line = null, schemaName = "", tableName = "", sql = "";
    	StringBuffer missing = new StringBuffer();
		
		DBData data = new DBData(dbTargetName, dstServer, dstPort, dstDBName, dstUID, dstPWD, "", 0);
		data.setAutoCommit(false);
		data.setPrintStream(ps);
		//data.getConnection();
		
		Message = data.Message;

    	try
    	{
        	inputFileReader = new BufferedReader(new FileReader(tableFile));
            String level = getDB2ReleaseLevel(data);
    	    log(ps, "Checking tables in Netezza for existence. Please wait .....");
    	    int i = 0;
            while ((line = inputFileReader.readLine()) != null)
            {
            	String status;
            	if (!line.trim().equals(""))
            	{
                    if (!line.startsWith("#"))
                    {
                       tableName = line.substring(0, line.indexOf(":"));
                       schemaName = tableName.substring(0, tableName.indexOf("."));
                       tableName = tableName.substring(tableName.indexOf('.')+1);
                       sql = "SELECT COUNT(*) FROM _v_obj_relation WHERE OBJCLASS In (4905,4961,4953) AND DATABASE = '" + removeQuote(schemaName) + "' AND OBJNAME = '" +
                            removeQuote(tableName) + "'";
                       status = data.queryNZFirstRow(schemaName, sql);
                       if (status.equals("0"))
                       {
                    	   missing.append(schemaName + ".." + tableName + " is missing" + linesep);
                    	   missingTables = true;
                       }
                       ++i;
                    }
            	}
            }
            inputFileReader.close();    		
            if (missingTables)
            {
            	log(ps,missing.toString());
            	Message = "Tables are missing in Netezza. Please create them and rerun.";
            }
            data.close();
    	    log(ps, "Completed...");
    	}
		catch (Exception e)
		{
			e.printStackTrace(ps);
		}    	
    }
    
    public static void checkTargetTablesStatus(PrintStream ps, boolean loadException, String caseSensitiveTabColName, String exceptSchemaSuffix, String exceptTableSuffix, String tableFile, String dbTargetName, String dstServer, int dstPort, 
    		String dstDBName, String dstUID, String dstPWD)
    {
    	
    	if (Constants.netezza())
    	{
    		checktargetNetezzaTableStatus(ps, caseSensitiveTabColName, tableFile, Constants.getDbTargetName(), dstServer, dstPort, dstDBName, dstUID, dstPWD);
    		return;
    	}
    	    	
    	StringBuffer checkBuffer = new StringBuffer();
    	boolean ok = true, missingTables = false;
    	BufferedReader inputFileReader;
    	BufferedWriter writer = null;
    	StringBuffer sb = new StringBuffer();
    	StringBuffer missing = new StringBuffer();
    	String OUTPUT_DIR = null, line = null, schemaName, tableName, sql = "";
    	
		DBData data = new DBData(dbTargetName, dstServer, dstPort, dstDBName, dstUID, dstPWD, "", 0);
		data.setAutoCommit(false);
		data.setPrintStream(ps);
		data.getConnection();
		Message = data.Message;
		
		if (data.connection == null)
			return;
		
		try
		{
			OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
        	if (OUTPUT_DIR == null || OUTPUT_DIR.equals(""))
        	{
        		OUTPUT_DIR = ".";
        	}		
        	inputFileReader = new BufferedReader(new FileReader(tableFile));
            String level = getDB2ReleaseLevel(data);
     	    float rel = -1.0F;
    	    try {  rel = Float.valueOf(level); } catch (Exception e) {}
    	    log(ps, "Checking tables for existence, load pending and check pending in DB2. Please wait .....");
    	    int i = 0;
            while ((line = inputFileReader.readLine()) != null)
            {
            	String status;
            	if (!line.trim().equals(""))
            	{
                    if (!line.startsWith("#"))
                    {
                       tableName = line.substring(0, line.indexOf(":"));
                       schemaName = tableName.substring(0, tableName.indexOf("."));
                       tableName = tableName.substring(tableName.indexOf('.')+1);
                       if (i > 0)
                    	   checkBuffer.append(",");
                       checkBuffer.append("('"+removeQuote(schemaName)+"','"+removeQuote(tableName)+"')");
                       sql = "SELECT TRIM(CREATOR)||'.'||NAME FROM SYSIBM.SYSTABLES WHERE CREATOR = '" + removeQuote(schemaName) + "' AND NAME = '" +
                            removeQuote(tableName) + "' AND TYPE IN ('T','G','S','U')";
                       status = executeSQL(data.connection, sql, "~", false);
                       if (status == null || status.length() == 0)
                       {
                    	   missing.append(schemaName + "." + tableName + " is missing" + linesep);
                    	   missingTables = true;
                       }
                       if (loadException)
                       {
	                       sql = "SELECT TRIM(CREATOR)||'.'||NAME FROM SYSIBM.SYSTABLES WHERE CREATOR = '" + removeQuote(schemaName) + removeQuote(exceptSchemaSuffix) + "' AND NAME = '" +
	                       removeQuote(tableName) + removeQuote(exceptTableSuffix) +"' AND TYPE IN ('T','G','S','U')";
			               status = executeSQL(data.connection, sql, "~", false);
			               if (status == null || status.length() == 0)
			               {
			               	   missing.append(schemaName + removeQuote(exceptSchemaSuffix) + "." + tableName + removeQuote(exceptTableSuffix)+ " is missing" + linesep);
			               	   missingTables = true;
			               }
                       }
                       if (level.length() > 0)
                       {
                    	   if (rel >= 9.5F)
                    	   {
                    		   sql = "SELECT LOAD_STATUS FROM TABLE (SYSPROC.ADMIN_GET_TAB_INFO('" + removeQuote(schemaName) + 
            		                        "', '" + removeQuote(tableName) + "')) AS T";
                    		   status = executeSQL(data.connection, sql, "~", false);
                    		   if (status.equalsIgnoreCase("PENDING"))
                    		   {
                    			   ok = false;
                    	           if (Constants.win())
                    	        	   sb.append("LOAD FROM NUL: OF DEL TERMINATE INTO " + putQuote(getCaseName(caseSensitiveTabColName,schemaName))+"."+putQuote(getCaseName(caseSensitiveTabColName,tableName)) + ";" + linesep);
                    	           else
                    	        	   sb.append("LOAD FROM /dev/null OF DEL TERMINATE INTO " + putQuote(getCaseName(caseSensitiveTabColName,schemaName))+"."+putQuote(getCaseName(caseSensitiveTabColName,tableName)) + ";" + linesep);        	   
                    		   }
                    	   }                       	    
                       }
                       ++i;
                    }
            	}
            }
            inputFileReader.close();
            if (missingTables)
            {
            	log(ps,missing.toString());
            	Message = "Tables are missing in DB2. Please create them and rerun.";
            	log(ps,Message);
            } else
            {
            	int bruteForceCount = 1;
            	int checkIterationCount = RunSetIntegrity(ps, caseSensitiveTabColName, data.connection, checkBuffer.toString(), exceptSchemaSuffix, exceptTableSuffix);
            	while (bruteForceCount < 11 && checkIterationCount > 0)
            	{
            		log(ps, checkIterationCount + " tables were in check pending. Using brute force " + bruteForceCount + "/10");
            		checkIterationCount = RunSetIntegrity(ps, caseSensitiveTabColName, data.connection, checkBuffer.toString(), exceptSchemaSuffix, exceptTableSuffix);
            		bruteForceCount++;
            	}
            	if (checkIterationCount > 0)
            	{
	            	sql = " WITH GEN(tabname, seq) AS (SELECT RTRIM(TABSCHEMA)||'.' ||RTRIM(TABNAME) AS TABNAME, ROW_NUMBER() " +
	            			"	OVER (PARTITION BY STATUS) as seq FROM (SELECT t1.TABSCHEMA TABSCHEMA,t1.TABNAME TABNAME,t1.STATUS STATUS " +
	            			"	FROM SYSCAT.TABLES t1, (VALUES  " + checkBuffer.toString() + " " +
	            			"   ) AS T(TSCHEMA,TNAME) " +
	            			"   WHERE T1.TABSCHEMA = T.TSCHEMA " +
	            			"    AND T1.TABNAME = T.TNAME " +
	            			"    AND T1.STATUS = 'C' " +
	            			"	)) , r(a, seq1) AS " +
	            			"	(SELECT CAST(TABNAME as VARCHAR(32000)), SEQ FROM gen WHERE seq=1 UNION ALL " +
	            			" 	SELECT CAST(r.a ||','||RTRIM(gen.tabname) AS VARCHAR(32000)), gen.seq FROM gen, r " +
	            			"	WHERE (r.seq1+1)=gen.seq), r1 AS (SELECT a, seq1 FROM r) " +
	            			"	SELECT 'SET INTEGRITY FOR ' || a || ' IMMEDIATE CHECKED' FROM r1 WHERE seq1=(SELECT MAX(seq1) FROM r1)";
				    String setIntegrityCommand = executeSQL(data.connection, sql, "~", false);
		            if (setIntegrityCommand.length() > 0)
		            {
			    	    log(ps, "Will run Set Integrity Commad = " + setIntegrityCommand);
			    	    int sqlCode = executeUpdate(data.connection, setIntegrityCommand);
			    	    log(ps, "Status = " + sqlCode);
		            }
            	}
            	if (!ok)
            	{
            		String file = OUTPUT_DIR + filesep + "db2fixtables.db2";
	            	writer = new BufferedWriter(new FileWriter(file, false));	            	
	            	writer.write("CONNECT TO " + dstDBName + " USER " + dstUID + " USING " + dstPWD + ";" + linesep);
	            	writer.write(sb.toString());
	            	writer.write("TERMINATE;" + linesep);
	            	writer.close();
	            	Message = "Please review " + file + "and run it.";
		    	    log(ps, Message);
            	}
            }
            data.close();
    	    log(ps, "Completed...");
		} catch (Exception e)
		{
			e.printStackTrace(ps);
		}
    }
    
    private static String getSrctoDstSchema(String db2SchemaName, String schemaList, String schema)
    {
    	if (db2SchemaName == null || db2SchemaName.length() == 0 || db2SchemaName.equalsIgnoreCase("ALL"))
    	{
    		return schema;
    	} else
    	{
	    	String[] srcSchName = schemaList.split(":");
	    	String[] schemaName = db2SchemaName.split(":");
	    	if (srcSchName.length != schemaName.length)
	    	{
	    		log("# of dstSchemaName does not match with # of srcSchemaName in "+IBMExtractUtilities.getConfigFile()+".");
	    		return schema;
	    	}
	    	for (int i = 0; i < srcSchName.length; ++i)
	    	{
	    		if (schema.equals(removeQuote(srcSchName[i])))
	    			return removeQuote(schemaName[i]);
	    	}
    	}
    	return schema;
    }
    
    public static String getFullPathName(String name)
    {
    	File fp = new File(name);
    	String namePath;
		try
		{
			namePath = fp.getCanonicalPath();
		} catch (IOException e)
		{
			e.printStackTrace();
			return name;
		}
    	return namePath;
    }
    
    public static StringBuffer CommentString(String commentCard, StringBuffer buffer)
    {
    	StringBuffer newBuffer = new StringBuffer();
    	String regex = "^(.*)";
		String s;
		Pattern p = Pattern.compile(regex,Pattern.MULTILINE);
		Matcher m = p.matcher(buffer.toString());
		while (m.find())
		{
            newBuffer.append("--| " + m.group() + linesep);
		}
		newBuffer.insert(0, "--| " + commentCard + linesep);
    	return newBuffer;
    }
    
    public static void CheckSourceSQL(Connection bladeConn, String Vendor, String sql) throws SQLException
    {
    	ResultSet Reader = null;
        Statement statement;
        statement = bladeConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
        Reader = statement.executeQuery(sql);
        ResultSetMetaData rsmtadta = Reader.getMetaData();
        int colCount = rsmtadta.getColumnCount();
        while (Reader.next())
        {
            for( int i = 1; i <= colCount; i++ ) 
            {
            	String tmp = Reader.getString(i);
            }
        }
        if (Reader != null)
        	Reader.close();
        if (statement != null)
        	statement.close();
    }
    
    public static void ExecuteUpdateSQLinSeparateConnection(String Vendor, String server, int port, 
    		String dbName, String userid, String pwd, String sql)
    {
    	Connection conn = null;
    	String url;
    	
    	DBData data = new DBData(Vendor, server, port, dbName, userid, pwd, "", 0);
    	conn = data.getConnection();
    	
    	if (conn == null)
    		return;
    	
    	
        Statement statement = null;		
        try
		{
	        statement = conn.createStatement();
	        statement.executeUpdate(sql);
	        if (statement != null)
	        	statement.close();
	        if (conn != null)
	        	conn.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}    	
    }
    
    public static String ReBuildSQLQuery(DBData data, String sql, String schema, String table,
    		String server, int port, String dbName, String uid, String pwd)
    {
        PreparedStatement queryStatement = null;
        ResultSet         Reader = null;
        StringBuffer buffer = new StringBuffer();
        String value, colName, key, colSQL = "", colType;
        int count;
        boolean defined = false;

    	if (!(data.Mssql() || data.Mysql()))
    		return sql;

    	if (custDatamap == null)
    	   custDatamap = InstantiateCustomProperties(data.dbSourceName, "CustDataMapPropFile");

    	if (custDatamap == null || custDatamap.size() == 0)
			return sql;
		
		try
		{
            colSQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
				"WHERE TABLE_SCHEMA = '" + removeQuote(schema) + "' " +
				"AND TABLE_NAME = '" + removeQuote(table) + "' " +
				"ORDER BY ORDINAL_POSITION ASC";

            count = 0;
    		queryStatement = data.connection.prepareStatement(colSQL);
    		Reader = queryStatement.executeQuery();
    		while (Reader.next())
    		{
    			if (count > 0)
    				buffer.append(", ");
    			value = null;
    			colName = trim(Reader.getString(1));
    			key = removeQuote(schema) + "." + removeQuote(table) + "." + removeQuote(colName);
    			if (custDatamap.containsKey(key))
    			{
    				value = custDatamap.getProperty(key);
    				if (value != null)
    				{
	    				String[] vals = value.split(":");
	    				if (data.Mssql())
	    				{
	        				colType = vals[0];
		    				if (vals.length == 2)
		    				{
		        				defined = true;
		    					if (colType.matches("(?i)VARCHAR.*") || colType.matches("(?i)CLOB.*"))
		    					   buffer.append("convert(varchar,DecryptByKeyAutoCert(cert_id('"+vals[1]+"'),null,"+colName+")) " + colName);
		    					else if (colType.matches("(?i)VARGRAPHIC.*") || colType.matches("(?i)DBCLOB.*"))
				    			   buffer.append("convert(nvarchar,DecryptByKeyAutoCert(cert_id('"+vals[1]+"'),null,"+colName+")) " + colName);
		    					else if (colType.matches("(?i)BLOB.*") || colType.matches("(?i)VARCHAR.*FOR\\s+BIT\\+DATA.*")
		    							|| colType.matches("(?i)CHAR.*FOR\\s+BIT\\+DATA.*"))
			    				   buffer.append("convert(varbinary,DecryptByKeyAutoCert(cert_id('"+vals[1]+"'),null,"+colName+")) " + colName);
		    					else
		    						buffer.append(colName);
		    				} else if (vals.length == 5)
		    				{
		        				defined = true;
		    					buffer.append(vals[2] + " " + colName);	 
		    					// Open certificate in a separate connection
		    					String openKeySQL = "OPEN SYMMETRIC KEY "+vals[3]+" DECRYPTION BY CERTIFICATE "+vals[4];
		    					executeUpdate(data.connection, openKeySQL);
		    					//ExecuteUpdateSQLinSeparateConnection(data.dbSourceName, server, port, dbName, login, openKeySQL);		    					
		    				} else
		    				{
		    					buffer.append(colName);	    							    					
		    				}
	    				}
	    				else if (data.Mysql())
	    				{
	        				defined = true;
		    				if (vals.length == 3)
		    				{
		    					buffer.append(vals[2] + " " + colName);	    					
		    				} else
		    				{
		    					buffer.append(colName);	    							    					
		    				}
	    				}
    				} else
    				{
        				buffer.append(colName);   
        				log("Something is wrong in your " + Constants.CUSTMAP_PROP_FILE + " for = " + key);
    				}
    			} else
    			{
    				buffer.append(colName);    				    				
    			}
    			count++;
    		}    		
            if (Reader != null)
            	Reader.close();
            if (queryStatement != null)
            	queryStatement.close();
		} catch (Exception e)
		{
    		buffer.setLength(0);
    		buffer.append("*");
			e.printStackTrace();
		}
		return (defined) ? "SELECT " + buffer.toString() + " FROM " + putQuote(schema) + "." + putQuote(table) : sql;
    }
    
    public static Properties InstantiateProperties(String dToken)
    {
    	Properties prop = new Properties();
    	String propFile = System.getProperty(dToken);
    	if (propFile == null || propFile.equals(""))
    	{
    		if (dToken.equalsIgnoreCase("DataMapPropFile"))
    			propFile = Constants.DATAMAP_PROP_FILE;
     		if (dToken.equalsIgnoreCase("DataMapNZPropFile"))
    			propFile = Constants.DATAMAPNZ_PROP_FILE;
    	} else
    	{
    		if (dToken.equalsIgnoreCase("DataMapPropFile"))
     		   DATAMAP_PROP_FILE = propFile;
     		if (dToken.equalsIgnoreCase("DataMapNZPropFile"))
      		   DATAMAPNZ_PROP_FILE = propFile;
    	}
        try
        {
	        if (FileExists(propFile))
	        {
	        	prop.load(new FileInputStream(propFile));
	            log("Local Configuration file loaded: '" + propFile + "'" + "("+prop.size()+")");            	
	        } else
	        {
	        	InputStream istream = ClassLoader.getSystemResourceAsStream(propFile);
	            if (istream == null)
	            {
	                prop.load(new FileInputStream(propFile));
	                log("Configuration file loaded: '" + propFile + "'" + "("+prop.size()+")");
	            } else
	            {
	                prop.load(istream);
	                log("Configuration file loaded from jar: '" + propFile + "'" + "("+prop.size()+")");
	            }            	
	        }
        } catch (Exception e)
        {
        	log("Error loading " + dToken + " property file " + propFile);
        	prop = null;
        	e.printStackTrace();
        }
        return prop;
    }
    
    public static Properties InstantiateCustomProperties(String vendor, String propDToken)
    {
    	String OUTPUT_DIR = null, propFileName;
    	Properties prop = new Properties();
    	OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
    	if (OUTPUT_DIR == null || OUTPUT_DIR.equals(""))
    	{
    		OUTPUT_DIR = ".";
    	}		
    	File tmpfile = new File(OUTPUT_DIR);
        tmpfile.mkdirs();
        if (propDToken.equalsIgnoreCase("CustDataMapPropFile"))
        {
        	propFileName = Constants.CUSTMAP_PROP_FILE;
        	CreateCustomDataMappingFile(vendor, OUTPUT_DIR + filesep + propFileName);
            loadPropertyFile(prop, OUTPUT_DIR + filesep + propFileName, propDToken);
        } 
        else if (propDToken.equalsIgnoreCase("CustTableSpaceMapPropFile"))
        {
        	propFileName = Constants.CUSTTSBP_PROP_FILE;
        	CreateCustomTableSpaceMappingFile(OUTPUT_DIR + filesep + propFileName);
            loadPropertyFile(prop, OUTPUT_DIR + filesep + propFileName, propDToken);
        } 
        else if (propDToken.equalsIgnoreCase("CustNullPropFile"))
        {
    	    propFileName = Constants.CUSTNULL_PROP_FILE;
        	CreateCustomNullMappingFile(OUTPUT_DIR + filesep + propFileName);
            loadPropertyFile(prop, OUTPUT_DIR + filesep + propFileName, propDToken);
        } else if (propDToken.equalsIgnoreCase("CustColNamePropFile"))
        {
        	propFileName = Constants.CUSTCOLNAME_PROP_FILE;
        	CreateCustomColNameMappingFile(OUTPUT_DIR + filesep + propFileName);
            loadPropertyFile(prop, OUTPUT_DIR + filesep + propFileName, propDToken);
        } else if (propDToken.equalsIgnoreCase("CustomColumnDistribution"))
        {
        	propFileName = Constants.CUSTOM_COLUMN_DISTRIBUTION;
        	CreateCustomColumnDistribution(OUTPUT_DIR + filesep + propFileName);
            loadPropertyFile(prop, OUTPUT_DIR + filesep + propFileName, propDToken);
        }
        return prop;
    }
    
    public static int CreateTableScript(String vendor, String dstSchNames, String srcSchNames, String selectedSchemaList, String server, 
    		int port, String dbName, String userid, String pwd, boolean compatibility, String caseSensitive)
    {
    	partitionMap = null;
    	mviewMap = null;
    	String OUTPUT_DIR = null, url = "";
        InstantiateCustomProperties(vendor, "CustDataMapPropFile");
        InstantiateCustomProperties(vendor, "CustNullPropFile");
        InstantiateCustomProperties(vendor, "CustColNamePropFile");
       
    	int majorSourceDBVersion = -1;
    	boolean putComment = true;
    	Connection mainConn = null;
    	String sql = "", tableName = "", schemaName = "", tableType[] = {"TABLE"}, targetSchema, colNames;
    	PreparedStatement statement = null;
        ResultSet         Reader = null;
        DatabaseMetaData  dbMetaData;
        
        DBData data = new DBData(vendor, server, port, dbName, userid, pwd, "", 0);
        mainConn = data.getConnection();
        
        if (mainConn == null)
        	return -1;

        if (mviewMap == null && data.Oracle())
        	BuildMViewMap(mainConn, GetSingleQuotedString(selectedSchemaList,":"));
		StringBuffer buffer = new StringBuffer(); 
		caseSensitive = (caseSensitive == null || caseSensitive.length() == 0) ? "fasle" : caseSensitive.trim();

		Message = "";
		try
        {
	    	OUTPUT_DIR = System.getProperty("OUTPUT_DIR");
	    	if (OUTPUT_DIR == null || OUTPUT_DIR.equals(""))
	    	{
	    		OUTPUT_DIR = ".";
	    	}		
	    	File tmpfile = new File(OUTPUT_DIR);
	        tmpfile.mkdirs();

            if (srcSchNames == null || srcSchNames.equals("") || srcSchNames.equalsIgnoreCase("all"))
            {
            	srcSchNames = GetSchemaList(vendor, server, port, dbName, userid, pwd);
            }
            if (dstSchNames == null || dstSchNames.equals(""))
            {
            	dstSchNames = srcSchNames;
            }

            if (data.Access() || data.Domino())
            {
                dbMetaData = mainConn.getMetaData();            
                Reader = dbMetaData.getTables(null, null, null, null);                
            }
            else if (data.Oracle())
            {
            	
            	sql = "select a.owner,a.owner,a.table_name from dba_tables a where nvl(a.iot_type,'TABLE') in ('TABLE','IOT') and " +
            			"a.owner in ("+GetSingleQuotedString(selectedSchemaList,":")+") order by a.owner";
            	statement = mainConn.prepareStatement(sql);
            	Reader = statement.executeQuery();
            }
            else if (data.DB2() || data.zDB2())
            {
            	
            	sql = "select a.creator,a.creator,a.name from sysibm.systables a where a.type = 'T' and " +
            			"a.creator in ("+GetSingleQuotedString(selectedSchemaList,":")+") order by a.creator";
            	statement = mainConn.prepareStatement(sql);
            	Reader = statement.executeQuery();
            }
            else
            {
                dbMetaData = mainConn.getMetaData();            
                Reader = dbMetaData.getTables(null, "%", "%", tableType);
            }
            
            try {majorSourceDBVersion = mainConn.getMetaData().getDatabaseMajorVersion(); log("CreateTableScript Database Major Version :" + majorSourceDBVersion); } catch (Exception e) {}

            tableList.clear();
            buffer.setLength(0);
            while (Reader.next()) 
            {
            	if (putComment)
            	{
            		putComment = false;
                    buffer.append("#### Comments begin with the # sign at the beginning of the line at col 1." + linesep);
                    buffer.append("#### This file is an input to the unload command. The format of this file is:" + linesep);
                    buffer.append("#### <TargetSchemaName>.<TargetTableName>:<Query to run on the source server>" + linesep);
                    buffer.append("#### \"DMT\".\"EMP\":SELECT * FROM \"DMPTEST\".\"EMP\"" + linesep);
                    buffer.append("#### The DMPTEST.EMP table will migrate to DMT.EMP table" + linesep);
                    buffer.append("#### Edit this file manually to change the target schema name." + linesep);            		
                    buffer.append("#### or, You can edit dstSchemaName field in "+IBMExtractUtilities.getConfigFile()+" file to map source schema"+linesep);
                    buffer.append("#### to destination schema name and run geninput command again to generate this file." + linesep + linesep);            		
            	}
                tableName = trim(Reader.getString(3));
                String dstTableName = caseSensitive.equalsIgnoreCase("true") ? tableName : tableName.toUpperCase();
                schemaName = (data.Mysql()) ? trim(Reader.getString(1)) : trim(Reader.getString(2));
                if (data.Access() && tableName.startsWith("MSys"))
                	continue;
                if (data.Oracle())
                {
                	if (tableName.startsWith("BIN$") || tableName.matches("SYS_.*=="))
                	   continue;
                	if (mviewMap.contains(schemaName+"."+tableName))
                		continue;
                }
                if (data.Sybase())
                {
                	if (tableName.equals("rs_lastcommit") || tableName.equals("rs_threads") || tableName.equals("rs_ticket_history"))
                	continue;
                }
                if (schemaName == null || schemaName.equals(""))
                {
                    colNames = "*";
                    if (dstSchNames == null || dstSchNames.length() == 0)
                	   targetSchema = caseSensitive.equalsIgnoreCase("true") ? dbName : dbName.toUpperCase();
                    else
                       targetSchema = caseSensitive.equalsIgnoreCase("true") ? dstSchNames : dstSchNames.toUpperCase();
                    if (data.Mysql() || data.Sybase())
                    {
                        buffer.append(putQuote(targetSchema)+"."+putQuote(dstTableName)+":SELECT "+colNames+" FROM " + tableName + linesep);
                    } else
                    {
                        buffer.append(putQuote(targetSchema)+"."+putQuote(dstTableName)+":SELECT "+colNames+" FROM " + putQuote(tableName) + linesep);
                    }
                }
                else
                {
                	targetSchema = getSrctoDstSchema(dstSchNames, srcSchNames, schemaName);
                	targetSchema = caseSensitive.equalsIgnoreCase("true") ? targetSchema : targetSchema.toUpperCase();
                	targetSchema = targetSchema.replaceAll("\\\\", "\\\\\\\\");
                	String[] strArray = selectedSchemaList.split(":");
            		for (int i = 0; i < strArray.length; ++i)
            		{
            			if (strArray[i].equalsIgnoreCase(schemaName))
            			{
                            colNames = "*";
            				if (data.Sybase() && compatibility)
                    		{
                    			targetSchema = dbName + "_" + targetSchema.toUpperCase();
                    		}
                    		if (data.Oracle())
                    		{
                    			buffer.append(getOracleTableString(mainConn, GetSingleQuotedString(selectedSchemaList,":"), targetSchema, dstTableName, schemaName, tableName));
                    		} else
                    		{
                    			if (data.Sybase())
                    			{
                    				if (majorSourceDBVersion == -1 || majorSourceDBVersion <= 12)
                    				{
                    			        buffer.append(putQuote(targetSchema)+"."+putQuote(dstTableName)+":SELECT "+colNames+" FROM " + putQuote(schemaName)+"."+tableName + linesep);
                    				} else
                    				{
                    			        buffer.append(putQuote(targetSchema)+"."+putQuote(dstTableName)+":SELECT "+colNames+" FROM " + putQuote(schemaName)+"."+putQuote(tableName) + linesep);
                    				}
                    			} else
                    			{
                    			    buffer.append(putQuote(targetSchema)+"."+putQuote(dstTableName)+":SELECT "+colNames+" FROM " + putQuote(schemaName)+"."+putQuote(tableName) + linesep);                    				
                    			}
                    		}
            			}
            		}
                }
            	tableList.add(targetSchema+"."+dstTableName);
            }  
            log(buffer.toString());
            String fileName = OUTPUT_DIR + filesep + dbName+".tables";
            BufferedWriter inputFileWriter = new BufferedWriter(new FileWriter(fileName, false));
            inputFileWriter.write(buffer.toString());
            inputFileWriter.close();      
            log("Table list is saved in " + (new File(fileName)).getCanonicalFile());
     	   if (!data.Informix())
               mainConn.commit();
            if (statement != null)
              statement.close();
            if (Reader != null)
              Reader.close();
            mainConn.close();
        } catch (Exception e)
        {
        	if (data.Access())
        	{
        		if (e.getMessage().startsWith("[Microsoft][ODBC Driver Manager] Data source name not found"))
        		{
        			log("It appears that you do not have " + System.getProperty("sun.arch.data.model") + " bit MDAC driver installed on this system for ACCESS database.");
        			log("Or Override javaHome property in IBMExtract.properties file with 32 bit Java and run again.");
        			log("Or Change JAVA_HOME in the script to the 32 bit Java and try again from the command line.");
        		} else
        		{
                    log("Error for url " + url + " Error Message :" + e.getMessage());
                    Message = data.dbSourceName + " Error encountered. Please see console's output.";        			
        		}
        	} else
        	{
               log("Error for url " + url + " Error Message :" + e.getMessage());
               Message = data.dbSourceName + " Error encountered. Please see console's output.";
               e.printStackTrace();
        	}
        }
        if (Constants.netezza())
           InstantiateCustomProperties(data.dbSourceName, "CustomColumnDistribution");
        else
           InstantiateCustomProperties(data.dbSourceName, "CustTableSpaceMapPropFile");
        return tableList.size();
    }
    
    private static boolean isDB2CompatiblitySet(DBData data)
    {
    	String methodName = "isDB2CompatiblitySet";
		String line = null, db2Compatibility = null;
		Process p = null;
		boolean found = false;

		if (data.connection == null)
		{
			found = true;
		} 
		else
		{
			SQL s = new SQL(data.connection);
	        try
	        {
		    	String sql = "SELECT REG_VAR_VALUE " +
		    			"FROM SYSIBMADM.REG_VARIABLES " +
		    			"WHERE REG_VAR_NAME = 'DB2_COMPATIBILITY_VECTOR' " +
		    			"FETCH FIRST ROW ONLY";
		    	s.PrepareExecuteQuery(methodName, sql);
				while (s.next()) 
				{
					db2Compatibility = s.rs.getString(1);
					found = true;
				}
				s.close(methodName);
	        } catch (Exception e)
	        {
				db2Compatibility = "";
	        	e.printStackTrace();
	        }
	        if (!found)
	        {
				log("*** WARNING ***. The DB2_COMPATIBILITY_VECTOR is not set.");
				log("To set compatibility mode, discontinue this program and run the following commands");
				log("For Oracle:");
				log("db2set DB2_COMPATIBILITY_VECTOR=ORA");
				log("For Sybase:");
				log("db2set DB2_COMPATIBILITY_VECTOR=SYB");
				log("db2stop force");
				log("db2start");			
				Message = "DB2_COMPATIBILITY_VECTOR is not set for DB2.";
	        }
	        log("DB2 Compatibility Vector=" + db2Compatibility);
		}
		return found;    	
    }
    
    public static boolean isJDBCLicenseAdded(String vendor, String jarNames)
    {
    	if (vendor.equals(Constants.db2luw) || vendor.equals(Constants.zdb2))
    	{
    		if (jarNames.contains("db2jcc_license_cu.jar") || jarNames.contains("db2jcc4_license_cu.jar") ||
    		    jarNames.contains("db2jcc_license_cisuz.jar") || jarNames.contains("db2jcc4_license_cisuz.jar"))
    			return true;
    		else
    		{
    			Message = "db2jcc_license_cu.jar or db2jcc4_license_cu.jar or db2jcc_license_cisuz.jar or db2jcc4_license_cisuz.jar file not included.";
    			return false;
    		}
    	} else
    	  return true;
    }    

    public static String CheckUnixProcesses()
    {
        String[] cmd = new String[]{IBMExtractUtilities.getShell(),"-c", "ps -ef | grep \"IBMExtractPropFile\" | grep -v grep"};             
    	String pname = "";
		String line = null;
		Process p = null;
		BufferedReader stdInput = null;

		if (!Constants.win())
		{
			try
			{
				p = Runtime.getRuntime().exec(cmd);
				stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = stdInput.readLine()) != null)
				{
					pname += "," + line.split(" ")[1];
				}
				if (stdInput != null)
					stdInput.close();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		return pname;
    }

    public static String getBashShellName()
    {
    	if (bashShellName == null)
    	{
			String line = null;
			Process p = null;
			BufferedReader stdInput = null;
	
			try
			{
				p = Runtime.getRuntime().exec("which bash");
				stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = stdInput.readLine()) != null)
				{
					bashShellName = line;
				}
				if (stdInput != null)
					stdInput.close();
			} catch (Exception e)
			{
				
			}
			//log("bash shell name " + bashShellName);
    	}
		return bashShellName;    	    	
    }
    
    public static boolean isDB2COMMSet()
    {
		String line = null;
		Process p = null;
		boolean found = false;
		BufferedReader stdInput = null;

		try
		{
			p = Runtime.getRuntime().exec("db2set -all");
			stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = stdInput.readLine()) != null)
			{
				if (line.contains("DB2COMM"))
				{
					found = true;
				}
			}
			if (stdInput != null)
				stdInput.close();
		} catch (Exception e)
		{
			found = false;
		}
		if (!found)
		{
			log("*** WARNING ***. I did not detect DB2COMM set for TCPIP.");
			log("To set db2comm mode, discontinue this program and run the following commands");
			log("db2set DB2COMM=TCPIP");
			log("db2stop force");
			log("db2start");			
			Message = "DB2COMM is not set for DB2.";
		}
		return found;    	
    }
    
    public static String getShell()
    {
    	String shell = null;
    	if (Constants.win())
    		shell = "cmd";
    	else
    	{
    		String line;
    		Process p;
			try
			{
				p = Runtime.getRuntime().exec("env");
	    		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = stdInput.readLine()) != null)
				{
					if (line.startsWith("SHELL"))
					{
						shell = line.substring(line.indexOf("=")+1);
						break;
					}
				}				
				p.destroy();
				if (stdInput != null)
					stdInput.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
			if (shell.equalsIgnoreCase("cmd"))
				shell = "/bin/sh";
    	}
    	return shell;
    }
    
    public static String getDB2PathName()
    {
    	if (DB2Path.length() == 0)
    	{
    		DB2Path = getDB2PathName2();
    	}
    	return DB2Path;
    }
    
    public static int GetFileNumLimit()
    {
        int n = Integer.MAX_VALUE;
    	if (!Constants.win())
    	{
    		try
    		{
    			String line, cmdArg = "-c", ulimit = "ulimit -n";
    			String shellName = IBMExtractUtilities.getShell();
				String[] cmd = new String[] {shellName, cmdArg, ulimit};
        	    Process p = Runtime.getRuntime().exec(cmd);
        		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
    			while ((line = stdInput.readLine()) != null)
    			{
    				if (line.equalsIgnoreCase("unlimited"))
    					n = Integer.MAX_VALUE;
    				else
    				{
    				    try { n = Integer.parseInt(line); } catch (Exception ex) { ; }
    				}
    			}
    			p.destroy();				
    			if (stdInput != null)
    			   stdInput.close();
    		} catch (Exception e)
    		{
    			e.printStackTrace();
    		}    		
    	}
    	return n;
    }
    
    public static String getDB2PathName2()
    {
    	String home = "";
		String line;
		Process p;
		try
		{
			p = (Constants.win()) ? Runtime.getRuntime().exec("db2cmd /c /i /w set ") : Runtime.getRuntime().exec("env");
    		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = stdInput.readLine()) != null)
			{
		    	if (Constants.win())
		    	{
					if (line.startsWith("DB2PATH"))
					{
						home = line.substring(line.indexOf("=")+1);
						break;
					}
		    	} else
		    	{
					if (line.startsWith("CLASSPATH"))
					{						
						String[] tok = line.split(":");
						for (int i = 0; i < tok.length; ++i)
						{
							if (tok[i].endsWith("db2jcc.jar") || tok[i].endsWith("db2jcc4.jar"))
							{
								home = tok[i].substring(0,tok[i].indexOf("sqllib/")+6);
								break;
							}
						}
					}
		    	}
			}
			p.destroy();
			if (stdInput != null)
				stdInput.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		//log("DB2 PATH is " + home);
    	return home;
    }

    public static String getEnvironmentName(String name)
    {
    	String value = "";
		String line;
		Process p;
		try
		{
			p = (Constants.win()) ? Runtime.getRuntime().exec("db2cmd /c /i /w set ") : Runtime.getRuntime().exec("env");
    		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = stdInput.readLine()) != null)
			{
				if (line.startsWith(name))
				{
					value = line.substring(line.indexOf("=")+1);
					break;
				}
			}
			p.destroy();
			if (stdInput != null)
				stdInput.close();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		log("Value of Environment variable " + name + "=" + value);
    	return value;
    }

    public static boolean isDB2Installed(boolean remote)
	{
		String line = null;
		Process p = null;
		boolean found = false;
		BufferedReader stdInput = null;

		if (remote)
			return true; // Assume DB2 is installed when connecting remotely to the db2.
		try
		{
			if (Constants.win())
			{
				p = Runtime.getRuntime().exec("db2cmd /c /i /w set ");
				stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = stdInput.readLine()) != null)
				{
					if (line.startsWith("DB2PATH"))
					{
						found = true;
					}
				}
			} else
			{
				p = Runtime.getRuntime().exec("env");
				stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = stdInput.readLine()) != null)
				{
					if (line.startsWith("DB2INSTANCE"))
					{
						found = true;
					}
				}
			}
			if (stdInput != null)
				stdInput.close();
		} catch (Exception e)
		{
			found = false;
		}
		if (!found)
		{
			if (Constants.win())
			{
				log("*** WARNING ***. I did not detect DB2 environment.");
			}
			else
			{
				log("*** WARNING ***. I did not detect DB2 environment.");
				log("Are you sure that you are calling db2profile from your shell profile?");
			}
			Message = "You are not running this application from within DB2 environment.";
		}
		return found;
	}

	public static String db2JavaPath()
	{
		String path, db2Path = getDB2PathName(), javaHome = System.getProperty("java.home");
		String arch = System.getProperty( "sun.arch.data.model" );

		path = (db2Path == null || db2Path.length() == 0) ? javaHome : 
			db2Path + filesep + "java" + filesep + "jdk" + (arch.equals("64") ? "64" : "32") + filesep + "jre";
		
		if (FileExists(path))
			return path;
		else
		{
			path = (db2Path == null || db2Path.length() == 0) ? javaHome : 
				db2Path + filesep + "java" + filesep + "jdk" + (arch.equals("64") ? "64" : "") + filesep + "jre";
			if (FileExists(path))
				return path;
			else
				return javaHome;
		}
	}
	
	public static String targetJDBCHome(String vendor)
	{
		if (vendor.equalsIgnoreCase(Constants.netezza))
		{
			if (!Constants.win())
			{
				return "/nz/kit/sbin";
			} else
				return ".";
		} else if (vendor.equalsIgnoreCase(Constants.db2luw) || vendor.equalsIgnoreCase(Constants.db2luw_compatibility))
		   return getDB2PathName() + filesep + "java";
		else
			return ".";
	}	
	
	private static String encry(int encKey, String toEnc)
	{

		int t = 0;
		String tog = "";
		if (encKey > 0)
		{
			while (t < toEnc.length())
			{
				int a = toEnc.charAt(t);
				int c = a ^ encKey;
				char d = (char) c;
				tog = tog + d;
				t++;
			}
		}
		return tog;
	}

	private static String encryStr(String encKey, String toEnc)
	{
		int t = 0;
		int encKeyI = 0;
		while (t < encKey.length())
		{
			encKeyI += encKey.charAt(t);
			t += 1;
		}			
		return encry(encKeyI, toEnc);
	}
	
	private static String Str2Unicode(String str)
	{
		String result = "";
		char[] chars = str.toCharArray();		 
		for (int i = 0; i < chars.length; i++) 
		{
			String hexa = Long.toHexString((long)chars[i]).toUpperCase();
			result += ("0000" + hexa).substring(hexa.length(), hexa.length() + 4);
		}
		return result;
	}
	
	private static String Unicode2Str(String s)
	{
		int i = 0, len = s.length();
		char c;
		StringBuffer sb = new StringBuffer(len);
		while (i < len)
		{
			String t = s.substring(i, i + 4);
			c = (char) Integer.parseInt(t, 16);
			i += 4;
			sb.append(c);
		}
		return sb.toString();
	}
	
	public static String SaveObject(String sqlTerminator, String outputDirectory, String type, String schema, String objectName, String sqlSource)
	{		
		new File(outputDirectory + "/savedobjects").mkdir();
		String fileName = outputDirectory + "/savedobjects/"+schema.toLowerCase()+"_"+objectName.toLowerCase()+".sql";
		try
		{
			BufferedWriter db2SourceWriter = new BufferedWriter(new FileWriter(fileName, false));
			db2SourceWriter.write("--#SET TERMINATOR " + sqlTerminator + linesep + linesep);
			db2SourceWriter.write("--#SET :"+type+":"+schema+":"+objectName+"" + linesep);
			db2SourceWriter.write(sqlSource);
			db2SourceWriter.write(sqlTerminator + linesep + linesep);
			log("Object " + objectName + " saved in " + fileName);
			db2SourceWriter.close();
			return "Saved in " + fileName;
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return "Error saving source";
		
	}
	
	public static boolean isHexString(String str)
	{
		return str.matches("^[0-9A-F]+$");
	}

	public static String Encrypt(String str)
	{
		return Constants.EncrySeed + Str2Unicode(encryStr("IBMExtract", str));
	}
	
	public static String Decrypt(String str)
	{
		if (str != null && str.length() > Constants.EncrySeed.length() && str.substring(0,Constants.EncrySeed.length()).equals(Constants.EncrySeed))
		{
			return encryStr("IBMExtract", Unicode2Str(str.substring(Constants.EncrySeed.length())));
		} else
		{
			return str;
		}
	}
	
	public static boolean isIPLocal(String hostNameToCompare)
	{
		String remoteIPAddress = "", localIPAddress = "";
		java.net.InetAddress inetAdd;
		try
		{
			inetAdd = java.net.InetAddress.getByName(hostNameToCompare);
			remoteIPAddress = inetAdd.getHostAddress();
		} catch (UnknownHostException e1)
		{
			remoteIPAddress = "";
			e1.printStackTrace();
		}
	    
		try {			
			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
			for (NetworkInterface netint : Collections.list(nets))
			{
				Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
		        for (InetAddress inetAddress : Collections.list(inetAddresses)) 
		        {
		        	localIPAddress = inetAddress.getHostAddress();
		        	if (localIPAddress != null || localIPAddress.length() > 0)
		        		if (remoteIPAddress != null || remoteIPAddress.length() > 0)
		        			if (localIPAddress.equals(remoteIPAddress))
		        				return true;
		        }

			}
	    } catch (SocketException e)
		{
			e.printStackTrace();
			return true; // Assume local in case of error.
		}
		return false;
	}
		
	public static String getTimeZoneOffset()
	{
		Calendar cal = Calendar.getInstance();		
		TimeZone currentTimeZone = cal.getTimeZone();
		Calendar currentDt = new GregorianCalendar(currentTimeZone, Locale.US);
		int gmtOffset = currentTimeZone.getOffset(
		    currentDt.get(Calendar.ERA), 
		    currentDt.get(Calendar.YEAR), 
		    currentDt.get(Calendar.MONTH), 
		    currentDt.get(Calendar.DAY_OF_MONTH), 
		    currentDt.get(Calendar.DAY_OF_WEEK), 
		    currentDt.get(Calendar.MILLISECOND));
		int hour = gmtOffset / (60*60*1000);
		int min  = Math.abs((gmtOffset - hour * (60*60*1000))/(60*1000));		
		return "" + hour + ":" + min;
	}
	
	public static void replaceStandardOutput(String logFileName, boolean append) 
	{
        try 
        {
            PrintStream output = new PrintStream(new FileOutputStream(logFileName, append));
            PrintStream tee = new Tee(System.out, output);
            System.setOut(tee);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } 
		
	}
	
	public static void replaceStandardOutput(String logFileName) 
	{
		replaceStandardOutput(logFileName, false);
    }
	
	public static void replaceStandardError(String logFileName, boolean append) 
	{ 
        try 
        {
            PrintStream err = new PrintStream(new FileOutputStream(logFileName, append));
            PrintStream tee = new Tee(System.err, err);
            System.setErr(tee); 
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }		
	}
	
	public static void replaceStandardError(String logFileName) 
	{ 
		replaceStandardError(logFileName, false);
    }
	
	public static String GetWhatisNew()
	{
		return IBMExtractUtilities.readJarFile(Constants.WHATISNEW);
	}
	
	public static void GetLatestJar()
	{
		boolean getUpdatedVersion = false;
		int len, bytesRead = 0;
		int curMajor, curMinor, curBuild, updMajor, updMinor, updBuild;
		String filename, newfilename, updatedVersion = "";
		// CurrentVersion will return null if run from Eclipse since we are not reading manifest of the jar
		String currentVersion = IBMExtractUtilities.class.getPackage().getImplementationVersion();
		//String address = "file:c:/DB2DWB/datamovementtool/IBMDataMovementTool.jar";
		String address = "ftp://public.dhe.ibm.com/education/db2pot/demos/IBMDataMovementTool.jar";
		URL url = null;
		curMajor = (currentVersion == null) ? -1 : Integer.valueOf(currentVersion.substring(0,currentVersion.indexOf('.'))); 
		curMinor = (currentVersion == null) ? -1 : Integer.valueOf(currentVersion.substring(currentVersion.indexOf('.')+1,currentVersion.indexOf('-'))); 
		curBuild = (currentVersion == null) ? -1 : Integer.valueOf(currentVersion.substring(currentVersion.indexOf('b')+1)); 
		
		try
		{			
			url = new URL("jar:"+address+"!/");
			JarURLConnection conn = (JarURLConnection) url.openConnection();
			JarFile jarFile = conn.getJarFile();
			Manifest mf = jarFile.getManifest();
			updatedVersion = mf.getMainAttributes().getValue("Implementation-Version");
			updMajor = (updatedVersion == null) ? -1 : Integer.valueOf(updatedVersion.substring(0,updatedVersion.indexOf('.'))); 
			updMinor = (updatedVersion == null) ? -1 : Integer.valueOf(updatedVersion.substring(updatedVersion.indexOf('.')+1,updatedVersion.indexOf('-'))); 
			updBuild = (updatedVersion == null) ? -1 : Integer.valueOf(updatedVersion.substring(updatedVersion.indexOf('b')+1)); 			
			log("Current Version         = " + currentVersion);			
			log("Version at IBM FTP Site = " + updatedVersion);			
			if (updMajor >= curMajor && updMinor >= curMinor && updBuild >= curBuild)
			{
				if (!(updMajor == curMajor && updMinor == curMinor && updBuild == curBuild))
					getUpdatedVersion = true;
			}
			if (getUpdatedVersion)
			{
				url = new URL(address);
				filename = url.getFile();
				filename = filename.substring(filename.lastIndexOf('/') + 1);
				newfilename = filename + ".new"; 
			    FileOutputStream out = new FileOutputStream(newfilename);
				URLConnection conn2 = url.openConnection();
				int contentLength = conn2.getContentLength();
				InputStream in = new BufferedInputStream(conn2.getInputStream());
				byte[] data = new byte[200000];
			    while ((len = in.read(data)) > 0) 
			    {
			    	bytesRead += len;
				    out.write(data, 0, len);
			    }
			    in.close();
			    out.flush();
			    out.close();
			    if (bytesRead != contentLength)
			    {
			    	log("IBMDataMovementTool.jar download size did not match. Retry again ...");
			    } else
			    {
			    	log("Download of IBMDataMovementTool.jar successful bytes=" + contentLength);
			    }
			} else
			{
				log("Your version " + currentVersion + " is current");				
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Use this method instead of regular readLine as that does not treat \r\r\n as a line	 
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public static String readLine(BufferedReader reader) throws IOException
	{
		int ch = -1;
        StringBuffer line = new StringBuffer();
                
        ch = reader.read();
        while (ch != -1 && ch != '\r' && ch != '\n') 
        {
            line.append((char) ch);
            ch = reader.read();
        }

        if (ch == -1 && line.length() == 0) 
        {
            return null;
        }

        switch ((char) ch) 
        {
	        case '\r':
	            // Check for \r, \r\n and \r\r\n
	            // Regard \r\r not followed by \n as two lines
	            reader.mark(2);
	            switch ((ch = reader.read())) 
	            {
		            case '\r':
		                if (!((char) (ch = reader.read()) == '\n')) 
		                   reader.reset();
		                break;
		            case '\n':
		                break;
		            case -1:
		                break;
		            default:
		                reader.reset();
		                break;
		        }
		        break;
	        case '\n':
	            break;
        }
        return line.toString();
	}
	
	public static String CustomDataTypeMap(Properties custMap, String schema, String table, String column)
	{
		if (custMap == null || custMap.size() == 0)
			return null;
		String key = removeQuote(schema) + "." + removeQuote(table) + "." + removeQuote(column);
		String dataMapping = null;
		if (custMap.containsKey(key))
			dataMapping = custMap.getProperty(key);
		if (dataMapping != null)
		{
			if (debug) log("CustomDataTypeMap="+schema+":"+table+":column"+column+":"+dataMapping);
		}
		return (dataMapping == null) ? null : dataMapping.split(":")[0];
	}
	
	public static void loadPropertyFile(Properties propFile, String fileName, String type)
	{
    	String propFileName = System.getProperty(type);
    	if (!(propFileName == null || propFileName.equals("")))
    	{
    		fileName = propFileName;
    	}
        try
        {
        	FileInputStream fis = new FileInputStream(fileName);
        	propFile.load(fis);
        	int n = propFile.size();
        	fis.close();        	
        	log(type + " " + n + " values loaded from the properties file " + new File(fileName).getCanonicalPath());
        } catch (Exception e)
        {
        	log("Error loading " + type + " property file " + fileName);
        	propFile = null;
        	e.printStackTrace();
        }
	}
	
	public static void CreateCustomColumnDistribution(String fileName)
	{
    	String propFileName = System.getProperty("CustomColumnDistribution");
    	if (!(propFileName == null || propFileName.equals("")))
    	{
    		fileName = propFileName;
    	}
		if (!IBMExtractUtilities.FileExists(fileName))
		{
			BufferedWriter buffer;
			String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			try
			{
				buffer =  new BufferedWriter(new FileWriter(fileName, false));
				buffer.append("## This file was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
				buffer.append("## IBM Data Movement Tool Version : " + versionInfo + linesep + "##" + linesep);
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## You can modify 'distribute on' and 'organize on' clause for Netezza tables" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);	
				if (tableList.size() > 0)
				{
					buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
					buffer.append("## Modify DISTRIBUTE ON RANDOM to suit your needs." + linesep);						
					buffer.append("## e.g. DISTRIBUTE ON RANDOM can be modified to DISTRIBUTE ON (X) ORGANIZE ON (Y)" + linesep);						
					buffer.append("## -------------------------------------------------------------------------------------" + linesep);
					Iterator iter = tableList.iterator();
					while (iter.hasNext())
					{
						String val = (String) iter.next();
						buffer.append(val + "=DISTRIBUTE ON RANDOM" + linesep);						
					}
				}				
				buffer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}						
	}
	
	public static void CreateCustomTableSpaceMappingFile(String fileName)
	{
    	String propFileName = System.getProperty("CustTableSpaceMapPropFile");
    	if (!(propFileName == null || propFileName.equals("")))
    	{
    		fileName = propFileName;
    	}
		if (!IBMExtractUtilities.FileExists(fileName))
		{
			BufferedWriter buffer;
			String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			try
			{
				buffer =  new BufferedWriter(new FileWriter(fileName, false));
				buffer.append("## This file was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
				buffer.append("## IBM Data Movement Tool Version : " + versionInfo + linesep + "##" + linesep);
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## You can specify additional buffer pools and table spaces in this file for tables" + linesep+linesep);			
				buffer.append("##    *********** PLEASE READ ****************" + linesep+linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## The table space mapping specified here will take effect only when useBestPracticeTSNames=false" + linesep);		
				buffer.append("## is set in IBMExtract.properties file." + linesep+linesep);		
				buffer.append("## When useBestPracticeTSNames=false and source database is Oracle or z/OS DB2, table space definition" + linesep);		
				buffer.append("## is extracted from source database as is. But if you specify any entries in this file, this will override" + linesep);		
				buffer.append("## source database table space definitions." + linesep+linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## You need to specify 3 things in this file." + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## 1. Name of the BUFFERPOOL and its page size" + linesep);		
				buffer.append("## 2. Name of the TABLESPACE and its BUFFERPOOL name" + linesep);		
				buffer.append("## 3. Mapping information for the table and the table space" + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## 1. Create Buffer Pool" + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## BUFFERPOOL.NAME=PageSize" + linesep);			
				buffer.append("## For example:" + linesep + "##" + linesep);			
				buffer.append("## BUFFERPOOL.BUFPOOL8=8" + linesep + "##" + linesep);			
				buffer.append("## A buffer pool BUFPOOL8 of 8 KB page size with automatic storage will be created." + linesep + "##" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## 2.(a) Create Table Spaces (Automatic Storage)" + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## TABLESPACE.NAME=BUFFERPOOL_NAME" + linesep);			
				buffer.append("## For example:" + linesep + "##" + linesep);			
				buffer.append("## TABLESPACE.TSDATA8=BUFPOOL8" + linesep + "##" + linesep);			
				buffer.append("## A table space with name TSDATA8 using page size 8K and buffer pool BUFPOOL8 with automatic storage will be created." + linesep + "##" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## 2.(b) Create table space (Custom clause)" + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## TABLESPACE.NAME=[<custom clause>]" + linesep);			
				buffer.append("## For example:" + linesep + "##" + linesep);			
				buffer.append("## TABLESPACE.TSDATA8=[CREATE LARGE TABLESPACE TSDATA8 PAGESIZE 8K BUFFERPOOL BUFPOOL8]" + linesep + "##" + linesep);			
				buffer.append("## Specify your own table space clause within square bracket." + linesep + "##" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## 3. Table - Tablespace mapping" + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## SchemaName.TableName.DATA=DataTableSpaceName" + linesep);			
				buffer.append("## SchemaName.TableName.INDEX=IndexTableSpaceName" + linesep);			
				buffer.append("## SchemaName.TableName.LOBS=LobsTableSpaceName" + linesep + "##" + linesep);			
				buffer.append("## You can omit INDEX and LOBS entries for a table, if not required." + linesep);			
				buffer.append("## It is not necessary to specify DATA for every tables. If you omit an entry " + linesep);
				buffer.append("## for a table, DB2 will pick up a best table space for you." + linesep + "##" + linesep);			
				buffer.append("## Example:" + linesep);			
				buffer.append("## -------" + linesep + "##" + linesep);			
				buffer.append("## FRED.JOHN.DATA=DATA8TS" + linesep);			
				buffer.append("## Tool will use above mapping and add IN clause in the table for the data table space" + linesep + linesep);
				buffer.append("## FRED.JOHN.INDEX=IDX4TS" + linesep);			
				buffer.append("## Tool will use above mapping and add INDEX IN clause in the table for the index table space" + linesep+linesep);
				buffer.append("## FRED.JOHN.LONG=long4TS" + linesep);			
				buffer.append("## Tool will use above mapping and add LONG IN clause in the table for the LOB table space" + linesep + "##" + linesep);
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## Please note:" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## If Schema, table or column names are case sensitive in source database, make sure to use case sensitive names." + linesep);			
				buffer.append("## Please Note: Java Properties files are case sensitive. Please check value of caseSensitiveTabColName in "+IBMExtractUtilities.getConfigFile()+" file  " + linesep);	
				buffer.append("## and if it is true, everything should be in upper case. If not, use case sensitive name as they appear in source database." + linesep);			
				buffer.append("##" + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## A sample list of buffer pools are given here. Uncomment and add more if required." + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("#BUFFERPOOL.BUFPOOL4=4" + linesep);		
				buffer.append("#BUFFERPOOL.BUFPOOL8=8" + linesep);		
				buffer.append("#BUFFERPOOL.BUFPOOL16=16" + linesep);		
				buffer.append("#BUFFERPOOL.BUFPOOL32=32" + linesep + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## A sample list of data table spaces are given here. Uncomment and add more if required." + linesep);		
				buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
				buffer.append("#TABLESPACE.TSDATA4=BUFPOOL4" + linesep);		
				buffer.append("#TABLESPACE.TSDATA8=BUFPOOL8" + linesep);		
				buffer.append("#TABLESPACE.TSDATA16=BUFPOOL16" + linesep);		
				buffer.append("#TABLESPACE.TSDATA32=BUFPOOL32" + linesep + linesep);		
				buffer.append("## --------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## A sample list of index table spaces are given here. Uncomment and add more if required." + linesep);		
				buffer.append("## --------------------------------------------------------------------------------------" + linesep);			
				buffer.append("#TABLESPACE.TSIDX4=BUFPOOL4" + linesep);		
				buffer.append("#TABLESPACE.TSIDX8=BUFPOOL8" + linesep);		
				buffer.append("#TABLESPACE.TSIDX16=BUFPOOL16" + linesep);		
				buffer.append("#TABLESPACE.TSIDX32=BUFPOOL32" + linesep + linesep);		
				buffer.append("## --------------------------------------------------------------------------------------" + linesep);			
				buffer.append("## A sample list of long table spaces are given here. Uncomment and add more if required." + linesep);		
				buffer.append("## --------------------------------------------------------------------------------------" + linesep);			
				buffer.append("#TABLESPACE.TSLONG4=BUFPOOL4" + linesep);		
				buffer.append("#TABLESPACE.TSLONG8=BUFPOOL8" + linesep);		
				buffer.append("#TABLESPACE.TSLONG16=BUFPOOL16" + linesep);		
				buffer.append("#TABLESPACE.TSLONG32=BUFPOOL32" + linesep + linesep);		
				if (tableList.size() > 0)
				{
					buffer.append("## -------------------------------------------------------------------------------------" + linesep);			
					buffer.append("## A mapping of tables and table spaces are given here. Uncomment and modify if required." + linesep);						
					buffer.append("## -------------------------------------------------------------------------------------" + linesep);
					Iterator iter = tableList.iterator();
					while (iter.hasNext())
					{
						String val = (String) iter.next();
						buffer.append("#" + val + ".DATA=<your data table space name here>" + linesep);						
						buffer.append("#" + val + ".INDEX=<your index table space name here>" + linesep);						
						buffer.append("#" + val + ".LONG=<your long table space name here>" + linesep + linesep);						
					}
				}				
				buffer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void CreateCustomDataMappingFile(String Vendor, String fileName)
	{
    	String propFileName = System.getProperty("CustDataMapPropFile");
    	if (!(propFileName == null || propFileName.equals("")))
    	{
    		fileName = propFileName;
    	}
		if (!IBMExtractUtilities.FileExists(fileName))
		{
			BufferedWriter buffer;
			String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			try
			{
				buffer =  new BufferedWriter(new FileWriter(fileName, false));
				buffer.append("## This file was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
				buffer.append("## IBM Data Movement Tool Version : " + versionInfo + linesep + "##" + linesep);
				buffer.append("## You can specify custom data type information in this file to override Tool's mapping" + linesep);			
				buffer.append("## ------------------------------------------------------------------------------------" + linesep +"##" + linesep);		
				buffer.append("## The format of this file : " + linesep + "##" + linesep);		
				if (Vendor.equalsIgnoreCase("mssql"))
				{
					buffer.append("## Case-1 : Override data type of a particular column" + linesep);			
					buffer.append("## --------------------------------------------------" + linesep);			
					buffer.append("## SchemaName.TableName.ColumnName=DataMapping" + linesep);			
					buffer.append("## For example: If you want to override DS001.EMPLYOEE.EMPID mapping from default INT to CHAR(5)" + linesep);			
					buffer.append("## Create an entry like below in this file" + linesep);			
					buffer.append("## DS001.EMPLYOEE.EMPID=CHAR(5)" + linesep);			
					buffer.append("## Tool will use above mapping instead of one in the source database." + linesep + "##" + linesep);			
					buffer.append("## Case-2 : Decrypt data during unload" + linesep);			
					buffer.append("## -----------------------------------" + linesep);			
					buffer.append("## SchemaName.TableName.ColumnName=DataMapping:CertificateName" + linesep);			
					buffer.append("## For example: If SQL Server table has a column defined as VARBINARY and it contains encrypted" + linesep);
					buffer.append("## data, specify target datatype and SQL Server certificate name to decrypt the data." + linesep + "##" + linesep);			
					buffer.append("## DS001.EMPLYOEE.SSN=CHAR(11):SSNCert" + linesep + "##" + linesep);			
					buffer.append("## Tool will use SSNCert certificate in SQL Server function DecryptByKeyAutoCert to decrypt data" + linesep);
					buffer.append("## e.g. convert(varchar,DecryptByKeyAutoCert(cert_id('SSNCert'),null,SSN))" + linesep + "##" + linesep);			
					buffer.append("## Case-3 : Decrypt data during unload using your defined functions" + linesep);			
					buffer.append("## ----------------------------------------------------------------" + linesep);			
					buffer.append("## DS001.EMPLYOEE.SSN=CHAR(11)::CustomFunctionUsage:Key:Certificate" + linesep);			
					buffer.append("## For example: If you have a custom user defined function MyDecryptFunc written in MS SQL," + linesep);
					buffer.append("## specify that function name with all arguments after ::" + linesep + "##" + linesep);			
					buffer.append("## DS001.EMPLYOEE.SSN=CHAR(11)::cast(dbo.FnDecrypt(SSN) as varchar(max)):MyKey:MyCertificate" + linesep + "##" + linesep);			
					buffer.append("## Tool will open key using certificate and decrypt data by using your function" + linesep);
					buffer.append("## Please pay attention to the exact format used" + linesep + "##" + linesep);
				} else if (Vendor.equalsIgnoreCase("mysql"))
				{
					buffer.append("## Case-1 : Override data type of a particular column" + linesep);			
					buffer.append("## --------------------------------------------------" + linesep);			
					buffer.append("## SchemaName.TableName.ColumnName=DataMapping" + linesep);			
					buffer.append("## For example: If you want to override DS001.EMPLYOEE.EMPID mapping from default INT to CHAR(5)" + linesep);			
					buffer.append("## Create an entry like below in this file" + linesep);			
					buffer.append("## DS001.EMPLYOEE.EMPID=CHAR(5)" + linesep);			
					buffer.append("## Tool will use above mapping instead of one in the source database." + linesep + "##" + linesep);			
					buffer.append("## Case-2 : Decrypt data during unload" + linesep);			
					buffer.append("## -----------------------------------" + linesep);			
					buffer.append("## SchemaName.TableName.ColumnName=DataMapping::DecryptFunctionName" + linesep);			
					buffer.append("## For example: If MySQL Server table has a column defined as VARBINARY and it contains encrypted" + linesep);
					buffer.append("## data, specify target datatype and MySQL Server decryption function name to decrypt the data." + linesep + "##" + linesep);			
					buffer.append("## DS001.EMPLYOEE.SSN=CHAR(11):AES_DECRYPT(SSN,'password')" + linesep + "##" + linesep);			
					buffer.append("## Tool will use AES_DECRYPT function of MySQL Server to decrypt data" + linesep);
				} else
				{
					buffer.append("## SchemaName.TableName.ColumnName=DataMapping" + linesep);			
					buffer.append("## For example: If you want to override DS001.EMPLYOEE.EMPID mapping from default INT to CHAR(5)" + linesep);			
					buffer.append("## Create an entry like below in this file" + linesep);			
					buffer.append("## DS001.EMPLYOEE.EMPID=CHAR(5)" + linesep);			
					buffer.append("## Tool will use above mapping as defined instead of one in the source database." + linesep + "##" + linesep);			
				}
				buffer.append("## Do not use double quote to surround names." + linesep);			
				buffer.append("## If Schema, table or column names are case sensitive in source database, make sure to use case sensitive names." + linesep);			
				buffer.append("## Please Note: Java Properties file case sensitive in nature. So, Schema, Table and Column Names' case sensitivity " + linesep);
				buffer.append("## is dependent upon setting of caseSensitiveTabColName param in "+IBMExtractUtilities.getConfigFile()+" file." + linesep);			
				buffer.append("## If you still do not see mapping taking effect, it is most likely due to the case sensitive issue." + linesep);			
				buffer.append("##" + linesep + "##" + linesep);						
				buffer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public static String CustomColNameMapping(Properties custColMap, String schema, String table, String column)
	{
		if (custColMap == null || custColMap.size() == 0)
			return column;
		String key = removeQuote(schema) + "." + removeQuote(table) + "." + removeQuote(column);
		String newColName = null;
		if (custColMap.containsKey(key))
			newColName = custColMap.getProperty(key);
		return (newColName == null || newColName.length() == 0) ? column : newColName;
	}
	
	public static String CustomNullMapping(Properties nullMap, String schema, String table, String column)
	{
		if (nullMap == null || nullMap.size() == 0)
			return null;
		String key = removeQuote(schema) + "." + removeQuote(table) + "." + removeQuote(column);
		String nullMapping = null;
		if (nullMap.containsKey(key))
			nullMapping = nullMap.getProperty(key);
		return nullMapping;
	}
	
	public static void CreateCustomNullMappingFile(String fileName)
	{
    	String propFileName = System.getProperty("CustNullPropFile");
    	if (!(propFileName == null || propFileName.equals("")))
    	{
    		fileName = propFileName;
    	}
		if (!IBMExtractUtilities.FileExists(fileName))
		{
			BufferedWriter buffer;
			String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			try
			{
				buffer =  new BufferedWriter(new FileWriter(fileName, false));
				buffer.append("## This file was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
				buffer.append("## IBM Data Movement Tool Version : " + versionInfo + linesep + "##" + linesep);
				buffer.append("## You can use custom NOT NULL / NULL mapping information to override Source database settings" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------------" + linesep + "##" + linesep);			
				buffer.append("## The format of this file : " + linesep);			
				buffer.append("## SchemaName.TableName.ColumnName=NullMapping" + linesep);			
				buffer.append("## For example: If you want to override DS001.EMPLYOEE.EMPID mapping to NOT NULL from NULL" + linesep);			
				buffer.append("## Create an entry like below in this file" + linesep + "##" + linesep);			
				buffer.append("## DS001.EMPLYOEE.EMPID=NOT NULL" + linesep + "## OR for NULL" + linesep);			
				buffer.append("## DS001.EMPLYOEE.EMPID=NULL" + linesep + "##" + linesep);			
				buffer.append("## Tool will use above mapping as defined instead of one in the source database." + linesep);			
				buffer.append("## Do not use double quote to surround names." + linesep);			
				buffer.append("## Please Note: Schema and Table names are names used in your source database as defined in your .tables file." + linesep);			
				buffer.append("##" + linesep + "##" + linesep);		
				buffer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void CreateCustomColNameMappingFile(String fileName)
	{
    	String propFileName = System.getProperty("CustColNamePropFile");
    	if (!(propFileName == null || propFileName.equals("")))
    	{
    		fileName = propFileName;
    	}
		if (!IBMExtractUtilities.FileExists(fileName))
		{
			BufferedWriter buffer;
			String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
			try
			{
				buffer =  new BufferedWriter(new FileWriter(fileName, false));
				buffer.append("## This file was generated on : " + 
			    		(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date())) + linesep);
				buffer.append("## IBM Data Movement Tool Version : " + versionInfo + linesep + "##" + linesep);
				buffer.append("## You can use custom column name mapping information to override Source database column name" + linesep);			
				buffer.append("## -------------------------------------------------------------------------------------------" + linesep + "##" + linesep);			
				buffer.append("## The format of this file : " + linesep);			
				buffer.append("## SchemaName.TableName.ColumnName=NewColumnName" + linesep);			
				buffer.append("## For example: If you want to rename SSN_ENCRY column to SSN" + linesep);			
				buffer.append("## Create an entry like below in this file" + linesep);			
				buffer.append("## DS001.EMPLYOEE.SSN_ENCRY=SSN" + linesep);			
				buffer.append("## Tool will use above mapping and rename column SSN_ENCRY to SSN" + linesep);			
				buffer.append("## Do not use double quote to surround names." + linesep);			
				buffer.append("## Please Note: Schema and Table names are names used in your source database as defined in your .tables file." + linesep);			
				buffer.append("##              Column name change is supported only for tables, PK, FK, indexes and LOAD." + linesep);			
				buffer.append("##" + linesep + "##" + linesep);		
				buffer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

    public static String getDefaultString(String value, String defaultValue, boolean emptyStringValid)
    {
    	if (emptyStringValid)
    	{
        	if (value == null || value.equalsIgnoreCase("null"))
        		return defaultValue;
        	else
        		return value;    		
    	} else
    	{
        	if (value == null || value.length() == 0 || value.equalsIgnoreCase("null"))
        		return defaultValue;
        	else
        		return value;    		
    	}
    }
    
    public static String getDefaultString(String value, String defaultValue)
    {
    	return getDefaultString(value, defaultValue, false);
    }
    
	public static void putHelpInformation(BufferedWriter buffer, String fileName)
	{
		String versionInfo = IBMExtractUtilities.class.getPackage().getImplementationVersion();
		String helpType, ext = "";
		String dropScript = Constants.dropscripts;
		
		if (fileName == null || fileName.length() == 0)
			return;
		
		int pos = fileName.lastIndexOf(".");
		if (pos > 0)
		{
		   helpType = fileName.substring(0, pos);
		   ext =  fileName.substring(pos);
		} else
		   helpType = fileName;
		
		try
		{
			if (ext.equalsIgnoreCase("sh"))
			{
			   buffer.append("#-- This file was generated on : " + 
			    		(new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(new Date())) + linesep);
			   buffer.append("#-- IBM Data Movement Tool Version : " + versionInfo + linesep + "#--" + linesep);				
			} else
			{
				buffer.append("-- This file was generated on : " + 
				    		(new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(new Date())) + linesep);
				buffer.append("-- IBM Data Movement Tool Version : " + versionInfo + linesep + "--" + linesep);
			}
			if (helpType.equalsIgnoreCase("db2temptables"))
			{
			   buffer.append("-- This file will not be overwritten if you extract objects again." + linesep);
			   buffer.append("-- In this file, you can write temporaray tables which are requred by the stored procedure compilation." + linesep);
			   buffer.append("-- This script will be executed once when SPs are compiled through Interactive GUI." + linesep);
			   buffer.append("-- The terminator used in the script is controlled through --#SET TERMINATOR command." + linesep);
			   buffer.append("-- For example, for deploying SP using Sybase SKIN feature, use the following statement." + linesep);
			   buffer.append("-- e.g. --#SET TERMINATOR go" + linesep + "--" + linesep);
			   buffer.append("-- For example, for deploying DB2 SP using external temp tables, use the following statement." + linesep);
			   buffer.append("-- e.g. --#SET TERMINATOR ;" + linesep + "--" + linesep);
			} 
			if (helpType.equalsIgnoreCase("db2check"))
			{
			   buffer.append("-- This file contains check constraints extracted from source database." + linesep);
			   buffer.append("-- This script is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2load"))
			{
			   buffer.append("-- This file contains DB2 Load statements for loading data in DB2." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("nzload"))
			{
			   buffer.append("-- This file contains Netezza Load statements for loading data through the JDBC." + linesep);
			   buffer.append("-- This file is meant to be run through the IBM Data Movement Tool." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("runnzload"))
			{
			   buffer.append("#-- This file contains nzload statements for loading data in Netezza." + linesep);
			   buffer.append("#-- This file can be run from either from the Netezza server or from a machine where Natezza client is installed." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2excepttabcount"))
			{
			   buffer.append("-- This file contains count statement for exception tables." + linesep);
			   buffer.append("-- The exception tables are used by the DB2 LOAD statement and may contain data if there were exceptions in data due to constraints checking during load." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2exceptiontables"))
			{
			   buffer.append("-- This file contains exception tables that will be used by the DB2 Load utility to dump data which does not respect unique keys, foreign keys etc." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2runstats"))
			{
			   buffer.append("-- This file contains DB2 RUNSTATS commands to generate table statistics." + linesep);
			   buffer.append("-- This script is not part of the deployment script so you will need to run this manually." + linesep);
			   buffer.append("-- To run this script, open DB2 command window and use following command." + linesep);
			   buffer.append("-- db2 -tvf db2runstats.sql" + linesep);
			} 
			else if (helpType.equalsIgnoreCase("nzrunstats"))
			{
			   buffer.append("-- This file contains Netezza GENERATE STATISTICS commands to generate table statistics." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2tabstatus"))
			{
			   buffer.append("-- This script is part of deployment script and it is not necessary to run this individually." + linesep);
			   buffer.append("-- This file contains queries for each table to determine status of DB2 table after data load." + linesep);
			   buffer.append("-- You will see output for column FK_CHECKED CC_CHECKED STATUS" + linesep);
			   buffer.append("-- The value of Y Y and N indicate success." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2tabcount"))
			{
			   buffer.append("-- This script is part of deployment script and it is not necessary to run this individually." + linesep);
			   buffer.append("-- This file contains queries to do the row count for each table for which data was loaded." + linesep);
			   buffer.append("-- After movement of data, you should run rowcount script as that will tell you row count from source and target database." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2loadterminate"))
			{
			   buffer.append("-- This script is not part of the deployment script and you will need to run it manually if required." + linesep);
			   buffer.append("-- Sometimes a DB2 table can remain in LOAD PENDING status due to some failures." + linesep);
			   buffer.append("-- For such cases, if you try to run db2load.sql, the LOAD may fail." + linesep);
			   buffer.append("-- In those situations, run this script to TERMINATE the pending LOAD." + linesep);
			   buffer.append("-- You can also run this script from GUI by choosing Execute DB2 Script" + linesep);
			   buffer.append("-- To run this script, open DB2 command window and use following command." + linesep);
			   buffer.append("-- db2 -tvf db2loadterminate.db2" + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2checkpending"))
			{
			   buffer.append("-- This script is not part of the deployment script and it might not be necessary to run this script." + linesep);
			   buffer.append("-- A better way to remove check pending status is done through db2checkRemoval.cmd script." + linesep);
			   buffer.append("-- db2checkRemoval.cmd or db2checkRemoval.sh is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2tables"))
			{
			   buffer.append("-- This script contains DB2 DDL for tables and this is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2fkeys"))
			{
			   buffer.append("-- This script contains foreign keys and is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2droptables"))
			{
			   buffer.append("-- This script contains DROP TABLE statements and is part of the "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2dropexceptiontables"))
			{
			   buffer.append("-- This script contains DROP TABLE statements for the exception tables and is part of  "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2pkeys"))
			{
			   buffer.append("-- This script contains primary key constraints." + linesep);
			   buffer.append("-- This script is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2indexes"))
			{
			   buffer.append("-- This script contains indexes and unique indexes." + linesep);
			   buffer.append("-- This script is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2dropfkeys"))
			{
			   buffer.append("-- This script will drop FOREIGN KEY constraints and is part of the "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2uniq"))
			{
			   buffer.append("-- This script is defunct now and can be discarded." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2sequences"))
			{
			   buffer.append("-- This script contains DB2 sequences and is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2dropsequences"))
			{
			   buffer.append("-- This script contains DROP SEQUENCE statements and is part of the "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2dropobjects"))
			{
			   buffer.append("-- This script contains DROP statements for PL/SQL objects and is part of the "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2roleprivs"))
			{
			   buffer.append("-- This script contains privileges granted to a ROLE in DB2. This script is not part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			   buffer.append("-- db2 -tvf db2roleprivs.db2" + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2objprivs"))
			{
			   buffer.append("-- This script contains object privileges granted to a user. This script is not part of the deployment script." + linesep);
			   buffer.append("-- You can also run this script from GUI by choosing Execute DB2 Script" + linesep);
			   buffer.append("-- To run this script, open DB2 command window and use following command." + linesep);
			   buffer.append("-- db2 -tvf db2objprivs.db2" + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2synonyms"))
			{
			   buffer.append("-- This script contains synonyms extracted and it is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2dropsynonyms"))
			{
			   buffer.append("-- This script contains DROP statements for synonyms and is part of the "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2mviews"))
			{
			   buffer.append("-- This script contains equivalent of materialized views in DB2" + linesep);
			   buffer.append("-- This script is not part of the deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2udf"))
			{
			   buffer.append("-- This script contains DB2 UDFs generated by the tool if required. This script is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2tsbp"))
			{
			   buffer.append("-- This script contains necessary and required DB2 table spaces and buffer pool using automatic storage." + linesep);
			   buffer.append("-- This script is part of the deployment script." + linesep);
			   buffer.append("-- If you have chosen option useBestPracticeTSNames=false, you will see source database" + linesep);
			   buffer.append("-- tablespaces name used in DB2 as it is." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2droptsbp"))
			{
			   buffer.append("-- This script contains DROP statements for table spaces and buffer pools and is part of the "+dropScript+" script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2default"))
			{
			   buffer.append("-- This script contains ALTER TABLE to add default values to the columns. This script is part of the deployment script." + linesep);
			   buffer.append("-- The contents of the file will also be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2TruncName"))
			{
			   buffer.append("-- This script contains a list of original table / column names and truncated name if they did happen." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2type_body"))
			{
			   buffer.append("-- This script contains DB2 TYPE BODY statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2type"))
			{
			   buffer.append("-- This script contains DB2 TYPE statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2trigger"))
			{
			   buffer.append("-- This script contains TRIGGER statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2procedure"))
			{
			   buffer.append("-- This script contains PROCEDURE statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2routine"))
			{
			   buffer.append("-- This script contains DB2 Non-SQL PROCEDURE and FUNCTIONS statments and this is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2xmlschema"))
			{
			   buffer.append("-- This script contains DB2 XSR definitions. This script is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2package_body"))
			{
			   buffer.append("-- This script contains PACKAGE BODY statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2package"))
			{
			   buffer.append("-- This script contains PACKAGE statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2function"))
			{
			   buffer.append("-- This script contains FUNCTION statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2views"))
			{
			   buffer.append("-- This script contains VIEWS statments and is not part of deployment script." + linesep);
			   buffer.append("-- However, contents of the file will be loaded in GUI interactive deployment." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2module"))
			{
			   buffer.append("-- This script contains DB2 Module components and is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2variable"))
			{
			   buffer.append("-- This script contains DB2 Global Variables and is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2directory"))
			{
			   buffer.append("-- This script contains UTL_DIR Create Directory command to create directory." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2users"))
			{
			   buffer.append("-- This script contains users and groups information extracted from Sybase for use by the DB2 SKIN feature." + linesep);
			   buffer.append("-- This file needs to be deployed through DB2 SKIN feature." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2login"))
			{
			   buffer.append("-- This script contains login information extracted from Sybase for use by the DB2 SKIN feature." + linesep);
			   buffer.append("-- This file needs to be deployed through DB2 SKIN feature." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("db2fixlobs"))
			{
			   buffer.append("-- This script contains UPDATE statements to fix LOBS when using DB2 SKIN feature." + linesep);
			   buffer.append("-- This script is part of the deployment script." + linesep);
			} 
			else if (helpType.equalsIgnoreCase("sybase"))
			{
			   buffer.append("-- This script contains objects extracted from Sybase for use by the DB2 SKIN feature." + linesep);
			   buffer.append("-- This file needs to be deployed through DB2 SKIN feature." + linesep);
			} 
			if (ext.equalsIgnoreCase("sh"))
			{
			   buffer.append("#-- " + linesep);				
			} else
			{
		       buffer.append("-- " + linesep);
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static String getVMMemoryParam()
	{
		String memoryArg = "-Xmx256m";
		RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
		List<String> aList=RuntimemxBean.getInputArguments();
		for (int i=0; i< aList.size(); i++) 
		{
			String tmpArg = aList.get(i);
			if (tmpArg.startsWith("-Xmx"))
			{
				return tmpArg;
			}
		}
		return memoryArg;
	}
	
	public static String trimLine(String t)
	{
        return t.replaceAll("\\r|\\n|\\r\\n","");
	}
	
	public static String getSize(double bytes, String token)
	{
		final double BASE = 1024, KB = BASE, MB = KB*BASE, GB = MB*BASE;
	    final DecimalFormat df = new DecimalFormat("#.##");

	    if (bytes == 0L)
	    	return "";
        if(bytes >= GB) 
        {
            return df.format(bytes/GB) + " GB" + token;
        }
        if(bytes >= MB) 
        {
            return df.format(bytes/MB) + " MB" + token;
        }
        if(bytes >= KB) 
        {
            return df.format(bytes/KB) + " KB" + token;
        }
        return "" + (int)bytes + " bytes" + token;
	}
	
	public static String getAvgNetworkSpeed(double bytes, long elapsed)
	{
		final double BASE = 1024, Kb = BASE, Mb = Kb*BASE, Gb = Mb*BASE;
		double rate = (double) (bytes * 8000) / elapsed;
	    final DecimalFormat df = new DecimalFormat("#.##");

	    if (bytes == 0L)
	    	return "";
        if(rate >= Gb) 
        {
            return df.format(rate/Gb) + " Gbps";
        }
        if(rate >= Mb) 
        {
            return df.format(rate/Mb) + " Mbps";
        }
        if(rate >= Kb) 
        {
            return df.format(rate/Kb) + " Kbps";
        }
        return "" + (int) rate + " bps";
	}
	
	public static String getExtractionRate(double bytes, long elapsed)
	{
		double rate = (double) (bytes * 1000) / elapsed;
		rate = rate * 3600;
		return getSize(rate, " / hour");
	}
	
	public static String getElapsedTime(long start)
	{
		long now = System.currentTimeMillis();
		String elapsedTimeStr = "";
		long elapsed = (now - start) / 1000;
		int days, hours, min, sec, ms;
		ms = (int)((now - start) - (elapsed * 1000));
		if (elapsed >= 86400)
		{
			days = (int) elapsed / 86400;
			if (days > 1)
			   elapsedTimeStr = days + " days ";
			else
			   elapsedTimeStr = days + " day ";
		}
		elapsed = elapsed % 86400;
		if (elapsed >= 3600 && elapsed < 86400)
		{
			hours = (int) elapsed / 3600;
			if (hours > 1)
				elapsedTimeStr += hours + " hour ";
			else
				elapsedTimeStr += hours + " hours ";
		}
		elapsed = elapsed % 3600;
		if (elapsed >= 60 && elapsed < 3600)
		{
			min = (int) elapsed / 60;
			if (min > 1)
				elapsedTimeStr += min + " min ";
			else
				elapsedTimeStr += min + " mins ";
		}
		sec = (int) elapsed % 60;
	    elapsedTimeStr += sec + "." + ms + " sec";
		return elapsedTimeStr;
	}
	
	public static String getLobSize(double bytes)
	{
		int m;
		final double BASE = 1024, KB = BASE, MB = KB*BASE, GB = MB*BASE;
	    final DecimalFormat df = new DecimalFormat("#");

        if (bytes >= GB) 
        {
        	m = (int) Math.ceil(bytes/GB);
        	if (m > 2)
        		m = 2;
            return df.format(m) + "G";
        }
        else if (bytes >= MB) 
        {
        	m = (int) Math.ceil(bytes/MB);
            return df.format(m) + "M";
        } else
        {
        	return "-1";
        }
        /*else if (bytes >= KB) 
        {
        	m = (int) Math.ceil(bytes/KB);
            return df.format(m) + "K";
        } else
        {
        	m = (int) (bytes/10)+1;
        	m = m * 10;
            return df.format(m);
        }*/
	}
	
	public static void printHashMap(HashMap map)
	{
		String key, value;
		System.out.print(map.getClass().getSimpleName() + "[");
		boolean firstIteration = true;
		Iterator iterator = map.keySet().iterator();
		while (iterator.hasNext()) 
		{
			if (!firstIteration)
				System.out.print(", ");
			key = iterator.next().toString(); 
			value = map.get(key).toString();
			System.out.print(key);
			System.out.print("=");
			System.out.print(value);
			firstIteration = false;
		}
		System.out.print("]");
		System.out.println();
	}
	
	public static String execCommand(String command) 
	{	        
        String line;
        String output = "";
        
        if (command == null || command.length() == 0)
        	return output;

        try 
        {
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) 
            {
                output += (line + '\n');
            }
            input.close();
        } 
        catch (Exception ex) 
        {
            output = "";
        }
        return output;
	}
	 
	public static void main(String[] args)
	{
		String str = getLobSize((double)12583000000.0);
		System.out.println(str);
	}
}
