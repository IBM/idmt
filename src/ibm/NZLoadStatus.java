package ibm;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NZLoadStatus
{
	private String fileName;
	private PrintStream ps;
	private ArrayList al = new ArrayList();

	public class LoadData
	{
		private String schema, table, numRecordsRead, numBadRecords, numRecordsLoaded, elapsedTime;

		public LoadData(String schema, String table, String numRecordsRead,
				String numBadRecords, String numRecordsLoaded,
				String elapsedTime)
		{
			this.schema = schema;
			this.table = table;
			this.numRecordsRead = numRecordsRead;
			this.numBadRecords = numBadRecords;
			this.numRecordsLoaded = numRecordsLoaded;
			this.elapsedTime = elapsedTime;
		}
	}

	public NZLoadStatus(PrintStream ps, String fileName)
	{
		this.ps = ps;
		this.fileName = fileName;
	}
	
	private String matchedStr(String regex, String line)
	{
        Pattern pattern = Pattern.compile(regex);
	    int matches = 0;
	    Matcher matcher = pattern.matcher(line);
	  
	    if (matcher.find())
	      return matcher.group(1);
	    return "";
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
    
	public void reportFromFile() throws Exception
	{
		String line, schema, table;
		String numRecordsRead, numBadRecords, numRecordsLoaded;
		String elapsedTime;
		DataInputStream fp = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
		schema = table = numRecordsRead = numBadRecords = numRecordsLoaded = elapsedTime = "";
		while ((line = fp.readLine()) != null)
		{
			if (line.matches("(?i)\\s+Database:\\s+(.*)"))
			   schema = matchedStr("(?i)\\s+Database:\\s+(.*)", line);
			if (line.matches("(?i)\\s+Tablename:\\s+(.*)"))
				table = matchedStr("(?i)\\s+Tablename:\\s+(.*)", line);
			if (line.matches("(?i)\\s+number of records read:\\s+(.*)"))
				numRecordsRead = matchedStr("(?i)\\s+number of records read:\\s+(.*)", line);
			if (line.matches("(?i)\\s+number of bad records:\\s+(.*)"))
				numBadRecords =  matchedStr("(?i)\\s+number of bad records:\\s+(.*)", line);
			if (line.matches("(?i)\\s+number of records loaded:\\s+(.*)"))
				numRecordsLoaded = matchedStr("(?i)\\s+number of records loaded:\\s+(.*)", line);
			if (line.matches("(?i)\\s+Elapsed Time \\(sec\\):\\s+(.*)"))
			{
			    elapsedTime =  matchedStr("(?i)\\s+Elapsed Time \\(sec\\):\\s+(.*)", line);
			    al.add(new LoadData(schema, table, numRecordsRead, numBadRecords, numRecordsLoaded, elapsedTime));
				schema = table = numRecordsRead = numBadRecords = numRecordsLoaded = elapsedTime = "";
			}
		}
		fp.close();
		String s = String.format("%12s %30s %10s %10s %10s %10s", 
				fold("Schema Name",12), fold("Table Name",30), "# Read", "# of Bad", "# Loaded", "Time");
		IBMExtractUtilities.log(s, 1);
		IBMExtractUtilities.log(ps, s, 1);
		Iterator itr = al.iterator();
		while (itr.hasNext())
		{
			LoadData data = (LoadData) itr.next();
			s = String.format("%12s %30s %10s %10s %10s %10s", 
					data.schema, data.table, data.numRecordsRead, data.numBadRecords, data.numRecordsLoaded, data.elapsedTime);
			IBMExtractUtilities.log(s, 1);
			IBMExtractUtilities.log(ps, s, 1);
		}
	}
	
	public static void main(String[] args)
	{
		String logFileName = IBMExtractUtilities.combinePipeLogs(Constants.OUTPUT_DIR + "nzlog" + Constants.filesep, ".nzlog", "unload");
        IBMExtractUtilities.log("Check file "+logFileName+" for the LOAD status");
    	IBMExtractUtilities.deleteFiles(Constants.OUTPUT_DIR + "nzlog" + Constants.filesep, ".nzlog");  
    	try
		{
			PrintStream ps = new PrintStream(new FileOutputStream(Constants.OUTPUT_DIR + "nzloadstatus.log", false));
			NZLoadStatus ns = new NZLoadStatus(ps, logFileName);
			ns.reportFromFile();
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}		
}
