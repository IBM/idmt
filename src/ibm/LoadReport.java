package ibm;

import java.util.*;

public class LoadReport
{
	private Hashtable ht = new Hashtable();
	private String outputDirectory, extension, baseName, logFileName;
	
	public LoadReport(String outputDirectory, String extension, String baseName)
	{
		this.outputDirectory = outputDirectory;
		this.extension = extension;
		this.baseName = baseName;
		
    	if (Constants.win())
    		logFileName = baseName + "_OUTPUT.TXT";
    	else
    		logFileName = baseName + ".log";

	}
	
	public void reset()
	{
		ht.clear();
	}
	
	public class LoadReportItems
	{
		public String schemaName;
		public String tableName;
		public long rowsRead, rowsSkipped, rowsLoaded, rowsRejected, rowsDeleted, rowsCommitted;
	}
}
