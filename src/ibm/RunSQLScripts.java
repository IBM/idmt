package ibm;

import ibm.lexer.GPLexer;

import java.util.ArrayList;
import java.util.Iterator;

public class RunSQLScripts
{
	private String sqlFileName, outputDirectory, skin;
	private DBData data;
	private ArrayList sqlCommands = null;
	
	public RunSQLScripts(DBData data, String outputDirectory, String sqlFileName, String skin)
	{
		this.data = data;
		this.outputDirectory = outputDirectory;
		this.sqlFileName = sqlFileName;
		this.skin = skin;
	}
	
	public void deployTempTables()
	{
		String sqlBody;
		try
		{
		    sqlCommands = GPLexer.parseTempTableFile(outputDirectory + Constants.filesep + sqlFileName);
		} catch (Exception e)
		{
			IBMExtractUtilities.log("Error with the file operation " + sqlFileName + " Error Message : " + e.getMessage());
			return;
		}		
		if (sqlCommands == null)
			return;
		Iterator iter = sqlCommands.iterator();
		int i = 0;
		while (iter.hasNext()) 
		{
			sqlBody = (String) iter.next();
			IBMExtractUtilities.DeployObject(data, outputDirectory, "TEMP_TABLE", "TEMP_TABLE", "TEMP_TABLE", skin, sqlBody, true);
			++i;
		}
		IBMExtractUtilities.log("Deployed " + i + " objects from " + sqlFileName);
	}
}
