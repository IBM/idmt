package ibm;

import ibm.GenerateParallelScripts.TableFileInfo;

import java.sql.*;
import java.util.regex.Pattern;

public class GetTableCount implements Runnable
{
	String dbName, dbSourceName;
	Connection conn = null;
	TableFileInfo tabInfo = null;
	
	public GetTableCount(String dbSourceName, String dbName, Connection conn, TableFileInfo tabInfo)
	{		
		this.dbSourceName = dbSourceName;
		this.dbName = dbName;
		this.conn = conn;
		this.tabInfo = tabInfo;
	}
	
	public void run()
	{
		String tmp;
		if (Pattern.matches("^\\s*(exec|\\{\\s*call).*", tabInfo.countSQL.toLowerCase()))
		{
			tmp = IBMExtractUtilities.executeSPSQL(dbSourceName, dbName, conn, tabInfo.countSQL, tabInfo.colPosition);	
			if (tmp != null)
			{
				tmp = tmp.trim().toLowerCase();
				if (tmp.endsWith("kb"))
				   tmp = tmp.substring(0,tmp.indexOf("kb"));
			}
		} else
		{
			tmp = IBMExtractUtilities.executeSQL(conn, tabInfo.countSQL, "", false);
		}
		if (tmp == null || tmp.length() == 0)
		{
			tabInfo.rowCount = 0L;
		} 
		else
		{
			try
			{
			   tabInfo.rowCount = (long) Double.parseDouble(tmp);
			} catch (Exception e)
			{
				tabInfo.rowCount = 0L;
				e.printStackTrace();
			}
		}
		IBMExtractUtilities.log(tabInfo.schemaName+"."+tabInfo.tableName+" size="+tabInfo.rowCount);
	}
}
