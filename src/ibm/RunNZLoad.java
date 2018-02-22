package ibm;

import java.io.BufferedReader;
import java.io.StringReader;

public class RunNZLoad  implements Runnable
{

	private DBData data;
	StringBuffer buffer;
	String schema, table;
	
	public RunNZLoad(DBData data, StringBuffer buffer)
	{
		this.data = data;
		this.buffer = buffer;		
	}
	
	private String parseBuffer()
	{
		StringBuffer sql = new StringBuffer();
		String line, key;
		String[] keys;
		BufferedReader reader = new BufferedReader(new StringReader(buffer.toString()));
    	try
		{
			while ((line = reader.readLine()) != null)
			{
				if (line.startsWith("--#SET :"))
				{
					key = line.substring(line.indexOf(":")+1);	
					keys = key.split(":");	
					schema = keys[1];
					table = keys[2];
				} else if (line.equals(";"))
				{
					;
				} 
				else
				{
					sql.append(line + " ");
				}
			}
	    	reader.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return sql.toString();
	}
	
	
	public void run()
	{
		String sql = parseBuffer();
		data.executeNZSQL(schema, table, sql);
	}
}
