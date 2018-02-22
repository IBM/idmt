package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class FKData
{
	public String pkTableCatalog;
	public String pkTableSchema;
	public String pkTableName;
	public String pkColumnName;
	public String fkTableCatalog;
	public String fkTableSchema;
	public String fkTableName;
	public String fkColumnName;
	public short keySeq;
	public short updateRule;
	public short deleteRule;
	public String fkName;
	public String pkName;
	public short deferability;
	  
	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}
	
	public FKData(ResultSet rs)
	{
		try
		{
			this.pkTableCatalog = trim(rs.getString(1));
			this.pkTableSchema = trim(rs.getString(2));
			this.pkTableName = trim(rs.getString(3));
			this.pkColumnName = trim(rs.getString(4));
			this.fkTableCatalog = trim(rs.getString(5));
			this.fkTableSchema = trim(rs.getString(6));
			this.fkTableName = trim(rs.getString(7));
			this.fkColumnName = trim(rs.getString(8));
			this.keySeq = rs.getShort(9);
			this.updateRule = rs.getShort(10);
			this.deleteRule = rs.getShort(11);
			this.fkName = trim(rs.getString(12));
			this.pkName = trim(rs.getString(13));
			this.deferability = rs.getShort(14);

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public static ArrayList Sort(ArrayList al)
	{
    	Hashtable hash = new Hashtable();
		String key; 
		FKData fkData;
		ArrayList sortedAl = new ArrayList();
				
    	Iterator itr = al.iterator();
    	while (itr.hasNext())
    	{
    		fkData = (FKData) itr.next();
			key = fkData.fkTableSchema + fkData.fkTableName + fkData.fkName + IBMExtractUtilities.pad(("" + fkData.keySeq), 4, "0");
			hash.put(key, fkData);
    	}
        Vector v = new Vector(hash.keySet());
        Collections.sort(v);	            
        for (Enumeration e = v.elements(); e.hasMoreElements();) 
        {
        	fkData = (FKData) hash.get((String)e.nextElement());
        	sortedAl.add(fkData);
        }
		return sortedAl;
	}
}
