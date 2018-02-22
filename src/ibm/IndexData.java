package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;

public class IndexData
{
	public String tableCatalog;
	public String tableSchema;
	public String tableName;
	public boolean nonUnique;
	public String indexQualifier;
	public String indexName;
	public String type;
	public short ordinalPosition;
	public String columnName;
	public String ascDesc;
	public int cardinality = -1;
	public int pages = -1; 
	public String partitioned;
	
	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}
		
	public IndexData(ResultSet rs, DBData data)
	{
		try
		{
			this.tableCatalog = trim(rs.getString(1));
			this.tableSchema = trim(rs.getString(2));
			this.tableName = trim(rs.getString(3));
			if (data.Oracle())
			{
				String tmp = rs.getString(4);
				if (tmp != null)
				{
					this.nonUnique = tmp.equalsIgnoreCase("UNIQUE") ? false : true;
				} else
					this.nonUnique = true;				
			} else
				this.nonUnique = rs.getBoolean(4);
			this.indexQualifier = trim(rs.getString(5));
			this.indexName = trim(rs.getString(6));
			if (rs.getString(7) == null)
			   this.type = "";
			else
			   this.type = rs.getString(7);
			this.ordinalPosition = rs.getShort(8);
			this.columnName = trim(rs.getString(9));
			this.ascDesc = trim(rs.getString(10));
			// this.pages = rs.getShort(12);
			// this.cardinality = rs.getShort(11);
			// Above 2 lines commented since Sybase throws error on above. We are not using it at the moment.
			if (rs.getString(13) == null)
				this.partitioned = "";
			else
			    this.partitioned = trim(rs.getString(13));

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}
