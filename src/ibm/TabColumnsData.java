package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TabColumnsData
{
	public String schemaName;
	public String tableName;
	public String columnName;
	public int ordinalPosition;
	public String dataType;
	
	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}
		
	// Used by BuildPartitionColumnTypesMap() in GenerateExtract
	public TabColumnsData(ResultSet rs)
	{
		try
		{
			this.schemaName = trim(rs.getString(1));
			this.tableName = trim(rs.getString(2));
			this.dataType = trim(rs.getString(3));

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	// Used by BuildPartitionColumnsMap in GenerateExtract
	public TabColumnsData(ResultSet rs, String dummy)
	{
		try
		{
			this.schemaName = trim(rs.getString(1));
			this.tableName = trim(rs.getString(2));
			this.columnName = trim(rs.getString(3));

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

}
