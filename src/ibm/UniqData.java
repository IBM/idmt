package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UniqData
{
	public String schema;
	public String table;
	public String column;
	public String constraintName;
	public short position;

	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}
	
	public UniqData(ResultSet rs)
	{
		try
		{
			this.schema = trim(rs.getString(1));
			this.table = trim(rs.getString(2));
			this.column = trim(rs.getString(3));
			this.constraintName = trim(rs.getString(4));
			this.position = rs.getShort(5);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}
