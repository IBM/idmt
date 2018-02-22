package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;
 
public class ProcColumnsData
{
	public String routineSchema, specificName, columnType, columnName, typeName, columnSize, decimalDigits, paramList, parmName;
	int length, scale, codePage;
	public String typeSchema, locator;
	
	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}

	public ProcColumnsData(ResultSet rs, DBData data)
	{
		try
		{
			if (data.iDB2())
			{
				this.columnType = trim(rs.getString(3));
				this.columnName = trim(rs.getString(4));
				this.typeName = trim(rs.getString(5));
				this.columnSize = trim(rs.getString(6));
				this.decimalDigits = trim(rs.getString(7));
			} else if (data.Mysql())
			{
				this.paramList = trim(rs.getString(3));				
			} else if (data.DB2())
			{
				this.columnType = trim(rs.getString(3));
				this.parmName = trim(rs.getString(4));
				this.typeName = trim(rs.getString(5));
				this.length = rs.getInt(6);
				this.scale = rs.getInt(7);
				this.codePage = rs.getInt(8);
				this.typeSchema = trim(rs.getString(9));
				this.locator = trim(rs.getString(10));
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}
