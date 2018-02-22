package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PartitionData
{
	public String tableOwner;
	public String tableName;
	public String partitionName;
	public String highValue;
	public String highValueLength;
	public short partNum;
	public String limitKey;
	
	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}
		
	public PartitionData(ResultSet rs, DBData data)
	{
		try
		{
			this.tableOwner = trim(rs.getString(1));
			this.tableName = trim(rs.getString(2));
			if (data.Oracle())
			{
				this.partitionName = trim(rs.getString(3));
				this.highValue = trim(rs.getString(4));
				this.highValueLength = trim(rs.getString(5));
			} else if (data.zDB2())
			{
				this.partNum = rs.getShort(3);
				this.limitKey = trim(rs.getString(4));
			}

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}
