package ibm;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RoutineOptData
{
	public String routineschema, specificname, isolation, blocking, insert_buf, reoptvar, queryopt, sqlmathwarn, degree, intra_parallel, refreshage;
	
	private String trim(String name)
	{
		return IBMExtractUtilities.trim(name);
	}

	public RoutineOptData(ResultSet rs)
	{
		try
		{
			this.routineschema = trim(rs.getString(1));
			this.specificname = trim(rs.getString(2));
			this.isolation = trim(rs.getString(3));
			this.blocking = trim(rs.getString(4));
			this.insert_buf = trim(rs.getString(5));
			this.reoptvar = trim(rs.getString(6));
			this.queryopt = trim(rs.getString(7));
			this.sqlmathwarn = trim(rs.getString(8));
			this.degree = trim(rs.getString(9));
			this.intra_parallel = trim(rs.getString(10));
			this.refreshage = trim(rs.getString(11));

		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}
