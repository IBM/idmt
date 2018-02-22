package ibm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQL
{
	private Connection connection = null;
	public ResultSet rs = null;
	public Statement statement = null;
	public PreparedStatement queryStatement = null;
	private boolean debug = IBMExtractUtilities.debug;
	private int fetchSize = IBMExtractUtilities.fetchSize;

	public SQL(Connection conn)
	{
		this.connection = conn;
	}
	
	private void log(String s)
	{
		IBMExtractUtilities.log(s);
	}
	
    public PreparedStatement Prepare(String procName, String sql) throws SQLException
    {
    	long start = 0L;
    	if (debug) start = System.currentTimeMillis();
    	queryStatement = connection.prepareStatement(sql);
    	statement.setFetchSize(fetchSize);
    	if (debug) log(procName + " ("+IBMExtractUtilities.getElapsedTime(start)+") SQL=" + sql);
    	return queryStatement;
    }    
    
    public ResultSet ExecuteQuery(String procName, String sql) throws SQLException
    {
    	long start = 0L;
    	if (debug) start = System.currentTimeMillis();
    	rs = queryStatement.executeQuery();
    	if (debug) log(procName + " ("+IBMExtractUtilities.getElapsedTime(start)+") SQL=" + sql);
    	return rs;
    }
    
    public ResultSet ExecuteStatementQuery(String procName, String sql) throws SQLException
    {
    	long start = 0L;
    	if (debug) start = System.currentTimeMillis();
    	rs = statement.executeQuery(sql);
    	if (debug) log(procName + " ("+IBMExtractUtilities.getElapsedTime(start)+") SQL=" + sql);   
    	return rs;
    }
    
    public ResultSet PrepareExecuteQuery(String procName, String sql) throws SQLException
    {
    	long start = 0L;
    	if (debug) start = System.currentTimeMillis();
    	queryStatement = connection.prepareStatement(sql);
    	queryStatement.setFetchSize(fetchSize);
    	rs = queryStatement.executeQuery();
    	if (debug) log(procName + " ("+IBMExtractUtilities.getElapsedTime(start)+") SQL=" + sql);
    	return rs;
    }
    
    public boolean next()
    {
    	boolean n = false;
    	try
		{
			n = rs.next();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		return n;
    }
     
	public void close(String methodName)
	{
		try
		{
    		if (rs != null)
    		  rs.close();
		} catch (Exception e)
		{
		   log("Error in closing Reader in method " + methodName);
		}
		try
		{
		if (queryStatement != null)
		  queryStatement.close();
		} catch (Exception e)
		{
		   log("Error in closing PreparedStatement in method  " + methodName);
		}
		try
		{
		if (statement != null)
		   statement.close();
		} catch (Exception e)
		{
		   log("Error in closing Statement in method  " + methodName);
		}
	}
}

