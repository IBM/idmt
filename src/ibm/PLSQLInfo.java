/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

public class PLSQLInfo
{
	public String codeStatus;
	public String type;
	public String schema;
	public String object;
	public String plSQLCode;
	public String oldPLSQLCode = null;
	public String lineNumber;
	public String skin;
	
	public PLSQLInfo(String codeStatus, String type, String schema, String object, String lineNumber, String plSQLCode, String skin)
	{
		this.codeStatus = codeStatus;
		this.type = type;
		this.schema = schema;
		this.object = object;   
		this.plSQLCode = plSQLCode;
		this.lineNumber = lineNumber;
		this.skin = skin;
	}
	
	public String toString()
	{
		return codeStatus+object;
	}
}
