package ibm;

import java.util.ArrayList;

public class SchemaData
{
	public String[] srcSchemaNames, dstSchemaNames;
	public ArrayList schemaTableArrayList;
	public IBMExtractConfig cfg;
	public String srcSchemaName, srcTableName, dstSchemaName, dstTableName, query, partition;
	public boolean commented;
	
	public SchemaData(String[] dstSchemaNames, String[] srcSchemaNames, IBMExtractConfig cfg, ArrayList schemaTableArrayList)
	{
		this.srcSchemaNames = srcSchemaNames;
		this.dstSchemaNames = dstSchemaNames;
		this.cfg = cfg;
		this.schemaTableArrayList = schemaTableArrayList;
	}
	
	public SchemaData(String srcSchemaName, String srcTableName, String dstSchemaName, String dstTableName, String query, String partition, boolean commented)
	{
		this.srcSchemaName = srcSchemaName;
		this.srcTableName = srcTableName;
		this.dstSchemaName = dstSchemaName;
		this.dstTableName = dstTableName;
		this.query = query;
		this.partition = partition;
		this.commented = commented;
	}
}
