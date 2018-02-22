package ibm;

import ibm.GenerateExtract.TableInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TableList
{
	private String tableFileName;
	private String linesep = IBMExtractUtilities.linesep, caseSensitiveTabColName = "true";
	
	public ArrayList tableList = null, tableFileArrayList;
	public String Message = "";
	public String[] query, dstSchemaName, exceptSchemaName, exceptTableName, dstTableName, srcTableName, srcSchName, countSrcSQL, countDstSQL, countExceptSQL, partition;
	public int[] multiTables;
	public int totalTables;
	public Hashtable<String, TableInfo> tabInfo;

    public String getCaseName(String name)
    {
        return caseSensitiveTabColName.equalsIgnoreCase("true") ? name : name.toUpperCase();
    }

	public TableList (String caseSensitiveTabColName, String srcVendor, String tableFileName, String exceptSchemaSuffix, String exceptTableSuffix)
	{
		this.caseSensitiveTabColName = caseSensitiveTabColName;
		int i;
		String line;
		SchemaData sd;
		ArrayList al = readTableList(tableFileName);
	    tabInfo = new Hashtable<String, TableInfo>();
		totalTables = al.size();
		if (totalTables == 0)
		   return;
        query = new String[totalTables];
        dstSchemaName = new String[totalTables];
        exceptSchemaName = new String[totalTables];
        exceptTableName = new String[totalTables];
        dstTableName = new String[totalTables];
        srcTableName = new String[totalTables];
        srcSchName = new String[totalTables];
        countSrcSQL = new String[totalTables];
        countDstSQL = new String[totalTables];
        countExceptSQL = new String[totalTables];
        multiTables = new int[totalTables];
        partition = new String[totalTables];
        for (i = 0; i < totalTables; ++i)
        {
        	exceptSchemaName[i] = new String();
        	exceptTableName[i] = new String();
        	dstSchemaName[i] = new String();
        	dstTableName[i] = new String();
            srcTableName[i] = new String();
            srcSchName[i] = new String();
            partition[i] = new String();
        }
        i = 0;
        Iterator itr = al.iterator();
        while (itr.hasNext())
        {
           line = (String) itr.next();
           sd = getSchemaTable(line);
           dstSchemaName[i] = sd.dstSchemaName;
           exceptSchemaName[i] = IBMExtractUtilities.putQuote(getCaseName(IBMExtractUtilities.removeQuote(dstSchemaName[i])) + exceptSchemaSuffix);
           query[i] = line.substring(line.indexOf(":")+1);
           dstTableName[i] = sd.dstTableName;
           exceptTableName[i] = IBMExtractUtilities.putQuote(getCaseName(IBMExtractUtilities.removeQuote(dstTableName[i]) + exceptTableSuffix));
           srcSchName[i] = sd.srcSchemaName;
           srcTableName[i] = sd.srcTableName;
           partition[i] = sd.partition;
           
           if (tabInfo.containsKey(dstSchemaName[i]+"."+dstTableName[i]))
           {
               ++multiTables[i];
               tabInfo.get(dstSchemaName[i]+"."+dstTableName[i]).incrTableCount();
           } else
           {
        	   tabInfo.put(dstSchemaName[i]+"."+dstTableName[i], new TableInfo(i));
           }
           if (srcSchName[i] != null)
           {
              if (srcVendor.equalsIgnoreCase("mssql") || srcVendor.equalsIgnoreCase("zdb2") || srcVendor.equalsIgnoreCase("db2") || 
            		  srcVendor.equalsIgnoreCase("sybase")) {
                  countSrcSQL[i] = "SELECT COUNT_BIG(*) FROM " + srcSchName[i]+"."+srcTableName[i];                   
              } else {
                  countSrcSQL[i] = "SELECT COUNT(*) FROM " + srcSchName[i]+"."+srcTableName[i];                    
              }                    
              countDstSQL[i] = "SELECT COUNT"+(Constants.netezza() ? "" : "_BIG")+"(*) FROM " + getCaseName(dstSchemaName[i])+"."+getCaseName(dstTableName[i]);                   
              countExceptSQL[i] = "SELECT COUNT_BIG(*) FROM " + exceptSchemaName[i] + "." + exceptTableName[i];                   
           } else
           {
               srcSchName[i] = dstSchemaName[i];
               if (srcVendor.equalsIgnoreCase("mssql") || srcVendor.equalsIgnoreCase("zdb2") || srcVendor.equalsIgnoreCase("db2") || 
            		   srcVendor.equalsIgnoreCase("sybase")) {
                   countSrcSQL[i] = "SELECT COUNT_BIG(*) FROM " + srcTableName[i];
               } else {
                   countSrcSQL[i] = "SELECT COUNT(*) FROM " + srcTableName[i];
               }
               countDstSQL[i] = "SELECT COUNT"+(Constants.netezza() ? "" : "_BIG")+"(*) FROM " + getCaseName(dstSchemaName[i])+"."+getCaseName(dstTableName[i]);                   
               countExceptSQL[i] = "SELECT COUNT_BIG(*) FROM " + exceptSchemaName[i] + "." + exceptTableName[i];
           }
           ++i;               
        }
	}
	
	public TableList(String tableFileName)
    {
		this.tableFileName = tableFileName;
    	String schemaName, tableName, key;
    	String line, values;
		tableFileArrayList = ReadTableFile(tableFileName);
		if (tableFileArrayList != null)
		{
			if (tableList == null)
			   tableList = new ArrayList();
	    	tableList.clear();
	        Iterator itr = tableFileArrayList.iterator();
	        while (itr.hasNext())
	        {
	            line = (String) itr.next();
	      	    if (line.trim().equals("")) continue;
	      	    try
	      	    {
	      	    	if (line.startsWith("#### "))
	      	    		continue;
	      	    	SchemaData sd = getSchemaTable(line);
	      	    	if (line.startsWith("#"))
	      	    	   key = sd.srcSchemaName + "." + sd.srcTableName + "." + "0";
	      	    	else
	      	    	   key = sd.srcSchemaName + "." + sd.srcTableName + "." + "1";
		            if (!tableList.contains(key))
		            {
		            	tableList.add(key);
		            }
	      	    } catch (Exception e)
	      	    {
	      	    	e.printStackTrace();
	      	    }
	        }
		}
    }

	private ArrayList ReadTableFile(String tableFileName)
	{
    	String line;
    	ArrayList al = new ArrayList();
    	try
        {
           LineNumberReader in = new LineNumberReader(new FileReader(tableFileName));
           while ((line = in.readLine()) != null)
           {
              al.add(line);
           }
           in.close();
        } catch (Exception ex)
        {
        	return null;
        }        
        return al;
	}
	 
    public static ArrayList readTableList(String tableFileName)
    {
    	String line;
    	ArrayList al = new ArrayList();
    	try
        {
           LineNumberReader in = new LineNumberReader(new FileReader(tableFileName));
           while ((line = in.readLine()) != null)
           {
        	  if (!line.trim().equals(""))
                if (!line.startsWith("#"))
                   al.add(line);
           }
        } catch (FileNotFoundException e)
        {
           IBMExtractUtilities.log("Configuration file '" + tableFileName + "' not found. Existing ...");
           System.exit(-1);
        } catch (Exception ex)
        {
           IBMExtractUtilities.log("Error reading configuration file '" + tableFileName + "'");
           System.exit(-1);          
        }
        
        if (al.size() == 0)
        {
        	IBMExtractUtilities.log("It looks that the " + tableFileName + " is empty.");
        	IBMExtractUtilities.log("There are no tables in the source schema that you selected.");
        	//System.exit(-1);
        }
        return al;
    }
    
    private String getModLine(SchemaData schemaData, SchemaData sd)
    {
    	String schema, srcSchema, dstSchema = "NULL", line;
    	schema = IBMExtractUtilities.removeQuote(sd.srcSchemaName);
    	for (int i = 0; i < schemaData.srcSchemaNames.length; ++i)
    	{
    		srcSchema = IBMExtractUtilities.removeQuote(schemaData.srcSchemaNames[i]);
    		if (schema.equals(srcSchema))
    		{
    			dstSchema = IBMExtractUtilities.removeQuote(schemaData.dstSchemaNames[i]);
    			break;
    		}
    	}
    	line = (sd.commented ? "#" : "") + IBMExtractUtilities.putQuote(dstSchema) + "." + sd.dstTableName + ":" + sd.query;
    	return line;
    }
    
    private SchemaData getSchemaTable(String line)
    {
    	boolean commented = false;
        int m;
    	String values, query = "", tmpStr, srcSchema = "", srcTable = "", dstSchema = "", dstTable = "", partition = "";
    	
    	if (line == null || line.length() == 0)
    	   return new SchemaData(srcSchema, srcTable, dstSchema, dstTable, query, partition, commented);
    	
    	tmpStr = line.toUpperCase();
    	
    	int pos1 = line.indexOf(":"), pos2, pos3 = tmpStr.indexOf(" FROM "), partPos;
    	
    	if (line.startsWith("#"))
    		commented = true;
    	
    	if (pos1 != -1 && pos3 != -1)
    	{
            values = line.substring(0, line.indexOf(":"));
            dstSchema = values.substring(0, values.indexOf(".")).trim();
            dstTable = values.substring(values.indexOf(".")+1).trim();
            m = dstSchema.indexOf('"');
            if (m > 0)
            	dstSchema = dstSchema.substring(m).trim();
    		query = line.substring(pos1+1);
    		tmpStr = query.toUpperCase();
    		pos2 = tmpStr.indexOf(" FROM ");
            srcSchema = query.substring(pos2+6).trim();
            pos2 = srcSchema.indexOf(".");
            if (pos2 != -1)
            {
              srcTable = srcSchema.substring(pos2+1).trim();
              srcSchema = srcSchema.substring(0, pos2).trim();
            } else
            {
                srcTable = srcSchema;
                srcSchema = null;            	
            }
            m = srcTable.indexOf('"', 1);
            if (m > 0)
            	srcTable = srcTable.substring(0,m+1).trim();
            partPos = line.toUpperCase().lastIndexOf("PARTITION");
            if (partPos > 0)
            {
            	partition = line.substring(partPos).trim();
            }
            
    	}
 	    return new SchemaData(srcSchema, srcTable, dstSchema, dstTable, query, partition, commented);
    }

	public void WriteTableFile(SchemaData schemaData)
	{
    	String schemaName, tableName, key1, key2;
    	String line, query, tmpStr;
    	StringBuffer buffer = new StringBuffer();
    	SchemaData sd;

		if (tableFileArrayList != null)
		{
	        Iterator itr = tableFileArrayList.iterator();
	        while (itr.hasNext())
	        {
	            line = (String) itr.next();
	      	    if (line.trim().equals("")) 
      	    	{
	      	    	buffer.append(line+linesep);
	      	    	continue;
      	    	}
      	    	if (line.startsWith("#### "))
      	    	{
	      	    	buffer.append(line+linesep);	      	    		
	      	    	continue;
      	    	}
	      	    try
	      	    {
	      	    	sd = getSchemaTable(line);
	      	    	key1 = sd.srcSchemaName + "." + sd.srcTableName + "." + "0";
	                key2 = sd.srcSchemaName + "." + sd.srcTableName + "." + "1";
	                line = getModLine(schemaData, sd);
		            if (sd.commented)
		            {            	
		                if (schemaData.schemaTableArrayList.contains(key1))
		                {
		                	buffer.append(line+linesep);
		                }
		                if (schemaData.schemaTableArrayList.contains(key2))
		                {
		                	buffer.append(line.substring(1)+linesep);
		                }
		            } else
		            {
		                if (schemaData.schemaTableArrayList.contains(key1))
		                {
		                	buffer.append("#"+line+linesep);
		                }
		                if (schemaData.schemaTableArrayList.contains(key2))
		                {
		                	buffer.append(line+linesep);
		                }
		            }
	      	    } catch (Exception e)
	      	    {
	      	    	e.printStackTrace();
	      	    }
	        }
		}
		try
		{
	        BufferedWriter inputFileWriter = new BufferedWriter(new FileWriter(tableFileName, false));
	        inputFileWriter.write(buffer.toString());
	        inputFileWriter.close();  
	        Message = "Table list is saved in " + (new File(tableFileName)).getCanonicalFile();
	        IBMExtractUtilities.log(Message);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public String[] getTargetSchemas()
	{
		List list = Arrays.asList(dstSchemaName);
		Set set = new HashSet(list);
		String[] result = new String[set.size()];
		set.toArray(result);
		return result;
	}
	
	public String[] getSchemaList()
	{
		String[] schemaArray;
    	ArrayList alSchema = new ArrayList();
		String schema;
		if (tableList != null)
		{
	        Iterator itr = tableList.iterator();
	        while (itr.hasNext())
	        {
	        	String[] tmpArray = ((String) itr.next()).split("\\.");
	        	if (tmpArray != null && tmpArray.length > 1)
	        	{
		        	schema = tmpArray[0];
		        	if (!alSchema.contains(schema))
		        	{
		        		alSchema.add(schema);
		        	}
	        	}
	        }
	        schemaArray = new String[alSchema.size()];
	        alSchema.toArray(schemaArray);
		} else
			schemaArray = null;
        return schemaArray;
	}
	
	public String[] getTableList(String forSchema)
	{
		String[] tlist;
		int i = 0;
		ArrayList alTable = new ArrayList();
		String schemaTableList = "", schema, table, commented;
		if (tableList != null)
		{
	        Iterator itr = tableList.iterator();
	        while (itr.hasNext())
	        {
	        	String[] schemaTable = ((String) itr.next()).split("\\.");
	        	if (schemaTable != null && schemaTable.length > 1)
	        	{
		        	schema = schemaTable[0];
		        	table = schemaTable[1];
		        	commented = schemaTable[2];
		        	if (schema.equals(forSchema))
		        	{
		        		alTable.add(table+"~"+commented);
		        	}
	        	}
	        }
	        tlist = new String[alTable.size()];
			alTable.toArray(tlist);
		} else
			tlist = null;
		return tlist;
	}
}
