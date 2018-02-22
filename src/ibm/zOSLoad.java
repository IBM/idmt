package ibm;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.concurrent.Callable;

public class zOSLoad implements Callable<Integer>
{
	private PrintStream ps = null;
	private IBMExtractConfig cfg = null;
	private DBData data;
	private String schemaName, tableName, dataFileName, lobsDirectory;
	private byte colsep;
	private TableList t;
	private int tid, colpos;
	private HashMap dataTypeMap, columnList;
	private boolean tableExists, dataFileExists, dataCommitted = false;
	private boolean blobOpen = false, clobOpen = false, xmlOpen = false;
	private int columnCount, currentRow, numOfMultiRows, batchSize;
	private long rowsProcessed = 0L, numRowsToProcess;
	private PreparedStatement prepareStatement = null;
	private BinData[] columns;
	private String[] dataTypes;
	private BufferedInputStream bis;
	private FileInputStream fis;
	private ProcessLOBFile plfBlob = null, plfClob = null, plfXML = null;
	private IOBuffer[] ioBlob = null, ioClob = null, ioXML = null;
	
	public zOSLoad(int tid, byte colsep, String OUTPUT_DIR, int commitCount, IBMExtractConfig cfg, PrintStream ps, 
			TableList t, DBData data, HashMap dataTypeMap, HashMap columnList,
			HashMap columnLengthMap)
	{
		this.tid = tid;
		this.colsep = colsep;
		this.cfg = cfg;
		this.ps = ps;
		this.t = t;
		this.data = data;
		this.dataTypeMap = dataTypeMap;
		this.columnList = columnList;
		colpos = 0;
		try
    	{
    	    batchSize = Integer.parseInt(cfg.getBatchSizeDisplay());
    	} catch (Exception e)
    	{
    	    batchSize = 0;
    	}
    	if (batchSize <= 0)
    		batchSize = 0;
		String loadDirectory = cfg.getLoadDirectory();
		if (loadDirectory == null || loadDirectory.length() == 0)
			loadDirectory = OUTPUT_DIR;
		dataFileName = IBMExtractUtilities.getInputFileName(OUTPUT_DIR, loadDirectory, Boolean.valueOf(cfg.getUsePipe()), tid, t);
		lobsDirectory = IBMExtractUtilities.getLobsDirectoryName(OUTPUT_DIR, loadDirectory, Boolean.valueOf(cfg.getUsePipe()), tid, t);
		schemaName = IBMExtractUtilities.removeQuote(t.dstSchemaName[tid]);
		tableName = IBMExtractUtilities.removeQuote(t.dstTableName[tid]);
		tableExists = checkTableExistence();
		dataFileExists = IBMExtractUtilities.FileExists(dataFileName);
		columnCount = getColumnsCount();
		String rowsToProcessStr = IBMExtractUtilities.getDefaultString(cfg.getLimitLoadRows(), "ALL");
		numRowsToProcess = rowsToProcessStr.equals("ALL") ? -1L : Long.valueOf(rowsToProcessStr);
		if (columnCount > 0)
		{
			numOfMultiRows = Constants.parameterMarkersLimit / columnCount;
			numOfMultiRows = numOfMultiRows > commitCount ? commitCount : numOfMultiRows;
			columns = new BinData[columnCount];
			currentRow = 1;
			String cols = "", key = schemaName + "." + tableName;
			if (columnLengthMap.containsKey(key))
			{
				cols = (String) columnLengthMap.get(key);
				String[] lengths = cols.split(",");
				for (int i = 0; i < columnCount; ++i)
				{
					columns[i] = new BinData(Integer.valueOf(lengths[i])+3);
				}
			}
			if (dataTypeMap.containsKey(key))
			{
				dataTypes = ((String) dataTypeMap.get(key)).split(",");
			}
			ioBlob = new IOBuffer[numOfMultiRows];
			ioClob = new IOBuffer[numOfMultiRows];
			ioXML = new IOBuffer[numOfMultiRows];
		}
	}
	
	public void blog(String msg)
	{
		IBMExtractUtilities.blog(ps, msg, 1);
	}
	
	public void log(String msg, boolean line)
	{
		IBMExtractUtilities.log(ps, msg, line);
	}
	
	public void log(String msg, int flag)
	{
		IBMExtractUtilities.log(ps, msg, flag);
	}
	
	public void log(String msg)
	{
		IBMExtractUtilities.log(ps, msg, 1);
	}

	private boolean checkTableExistence()
	{
		return (columnList.containsKey(schemaName + "." + tableName)) ? true : false;
	}
	
	private int getColumnsCount()
	{
		int colCount = 0;
		String cols = "", key = schemaName + "." + tableName;
		
		if (columnList.containsKey(key))
		{
			cols = (String) columnList.get(key);
			String[] s = cols.split(",");
			colCount = s.length;
		}
		return colCount;		
	}
	
	private String getColumnsList()
	{
		String cols = "", key = schemaName + "." + tableName;
		if (columnList.containsKey(key))
		{
			cols = (String) columnList.get(key);
		}
		return cols;		
	}
	
	private String buildInsertStatement()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("INSERT INTO " + t.dstSchemaName[tid] + "." + t.dstTableName[tid]);
		buffer.append(" (" + getColumnsList() + ") VALUES (");
		for (int i = 0; i < columnCount; ++i)
			buffer.append((i == 0 ? "" : ",") + "?");
		buffer.append(")");
		return buffer.toString();
	}
	
	private String getString(byte[] bins)
	{
		return (bins == null) ? "" : new String(bins, 0, bins.length);
	}
	
	private void streamLOB(int colIndex, final long offset, 
			final int length, String lobFileName, String lobsDirectory, final IOBuffer iob, 
			final ProcessLOBFile plf, final boolean binary)
	{
		Thread t;
		t = new Thread
		(		
			new Runnable() 
			{
				public void run()
				{
					try
					{
					   if (binary)
						   plf.queueOutputStream(offset, length, iob.out);
					   else
					       plf.queueOutputStream(offset, length, iob.rout);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				}
			}
		);
		t.start();
		try
		{
			if (binary)
			    prepareStatement.setBinaryStream(colIndex, iob.in, length);
			else
				prepareStatement.setCharacterStream(colIndex, iob.rin, length);
		} catch (SQLException e)
		{
			e.printStackTrace(ps);
		}
		try
		{
			t.join();
		} catch (InterruptedException e)
		{
			e.printStackTrace(ps);
		}
	}
	
	private void bindLOB(int colIndex, String token, String which)
	{
		String lobFileName;
		int pos2, pos;
		final int length;
		final long offset;
		
		//blog("Working on " + token);
		if (which.equals("BLOB") || which.equals("CLOB"))
		{
			pos = token.lastIndexOf('.');
			length = Integer.valueOf(token.substring(pos+1, token.length()-1));
			pos2 = token.lastIndexOf('.', pos-1);
			offset = Long.valueOf(token.substring(pos2+1, pos));
			lobFileName = token.substring(0, pos2);
			if (which.equals("BLOB"))
			{
				if (blobOpen)
				{
					ioBlob[currentRow%numOfMultiRows].clear();
				} else
				{
					plfBlob = new ProcessLOBFile(ps, lobsDirectory, lobFileName);
					plfBlob.open(lobsDirectory, lobFileName);
					for (int i = 0; i < numOfMultiRows; ++i)
					   ioBlob[i] = new IOBuffer(true);
					blobOpen = true;
				}
				if (length > Constants.readLobBuffer)
				{
				    streamLOB(colIndex, offset, 
						length, lobFileName, lobsDirectory, ioBlob[currentRow%numOfMultiRows], 
						plfBlob, true);
				} else
				{
					try
					{
						prepareStatement.setBytes(colIndex, plfBlob.getBytes(offset, length));
					} catch (Exception e)
					{
						e.printStackTrace(ps);
					}
				}
			} else if (which.equals("CLOB"))
			{
				if (clobOpen)
				{
					ioClob[currentRow%numOfMultiRows].clear();
				} else
				{
					plfClob = new ProcessLOBFile(ps, lobsDirectory, lobFileName);
					plfClob.open(lobsDirectory, lobFileName);
					for (int i = 0; i < numOfMultiRows; ++i)
					   ioClob[i] = new IOBuffer(false);
					clobOpen = true;
				}
				if (length > Constants.readLobBuffer)
				{
				    streamLOB(colIndex, offset, 
						length, lobFileName, lobsDirectory, ioClob[currentRow%numOfMultiRows], 
						plfClob, false);
				} else
				{
					try
					{
						prepareStatement.setCharacterStream(colIndex, plfClob.getClob(offset, length), length);
					} catch (Exception e)
					{
						e.printStackTrace(ps);
					}					
				}
			}
		} else
		{
			pos = token.indexOf('\'');
			pos2 = token.indexOf('\'', pos+1);
			lobFileName = token.substring(pos+1,pos2);
			pos = token.indexOf('\'', pos2+1);
			pos2 = token.indexOf('\'', pos+1);
			offset = Long.valueOf(token.substring(pos+1, pos2));
			pos = token.indexOf('\'', pos2+1);
			pos2 = token.indexOf('\'', pos+1);
			length = Integer.valueOf(token.substring(pos+1, pos2));
			if (xmlOpen)
			{
				ioXML[currentRow%numOfMultiRows].clear();
			} else
			{
				plfXML = new ProcessLOBFile(ps, lobsDirectory, lobFileName);
				xmlOpen = true;
				for (int i = 0; i < numOfMultiRows; ++i)
				  ioXML[i] = new IOBuffer(false);
			}
			plfXML.open(lobsDirectory, lobFileName);
			if (length > Constants.readLobBuffer)
			{
			   streamLOB(colIndex, offset, 
					length, lobFileName, lobsDirectory, ioXML[currentRow%numOfMultiRows], 
					plfXML, false);
			} else
			{
				try
				{
					prepareStatement.setCharacterStream(colIndex, plfXML.getClob(offset, length), length);
				} catch (Exception e)
				{
					e.printStackTrace(ps);
				}
			}
		} 
	}
	
	private int getJavaSQLType(int col)
	{
		if (dataTypes[col].equalsIgnoreCase("VARCHAR") || dataTypes[col].equalsIgnoreCase("VARGRAPHIC"))
			return java.sql.Types.VARCHAR;
		else if (dataTypes[col].equalsIgnoreCase("CHAR") || dataTypes[col].equalsIgnoreCase("CHARACTER") || dataTypes[col].equalsIgnoreCase("GRAPHIC"))
			return java.sql.Types.CHAR;
		else if (dataTypes[col].equalsIgnoreCase("NUMERIC"))
			return java.sql.Types.NUMERIC;
		else if (dataTypes[col].equalsIgnoreCase("DECIMAL"))
			return java.sql.Types.DECIMAL;
		else if (dataTypes[col].equalsIgnoreCase("BIT"))
			return java.sql.Types.BIT;
		else if (dataTypes[col].equalsIgnoreCase("TINYINT"))
			return java.sql.Types.TINYINT;
		else if (dataTypes[col].equalsIgnoreCase("INT") || dataTypes[col].equalsIgnoreCase("INTEGER"))
			return java.sql.Types.INTEGER;
		else if (dataTypes[col].equalsIgnoreCase("SMALLINT"))
			return java.sql.Types.SMALLINT;
		else if (dataTypes[col].equalsIgnoreCase("BIGINT"))
			return java.sql.Types.BIGINT;
		else if (dataTypes[col].equalsIgnoreCase("BLOB"))
			return java.sql.Types.BLOB;
		else if (dataTypes[col].equalsIgnoreCase("CLOB") || dataTypes[col].equalsIgnoreCase("DBCLOB"))
			return java.sql.Types.CLOB;
		else if (dataTypes[col].equalsIgnoreCase("VARCHAR FOR BIT DATA"))
			return java.sql.Types.VARBINARY;
		else if (dataTypes[col].equalsIgnoreCase("CHAR FOR BIT DATA"))
			return java.sql.Types.BINARY;
		else if (dataTypes[col].equalsIgnoreCase("DATE"))
			return java.sql.Types.DATE;
		else if (dataTypes[col].equalsIgnoreCase("TIME"))
			return java.sql.Types.TIME;
		else if (dataTypes[col].equalsIgnoreCase("TIMESTAMP") || dataTypes[col].equalsIgnoreCase("TIMESTMP"))
			return java.sql.Types.TIMESTAMP;
		else if (dataTypes[col].equalsIgnoreCase("DOUBLE"))
			return java.sql.Types.DOUBLE;
		else if (dataTypes[col].equalsIgnoreCase("LONG VARCHAR"))
			return java.sql.Types.LONGVARCHAR;
		else if (dataTypes[col].equalsIgnoreCase("LONG VARGRAPHIC"))
			return java.sql.Types.LONGVARCHAR;
		else if (dataTypes[col].equalsIgnoreCase("XML"))
			return java.sql.Types.CLOB;
		else
			return java.sql.Types.OTHER;
	}
	
	private boolean bindColumn(int col, byte[] bins)
	{
		int colIndex = col+1;
		try
		{
			if (bins == null)
				prepareStatement.setNull(colIndex, getJavaSQLType(col));
			else if (dataTypes[col].equalsIgnoreCase("VARCHAR FOR BIT DATA") || dataTypes[col].equalsIgnoreCase("CHAR FOR BIT DATA"))
			{
				prepareStatement.setBytes(colIndex, bins);			
			} else if (dataTypes[col].equalsIgnoreCase("BLOB"))
			{
				bindLOB(colIndex, getString(bins), "BLOB");
			} else if (dataTypes[col].equalsIgnoreCase("CLOB"))
			{
				bindLOB(colIndex, getString(bins), "CLOB");
			} else if (dataTypes[col].equalsIgnoreCase("XML"))
			{
				bindLOB(colIndex, getString(bins), "XML");
			} else
			{
				prepareStatement.setString(colIndex, getString(bins));
			}
		} catch (SQLException e)
		{
			blog("Exception binding " + schemaName + "." + tableName +  " data col = " + colIndex + " Data Type " + dataTypes[col] + " value = " + getString(bins));
			return false;
		}
		return true;
	}
	
	private void activateNotLogged() throws SQLException
	{
		if (data.DB2())
		{
			data.connection.createStatement().execute("ALTER TABLE " + IBMExtractUtilities.putQuote(schemaName) + "." + 
					IBMExtractUtilities.putQuote(tableName) + " ACTIVATE NOT LOGGED INITIALLY");
		} else if (data.zDB2())
		{
			String sql = "SELECT '\"'||DBNAME||'\".\"'||TSNAME||'\"' FROM SYSIBM.SYSTABLES WHERE CREATOR = '"+schemaName+"' AND NAME = '"+tableName+"'";
			String tsName = data.queryFirstRow(schemaName, sql);
			data.connection.createStatement().execute("ALTER TABLE " + IBMExtractUtilities.putQuote(schemaName) + "." + 
					IBMExtractUtilities.putQuote(tableName) + " DATA CAPTURE NONE");
			data.commit();
			data.connection.createStatement().execute("ALTER TABLESPACE " + tsName + " NOT LOGGED");
		}
	}
	
	private void commit()
	{
	    try
		{
			if (currentRow >= numOfMultiRows)
			{
				if (numOfMultiRows > 1)
				   prepareStatement.executeBatch();
				else
				   prepareStatement.executeUpdate();	
			    data.commit();
			    activateNotLogged();
			    dataCommitted = true;
			    currentRow = 0;
			}
		} catch (BatchUpdateException e)
		{
			log("Contents of BatchUpdateException:");
		    log(" Update counts: ");
		    int [] updateCounts = e.getUpdateCounts();             
		    for (int i = 0; i < updateCounts.length; i++) 
		    {
		       log("  Statement " + i + ":" + updateCounts[i]);
		    }
		    log(" Message: " + e.getMessage());     
		    log(" SQLSTATE: " + e.getSQLState());
		    log(" Error code: " + e.getErrorCode());
		    SQLException ex = e.getNextException();                
		    while (ex != null) 
		    {                                      
		       log("SQL exception:");
		       log(" Message: " + ex.getMessage());
		       log(" SQLSTATE: " + ex.getSQLState());
		       log(" Error code: " + ex.getErrorCode());
		       ex = ex.getNextException();
		    }
			e.printStackTrace(ps);
		} catch (SQLException e)
		{
			e.printStackTrace(ps);
		}
	}
	
	private void bindLine() throws SQLException
	{
		boolean bad = false;
		String str;
		byte[] bins;
		for (int i = 0; i < columnCount; ++i)
		{
			bins = columns[i].trimQuote(colsep);
			str = (bins == null) ? "" : new String(bins, 0, bins.length);
			if (!bindColumn(i, bins))
				bad = true;
			//System.out.print(str + "|");
			columns[i].curpos = 0;
		}
		//System.out.println();
		dataCommitted = false;
		if (!bad)
		{
			if (numOfMultiRows > 1)
		       prepareStatement.addBatch();
		}
	    commit();
		++currentRow;
		++rowsProcessed;	
	}
	
	private boolean endQuote(BinData bin)
	{
		byte b1, b2;
		while (bin.curpos < bin.length)
		{
			b1 = getByte(bin);
			b2 = (bin.curpos < bin.length) ? bin.buffer[bin.curpos] : (bin = requestBuffer(bin)).buffer[bin.curpos];
			if (b1 == Constants.dbquote && b2 == Constants.dbquote)
			{
				getByte(bin);
				continue;
			}
			if (b1 == Constants.dbquote)
			{
				return true;
			}
		}
		return false;
	}
	
	private byte getByte(BinData bin)
	{
		byte b = bin.buffer[bin.curpos];
		columns[colpos].putByte(b);
		bin.curpos++;
		return b;
	}
	
	private void processAllRows() throws SQLException
	{
		long last = System.currentTimeMillis();
		BinData bin = new BinData(10);
		byte b1, b2;
		while (true)
		{
			if ((bin = requestBuffer(bin)) == null)
				break;
			while (bin.curpos < bin.length)
			{
				b1 = getByte(bin);
				if (b1 == Constants.dbquote)
				{
					while (!endQuote(bin))
					{
						bin = requestBuffer(bin);
						bin.curpos = 0;
					}
					continue;
				}
				b2 = (bin.curpos < bin.length) ? bin.buffer[bin.curpos] : (bin = requestBuffer(bin)) == null ? (byte) 0 : bin.buffer[bin.curpos];
				if (b1 == colsep)
				{
					colpos++;
				}
				if ((b1 == Constants.CR && b2 == Constants.LF) || (b1 == Constants.CR && b2 != Constants.LF) || (b1 == Constants.LF && b2 != Constants.CR))
				{
					if (colpos == (columnCount-1))
					{
					   bindLine();
					   if (numRowsToProcess != -1L && rowsProcessed > numRowsToProcess)
					   {
						   blog("# of rows to insert limit is set for "+schemaName + "." + tableName+" at " + numRowsToProcess);
						   return;
					   }
					   if (batchSize != 0 && (rowsProcessed % batchSize == 0))
                       {
                           long now = System.currentTimeMillis();
                           DecimalFormat myFormatter = new DecimalFormat("###.###");
                           blog("Rows inserted " + schemaName + "." + tableName + " " + batchSize + " in " +
                           		myFormatter.format((now - last)/1000.0) + " sec");
                           last = now;
                       }
					}
					/*if (b1 == Constants.CR)
					{
						if (bin.curpos < bin.length) 
						   bin.curpos++;
						else
						   bin = requestBuffer(bin);
					}*/
					if (b2 == Constants.LF)
					{
						if (bin.curpos < bin.length) 
					       bin.curpos++;
						else
						   bin = requestBuffer(bin);
					}
					colpos = 0;
				}
				if (bin == null)
					break;
			}
		}
	}
	
	private BinData requestBuffer(BinData bin)
	{
		try
		{
			if (bis.available() > 0)
			{
				bin.length = bis.read(bin.buffer);
				bin.curpos = 0;
				return bin;
			}
		} catch (Exception e)
		{
			e.printStackTrace(ps);
		}
		return null;
	}
	
	private boolean open()
	{
		if (!dataFileExists) 
		{
		    log(dataFileName + " does not exist.");
			return false;
		}
		File file = new File(IBMExtractUtilities.removeQuote(dataFileName));
	    if (!(file.isFile() && file.canRead())) 
	    {
	        blog(file.getName() + " cannot be read from.");
	        return false;
	    }		
	    try 
	    {
	       fis = new FileInputStream(file);
	       bis = new BufferedInputStream(fis);
	    } catch (IOException e) 
	    {
	       e.printStackTrace(ps);
	       return false;
	    }
	    return true;
	}
	
	private void close()
	{
	    try 
	    {
	       bis.close();
	       fis.close();
	       if (plfBlob != null)
	          plfBlob.close();
	       if (plfClob != null)
	          plfClob.close();
	       if (plfXML != null)
	          plfXML.close();
	    } catch (IOException e) 
	    {
	       e.printStackTrace(ps);
	    }		
	}
	
	private long processData(int id)
	{
    	long start = System.currentTimeMillis(), end;
		String insertSQL;
		log("dataFileName="+dataFileName+(dataFileExists ? " found " : " not found ") +" Processing ("+id+")for " + schemaName + "." + tableName + (tableExists ? " exists " : " does not exist ") +  " Column Count = " + columnCount + " Thread " + Thread.currentThread().getName());
		insertSQL = buildInsertStatement();
		//log("Insert Statement="+insertSQL);
		try
		{
			activateNotLogged();
			prepareStatement = data.connection.prepareStatement(insertSQL);
			if (open())
			{
				processAllRows();	
				if (!dataCommitted)
				{
					currentRow = Integer.MAX_VALUE;
					commit();
				}
				close();
				blog(IBMExtractUtilities.padLeft(""+rowsProcessed, 10) + " rows have been loaded in " + IBMExtractUtilities.padRight(schemaName + "." + tableName, 50) + " in " + IBMExtractUtilities.getElapsedTime(start));
			}
		} catch (SQLException e)
		{
			if (e.getErrorCode() == -551)
			{
				blog(data.getUserID() + " does not have INSERT privilege on " + schemaName + "." + tableName);				
			} else if (e.getErrorCode() == -1477)
			{
				blog("You need to drop and recreate table " + schemaName + "." + tableName + " due to ACTIVATE NOT LOGGED problem");				
			} else
			{
				blog(schemaName + "." + tableName + " SQL Error = " + e.getErrorCode());
			    e.printStackTrace(ps);
			}
		} catch (Exception ex)
		{
			blog(schemaName + "." + tableName + " Error : " + ex.getMessage());
			ex.printStackTrace(ps);			
		}
		return 0L;
	}

	public Integer call() throws Exception
	{
		if (columnCount > 0)
		{
		   processData(tid);
		} else
		{
			blog(schemaName + "." + tableName + " does not exist.");			
		}
		return tid;
	}
}
