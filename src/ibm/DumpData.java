package ibm;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DumpData
{
    public ResultSetMetaData resultsetMetadata;
    public ResultSet rs = null;
	public BlobVal bval;

	private String OUTPUT_DIR = System.getProperty("OUTPUT_DIR") == null ? "." : System.getProperty("OUTPUT_DIR");
    private String filesep = System.getProperty("file.separator");
    private int filesizelimit = 524288000;
    private Statement         statement = null;
    private PreparedStatement queryStatement = null;
    private DBData data;
    private int fetchSize;
    private long lobNumber = 1;
    private boolean dataUnload, usePipe, graphic, dbclob, db2_compatibility, lobsToFiles, netezza;
	private String mapTimeToTimestamp, mapCharToVarchar, encoding, customMapping, varcharLimit = "4096", oracleNumberMapping;
	private String convertOracleTimeStampWithTimeZone2Varchar;
	byte colsep;
	private float releaseLevel;
	private Properties propDataMapNZ, propDataMap, udtProp; 
	private TableList t;
    private BufferedOutputStream blobWriter, clobWriter, xmlWriter;
    
    public DumpData(DBData data, char colsep, int fetchSize, boolean netezza, 
    		Properties propDataMapNZ, Properties propDataMap, Properties udtProp,  
    		TableList t, String oracleNumberMapping, boolean dataUnload, boolean usePipe, boolean graphic, boolean dbclob, boolean db2_compatibility, boolean lobsToFiles,
    		String mapTimeToTimestamp, String convertOracleTimeStampWithTimeZone2Varchar, 
    		String mapCharToVarchar, String customMapping, float releaseLevel, String encoding, String varcharLimit)
    {
    	this.data = data;
    	this.colsep = (byte) colsep;
    	this.fetchSize = fetchSize;
    	this.netezza = netezza;
    	this.propDataMapNZ = propDataMapNZ;
    	this.propDataMap = propDataMap;
    	this.udtProp = udtProp;
    	this.t = t;
    	this.oracleNumberMapping = oracleNumberMapping;
    	this.customMapping = customMapping;
    	this.dataUnload = dataUnload;
    	this.usePipe = usePipe;
    	this.graphic = graphic;
    	this.dbclob = dbclob;
    	this.db2_compatibility = db2_compatibility;
    	this.lobsToFiles = lobsToFiles;
    	this.mapTimeToTimestamp = mapTimeToTimestamp;
    	this.mapCharToVarchar = mapCharToVarchar;
    	this.convertOracleTimeStampWithTimeZone2Varchar = convertOracleTimeStampWithTimeZone2Varchar;
    	this.releaseLevel = releaseLevel;
    	this.encoding = encoding;
    	this.varcharLimit = varcharLimit;
    }

    private String getLobSeq(String name)
    {
    	// lobnumber scope needs to be global and not thread.
       String lobName = "" + lobNumber;
       int i = lobName.length();
       while (i < 10)
       {
          lobName = "0" + lobName;
          i++;
       }
       lobNumber++;
       return name + lobName;
    }
    
    private String getLobFileName(int id, int ColIndex, String initValue) throws IOException
    {
       String fileName;
       fileName = t.dstSchemaName[id].toLowerCase()+"_"+t.dstTableName[id].toLowerCase();
       fileName = fileName.replace("\"", "");
       fileName = OUTPUT_DIR + filesep + "data" + filesep + fileName;
       File lobDir = new File(fileName);
       if(!lobDir.exists())
           lobDir.mkdirs();
       if (lobsToFiles)
       {
          fileName = fileName + filesep + getLobSeq("L") + ".lob";   
          File tmpfile = new File(fileName);
          fileName = tmpfile.getCanonicalPath();
          blobWriter = new BufferedOutputStream(new FileOutputStream(fileName));            
       } else
       {
    	   if (initValue != null)
    	   {
    		   if (initValue.equalsIgnoreCase("blob"))
    			   fileName = fileName + filesep + "B0.lob";
    		   else if (initValue.equalsIgnoreCase("clob"))
    			   fileName = fileName + filesep + "C0.lob";
    		   else if (initValue.equalsIgnoreCase("xml"))
    			   fileName = fileName + filesep + "X0.lob";
    	   }
    	   else
    	   {
        	   if (bval.isBLOB[ColIndex-1])
        	   {
                   if (filesizelimit < bval.blobOffset)
                   {
                       bval.blobFileCounter++; 
                       bval.blobOffset = 0;
                       if (blobWriter != null)
                       {
                          blobWriter.close();
                       }
                       if (t.multiTables[id] == 0)
                       {
                          fileName = fileName + filesep + "B"+ bval.blobFileCounter + ".lob";
                       } else
                       {
                          fileName = fileName + filesep + "B"+ bval.blobFileCounter + ".lob" + t.multiTables[id];                     
                       }
                       blobWriter = new BufferedOutputStream(new FileOutputStream(fileName));            
                   } else
                   {
                      if (t.multiTables[id] == 0)
                      {
                         fileName = fileName + filesep + "B"+ bval.blobFileCounter + ".lob";
                      } else
                      {
                         fileName = fileName + filesep + "B"+ bval.blobFileCounter + ".lob" + t.multiTables[id];                    
                      }
                   }        		   
        	   }
        	   if (bval.isCLOB[ColIndex-1])
        	   {
                   if (filesizelimit < bval.clobOffset)
                   {
                       bval.clobFileCounter++; 
                       bval.clobOffset = 0;
                       if (clobWriter != null)
                       {
                          clobWriter.close();
                       }
                       if (t.multiTables[id] == 0)
                       {
                          fileName = fileName + filesep + "C"+ bval.clobFileCounter + ".lob";
                       } else
                       {
                          fileName = fileName + filesep + "C"+ bval.clobFileCounter + ".lob" + t.multiTables[id];                     
                       }
                       clobWriter = new BufferedOutputStream(new FileOutputStream(fileName));            
                   } else
                   {
                      if (t.multiTables[id] == 0)
                      {
                         fileName = fileName + filesep + "C"+ bval.clobFileCounter + ".lob";
                      } else
                      {
                         fileName = fileName + filesep + "C"+ bval.clobFileCounter + ".lob" + t.multiTables[id];                    
                      }
                   }        		   
        	   }
        	   if (bval.isXML[ColIndex-1])
        	   {
                   if (filesizelimit < bval.xmlOffset)
                   {
                       bval.xmlFileCounter++; 
                       bval.xmlOffset = 0;
                       if (xmlWriter != null)
                       {
                          xmlWriter.close();
                       }
                       if (t.multiTables[id] == 0)
                       {
                          fileName = fileName + filesep + "X"+ bval.xmlFileCounter + ".lob";
                       } else
                       {
                          fileName = fileName + filesep + "X"+ bval.xmlFileCounter + ".lob" + t.multiTables[id];                     
                       }
                       xmlWriter = new BufferedOutputStream(new FileOutputStream(fileName));            
                   } else
                   {
                      if (t.multiTables[id] == 0)
                      {
                         fileName = fileName + filesep + "X"+ bval.xmlFileCounter + ".lob";
                      } else
                      {
                         fileName = fileName + filesep + "X"+ bval.xmlFileCounter + ".lob" + t.multiTables[id];                    
                      }
                   }        		   
        	   }
    	   }
       }
       return fileName;
    }
    
    public void openLobWriters(int id) throws FileNotFoundException, IOException
    {
    	if (!lobsToFiles)
    	{
    		if (!netezza)
    		{
	            blobWriter = clobWriter = xmlWriter = null;
		        if (bval.isBlob)
		           blobWriter = new BufferedOutputStream(new FileOutputStream(getLobFileName(id, 0, "BLOB")));
		        if (bval.isClob)
		           clobWriter = new BufferedOutputStream(new FileOutputStream(getLobFileName(id, 0, "CLOB")));
		        if (bval.isXml)
		           xmlWriter = new BufferedOutputStream(new FileOutputStream(getLobFileName(id, 0, "XML")));
    		}
    	}
    }
    
    public void closeLobWriters() throws IOException
    {
        if (!lobsToFiles)
        {
        	if (!netezza)
        	{
	           if (blobWriter != null)
	              blobWriter.close();
	           if (clobWriter != null)
	               clobWriter.close();
	           if (xmlWriter != null)
	               xmlWriter.close();
        	}
        }
    }
 
    private void flushLOBS() throws IOException
    {
        if (usePipe)
        {
        	if (!netezza)
        	{
	        	if (blobWriter != null)
	        		blobWriter.flush();
	        	if (clobWriter != null)
	        		clobWriter.flush();
	        	if (xmlWriter != null)
	        		xmlWriter.flush();
        	}
        }
        if (lobsToFiles)
        	if (!netezza)
               blobWriter.close();        	

    }
    
    private void log(String msg)
    {
    	if (Constants.zos())
    	{
            System.out.println(Constants.timestampFormat.format(new Date()) + ":" + msg);    		
    	} else 
    	{
            System.out.println("[" + Constants.timestampFormat.format(new Date()) + "] " + msg);    		
    	}
    }
    
	private String getInformixIntervalType(String srcType)
    {
    	String interval = "", regex = "\\w+", arg1 = null;
        Pattern pattern;
        Matcher matcher = null;
        pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(srcType);
    	while (matcher.find())
    	{
    		arg1 = matcher.group();
    	}
    	if (arg1 == null)
    		interval = "VARCHAR(25)";
    	else if (arg1.equalsIgnoreCase("fraction"))
    		interval = "DECIMAL(10,6)";
    	else if (arg1.equalsIgnoreCase("interval"))
        	interval = "VARCHAR(25)";
        else
    		interval = "SMALLINT";
    	return interval;
    }
    
    private DataMap getDataMap(DataMap map, String colType, int id)
    {        	
        String srcType = map.sourceDataType.replace(' ', '_');
        String targetType = "";
        
        if (netezza)
        {
        	if (colType.startsWith("INTERVAL"))
        	   targetType = colType.replaceAll("\\([^()]*\\)", "");
        	else
               targetType = (String) propDataMapNZ.get((String) data.dbSourceName.toUpperCase()+"."+srcType);                
        }
        else
            targetType = (String) propDataMap.get((String) data.dbSourceName.toUpperCase()+"."+srcType);
        
        if (convertOracleTimeStampWithTimeZone2Varchar.equalsIgnoreCase("true") && data.Oracle())
        {
        	if (srcType.startsWith("TIMESTAMP") && srcType.endsWith("TIME_ZONE"))
        	{
        		targetType = "VARCHAR(60)";
        	}
        }
        if (mapTimeToTimestamp.equalsIgnoreCase("true") && srcType.equalsIgnoreCase("time"))
        {
        	targetType = "TIMESTAMP";
        }
        if (targetType == null && data.Sybase())
        {
    		targetType = (String) udtProp.getProperty(srcType);
        }
        if (targetType == null && data.Informix() && srcType.toUpperCase().startsWith("INTERVAL"))
        {
        	targetType = getInformixIntervalType(srcType);
        }
        if (targetType == null || targetType == "")
        {
        	if (netezza)
        	{
                log("Missing data map for " + data.dbSourceName.toUpperCase() + "." + map.sourceDataType + " in file " + 
             		   IBMExtractUtilities.DATAMAPNZ_PROP_FILE + " for table " + t.srcTableName[id]);
                log("Please add the entry in file " + IBMExtractUtilities.DATAMAPNZ_PROP_FILE + " for datatype " + map.sourceDataType);
        	} else
        	{
                log("Missing data map for " + data.dbSourceName.toUpperCase() + "." + map.sourceDataType + " in file " + 
             		   IBMExtractUtilities.DATAMAP_PROP_FILE + " for table " + t.srcTableName[id]);
                log("Please add the entry in file " + IBMExtractUtilities.DATAMAP_PROP_FILE + " for datatype " + map.sourceDataType);            		
        	}
           map.targetDataType = "UNDEFINED /* Original data type was " + map.sourceDataType + " */" ;
           return map;
        } 
    	if (mapCharToVarchar.equalsIgnoreCase("true"))
    	{
    		if (srcType.startsWith("CHAR"))
    			targetType = targetType.replaceFirst("CHAR", "VARCHAR");            		
    	}
    	if (!graphic) 
    	{
    		targetType = (netezza) ? targetType.replaceFirst("NCHAR", "CHAR") : targetType.replaceFirst("GRAPHIC", "CHAR");
    	}
    	if (dbclob)
    	{
    		if (targetType.equals("CLOB"))
    		{
    		   targetType = "DBCLOB";
    		}
    	}
    	if (netezza)
    	{
    		; 
    	} 
    	else if (Constants.zdb2())
        {
    	    targetType = targetType.replaceFirst("CHAR FOR BIT DATA", "BINARY");
        }
    	else
    	{
        	if (data.Oracle())
        	{
        		if (db2_compatibility)
        		{
            		targetType = targetType.replaceFirst("NUMERIC", "NUMBER");
            		if (srcType.equalsIgnoreCase("DATE"))
            			targetType = srcType;
            		if (srcType.startsWith("VARCHAR2") || srcType.startsWith("NVARCHAR2"))
                    	targetType = targetType.replaceFirst("VARCHAR", "VARCHAR2");        			
        		} else
        		{
            		if (srcType.equalsIgnoreCase("DATE"))
            		{
      	        	  if (releaseLevel != -1.0F && releaseLevel >= 9.7F)
      	        		targetType = "TIMESTAMP(0)";            			
            		}
        			
        		}
        	}        		
    	}
        String[] tok2, tok = targetType.split(";");
        map.targetDataType = tok[0];
        if (tok.length > 1)
        {
            int i = 1;
            while (i < tok.length)
            {
                tok2 = tok[i].split("=");
                switch (i)
                {
                    case 1 :
                        if (tok2[0].equalsIgnoreCase("varlength") && tok2[1].equalsIgnoreCase("true"))
                        {
                            map.varlength = true;
                        }
                        break;
                    case 2 :
                        if (tok2[0].equalsIgnoreCase("default"))
                        {
                            map.defaultlength = Integer.parseInt(tok2[1]);
                        }
                        break;
                    default :
                        break;    
                }
                ++i;
            }
        }
        return map;
    }
    
    private DataMap db2Number(DataMap map, int precision, int scale)
    {
    	String dataType;
      	if (scale < 0 && scale != -127)
      	{
      		precision = precision - scale;                            		 
      		scale = 0;
      	} else if (precision < scale)
      		precision = scale;
      	map.precision = precision;
      	map.scale = scale;      	
      	if (scale == -127 || scale == 0)
      	{
            if (!customMapping.equalsIgnoreCase("false"))
            {
                  if (precision == 0)
                	  dataType = "DOUBLE";
                  else if (precision == 1)
                	  dataType = "SMALLINT";
                  else if (precision > 1 && precision < 11)
                	  dataType = "INTEGER";
                  else if (precision > 10 && precision < 21)
                	  dataType = "BIGINT";
                  else
                	  dataType = "DOUBLE";                                    
            }
            else
            {
              	 if (db2_compatibility)
              	 {
              		 if (oracleNumberMapping.equalsIgnoreCase("true"))
              		 {
                           if (precision == 0)
                           {
            	        	  if (releaseLevel != -1.0F && releaseLevel >= 9.5F)
            	        		  dataType = "NUMBER";
            	        	  else
            	        		  dataType = "DOUBLE";                                  	        		  
                           } else if (precision > 0 && precision < 5)
                        	   dataType = "SMALLINT";
                           else if (precision > 4 && precision < 10)
                        	   dataType = "INTEGER";
                           else if (precision > 9 && precision < 19)
                        	   dataType = "BIGINT";
                           else if (precision > 18 && precision < 32)
                           {
                      		  if (releaseLevel != -1.0F && releaseLevel >= 9.5F) 
                       		    dataType = "NUMBER";
             				  else
                        	    dataType = "FLOAT";
                           } else
                           {
                     		  if (releaseLevel != -1.0F && releaseLevel >= 9.5F) 
                     		    dataType = "NUMBER";
           				      else
           				    	dataType = "DOUBLE";
                           }                                         	                                     			 
              		 } else if (!oracleNumberMapping.equalsIgnoreCase("true") && !oracleNumberMapping.equalsIgnoreCase("false"))              			 
              		 {
              			dataType = oracleNumberMapping;
              		 }
              		 else
              		 {
                  		 if (precision > 31)
                  		 {
                  			  map.precision = 31;
              	        	  if (releaseLevel != -1.0F && releaseLevel >= 9.5F)
              	        		 dataType = "NUMBER";
              				  else
              					 dataType = "DOUBLE";
                  		 }                  		                       		 
                  		 else if (precision == 0)
                  		 {
                  			 dataType = "NUMBER";
                  		 } else
                  		 {
                  			 dataType = "NUMBER(" + precision + ")";
                  		 }
              		 }                                    			 
              	 } else
              	 {
              		  if (!oracleNumberMapping.equalsIgnoreCase("true") && !oracleNumberMapping.equalsIgnoreCase("false"))              			 
             		  {
             			 dataType = oracleNumberMapping;
             		  } else
             		  {
	                      if (precision == 0)
	                      {
	                    	  map.precision = (precision == 0) ? 31 : precision;
	         	        	  if (releaseLevel != -1.0F && releaseLevel >= 9.5F)
	         	        		 dataType = "DECFLOAT(16)";
	        				  else
	        					 dataType = "DOUBLE";
	                      } else if (precision > 0 && precision < 5)
	                    	  dataType = "SMALLINT";
	                      else if (precision > 4 && precision < 10)
	                    	  dataType = "INTEGER";
	                      else if (precision > 9 && precision < 19)
	                    	  dataType = "BIGINT";
	                      else if (precision > 18 && precision < 32)
	                      {
	                 		  if (releaseLevel != -1.0F && releaseLevel >= 9.5F) 
	                   		     dataType = "DECFLOAT(16)";
	         				  else
	                    	     dataType = "FLOAT";
	                      } else
	                      {
	                 		 if (releaseLevel != -1.0F && releaseLevel >= 9.7F)
	                 		    dataType = "DECFLOAT(16)";
	      				     else
	      				    	dataType = "DOUBLE";
	                      }
             		  }
              	 }
               }                            		 
      	 } else 
      	 {
      		 if (precision > 31)
      		 {
      			 map.precision = 31;
      			 if (db2_compatibility)
      			 {
      				 dataType = "NUMBER(31,"+scale+")";
      			 } else
      			 {
             		 if (releaseLevel != -1.0F && releaseLevel >= 9.7F)
               		    dataType = "DECFLOAT(16)";
             		 else
             			dataType = "DECIMAL(31,"+scale+")";      				 
      			 }
      		 } else
       		 {
       			  map.precision = (precision == 0) ? 31 : precision;
         		  if (!oracleNumberMapping.equalsIgnoreCase("true") && !oracleNumberMapping.equalsIgnoreCase("false"))              			 
         		  {
            		  dataType = oracleNumberMapping;         			  
         		  } else
         		  {
           			  dataType = (db2_compatibility) ? "NUMBER(" + ((precision ==0) ? 31 : precision) + "," + scale + ")" :
             				"DECIMAL(" + ((precision ==0) ? 31 : precision) + "," + scale + ")";         			  
         		  }
       		 }
      	}
      	map.targetDataType = dataType;
      	return map;
    }
    
    private DataMap nzNumber(DataMap map, int precision, int scale)
    {
    	String dataType;
      	if (scale < 0 && scale != -127)
      	{
      		precision = precision - scale;                            		 
      		scale = 0;
      	} else if (precision < scale)
      		precision = scale;
      	map.precision = precision;
      	map.scale = scale;      	
    	if (scale <= 0)        
    	{
    		if (precision == 0)
    		{
    	      	map.precision = 38;
    	      	map.scale = 16;      	
    			dataType = "NUMERIC(38,16)"; 
    		}
    		else if (precision >= 1 && precision <= 2)
        		dataType = "BYTEINT";
        	else if (precision >= 3 && precision <= 4)
        		dataType = "SMALLINT";
        	else if (precision >= 5 && precision <= 9)
        		dataType = "INT";
        	else if (precision >= 10 && precision <= 18)
        		dataType = "BIGINT";
        	else
        		dataType = "NUMERIC(" + precision + ")";	        	
    	} else
    	{
    		if (precision == 0)
    		{
    			map.precision = 38;
        		dataType = "NUMERIC(38," + scale + ")";
    		} else
    			dataType = "NUMERIC(" + precision + "," + scale + ")";
    	}
    	map.targetDataType = dataType;
    	return map;
    }
    
    public String getTargetDataType(int id, int colIndex, HashMap dataTypeMap) throws SQLException
    {
    	String targetDataType = "UNKNOWN";
        String colName = resultsetMetadata.getColumnName(colIndex);
        String key = IBMExtractUtilities.removeQuote(t.srcSchName[id]) + "." + IBMExtractUtilities.removeQuote(t.srcTableName[id]) + "." + colName;
        try
        {
        	synchronized (dataTypeMap)
			{
                targetDataType = ((DataMap) dataTypeMap.get(key)).targetDataType;				
			}
        } catch (Exception e)
        {
        	log("Error in getting targetDataType for " + key);
        	e.printStackTrace();
        }
        return targetDataType;
    }
    
    public  DataMap processDataMap(DataMap map, int colIndex, int octetLength, String lobLength) throws SQLException
    {
    	String lobType, colType = resultsetMetadata.getColumnTypeName(colIndex).toUpperCase();
    	int precision, scale, colDisplayWidth, columnSize;
    	
        try
        {
            precision = resultsetMetadata.getPrecision(colIndex);
        }
        catch (Exception e)
        {
        	precision = 2147483647;
        	log("Error: Precision found more than 2GB limiting it to 2GB");                	
        }
        colDisplayWidth = resultsetMetadata.getColumnDisplaySize(colIndex);
        
        if (data.Teradata() || data.Informix())
        {
     	   if (precision == 0 && colDisplayWidth > 0)
     		   precision = colDisplayWidth;
        } else if (data.Oracle())
        {
        	if (colType.equalsIgnoreCase("RAW"))
     	       precision = colDisplayWidth;
        	if (octetLength != -1)
        		precision = octetLength;
        } else if (data.Sybase() && !colType.equalsIgnoreCase("image"))
        {
     	   if (precision == 0 && colDisplayWidth > 0)
     		   precision = colDisplayWidth;            	   
        }
 	    scale = resultsetMetadata.getScale(colIndex); 
        map.precision = precision;
        map.scale = scale;
        
 	    if (map.varlength)
        {
     	    int nzMaxLen = bval.getNZLOBEstimatedLength();
            if ((map.targetDataType.equalsIgnoreCase("CHAR") || map.targetDataType.equalsIgnoreCase("CHAR FOR BIT DATA")) && precision > 254)
            {
            	map.precision = (netezza) ? ((precision == -1 || precision > Constants.netezzaMaxlength) ? nzMaxLen : precision) : precision;
         	    map.targetDataType = "VARCHAR("+map.precision+")";
            }
            else if (map.targetDataType.equalsIgnoreCase("BINARY") && precision > 254)
            {
            	map.precision = 254;
               // map.targetDataType = map.targetDataType + "("+map.precision+")";
            	map.targetDataType = "BINARY" + "("+map.precision+")"; 
            }
            else if (map.targetDataType.equalsIgnoreCase("GRAPHIC") && precision > 127)
            {
            	 map.precision = 127;
         	    // map.targetDataType = map.targetDataType + "("+map.precision+")";
            	 map.targetDataType = "GRAPHIC" + "("+map.precision+")"; 
            	 
            }
            else if ((map.targetDataType.equalsIgnoreCase("VARCHAR") || map.targetDataType.equalsIgnoreCase("VARGRAPHIC")) ||
                    (map.targetDataType.equalsIgnoreCase("DBCLOB") || map.targetDataType.equalsIgnoreCase("CLOB")))
            {
                if (!customMapping.equalsIgnoreCase("false"))
                {
                   if (data.Oracle())
                   {
                      columnSize = precision;
                      if (precision == 0)
                      {
                    	  map.precision = 4000;
                          map.targetDataType = (graphic) ? "VARGRAPHIC("+map.precision+")" :"VARCHAR("+map.precision+")";
                      } else if (precision == 1333)
                      {
                    	  map.precision = (graphic) ? 4000 : 1333;
                          map.targetDataType = (graphic) ? "VARGRAPHIC("+map.precision+")" : "VARCHAR("+map.precision+")"; 
                      }
                      else if (precision > 0 && precision < 1333)
                      {
                    	  map.precision = (graphic) ? columnSize : precision;
                          map.targetDataType = (graphic) ? "VARGRAPHIC("+columnSize+")" :"VARCHAR("+precision+")"; 
                      }                             
                      else
                      {
                          if (precision == -1 || columnSize > 536870912)
                          {
                        	  map.precision = (graphic) ? 536870912 : 536870912;
                              map.targetDataType = (graphic) ? "DBCLOB("+map.precision+")" :"CLOB("+map.precision+")"; 
                          } else
                          {
                        	  map.precision = (graphic) ? columnSize : precision;
                              map.targetDataType = (graphic) ? "DBCLOB("+map.precision+")" :"CLOB("+map.precision+")";
                          }
                          map.targetDataType += " LOGGED NOT COMPACT";
                      }
                   } else
                   {
                 	  columnSize = precision; // columnSize = precision / 2; This was again got changed to same size
                      if (precision == 0)
                      {
                    	  map.precision = (graphic) ? 4000 : 4000;
                          map.targetDataType = (graphic) ? "VARGRAPHIC("+map.precision+")" :"VARCHAR("+map.precision+")";
                      }
                      else if (precision > 0 && precision < 32673)
                      {
                    	  map.precision = (graphic) ? columnSize : precision;
                          map.targetDataType = (graphic) ? "VARGRAPHIC("+map.precision+")" :"VARCHAR("+map.precision+")"; 
                      }                             
                      else
                      {
                          if (precision == -1 || precision > 536870912)
                          {
                        	  map.precision = (graphic) ? 536870912 : 1073741824;
                              map.targetDataType = (graphic) ? "DBCLOB("+map.precision+")" :"CLOB("+map.precision+")"; 
                          } else
                          {
                        	  map.precision = (graphic) ? columnSize : precision;
                              map.targetDataType = (graphic) ? "DBCLOB("+columnSize+")" :"CLOB("+precision+")";
                          }
                          map.targetDataType += " LOGGED NOT COMPACT";
                      } 
                   }
                } else
                {
         		  if (map.targetDataType.startsWith("CLOB") && (map.sourceDataType.equalsIgnoreCase("TINYTEXT") || map.sourceDataType.equalsIgnoreCase("TEXT") ||
         		      map.sourceDataType.equalsIgnoreCase("MEDIUMTEXT") || map.sourceDataType.equalsIgnoreCase("LONGTEXT") ||
         		      map.sourceDataType.equalsIgnoreCase("CLOB")))
     		      {
         			  if (precision == -1)
         			  {
        				   map.precision = (netezza) ? nzMaxLen : precision;
         				   if (!lobLength.equals("-1"))
         				   {
         					   map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB(" + lobLength + ")" : "CLOB(" + lobLength + ")";
         				   } else
         			           map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB" : "CLOB";
         			  } else if (precision < 4096)
         			  {
         				   map.precision = precision;
                           map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "VARGRAPHIC("+map.precision+")" : "VARCHAR("+map.precision+")";
         			  } else
         			  {
         				  map.precision = precision;
       			          map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB("+map.precision+")" : "CLOB("+map.precision+")";                        				                      				  
         			  }
     		      }                        		  
         		  else if (map.targetDataType.startsWith("DBCLOB") && (map.sourceDataType.equalsIgnoreCase("NCLOB") || map.sourceDataType.equalsIgnoreCase("NTEXT") ||
         				  map.sourceDataType.equalsIgnoreCase("DBCLOB")))
         		  {
         			  if (precision == -1)
         			  {
         				  map.precision = (netezza) ? nzMaxLen : precision;
        				   if (!lobLength.equals("-1"))
         				   {
         					   map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB(" + lobLength + ")" : "CLOB(" + lobLength + ")";
         				   } else
         			         map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB" : "CLOB";
         			  } else if (precision < 4096)
         			  {
         				   map.precision = precision;
                           map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "VARGRAPHIC("+map.precision+")" : "VARCHAR("+map.precision+")";
         			  } else
         			  {
        				   map.precision = precision;
       			           map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB("+map.precision+")" : "CLOB("+map.precision+")";                        				                      				  
         			  }
         		  }                        		  
         		  else if (map.targetDataType.equalsIgnoreCase("VARCHAR") && (map.sourceDataType.equalsIgnoreCase("TINYTEXT") || map.sourceDataType.equalsIgnoreCase("TEXT") ||
             		      map.sourceDataType.equalsIgnoreCase("MEDIUMTEXT") || map.sourceDataType.equalsIgnoreCase("LONGTEXT") ||
             		      map.sourceDataType.equalsIgnoreCase("CLOB")))
     		      {
   				         map.precision = (netezza) ? (precision == -1 || precision > nzMaxLen ? nzMaxLen : precision) :    
            				  (map.defaultlength == 0 ? precision : map.defaultlength);
             			 map.targetDataType = "VARCHAR("+map.precision+")";
     		      } 
         		  else
     		      {
         			  int varcharLimitInt;
         			  try
         			  {
         				  varcharLimitInt = Integer.valueOf(varcharLimit);
         			  } catch (Exception e)
         			  {
         				  log("Invalid value specified for varcharLimit. Going with 4096.");
         				  varcharLimitInt = 4096;
         			  }
                       if (precision == -1)
                       {
                    	   map.precision = (netezza) ? nzMaxLen : precision;
         				   if (!lobLength.equals("-1"))
         				   {
         					   map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB(" + lobLength + ")" : "CLOB(" + lobLength + ")";
         				   } else
                     	       map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB" : "CLOB";
                       }
                       else if (precision <= varcharLimitInt) // Initial it was 32672 and changed to 4096 
                       {
                    	   map.precision = (netezza) ? (precision == 0 ? nzMaxLen : precision) : (graphic) ? precision :precision;
                           map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "VARGRAPHIC("+map.precision+")" :"VARCHAR("+map.precision+")";
                       }
                       else
                       {
                    	   map.precision = (netezza) ? nzMaxLen : (graphic) ? precision : precision;
         				   if (!lobLength.equals("-1"))
         				   {
         					   map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB(" + lobLength + ")" : "CLOB(" + lobLength + ")";
         				   } else
                               map.targetDataType = (netezza) ? "VARCHAR("+map.precision+")" : (graphic) ? "DBCLOB("+map.precision+")" :"CLOB("+map.precision+")";                                
                       }                    		    	  
     		      }
                }
            }
            else if (map.targetDataType.equalsIgnoreCase("BLOB") || map.targetDataType.equalsIgnoreCase("VARCHAR FOR BIT DATA"))
            {
               if (!customMapping.equalsIgnoreCase("false"))
               {
                  if (precision == -1 || precision == 0 || precision > 32672)
                  {
                	  map.precision = 1073741824;
          		      map.targetDataType = "BLOB("+map.precision+") LOGGED NOT COMPACT";
         		      precision = 1073741824;                        		 
                  } else if (precision > 0 && precision < 32673)
                  {
                	  map.precision = precision;
                      map.targetDataType = "VARCHAR("+map.precision+") FOR BIT DATA";
                  }  
                  if (colType.equalsIgnoreCase("RAW"))
                  {
                	  map.precision = precision;
                 	  map.targetDataType = "VARCHAR("+map.precision+") FOR BIT DATA";
                  }
               } else
               {
                  if (precision == -1 || precision == 0)
                  {
                	  map.precision = (netezza) ? precision : 1073741824;
                	  if (!lobLength.equals("-1"))
    				  {
                		 map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "BLOB("+lobLength+")"; 
    				  } else
                         map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "BLOB("+map.precision+")";
                  } else if (precision > 0 && precision < 32673)
                  {
                	  map.precision = precision;
                      map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "VARCHAR("+map.precision+") FOR BIT DATA";
                      if (db2_compatibility && data.Oracle())
                      {
                 	      map.targetDataType = map.targetDataType.replaceFirst("VARCHAR", "VARCHAR2");
                      }
                  }
                  else if (precision > 32672)
                  {
                	  map.precision = precision;         
                	  if (!lobLength.equals("-1"))
    				  {
                		 map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "BLOB("+lobLength+")";
    				  } else
                         map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "BLOB("+map.precision+")";
                  }
               }
            }                       
            else if (map.targetDataType.equalsIgnoreCase("CHAR FOR BIT DATA"))
            {
          	    map.precision = precision;                	  
                map.targetDataType = (netezza) ?  "CHAR("+map.precision+")" : "CHAR("+map.precision+") FOR BIT DATA";
            }                       
            else if (map.targetDataType.equalsIgnoreCase("IMAGE"))
            {
                if (precision == 0)
                {
                	map.precision = (netezza) ? nzMaxLen : 1073741824;
                    map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "BLOB("+map.precision+")";
                } else
                {
                	map.precision = (netezza) ? nzMaxLen : precision;
                    map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : map.targetDataType + "("+map.precision+")";                                   
                }
            }                       
            else if (map.targetDataType.equalsIgnoreCase("VARBINARY"))
            {
                if (precision == 0)
                {
                	map.precision = (netezza) ? nzMaxLen : 1073741824;
                    map.targetDataType = (netezza) ?  "VARCHAR("+map.targetDataType+")" : "BLOB("+map.targetDataType+")";
                }
                else if (precision >  32672)
                {
                	map.precision = (netezza) ?  nzMaxLen : precision;
                    map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "BLOB("+map.precision+")";
                } else
                {
                	map.precision = (netezza) ?  nzMaxLen : precision;
                    map.targetDataType = (netezza) ?  "VARCHAR("+map.precision+")" : "VARBINARY("+map.precision+")";                                   
                }
            }                       
            else if (map.targetDataType.equalsIgnoreCase("NUMERIC") || map.targetDataType.equalsIgnoreCase("NUMBER"))
            {
         	   if (netezza)
         	   {
             	  map = nzNumber(map, precision, scale);
         	   }
         	   else
         	   {
         	      map = db2Number(map, precision, scale);
         	   }
            }
            else if (map.targetDataType.equalsIgnoreCase("NVARCHAR"))
            {
          	   if (netezza)
         	   {
               	    map.precision = (precision == -1) ? nzMaxLen : precision;
               	    map.targetDataType = "NVARCHAR("+map.precision+")"; 
         	   } else
         	   {
              	    map.precision = 1073741824;
               	    map.targetDataType = (graphic) ? "DBCLOB" : "CLOB";          		   
         	   }            	
            }
            else
            {
            	int prec;

            	if (precision == 0)
            	{
            		if (map.defaultlength == 0)
            			prec = colDisplayWidth;
            		else
            			prec = map.defaultlength;
            	} else
            		prec = precision;
            	
            	if(map.targetDataType.contains("("))
            	{
            		int pos =  map.targetDataType.indexOf("(");
            		//log("pos = " + pos + " map.sourceDataType = " + map.sourceDataType + " map.targetDataType = " + map.targetDataType + " map.precision = " + map.precision + " map.scale = " + map.scale);
            		if (pos > 0)
            		    map.targetDataType = map.targetDataType.substring(0, pos -1);
            	}
            	map.targetDataType = (scale == 0) ? map.targetDataType + "(" + prec + ")"  
            			: map.targetDataType + "(" + ((prec ==0) ? 255 : prec) + "," + scale + ")"; 
            	
            	map.precision = (prec == 0) ? 255 : prec;
            	map.scale = scale;

            }
        }
        if ((map.targetDataType.startsWith("BLOB") || map.targetDataType.startsWith("CLOB") || map.targetDataType.startsWith("XML")) && precision > 1073741824)
        {
        	map.precision = precision;
     	    map.targetDataType = map.targetDataType + ((Constants.zdb2()) ? "" : " NOT LOGGED");  
        }
      	return map;
    }
    
    private void generateDataMapAndLOBS(int id, HashMap dataTypeMap, HashMap columnLengthMap, HashMap lobsLengthMap) throws SQLException
    {
    	DataMap map;
    	bval = new BlobVal();
       int i, ij, precision, octtetLength;
        String key, colType, colName, lobLength;
        
        bval.isBLOB = new boolean[resultsetMetadata.getColumnCount()];
        bval.isCLOB = new boolean[resultsetMetadata.getColumnCount()];
        bval.isXML = new boolean[resultsetMetadata.getColumnCount()];
        
        for (i = 1; i <= resultsetMetadata.getColumnCount(); ++i)
        {               
            ij = i - 1;
            colName = resultsetMetadata.getColumnName(i);
            colType = resultsetMetadata.getColumnTypeName(i).toUpperCase();
            key = IBMExtractUtilities.removeQuote(t.srcSchName[id]) + "." + IBMExtractUtilities.removeQuote(t.srcTableName[id]) + "." + colName;
            if (data.Mysql() || data.Oracle())
            {
            	synchronized (dataTypeMap)
				{
                    map = (dataTypeMap.containsKey(key)) ? (DataMap) dataTypeMap.get(key) : null;					
				}
                if (map == null)
                	map = new DataMap(colType);
            } else
            {
            	map = new DataMap(colType);
            }
            
            try
            {
               precision = resultsetMetadata.getPrecision(i);
            }
            catch (Exception e)
            {
            	precision = 2147483647;
            	log("Error: Precision found more than 2GB limiting it to 2GB");                	
            }
            synchronized (dataTypeMap)
			{
                dataTypeMap.put(key, getDataMap(map, colType, id));				
			}
			bval.isCLOB[ij] = false;
			bval.isBLOB[ij] = false;
			bval.isXML[ij] = false;
            if (data.Oracle() || data.DB2())
            {
                bval.isBLOB[ij] = (colType.equalsIgnoreCase("BLOB") || colType.equalsIgnoreCase("LONG RAW"));
                bval.isXML[ij] = (colType.equalsIgnoreCase("SYS.XMLTYPE") || colType.equalsIgnoreCase("XMLTYPE") || colType.equalsIgnoreCase("XML"));
            	if (netezza)
            	{
            		bval.isCLOB[ij] = (colType.equalsIgnoreCase("CLOB") || colType.equalsIgnoreCase("DBCLOB") || colType.equalsIgnoreCase("LONG"));
            		
            	} else
            	{
                	bval.isCLOB[ij] = ((colType.equalsIgnoreCase("CLOB") || colType.equalsIgnoreCase("NCLOB") || colType.equalsIgnoreCase("DBCLOB") || colType.equalsIgnoreCase("LONG")) &&
                				(map.targetDataType.equalsIgnoreCase("CLOB") || map.targetDataType.equalsIgnoreCase("DBCLOB")));
            	}
            } else if (data.Postgres())
            {
                bval.isBLOB[ij] = colType.equalsIgnoreCase("BYTEA");                		                		
            	if (netezza)
            	{
                    bval.isCLOB[ij] = colType.equalsIgnoreCase("TEXT");                		
            	} else
            	{
                    bval.isCLOB[ij] = (((map.targetDataType.equalsIgnoreCase("CLOB") || map.targetDataType.equalsIgnoreCase("DBCLOB")) && 
                         colType.equalsIgnoreCase("TEXT")) || ((map.targetDataType.equalsIgnoreCase("VARCHAR")) && (colType.equalsIgnoreCase("TEXT"))));                		
            	}
            } else if (data.Sybase())
            {
                bval.isBLOB[ij] = colType.equalsIgnoreCase("IMAGE");                		
            	if (netezza)
            	{
                    bval.isCLOB[ij] = (colType.equalsIgnoreCase("TEXT") || colType.equalsIgnoreCase("NTEXT") || colType.equalsIgnoreCase("UNITEXT"));                		
            	} else
            	{
                    bval.isCLOB[ij] = ((colType.equalsIgnoreCase("TEXT") || colType.equalsIgnoreCase("NTEXT") || colType.equalsIgnoreCase("UNITEXT")) &&
                    		(map.targetDataType.equalsIgnoreCase("CLOB") || map.targetDataType.equalsIgnoreCase("DBCLOB")) || 
                    		((map.targetDataType.equalsIgnoreCase("VARCHAR") || map.targetDataType.equalsIgnoreCase("VARGRAPHIC")) 
    	                			&& (colType.equalsIgnoreCase("TEXT"))));                		
            	}
            } else  if (data.Mssql())
            {
                bval.isXML[ij] = (colType.equalsIgnoreCase("XML")) ? true : false;
                if (colType.equalsIgnoreCase("IMAGE") || colType.equalsIgnoreCase("VARBINARY"))
            	{
            		if (colType.equalsIgnoreCase("VARBINARY"))
            		{
            			if (precision > 32000)
            			{
            				bval.isBLOB[ij] = true;
            			}
            		} else
            			bval.isBLOB[ij] = true;
            	} else if (colType.equalsIgnoreCase("VARCHAR"))
            	{
        			if (precision > 32000)
        			{
        				bval.isCLOB[ij] = true;
        			}
        			else
            			bval.isCLOB[ij] = false; // Original was set to true to make sure that the data is placed correctly in clob file.
            	}
                if (netezza)
                {
                	bval.isCLOB[ij] = colType.equalsIgnoreCase("TEXT") || colType.equalsIgnoreCase("NTEXT");
                } else
                {	                	
                	bval.isCLOB[ij] = ((colType.equalsIgnoreCase("TEXT") || colType.equalsIgnoreCase("NTEXT")) ||
                			(map.targetDataType.equalsIgnoreCase("CLOB") || map.targetDataType.equalsIgnoreCase("DBCLOB")));
                }
            } else if (data.Mysql())
            {
                if (map.sourceDataType.equalsIgnoreCase("BLOB") || map.sourceDataType.equalsIgnoreCase("MEDIUMBLOB") || map.sourceDataType.equalsIgnoreCase("LONGBLOB"))
                {
                   bval.isBLOB[ij] = true;
                } 
                else if (map.sourceDataType.equalsIgnoreCase("TEXT") || map.sourceDataType.equalsIgnoreCase("MEDIUMTEXT") 
                		|| map.sourceDataType.equalsIgnoreCase("LONGTEXT"))
                {
                	bval.isCLOB[ij] = (netezza) ? true : (map.targetDataType.startsWith("CLOB") || map.targetDataType.startsWith("DBCLOB")) ? true : false;
                }
            } else if (data.Hxtt())
            {
                bval.isBLOB[ij] = (colType.equalsIgnoreCase("OLE")) ? true : false;
            } else if (data.Access())
            {
                bval.isBLOB[ij] = (colType.equalsIgnoreCase("LONGBINARY")) ? true : false;
            } else if (data.Domino())
            {
               bval.isBLOB[ij] = (colType.equalsIgnoreCase("RICH TEXT") && precision > 15000) ? true : false;
            }
        }
        bval.isXml = bval.isClob = bval.isBlob = false;
        for (i = 1; i <= resultsetMetadata.getColumnCount(); ++i)
        {
            ij = i - 1;
            if (bval.isBLOB[ij])
            {
               bval.isBlob = true;
            }
            if (bval.isCLOB[ij])
            {
               bval.isClob = true;
            }
            if (bval.isXML[ij])
            {
               bval.isXml = true;
            }
        }
        for (i = 1; i <= resultsetMetadata.getColumnCount(); ++i)
        {
            colName = resultsetMetadata.getColumnName(i);
            colType = resultsetMetadata.getColumnTypeName(i).toUpperCase();
            key = IBMExtractUtilities.removeQuote(t.srcSchName[id]) + "." + IBMExtractUtilities.removeQuote(t.srcTableName[id]) + "." + colName;
            synchronized (dataTypeMap)
			{
                map = (DataMap) dataTypeMap.get(key);				
			}
            octtetLength = (columnLengthMap == null) ? -1 : (columnLengthMap.get(key) == null ? -1 : Integer.valueOf((String)columnLengthMap.get(key)));
            lobLength = (lobsLengthMap == null) ? "-1" : (lobsLengthMap.get(key) == null ? "-1" : (String)lobsLengthMap.get(key));
            
            	map = processDataMap(map, i, octtetLength, lobLength);
            
            synchronized (dataTypeMap)
			{
                dataTypeMap.put(key, map);				
			}
        }        
    }
                
    public int colCount()
    {
    	int colCount = 0;
        try
		{
			colCount = resultsetMetadata.getColumnCount();
		} catch (SQLException e)
		{
			colCount = 0;
			e.printStackTrace();
		}
        return colCount;
    }
    
    public void close()
    {
		try
		{
	        if (queryStatement != null)
			  queryStatement.close();
	        if (statement != null)
	          statement.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
    }
    
    public ResultSet getDataDumpResultSet(int id, HashMap dataTypeMap, HashMap columnLengthMap, HashMap lobsLengthMap, String sql) throws SQLException
    {
         if (data.Mysql() || data.Sybase() || data.Oracle())
        {
        	// Added Oracle here for Metavante. Seems like a prob for Ora 9 database. It cannot enum queryStatement.getMetaData() 
        	statement = data.connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
        	if (fetchSize == 0)
            {
               if (data.Mysql())
               {
                  String newSQL = sql;
                  if (sql.matches("(?i).*\\s+LIMIT\\s+.*"))
                  {
                	  newSQL = sql.replaceAll("(?i)(LIMIT\\s+.*)", "LIMIT 1");
                  } else
                     newSQL = sql + " LIMIT 1";
                  rs = statement.executeQuery(newSQL);

                  resultsetMetadata = rs.getMetaData();
                  generateDataMapAndLOBS(id, dataTypeMap, columnLengthMap, lobsLengthMap);
                  if (dataUnload)
                  {
                      rs.close();
                      statement.close();
                      statement = data.connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                          java.sql.ResultSet.CONCUR_READ_ONLY);
                      statement.setFetchSize(Integer.MIN_VALUE);
                      rs = statement.executeQuery(sql);
                  }
               } else
               {
                   rs = statement.executeQuery(sql);
                   resultsetMetadata = rs.getMetaData();
                   generateDataMapAndLOBS(id, dataTypeMap, columnLengthMap, lobsLengthMap);
               }
           }
           else
           {
         	  statement.setFetchSize(fetchSize);
              rs = statement.executeQuery(sql);
              resultsetMetadata = rs.getMetaData();
              generateDataMapAndLOBS(id, dataTypeMap, columnLengthMap, lobsLengthMap);
           }
        }
        else if (data.Teradata())
        {
        	statement = data.connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            String newSQL = sql;
            if (!sql.matches("(?i)\\s+SAMPLE\\s+"))
               newSQL = sql + " SAMPLE 1";
            rs = statement.executeQuery(newSQL);

            resultsetMetadata = rs.getMetaData();
            generateDataMapAndLOBS(id, dataTypeMap, columnLengthMap, lobsLengthMap);
            if (dataUnload)
            {
                rs.close();
                statement.close();
                statement = data.connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(fetchSize);
                rs = statement.executeQuery(sql);
            }
        }
        else
        {
            queryStatement = data.connection.prepareStatement(sql);
            queryStatement.setFetchSize(fetchSize);
            if (dataUnload || usePipe)
	            rs = queryStatement.executeQuery();          
            resultsetMetadata = queryStatement.getMetaData();
            generateDataMapAndLOBS(id, dataTypeMap, columnLengthMap, lobsLengthMap);
        }        
        return rs;
    }
    
    private String lobColName(boolean nullable, int length, int id, int colIndex, String fileName)
    {
        String lobColData = "";
        if (lobsToFiles)
        {
            //lobColData = "\"" + fileName + "\"";
        	if (length == 0)
        	  lobColData = "";
        	else
              lobColData = fileName;
        } else
        {
           if (length == 0 && nullable) return lobColData;
           if (bval.isXML[colIndex-1])
           {
              if (t.multiTables[id] == 0)
              {
                 lobColData = "\"<XDS FIL='X"+bval.xmlFileCounter+".lob' OFF='"+bval.xmlOffset+"' LEN='"+length+"'/>\"";
              } else
              {
                 lobColData = "\"<XDS FIL='X"+bval.xmlFileCounter+".lob"+t.multiTables[id]+"' OFF='"+bval.xmlOffset+"' LEN='"+length+"'/>\"";                     
              }
              bval.xmlOffset += length;              
           }
           if (bval.isBLOB[colIndex-1])
           {
              if (t.multiTables[id] == 0)
              {
                 lobColData = "B"+ bval.blobFileCounter + ".lob." + bval.blobOffset + "." + length + "/";
              } else
              {
                 lobColData = "B"+ bval.blobFileCounter + ".lob"+t.multiTables[id]+"." + bval.blobOffset + "." + length + "/";                     
              }
              bval.blobOffset += length;
           }
           if (bval.isCLOB[colIndex-1])
           {
              if (t.multiTables[id] == 0)
              {
                 lobColData = "C"+ bval.clobFileCounter + ".lob." + bval.clobOffset + "." + length + "/";
              } else
              {
                 lobColData = "C"+ bval.clobFileCounter + ".lob"+t.multiTables[id]+"." + bval.clobOffset + "." + length + "/";                     
              }
              bval.clobOffset += length;            		  
           }               
        }
        return lobColData;
    }
    
    private DataMap getDMap(HashMap dataTypeMap, int id, int colIndex)
    {
    	String colName, key;
		try
		{
			colName = resultsetMetadata.getColumnName(colIndex);
			key = IBMExtractUtilities.removeQuote(t.srcSchName[id]) + "." + IBMExtractUtilities.removeQuote(t.srcTableName[id]) + "." + colName;
			return (dataTypeMap.containsKey(key)) ? (DataMap) dataTypeMap.get(key) : null;
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
        return null;
    }
    
    private int countSpecialBytes(byte[] buffer, boolean quoted)
    {
    	int count = 0;
    	if (buffer == null) return 0;
    	for (int i = 0; i < buffer.length; ++i)
    	{
        	if (quoted)
        	{
        		if (buffer[i] == colsep || buffer[i] == Constants.dbquote ||
                		buffer[i] == Constants.NUL || buffer[i] == Constants.CR	|| 
                		buffer[i] == Constants.LF || buffer[i] == Constants.escape)
                		count++;
        	} else
        	{
        		if (buffer[i] == colsep || 
                		buffer[i] == Constants.NUL || buffer[i] == Constants.CR	|| 
                		buffer[i] == Constants.LF || buffer[i] == Constants.escape)
                		count++;
        	}
    	}
    	return count;
    }
    
    private byte[] escapeSpecialBytes(byte[] buffer, boolean quoted)
    {
    	if (buffer == null) return buffer;
    	int newLength, startPos;
    	if (quoted)
    	{
    		// 2 for starting and ending double quote and adjust for escaping each colsep
    		newLength = buffer.length + countSpecialBytes(buffer, quoted) + 2;      
    		startPos = 1;
    	} else
    	{
        	newLength = buffer.length + countSpecialBytes(buffer, quoted); 
        	startPos = 0;
    	}
    	byte[] newArray = new byte[newLength]; 
    	if (quoted) newArray[0] = Constants.dbquote;
    	for (int i = 0, j = startPos; i < buffer.length; ++i)
    	{
        	if (quoted)
        	{
        		if (buffer[i] == colsep || buffer[i] == Constants.dbquote ||
                    	buffer[i] == Constants.NUL || buffer[i] == Constants.CR	|| 
                    	buffer[i] == Constants.LF || buffer[i] == Constants.escape)
                		newArray[j++] = Constants.escape;
        	} else
        	{
        		if (buffer[i] == colsep || 
                    	buffer[i] == Constants.NUL || buffer[i] == Constants.CR	|| 
                    	buffer[i] == Constants.LF || buffer[i] == Constants.escape)
                		newArray[j++] = Constants.escape;
        	}
    		newArray[j++] = buffer[i];
    	}
    	if (quoted) newArray[newLength-1] = Constants.dbquote;
    	return newArray;
    }
    
    private BinData processLOB(InputStream input, int nzlen) throws IOException
    {
    	byte[] buffer = new byte[nzlen];
        int bytesRead = 0;
        bytesRead = input.read(buffer);
        return (bytesRead == -1) ? new BinData(0, null) : new BinData (bytesRead, escapeSpecialBytes(buffer, false));
    }
        
    private BinData processLOB(Reader input, int nzlen) throws IOException
    {
    	byte[] lob = null;
    	String buf = "";
    	char[] buffer = new char[nzlen];
        int charRead = 0, len = 0;
        charRead = input.read(buffer);
        if (charRead != -1)
        {
           buf = "";
           if (charRead == nzlen) 
           {
          	  buf = new String(buffer);                                    	 
           } else {
          	  buf = new String(buffer, 0, charRead);                                    	 
           }
           lob = buf.getBytes(encoding);
           len = lob.length;
        }
        return new BinData(len, escapeSpecialBytes(lob, false));
    }

    private BinData processLOB(String buffer, int nzlen) throws IOException
    {
    	String buf;
    	byte[] lob = null;
    	int charRead = 0, len = 0;
    	if (buffer != null)
    	{
    		charRead = buffer.length();
    		if (charRead > nzlen)
    		{
            	buffer = buffer.substring(0,nzlen);
    		}
    		lob = buffer.getBytes(encoding);
            len = lob.length;
    	}
    	return new BinData(len, escapeSpecialBytes(lob, false));
    }

    
    private BinData processLOB(byte[] buffer, int nzlen) throws IOException
    {
    	String buf;
    	byte[] lob = null;
    	int byteRead = 0, len = 0;
    	if (buffer != null)
    	{
    		byteRead = buffer.length;
    		if (byteRead > nzlen)
    		{
    			lob = new byte[nzlen];
    			System.arraycopy(buffer, 0, lob, 0, nzlen);
    		} else
    		{
    			len = buffer.length;
    			lob = new byte[nzlen];
    			System.arraycopy(buffer, 0, lob, 0, len);
    		}
            len = lob.length;
    	}
    	return new BinData(len, escapeSpecialBytes(lob, false));
    }

    public BinData extractLOBS(boolean nullable, long numRow, String colType, int id, int colIndex, HashMap dataTypeMap)
    {
    	String str = "";
        Blob blob = null;
        int lobLength = 0, nzlen = -1;
        byte[] lob = null;
        String lobFileName = "";
        BinData nzlob = new BinData(0, "".getBytes());
        
        try
        {
        	if (netezza)
        	{
                DataMap map;
                synchronized (dataTypeMap)
				{
                    map = getDMap(dataTypeMap, id, colIndex);					
				}
        		nzlen = map.precision > Constants.netezzaMaxlength ? Constants.netezzaMaxlength : map.precision;
        	} else
               lobFileName = getLobFileName(id,colIndex,null);                
            if (data.Oracle() || data.zDB2() || data.DB2())
            {
            	Object obj = null;
            	try
            	{
                    obj = rs.getObject(colIndex);
            	} catch (Exception e)
            	{
            		e.printStackTrace();
            		if (data.Oracle())
            		{
            		     log("Try using latest JDBC driver.");
            		}            		   
            	}
                if (obj != null)
                {
                    if (colType.equalsIgnoreCase("BLOB"))
                    {
                        blob = (Blob) obj;
                        InputStream input = blob.getBinaryStream();
                        if (netezza)
                        {
                        	nzlob = processLOB(input, nzlen);
                        } else
                        {
                            byte[] buffer = new byte[Constants.readLobBuffer];
                            int bytesRead = 0;
                            while ((bytesRead = input.read(buffer)) != -1)
                            {
                                lobLength += bytesRead;
                                blobWriter.write(buffer, 0, bytesRead);
                            }                        	
                        }
                    }
                    else if (colType.equalsIgnoreCase("CLOB") || colType.equalsIgnoreCase("DBCLOB") || colType.equalsIgnoreCase("XML"))
                    {
			            String buf = null;
                        Reader input = rs.getCharacterStream(colIndex);
                        if (netezza)
                        {
                        	nzlob = processLOB(input, nzlen);
                        } else
                        {
                            char[] buffer = new char[Constants.readLobBuffer];
                            int charRead = 0;
                            while ((charRead = input.read(buffer)) != -1)
                            {
    				           buf = "";
                               if (charRead == Constants.readLobBuffer) 
                               {
                              	  buf = new String(buffer);                                    	 
                               } else {
                              	  buf = new String(buffer, 0, charRead);                                    	 
                               }
                               if ((colType.equalsIgnoreCase("CLOB") && graphic) || colType.equalsIgnoreCase("DBCLOB"))
                               {
                            	  lob = IBMExtractUtilities.TrimBOM(buf.getBytes("UTF-16"));  
                               } else
                                  lob = buf.getBytes(encoding);
                               lobLength += lob.length;
                               if (colType.equalsIgnoreCase("XML"))
                                   xmlWriter.write(lob);
                                else
                                   clobWriter.write(lob);
    		                }                        	
                        }
                    }
                    else if (colType.equalsIgnoreCase("LONG"))
                    {
                    	String buf = obj.toString();
                    	if (netezza)
                    	{
                    		nzlob = processLOB(buf, nzlen);
                    	} else
                    	{
                            lob = buf.getBytes(encoding);
                            lobLength += lob.length;                        	
                            clobWriter.write(lob);                    		
                    	}
                    }
                    else if (colType.equalsIgnoreCase("LONG RAW"))
                    {
                    	lob = (byte[]) obj;
                    	if (netezza)
                    	{
                    		nzlob = processLOB(lob, nzlen);                    		
                    	} else
                    	{
                            lobLength += lob.length;                        	
                            blobWriter.write(lob);                    		
                    	}
                    }
                    else if (colType.equalsIgnoreCase("SYS.XMLTYPE") || colType.equalsIgnoreCase("XMLTYPE"))
                    {
                    	bval.isXml = true;            				
			            String buf = null;
		                try 
		                {
		                    oracle.xdb.XMLType xmlObj = (oracle.xdb.XMLType)obj;
		                    buf = xmlObj.getStringVal();
		                    if (netezza)
		                    {
		                    	nzlob = processLOB(buf, nzlen);
		                    } else
		                    {
	                            lob = buf.getBytes(encoding);
	                            lobLength += lob.length;
	                            xmlWriter.write(lob);		                    	
		                    }
		                } catch(Exception e)
		                { 
		                	e.printStackTrace(); 
		                } catch(NoClassDefFoundError nc)
		                {
		                	buf = "<ErrorNotice>Serious Error: Table " + t.srcTableName[id] + " has XML data and this could not be unloaded since " +
		                			" you forgot to include xdb.jar and xmlparserv2.jar in the classpath.</ErrorNotice>";
		                	if (netezza)
		                	{
		                    	nzlob = processLOB(buf, nzlen);		                		
		                	} else
		                	{
	                            lob = buf.getBytes(encoding);
	                            lobLength += lob.length;
	                            xmlWriter.write(lob);		                		
		                	}
		                	log(buf);
		                }
                    }
                }
            } else if (data.Sybase() || data.Mssql())
            {
                Object obj = rs.getObject(colIndex);
                if (obj != null)
                {
                    if (colType.equalsIgnoreCase("IMAGE") || colType.equalsIgnoreCase("VARBINARY"))
                    {
                        InputStream input = rs.getBinaryStream(colIndex);
                        if (netezza)
                        {
                        	nzlob = processLOB(input, nzlen);
                        } else
                        {
                            byte[] buffer = new byte[Constants.readLobBuffer];
                            int bytesRead = 0;
                            while ((bytesRead = input.read(buffer)) != -1)
                            {
                                lobLength += bytesRead;
                                blobWriter.write(buffer, 0, bytesRead);
                            }                        	
                        }
                    } 
                    else
                    {
                       if (colType.equalsIgnoreCase("XML"))
                    	   bval.isXml = true;
                       Reader input = rs.getCharacterStream(colIndex);
                       if (netezza)
                       {
                       	   nzlob = processLOB(input, nzlen);
                       } else
                       {
                           char[] buffer = new char[Constants.readLobBuffer];
                           int charRead = 0;
                           while ((charRead = input.read(buffer)) != -1)
                           {
                              String buf = "";
                              if (charRead == Constants.readLobBuffer) 
                              {
                             	 buf = new String(buffer);                                    	 
                              } else {
                             	 buf = new String(buffer, 0, charRead);                                    	 
                              }
                              if ((colType.equalsIgnoreCase("NTEXT") && graphic))
                              {
                           	     lob = IBMExtractUtilities.TrimBOM(buf.getBytes("UTF-16"));  
                              } else
                                 lob = buf.getBytes(encoding);
                              lobLength += lob.length;
                              if (colType.equalsIgnoreCase("XML"))
                                 xmlWriter.write(lob);
                              else
                                 clobWriter.write(lob);
                           }                    	   
                       }
                       
                    }
                }
            }
            else if (data.Postgres() || data.Mysql() 
                       || data.Hxtt() || data.Access())
            {
            	if (colType.endsWith("BLOB") || colType.endsWith("BYTEA"))
            	{
                    InputStream input = rs.getBinaryStream(colIndex);
                    if (netezza)
                    {
                    	nzlob = processLOB(input, nzlen);
                    } else
                    {
                        byte[] buffer = new byte[Constants.readLobBuffer];
                        int bytesRead = 0;
                        while ((bytesRead = input.read(buffer)) != -1)
                        {
                            lobLength += bytesRead;
                            blobWriter.write(buffer, 0, bytesRead);
                        }                                                	
                    }

            	} else
            	{
                    Reader input = rs.getCharacterStream(colIndex);
                    if (netezza)
                    {
                    	nzlob = processLOB(input, nzlen);
                    } else
                    {
                        char[] buffer = new char[Constants.readLobBuffer];
                        int charRead = 0;
                        while ((charRead = input.read(buffer)) != -1)
                        {
                           String buf = "";
                           if (charRead == Constants.readLobBuffer) 
                           {
                          	  buf = new String(buffer);                                    	 
                           } else {
                          	 buf = new String(buffer, 0, charRead);                                    	 
                           }
                           lob = buf.getBytes(encoding);
                           lobLength += lob.length;
                           clobWriter.write(lob);
                        }                 		                    	
                    }
                    
            	}
            }
            else if (data.Domino())
            {
               String buffer;
               try
               {
                  buffer = rs.getString(colIndex);
               } catch (SQLException qex)
               {
                  if (qex.getErrorCode() == 23316)
                  {
                     buffer =  rs.getString(colIndex);   
                  } else
                  {
                     buffer = "(null)";
                  }
               }                   
               if (buffer != null)
               {
            	   if (netezza)
            	   {
            		   nzlob = processLOB(buffer, nzlen); 
            	   } else
            	   {
                       lobLength = buffer.length();
                       blobWriter.write(buffer.getBytes(), 0, lobLength);  
            	   }
               }
            }
            flushLOBS();
            if (!netezza)
            {
               str = lobColName(nullable,lobLength,id,colIndex,lobFileName);
            }
        } catch (Exception ex)
        {
            log(t.srcTableName[id]+" Row[" + numRow +"] Col[" + colIndex + "] Error:" + ex.getMessage());
            str = "SkipThisRow"; 
            lobLength = str.length();
            if (netezza) 
               nzlob = new BinData(lobLength, str.getBytes());
        }
        try
		{
        	return (netezza) ? nzlob : new BinData(lobLength, str.getBytes(encoding));
		} catch (UnsupportedEncodingException e)
		{
            log(t.srcTableName[id]+" Row[" + numRow +"] Col[" + colIndex + "] Encoding Error:" + e.getMessage());
		}
		return null;
    }
}
