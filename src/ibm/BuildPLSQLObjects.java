/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class BuildPLSQLObjects
{
	private String outputDirectory = ".";
	private String srcVendor;
	private Properties deployedObjectsList;
	private boolean db2Compatibility;
	private String DEPLOYED_OBJECT_FILE = Constants.DEPLOYED_OBJECT_FILE;
	private String skin;

	/**
	 * @param outputDirectory
	 */
	public BuildPLSQLObjects(boolean db2Compatibility, String srcVendor, String outputDirectory)
	{
		this.db2Compatibility = db2Compatibility;
		this.deployedObjectsList = new Properties();
		this.srcVendor = srcVendor;
		this.outputDirectory = outputDirectory;
		
		String file = outputDirectory + "/savedobjects/"+DEPLOYED_OBJECT_FILE;
		if (IBMExtractUtilities.FileExists(file))
		{
	        try
			{
	        	FileInputStream inputStream = new FileInputStream(file);
	        	deployedObjectsList.load(inputStream);
	        	inputStream.close();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	public String getOutputDirectory()
	{
		return outputDirectory;
	}

	public void setOutputDirectory(String outputDirectory)
	{
		this.outputDirectory = outputDirectory;
	}

	private File[] listSavedObjects(String outputDirectory)
	{
		File[] fp = null;
		if (IBMExtractUtilities.FileExists(outputDirectory + "/savedobjects"))
		{
			File dir = new File(outputDirectory + "/savedobjects");
			FileFilter fileFilter = new FileFilter() {
		        public boolean accept(File file) {
		            return file.getName().endsWith(".sql");
		        }
		    };
		    fp = dir.listFiles(fileFilter);
		}
		return fp;
	}
	
	private File[] listFiles(String outputDirectory)
	{
		File[] savedObjects = listSavedObjects(outputDirectory);
		String[] files, compatibilityFiles;
		String fileList, compatibilityFileList;
		int sizeSavedObjects = (savedObjects == null) ? 0 : savedObjects.length;
		int sizecompatibilityFiles = 0;
		
		File[] fp;
		fileList = IBMExtractUtilities.getDeployFiles(srcVendor);
		if (db2Compatibility)
		{
		   compatibilityFileList = IBMExtractUtilities.getCompatibilityFiles(srcVendor);
		   compatibilityFiles = compatibilityFileList.split(",");
		   sizecompatibilityFiles = compatibilityFiles.length;
		}
		files = fileList.split(",");
		fp = new File[files.length+sizecompatibilityFiles+sizeSavedObjects];
		for (int i = 0; i < files.length; ++i)
		{
			fp[i] = new File(outputDirectory + "/" + files[i]);
		}
		if (db2Compatibility)
		{
			compatibilityFileList = IBMExtractUtilities.getCompatibilityFiles(srcVendor);
			compatibilityFiles = compatibilityFileList.split(",");
		    for (int j = files.length, i = 0; i < sizecompatibilityFiles; ++j, ++i)
		    {
			   fp[j] = new File(outputDirectory + "/" + compatibilityFiles[i]);
		    }
		}
		for (int j = files.length + sizecompatibilityFiles, i = 0; i < sizeSavedObjects; ++j, ++i)
		{
			fp[j] = savedObjects[i];
		}
		return fp;
	}
	
	/*private boolean isTerminatorSign(String type, String line, String terminator)
	{
		String terminatorString = (terminator == null) ? IBMExtractUtilities.sqlTerminator : terminator;
		String[] typeKeys = {"USER", "GRANT", "TYPE", "FUNCTION", "VIEW", "MQT", "TRIGGER", "PROCEDURE", "PACKAGE", "PACKAGE_BODY", "DIRECTORY"};
		
		for (int i = 0; i < typeKeys.length; ++i)
		{
			if (typeKeys[i].equals(type))
			{
				return (line.equals(terminatorString)) ? true : false;
			}
		}
		return line.equals(";") ? true : false;
	}*/
	
	private boolean isTerminatorSign(String type, String line, String terminator)
	{
		if (terminator == null)
		{
			return (line.trim().equalsIgnoreCase(";")) ? true : false;
		} else
		{
			return line.trim().equals(terminator);
		}
	}

	public Hashtable<String, PLSQLInfo> getPLSQLHash()
	{
		String terminatorToken = "--#SET TERMINATOR";
		String terminator, linesep = IBMExtractUtilities.linesep;
		Hashtable<String, PLSQLInfo> hash = 
			new Hashtable<String, PLSQLInfo>();
		String key = null;
		StringBuffer buffer = new StringBuffer();
		
		try
		{
			File[] fp = listFiles(outputDirectory);			
			for (int i = 0; i < fp.length; ++i)
			{
				try
				{
					BufferedReader in;
				    in = new BufferedReader(new FileReader(fp[i]));
					String line;
					boolean collectCode = false;	
					String[] keys = {"","",""};
					terminator = null;
					skin = "";
					while ((line = in.readLine()) != null)
					{
						if (line.startsWith("use "))
						{
							skin = line.substring(4);
							skin = skin.trim();
						}
						if (line.startsWith(terminatorToken))
						{
							terminator = line.substring(terminatorToken.length()+1);
							if (terminator != null)
								terminator = terminator.trim();
						}
						if (line.startsWith("--#SET :"))
						{
							collectCode = true;
						}
						if (collectCode)
						{
							if (line.startsWith("--#SET :"))
							{
								key = line.substring(line.indexOf(":")+1);	
								keys = key.split(":");						
								collectCode = true;
							}
						    else if (isTerminatorSign(keys[0],line, terminator))
							{
						    	String code = deployedObjectsList.getProperty(key);
						    	if (code == null)
						    		code = "0";
						    	if (hash.containsKey(key))
						    	{
							        hash.get(key).oldPLSQLCode = hash.get(key).plSQLCode;
							        hash.get(key).plSQLCode = buffer.toString();
						    	} else
						    	{
						    		hash.put(key, new PLSQLInfo(code, keys[0], keys[1], keys[2], "", buffer.toString(), skin));
						    	}
								collectCode = false;
								buffer.setLength(0);
							}
							else
							    buffer.append(line+linesep);
						}
					}
					in.close();
				} catch (FileNotFoundException e)
				{
					IBMExtractUtilities.log("File "+fp[i].getName()+" was not found. Skipping it");
					//e.printStackTrace();
				}
			}
		} 					
		catch (IOException ex)
		{
			ex.printStackTrace();
		}					
		return hash;		
	}
	
	public Hashtable<String, String> getTreeHash()
	{
		Hashtable<String, String> hash = new Hashtable<String, String>();
		String type, schema, name, val;
		String[] strArray = null;
		try
		{
			File[] fp = listFiles(outputDirectory);			
			for (int i = 0; i < fp.length; ++i)
			{
				BufferedReader in;
				try
				{
			        in = new BufferedReader(new FileReader(fp[i]));
					String line, key;					
					while ((line = in.readLine()) != null)
					{
						if (line.startsWith("--#SET :"))
						{
							strArray = line.substring(line.indexOf(":")+1).split(":");
							type = strArray[0];
							schema = strArray[1];
							name = strArray[2];
							key = schema + "." + name + ":"; 
							
							if (hash.containsKey(type))
							{
								val = (String) hash.get(type);
								int ix = val.indexOf(key);
								if (ix < 0)
								   val += key;
								hash.put(type, val);
							} else
							{
								hash.put(type, key);
							}
						}
					}
					in.close();
				} catch (FileNotFoundException e)
				{
					IBMExtractUtilities.log("File " + fp[i].getName() + " was not found for reading. Skipping it.");
					//e.printStackTrace();
				}
			}
		} 
					
		catch (IOException ex)
		{
			ex.printStackTrace();
		}					
		return hash;
	}

	public static void main(String[] args)
	{
		Hashtable<String, String> hashTree;
		Hashtable<String, PLSQLInfo> hashSource;
		String outputDir = "C:\\Vikram\\Prospects\\DB2Cobra\\testcase";
		BuildPLSQLObjects bp = new BuildPLSQLObjects(true, "oracle", outputDir);
		hashSource = bp.getPLSQLHash();
		Set<String> set = hashSource.keySet();
		Iterator<String> itr = set.iterator();
		while(itr.hasNext())
		{
			String key = itr.next();
			System.out.println("Key = " + key);
			System.out.println("Source\n"+ ((PLSQLInfo)hashSource.get(key)).codeStatus);
		}		
	}
}