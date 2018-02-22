/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

public class RunDeployObjects implements Runnable
{
	private javax.swing.JEditorPane topArea;
	private IBMExtractConfig cfg;
	private boolean runFullTree = true;
	private int curIndex = 0, failedCount = 0, successCount = 0, discardCount = 0, existsCount = 0;
	private JTree tree;
	private Object[][] tabData;
	private DBData data = null, ants = null;
	private boolean runOnce = true;
	private Properties deployedObjectsList;
	private String outputDirectory, DEPLOYED_OBJECT_FILE = "DeployedObjects.properties";
	public String sqlCode = "", sqlMessage = "", failedLineNumber = "";
	public String sqlTerminator;
	
	public RunDeployObjects(String sqlTerminator, String outputDirectory, JTree tree, boolean runFullTree, javax.swing.JEditorPane topArea)
	{
		this.sqlTerminator = sqlTerminator;
		this.runFullTree = runFullTree;
		this.outputDirectory = outputDirectory;
		this.deployedObjectsList = new Properties();
		this.tree = tree;
		this.topArea = topArea;
		cfg = new IBMExtractConfig();
    	cfg.loadConfigFile();
    	cfg.getParamValues();
    	
        String userid = cfg.getDstUid();
        String pwd = cfg.getDstPwd();        
        pwd = IBMExtractUtilities.Decrypt(pwd);
    	data = new DBData(cfg.getDstVendor(), cfg.getDstServer(), cfg.getDstPort(), cfg.getDstDBName(), userid, pwd, "", 0);
    	data.setAutoCommit(true);
    	data.getConnection();        	
    	
        if (cfg.getSrcVendor().equalsIgnoreCase("sybase") && cfg.getDB2Compatibility())
        {
        	ants = new DBData(Constants.DB2SKIN, cfg.getDstServer(), cfg.getDstPort(), cfg.getDstDBName(),
        			userid, pwd, "", 0);
        	ants.setAutoCommit(true);
        	ants.getConnection();
        }
	}
	
	private void deployTempTables(String skin)
	{
		if (runOnce)
		{
			RunSQLScripts rs = new RunSQLScripts(ants, outputDirectory, Constants.tempTables, skin);
			rs.deployTempTables();
			runOnce = false;
		}
	}
	
	public Object[][] getTabData()
	{
		return tabData;
	}

	public void setTabData(Object[][] tabData)
	{
		this.tabData = tabData;    	
	}

	/**
	 * Whatever changes are made to this proc, same changes are to be made in IBMExtractUtilities.DeployObject
	 * @param type
	 * @param schema
	 * @param objectName
	 * @param skin
	 * @param sql
	 */
	private void deployObject(String type, String schema, String objectName, String skin, String sql)
	{		
		String sqlerrmc = "";
		String value = "", key = type + ":" + schema + ":" + objectName;
    	int lineNumber = -1;
		sqlMessage = sqlCode = failedLineNumber = ""; 
		Statement statement = null;
		try
		{
			if (skin != null && skin.length() > 0)
			{
			    statement = ants.connection.createStatement();
		    	try
		    	{
				   statement.execute("use " + skin);
		    	} catch (Exception e)
		    	{
		    		statement.execute("use master");
		    		statement.execute("create database " + skin);
					statement.execute("use " + skin);
		    	}
			} else
			{
			    if (Constants.netezza())
			    {
			    	if (schema.equalsIgnoreCase(data.getDBName()))
			    	{
			    		statement = data.connection.createStatement();	
			    	} else
			    	{
			    		statement = data.changeDatabase(schema).createStatement();
			    		data.setDBName(schema);
			    	}
			    } else
			    {
				    statement = data.connection.createStatement();	
					statement.execute("SET CURRENT SCHEMA = '" + schema + "'");
					statement.execute("SET PATH = SYSTEM PATH,'" + schema +"'");			    	
			    }
			}
			statement.execute(sql);
			value = "1";
			data.commit();
			if (statement != null)
				statement.close();
		} catch (SQLException e)
		{
			if (Constants.netezza())
			{
				sqlMessage = e.getMessage();
				if (sqlMessage.matches("(?sim).*already\\s+exists.*"))
				{
				   sqlCode = "0";
				   sqlMessage = "";
				   value = "1";
				} 
				else
				{
				   sqlCode = "" + e.getErrorCode();					   
				   failedLineNumber = "-1";
				   value = "0";
				}				
			} else
			{
				if (e instanceof com.ibm.db2.jcc.DB2Diagnosable)
				{
					com.ibm.db2.jcc.DB2Sqlca sqlca = ((com.ibm.db2.jcc.DB2Diagnosable)e).getSqlca();
					if (sqlca != null)
					{
						lineNumber = sqlca.getSqlErrd()[2];
						sqlerrmc = sqlca.getSqlErrmc();					
					}
				}
				if (IBMExtractUtilities.CheckExistsSQLErrorCode(e.getErrorCode()))
				{
					sqlCode = "0";
					value = "1";
				} else
				{
				    sqlCode = "" + e.getErrorCode();
				    value = "0";
				    failedLineNumber = "" + lineNumber;
					if (skin != null && skin.length() > 0)
					{
						sqlMessage = e.getMessage();
					} else
					{
					    sqlMessage = IBMExtractUtilities.getSQLMessage(data, sqlCode, sqlerrmc);					
					}
				}
			}
		}
		if (sqlCode.equals("") || sqlCode.equals("0"))
			deployedObjectsList.setProperty(key, "1");
		else
			deployedObjectsList.setProperty(key, "0");
	}
	
	private Object[][] walk(Object[][] tabData, TreeModel model, Object o)
	{
		int cc;
		cc = model.getChildCount(o);
		for (int i = 0; i < cc; i++)
		{
			DefaultMutableTreeNode child = (DefaultMutableTreeNode)model.getChild(o, i);			
			if (child == null) return tabData;	    	
			//tree.
			Object nodeInfo = child.getUserObject();
			if (model.isLeaf(child))
			{
				PLSQLInfo plsql = (PLSQLInfo) nodeInfo;	
				if (plsql.codeStatus.equals("3"))
				{
					discardCount++;
					tabData[curIndex][0] = plsql.type;
					tabData[curIndex][1] = plsql.schema;
					tabData[curIndex][2] = plsql.object;
					tabData[curIndex][3] = plsql.codeStatus;
					tabData[curIndex][4] = "";
					tabData[curIndex][5] = "";
					tabData[curIndex][6] = "Object was not chosen to be deployed";					
				} 
				else
				{
					deployTempTables(plsql.skin);
					deployObject(plsql.type, plsql.schema, plsql.object, plsql.skin, plsql.plSQLCode);
					tabData[curIndex][0] = plsql.type;
					tabData[curIndex][1] = plsql.schema;
					tabData[curIndex][2] = plsql.object;
					if (sqlCode.equals(""))
					{
					   tabData[curIndex][3] = "1";
					   plsql.codeStatus = "1";
					   tabData[curIndex][4] = sqlCode;
					   tabData[curIndex][5] = "";
					   tabData[curIndex][6] = "Deployed";
					   successCount++;
					   IBMExtractUtilities.log(plsql.type+"->"+plsql.schema+"."+plsql.object+" deployed successfully.");
					}
					else if (sqlCode.equals("0"))
					{
					   tabData[curIndex][3] = "2";
					   plsql.codeStatus = "2";
					   tabData[curIndex][4] = sqlCode;
					   tabData[curIndex][5] = "";
					   tabData[curIndex][6] = "Already exists";
					   existsCount++;
					   IBMExtractUtilities.log(plsql.type+"->"+plsql.schema+"."+plsql.object+" already exists.");
					}
					else
					{
					   tabData[curIndex][3] = "0";
					   plsql.codeStatus = "0";
					   plsql.lineNumber = failedLineNumber;
					   tabData[curIndex][4] = sqlCode;
					   tabData[curIndex][5] = failedLineNumber;
					   tabData[curIndex][6] = sqlMessage;
					   failedCount++;
					   IBMExtractUtilities.log(plsql.type+"->"+plsql.schema+"."+plsql.object+" deployment failed ");
					}
				}
				child.setUserObject(plsql);				
				((DefaultTreeModel )tree.getModel()).nodeStructureChanged((TreeNode)child);
				tree.scrollPathToVisible(new TreePath(child.getPath()));
				++curIndex;
			}
			else
			{
				tabData = walk(tabData, model, child);
			}
		}
		return tabData;
	}
	
	private int leafWalk(int count, TreeModel model, Object o)
	{
		int cc;
		cc = model.getChildCount(o);
		for (int i = 0; i < cc; i++)
		{
			Object child = model.getChild(o, i);
			if (model.isLeaf(child))
			{
				count++;
			}
			else
			{
				count = leafWalk(count, model, child);
			}
		}
		return count;
	} 

	private int getTotalObjects(JTree tree)
	{
		int count = 0;
		TreeModel model = tree.getModel();
		if (model != null)
		{
			Object root = model.getRoot();
			count = leafWalk(count, model, root);
		}
		return count;
	}

	public void run()
	{
		if (runFullTree)
		   runFull();
		else
		   runSelected();
	}
	
	public void runSelected()
	{
		if (data.connection == null)
		{
			tabData = new Object[1][7];
			tabData[curIndex][0] = "Connection";
			tabData[curIndex][1] = "Failure";
			tabData[curIndex][2] = "";
			tabData[curIndex][3] = "2";
			tabData[curIndex][4] = "0";
			tabData[curIndex][5] = "";
			tabData[curIndex][6] = sqlMessage;
			return;
		}
		DefaultMutableTreeNode node;
		TreePath[] paths;
		int count, count2, successCount = 0, failedCount = 0, discardCount = 0, existsCount = 0;
		String md5_a, md5_b;
		
		paths = tree.getSelectionPaths();
		if (paths == null) return;
		
		count2 = 0;
		for (int i = 0; i < paths.length; ++i)
		{
			node = (DefaultMutableTreeNode) paths[i].getLastPathComponent();    	    
	    	if (node != null)
	    	{		    	
				if (node.isLeaf())
				{
					++count2;
				}
	    	}
		}
		tabData = new Object[count2+1][7];
		count = 0;
		for (int i = 0; i < paths.length; ++i)
		{
			node = (DefaultMutableTreeNode) paths[i].getLastPathComponent();    	    
	    	if (node == null) return;
	    	
	    	Object nodeInfo = node.getUserObject();
			if (node.isLeaf())
			{
				PLSQLInfo plsql = (PLSQLInfo) nodeInfo;				
				if (plsql.codeStatus.equals("3"))
				{
					discardCount++;
					tabData[count][0] = plsql.type;
					tabData[count][1] = plsql.schema;
					tabData[count][2] = plsql.object;
					tabData[count][3] = plsql.codeStatus;
					tabData[count][4] = "";
					tabData[count][5] = plsql.lineNumber;
					tabData[count][6] = "Object was not chosen to be deployed";					
				} 
				else
				{
					String fileSavedStatus = "";
					if (count2 == 1)
					{
						md5_a = IBMExtractUtilities.MD5(plsql.plSQLCode);
						md5_b = IBMExtractUtilities.MD5(topArea.getText());
						if (!md5_a.equals(md5_b))
						{
							plsql.oldPLSQLCode = plsql.plSQLCode; 
							plsql.plSQLCode = topArea.getText();
							fileSavedStatus = IBMExtractUtilities.SaveObject(sqlTerminator, outputDirectory, plsql.type, plsql.schema, plsql.object, topArea.getText());
						}
					}
			        if (cfg.getSrcVendor().equalsIgnoreCase("sybase") && cfg.getDB2Compatibility() && (plsql.skin != null && plsql.skin.length() > 0))
			        {
			        	deployTempTables(plsql.skin);
						IBMExtractUtilities.DeployObject(ants, outputDirectory, plsql.type, plsql.schema, plsql.object, plsql.skin, plsql.plSQLCode);			        	
			        } else
			        {
						IBMExtractUtilities.DeployObject(data, outputDirectory, plsql.type, plsql.schema, plsql.object, plsql.skin, plsql.plSQLCode);			        	
			        }

					tabData[count][0] = plsql.type;
					tabData[count][1] = plsql.schema;
					tabData[count][2] = plsql.object;
					if (IBMExtractUtilities.SQLCode.equals(""))
					{
						tabData[count][3] = "1";
						plsql.codeStatus = "1";
						tabData[count][4] = "Deployed";
						tabData[count][5] = "";
						tabData[count][6] = fileSavedStatus;
						successCount++;
						IBMExtractUtilities.log(plsql.type+"->"+plsql.schema+"."+plsql.object+" deployed successfully.");
					}
					else if (IBMExtractUtilities.SQLCode.equals("0"))
					{
						tabData[count][3] = "2";
						plsql.codeStatus = "2";
						tabData[count][4] = IBMExtractUtilities.SQLCode;
						tabData[count][5] = "";
						tabData[count][6] = "Object already exists in database";
						existsCount++;
						IBMExtractUtilities.log(plsql.type+"->"+plsql.schema+"."+plsql.object+" already exists.");
					}
					else
					{
						tabData[count][3] = "0";
						plsql.codeStatus = "0";
						plsql.lineNumber = IBMExtractUtilities.FailedLine;
						tabData[count][4] = IBMExtractUtilities.SQLCode;
						tabData[count][5] = IBMExtractUtilities.FailedLine;
						tabData[count][6] = IBMExtractUtilities.Message;
						failedCount++;
						IBMExtractUtilities.log(plsql.type+"->"+plsql.schema+"."+plsql.object+" deployment failed ");
					}
				}
				node.setUserObject(plsql);				
				((DefaultTreeModel )tree.getModel()).nodeStructureChanged((TreeNode)node);
				++count;
			}
		}
		if (count > 0)
		{
		  tabData[count][0] = "Deployed=" + successCount;
		  tabData[count][1] = "Failed=" + failedCount;
		  tabData[count][2] = "Discard=" + discardCount;
		  tabData[count][3] = "1";
		  tabData[count][4] = "";
		  tabData[count][5] = "";		  
		  tabData[count][6] = "Total objects = " + count;
		  IBMExtractUtilities.log("Deployed=" + successCount + " Exists already=" + existsCount + " Failed=" + failedCount + " Discard=" + discardCount + " Total objects = " + count); 
		  //refreshTable(tabData);
		}			
		data.close();
		IBMExtractUtilities.DeployCompleted = true; 
	}
	
	public void runFull()
	{
		if (data.connection == null)
		{
			tabData = new Object[1][7];
			tabData[curIndex][0] = "Connection";
			tabData[curIndex][1] = "Failure";
			tabData[curIndex][2] = "";
			tabData[curIndex][3] = "2";
			tabData[curIndex][4] = "0";
			tabData[curIndex][5] = "";
			tabData[curIndex][6] = sqlMessage;
			return;
		}
		int count = getTotalObjects(tree);
    	tabData = new Object[count+1][7];    
    	TreeModel model = tree.getModel();
		if (model != null)
		{
			curIndex = 0;
			Object root = model.getRoot();
			tabData = walk(tabData, model, root);
			tabData[count][0] = "Deployed=" + successCount;
			tabData[count][1] = "Failed=" + failedCount;
			tabData[count][2] = "Discard=" + discardCount;
			tabData[count][3] = "1";
			tabData[count][4] = "";
			tabData[count][5] = "";
			tabData[count][6] = "Total objects = " + count;
		    IBMExtractUtilities.log("Deployed=" + successCount + " Exists already=" + existsCount + " Failed=" + failedCount + " Discard=" + discardCount + " Total objects = " + count); 
		}
		data.close();
		IBMExtractUtilities.DeployCompleted = true; 
		
		try
		{
			if (!IBMExtractUtilities.FileExists(outputDirectory + "/savedobjects"))
			{
				new File(outputDirectory + "/savedobjects").mkdir();				
			}
			FileOutputStream ostream = new FileOutputStream(outputDirectory + "/savedobjects/"+DEPLOYED_OBJECT_FILE);
			deployedObjectsList.store(ostream, "-- Deployed Objects in DB2");
			ostream.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}		
	}
}
