/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.Timer;
import javax.swing.border.LineBorder;
import javax.swing.filechooser.FileFilter;

import com.jgoodies.forms.builder.DefaultFormBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class StateExtractTab extends JFrame implements ActionListener
{
	private static final long serialVersionUID = -6130743742136890628L;
	private JLabel lblDB2Database = new JLabel("Target Database");
	private JLabel lblMessage = new JLabel("Ready");
	private JLabel lblSplash = IBMExtractGUI2.lblSplash;
	private JLabel lblDB2Instance = new JLabel();
	private JLabel lblDB2VarcharCompat = new JLabel();
	private JLabel lblDB2DateCompat = new JLabel();
	private JLabel lblDB2NumberCompat = new JLabel();
	private JLabel lblDB2Decflt_rounding = new JLabel();
	private JLabel lblDatabaseName = new JLabel("Database Name:");
	private JLabel lblServerName = new JLabel("Server Name:");
	private JButton btnSelectAll = new JButton("Select All");
	private JButton btnSrcJDBC = new JButton("...");
	private JButton btnDstJDBC = new JButton("...");
	private JButton btnSrcTestConn = new JButton("Connect to Source");
	private JButton btnExtract = new JButton("Extract DDL/Data");
	private JButton btnDstTestConn = new JButton("Connect to DB2");
	private JButton btnDeploy = new JButton("Deploy DDL/Data");
	private JButton btnDropObjs = new JButton("Drop Objects");
	private JButton btnView = new JButton("View Script/Output");
	private JButton btnDB2Script = new JButton("Execute DB2 Script");
	private JButton btnOutputDir = new JButton("...");
	private JButton btnCreateScript = new JButton("Generate Data Movement Scripts");
	private JButton btnMeetScript = new JButton("Generate Input file for MEET");
	private JTextField textfieldOutputDir = new JTextField(40);
	private JTextField textfieldSrcServer = new JTextField(40);
	private JTextField textfieldInstanceName = new JTextField(40);
	private JTextField textfieldDstServer = new JTextField(40);
	private JTextField textfieldSrcPortNum = new JTextField(40);
	private JTextField textfieldDstPortNum = new JTextField(40);
	private JTextField textfieldSrcDatabase = new JTextField(40);
	private JTextField textfieldDstDatabase = new JTextField(40);
	private JTextField textfieldSrcUserID = new JTextField(40);
	private JTextField textfieldDstUserID = new JTextField(40);
	private JPasswordField textfieldSrcPassword = new JPasswordField(40);
	private JPasswordField textfieldDstPassword = new JPasswordField(40);
	private JTextField textfieldSrcJDBC = new JTextField(35);
	private JTextField textfieldDstJDBC = new JTextField(35);
	private JTextField textLimitExtractRows = new JTextField(5);
	private JTextField textfieldNumTreads = new JTextField(5);
	private JTextField textfieldNumJVM = new JTextField(5);
	private JTextField textLimitLoadRows = new JTextField(5);
	private JComboBox comboSrcVendor;
	private JComboBox comboDstVendor;
	private JCheckBox checkboxDB2 = new JCheckBox("DB2 is not installed", false);
	private JCheckBox checkboxDDL = new JCheckBox("DDL", true);
	private JCheckBox checkboxData = new JCheckBox("Data", true);
	private JCheckBox checkboxObjects = new JCheckBox("Objects", true);
	private JCheckBox checkboxPipe = new JCheckBox("Use Pipe", false);
	private JCheckBox checkboxSyncLoad = new JCheckBox("Sync Unload/Load", false);

	private Timer busy = null;
	private IBMExtractConfig cfg;
	private String txtMessage;
	private String sep = Constants.win() ? ";" : ":";
	private String executingScriptName = "";
	private boolean resetDstFields = true;

	private JPanel panelCheckBox = new JPanel();	
	private Box srcServerBox = Box.createHorizontalBox();	
	private JScrollPane schemaPaneBox = new JScrollPane(panelCheckBox);
	
	private ActionListener fillTextAreaTab3ActionListener;
	private ActionListener fillTextAreaWithFileActionListener;
	private ActionListener schemaTableChangeListener;
	
	private IBMExtractGUI2 maingui;
	private String[][] optionCodes;
	private java.util.concurrent.ExecutorService s = null;
	private int numTables = -1;
	
	public StateExtractTab(IBMExtractGUI2 maingui,	
			ActionListener fillTextAreaTab3ActionListener,
			ActionListener fillTextAreaWithFileActionListener,
			ActionListener schemaTableChangeListener,
			String[][] optionCodes)
	{
		this.maingui = maingui;
		this.optionCodes = optionCodes;
		this.fillTextAreaTab3ActionListener = fillTextAreaTab3ActionListener;
		this.fillTextAreaWithFileActionListener = fillTextAreaWithFileActionListener;
		this.schemaTableChangeListener = schemaTableChangeListener;
		
		s = java.util.concurrent.Executors.newFixedThreadPool(1);				
		RunIDMTVersion task = new RunIDMTVersion();
		s.execute(task);
		s.shutdown();
	}
	
	JComponent build()
	{
		FormLayout layout = new FormLayout(
			    "right:max(50dlu;pref), 3dlu, pref, 7dlu, pref", // columns
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu," +
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 5dlu, p, 2dlu, p");   // rows
		DefaultFormBuilder builder = new DefaultFormBuilder(layout);
		builder.setDefaultDialogBorder();
		builder.setOpaque(false);
		
		String[] srcChoices = { Constants.oracle, Constants.mssql, Constants.sybase,
				Constants.access, Constants.mysql, Constants.postgres, Constants.zdb2, Constants.idb2,
				Constants.db2luw, Constants.teradata, Constants.informix, Constants.netezza };
		String[] dstChoices = { Constants.db2luw_compatibility, Constants.db2luw, Constants.netezza, Constants.zdb2};
				
		
		schemaPaneBox.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		schemaPaneBox.setPreferredSize(new Dimension(300,100));
		
		btnSelectAll.setPreferredSize(new Dimension(100,30));
				
		comboSrcVendor = new JComboBox(srcChoices);
		comboDstVendor = new JComboBox(dstChoices);
		
		Box jdbcSrcBox = Box.createHorizontalBox();
		jdbcSrcBox.add(textfieldSrcJDBC);
		jdbcSrcBox.add(btnSrcJDBC);
		
		Box jdbcDstBox = Box.createHorizontalBox();
		jdbcDstBox.add(btnDstJDBC);
		jdbcDstBox.add(textfieldDstJDBC);
		
		Box dstBox = Box.createVerticalBox();
		dstBox.setPreferredSize(new Dimension(300,100));
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(lblDB2Instance);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(lblDB2DateCompat);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(lblDB2NumberCompat);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(lblDB2VarcharCompat);
		dstBox.add(Box.createVerticalGlue());
		dstBox.add(lblDB2Decflt_rounding);
		dstBox.add(Box.createVerticalGlue());
		dstBox.setBorder(new LineBorder(Color.BLUE));
				
		Box browseBox = Box.createHorizontalBox();
		browseBox.add(btnOutputDir);
		browseBox.add(textfieldOutputDir);

		Box migrationBox = Box.createHorizontalBox();
		migrationBox.add(checkboxDDL);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(checkboxData);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(checkboxObjects);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(checkboxPipe);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(checkboxSyncLoad);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(new JLabel("| Num Threads: "));
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(textfieldNumTreads);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(new JLabel("# of work dir: "));
		migrationBox.add(textfieldNumJVM);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(new JLabel("# Extract Rows: "));
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(textLimitExtractRows);
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(new JLabel("# Load Rows: "));
		migrationBox.add(Box.createRigidArea(new Dimension(5,0)));
		migrationBox.add(textLimitLoadRows);
		
		btnCreateScript.setMinimumSize(new Dimension(210,25));
		btnCreateScript.setPreferredSize(new Dimension(210,25));
		btnCreateScript.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		btnMeetScript.setMinimumSize(new Dimension(210,25));
		btnMeetScript.setPreferredSize(new Dimension(210,25));
		btnMeetScript.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		Box meetBox = Box.createHorizontalBox();
		meetBox.add(btnCreateScript);
		meetBox.add(Box.createHorizontalGlue());
		meetBox.add(btnMeetScript);		
		
		btnView.setMinimumSize(new Dimension(210,25));
		btnView.setPreferredSize(new Dimension(210,25));
		btnView.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		btnDB2Script.setMinimumSize(new Dimension(210,25));
		btnDB2Script.setPreferredSize(new Dimension(210,25));
		btnDB2Script.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		Box scriptBox = Box.createHorizontalBox();
		scriptBox.add(btnView);
		scriptBox.add(Box.createHorizontalGlue());
		scriptBox.add(btnDB2Script);
		
		btnDeploy.setMinimumSize(new Dimension(210,25));
		btnDeploy.setPreferredSize(new Dimension(210,25));
		btnDeploy.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		btnDropObjs.setMinimumSize(new Dimension(210,25));
		btnDropObjs.setPreferredSize(new Dimension(210,25));
		btnDropObjs.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		Box deployBox = Box.createHorizontalBox();
		deployBox.add(btnDeploy);
		deployBox.add(Box.createHorizontalGlue());
		deployBox.add(btnDropObjs);
		
		srcServerBox.add(textfieldSrcServer);
		
		Box db2Box = Box.createHorizontalBox();
		db2Box.add(lblDB2Database);
		db2Box.add(Box.createHorizontalGlue());
		db2Box.add(checkboxDB2);

		CellConstraints cc = new CellConstraints();

		// Add a titled separator to cell (1, 1) that spans 7 columns.
		builder.addLabel("Source Database",    cc.xy (3,  1));
		builder.add(db2Box,                    cc.xy (5,  1));
		builder.addSeparator("",               cc.xyw(1,  3, 5));
		builder.addLabel("Vendor",             cc.xy (1,  5));
		builder.add(comboSrcVendor,            cc.xy (3,  5));
		builder.add(comboDstVendor,            cc.xy (5,  5));
		builder.add(lblServerName,             cc.xy (1,  7));
		builder.add(srcServerBox,              cc.xy (3,  7));
		builder.add(textfieldDstServer,        cc.xy (5, 7));
		builder.addLabel("Port Number:",       cc.xy (1,  9));
		builder.add(textfieldSrcPortNum,       cc.xy (3,  9));
		builder.add(textfieldDstPortNum,       cc.xy (5, 9));
		builder.add(lblDatabaseName,           cc.xy (1,  11)); 
		builder.add(textfieldSrcDatabase,      cc.xy (3,  11));
		builder.add(textfieldDstDatabase,      cc.xy (5, 11));
		builder.addLabel("User ID:",           cc.xy (1,  13));
		builder.add(textfieldSrcUserID,        cc.xy (3,  13));
		builder.add(textfieldDstUserID,        cc.xy (5, 13));
		builder.addLabel("Password:",          cc.xy (1,  15));
		builder.add(textfieldSrcPassword,      cc.xy (3,  15));
		builder.add(textfieldDstPassword,      cc.xy (5, 15));
		builder.addLabel("JDBC Drivers:",      cc.xy (1,  17));
		builder.add(jdbcSrcBox,                cc.xy (3,  17));
		builder.add(jdbcDstBox,                cc.xy (5, 17));
		builder.addLabel("Test Connections:",  cc.xy (1,  19));
		builder.add(btnSrcTestConn,            cc.xy (3,  19));
		builder.add(btnDstTestConn,            cc.xy (5, 19));
		builder.addSeparator("",               cc.xyw(1,  21, 5));
		builder.addLabel("Source Schema:",     cc.xy (1,  23));
		builder.add(schemaPaneBox,             cc.xy (3,  23));
		builder.add(dstBox,                    cc.xy (5, 23));
		builder.addLabel("Output Directory:",  cc.xy (1,  25));
		builder.add(browseBox,                 cc.xyw(3,  25, 3));
		builder.addLabel("Migration:",         cc.xy (1,  27));
		builder.add(migrationBox,              cc.xyw(3,  27, 3));
		builder.addSeparator("",               cc.xyw(1,  29, 5));
		builder.addLabel("Extract/Deploy:",    cc.xy (1,  31));
		builder.add(btnExtract,                cc.xy (3,  31));
		builder.add(deployBox,                 cc.xy (5, 31));
		builder.addLabel("Create/Execute Scripts:",cc.xy (1,  33));
		builder.add(meetBox,           		   cc.xy (3,  33));
		builder.add(scriptBox,                 cc.xy (5, 33));
		builder.addSeparator("",               cc.xyw(1,  35, 5));
		builder.add(lblMessage,                cc.xyw(1,  37, 5));
		//builder.add(lblSplash,                 cc.xy(5, 39));

		addActionListeners();
		
		cfg = new IBMExtractConfig();
    	cfg.loadConfigFile();
    	cfg.getParamValues();
    	boolean isRemote = Boolean.valueOf(cfg.getRemoteLoad());
    	cfg.isTargetInstalled = IBMExtractUtilities.isDB2Installed(isRemote);    	
		if (cfg.isTargetInstalled)
		{
			if (!isRemote)
			   SetLabelMessage(lblMessage,"DB2 was detected.", false);
		} else
		{
			SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
		}		
		SetTimer();		
		SetCFGValues();
		return builder.getPanel();
	}
	
	public IBMExtractConfig getCfg()
	{
		return cfg;
	}

	public void setCfg(IBMExtractConfig cfg)
	{
		this.cfg = cfg;
	}
	
	private void addActionListeners() {
		btnSrcJDBC.addActionListener(this);
		btnDstJDBC.addActionListener(this);
		btnSrcTestConn.addActionListener(this);
		btnDstTestConn.addActionListener(this);
		btnExtract.addActionListener(this);
		btnDeploy.addActionListener(this);
		btnDropObjs.addActionListener(this);
		comboSrcVendor.addActionListener(this);
		comboDstVendor.addActionListener(this);
		//btnClose.addActionListener(this);
		btnView.addActionListener(fillTextAreaWithFileActionListener);
		//btnAbout.addActionListener(this);
		btnDB2Script.addActionListener(this);;
		btnOutputDir.addActionListener(this);
		btnCreateScript.addActionListener(this);
		btnMeetScript.addActionListener(this);
		checkboxDDL.addActionListener(this);
		checkboxData.addActionListener(this);
		checkboxObjects.addActionListener(this);
		checkboxPipe.addActionListener(this);
		checkboxSyncLoad.addActionListener(this);
		btnSelectAll.addActionListener(this);
		checkboxDB2.addActionListener(this);
	}

	private void SetDeployButton(boolean on)
	{
		setDeployButtonTitle();
		if (on)
		{
			if (!checkboxDB2.isSelected())
			   btnDeploy.setEnabled(true);
			else
			   btnDeploy.setEnabled(false);
		} else
		{
			btnDeploy.setEnabled(false);
		}
	}
	
	private void SetTimer()
	{
		if (busy == null)
		{
			busy = new Timer(500, new ActionListener()
			{
				public void actionPerformed(ActionEvent a)
				{
					if (s != null && s.isTerminated())
					{
						if (IBMExtractUtilities.DataExtracted)
						{
							IBMExtractUtilities.DataExtracted = false;
							SetLabelMessage(lblMessage,"Extract completeted ...", false);
							lblSplash.setVisible(false);
							if (!checkboxDB2.isSelected())
							{
								SetDeployButton(true);
								btnDropObjs.setEnabled(true);
								btnDB2Script.setEnabled(true);
							}
							ActionEvent e = new ActionEvent(this,0,executingScriptName);
							fillTextAreaTab3ActionListener.actionPerformed(e);
						} 
						else if (IBMExtractUtilities.ScriptExecutionCompleted)
						{
							IBMExtractUtilities.ScriptExecutionCompleted = false;
							SetLabelMessage(lblMessage,"Script Execution completeted ...", false);
							lblSplash.setVisible(false);
							ActionEvent e = new ActionEvent(this,0,executingScriptName);
							fillTextAreaTab3ActionListener.actionPerformed(e);
						}
						else if (IBMExtractUtilities.db2ScriptCompleted)
						{
							IBMExtractUtilities.db2ScriptCompleted = false;
							SetLabelMessage(lblMessage, Constants.getDbTargetName() + " script Execution completeted ...", false);
							lblSplash.setVisible(false);
						}
						s = null;
					}
				}
			});
			busy.start();
		}
	}
		
	private void AddJarsToClasspath(String jarList)
	{
		String[] tmp = jarList.split(sep);
		for (int i = 0; i < tmp.length; ++i)
		{
			File f = new File(tmp[i]);
			try
			{
				IBMExtractUtilities.AddFile(f);
			} catch (IOException e1)
			{
				e1.printStackTrace();
			}
		}		
	}

    private void SetLabelMessage(JLabel label, String message, boolean warning)
	{
		if (warning)
		{
			label.setForeground(Color.RED);		
		} else
		{
			label.setForeground(Color.BLUE);
		}
		label.setText(message);
	}

    private void setSyncPipeCheckBoxes(boolean state)
    {
    	if (state)
    	{
    	   checkboxPipe.setEnabled(true);
    	   checkboxSyncLoad.setEnabled(false);
    	} else
    	{
     	   checkboxPipe.setEnabled(false);
    	   checkboxSyncLoad.setEnabled(true);    		
    	}
    }
    
    private void setExtractButtonTitle()
    {
		if (checkboxDDL.isSelected() && !checkboxData.isSelected() && !checkboxObjects.isSelected())
		   btnExtract.setText("Extract DDL");
		else if (checkboxDDL.isSelected() && checkboxData.isSelected() && !checkboxObjects.isSelected())
		   btnExtract.setText("Extract DDL/Data");
		else if (checkboxDDL.isSelected() && checkboxData.isSelected() && checkboxObjects.isSelected())
		   btnExtract.setText("Extract DDL/Data/Objects");
		else if (!checkboxDDL.isSelected() && checkboxData.isSelected() && !checkboxObjects.isSelected())
		   btnExtract.setText("Extract Data");
		else if (!checkboxDDL.isSelected() && checkboxData.isSelected() && checkboxObjects.isSelected())
			   btnExtract.setText("Extract Data/Objects");
		else if (!checkboxDDL.isSelected() && !checkboxData.isSelected() && checkboxObjects.isSelected())
		   btnExtract.setText("Extract Objects");
		else if (checkboxDDL.isSelected() && !checkboxData.isSelected() && checkboxObjects.isSelected())
		   btnExtract.setText("Extract DDL/Objects");
		else
		   btnExtract.setText("Extract ?");
    }
    
    private void setDeployButtonTitle()
    {
    	if (checkboxDDL.isSelected() && !checkboxData.isSelected() && !checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy DDL");
    	else if (checkboxDDL.isSelected() && checkboxData.isSelected() && !checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy DDL/Data");
    	else if (checkboxDDL.isSelected() && checkboxData.isSelected() && checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy DDL/Data/Objects");
    	else if (!checkboxDDL.isSelected() && checkboxData.isSelected() && !checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy Data");
    	else if (!checkboxDDL.isSelected() && checkboxData.isSelected() && checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy Data/Objects");
    	else if (!checkboxDDL.isSelected() && !checkboxData.isSelected() && checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy Objects");
    	else if (checkboxDDL.isSelected() && !checkboxData.isSelected() && checkboxObjects.isSelected())
    		btnDeploy.setText("Deploy DDL/Objects");
    	else
    		btnDeploy.setText("Deploy ");
    }
    
    private boolean getDstCompatibility()
    {
		return ((String) comboDstVendor.getSelectedItem()).equalsIgnoreCase(Constants.db2luw_compatibility);
    }
    
    private String getDstVendor()
    {
		String vendor = (String) comboDstVendor.getSelectedItem();
		Constants.setDbTargetName(vendor);
		return vendor;
    }
    
	public void actionPerformed(ActionEvent e)
	{
		boolean compatibility = getDstCompatibility();
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		String dstVendor = getDstVendor();
		SetLabelMessage(lblMessage,"", false);
		if (e.getSource().equals(comboSrcVendor)) 
		{
			lblServerName.setText("Server Name:");
			lblDatabaseName.setText("Database Name:");
			resetDstFields = true;
			if (srcVendor.equalsIgnoreCase("mssql"))
			{
				btnMeetScript.setText("Generate Input for MTK");
			} else
			{
				btnMeetScript.setText("Generate Input for MEET");				
			}
			if (srcVendor.equals("oracle"))
			{
				if (!cfg.getCustomMapping().equalsIgnoreCase("false"))
				{
					comboDstVendor.setSelectedIndex(1);
				} else
				{
				    comboDstVendor.setSelectedIndex(0);
				}
			}
			else if (srcVendor.equals(Constants.sybase))			
            {
				comboDstVendor.setSelectedIndex(0);
				//comboDstVendor.setEnabled(true);
			} else
			{
				comboDstVendor.setSelectedIndex(1);
				//comboDstVendor.setEnabled(false);
			}
			if (srcVendor.equalsIgnoreCase(Constants.oracle))
				textfieldSrcPortNum.setText(Constants.oraclePort);
			else if (srcVendor.equalsIgnoreCase(Constants.mssql))
				textfieldSrcPortNum.setText(Constants.mssqlPort);
			else if (srcVendor.equalsIgnoreCase(Constants.sybase))
				textfieldSrcPortNum.setText(Constants.sybasePort);
			else if (srcVendor.equalsIgnoreCase(Constants.mysql))
				textfieldSrcPortNum.setText(Constants.mysqlPort);
			else if (srcVendor.equalsIgnoreCase(Constants.postgres))
				textfieldSrcPortNum.setText(Constants.postgresPort);
			else if (srcVendor.equalsIgnoreCase(Constants.db2luw))
				textfieldSrcPortNum.setText(Constants.db2luwPort);
			else if (srcVendor.equalsIgnoreCase(Constants.zdb2))
				textfieldSrcPortNum.setText(Constants.zdb2Port);
			else if (srcVendor.equalsIgnoreCase(Constants.access))
			{
			}
			else if (srcVendor.equalsIgnoreCase(Constants.idb2))
			{
				textfieldSrcPortNum.setText(Constants.idb2Port);
			}
			else if (srcVendor.equalsIgnoreCase(Constants.teradata))
			{
				textfieldSrcPortNum.setText(Constants.teradataPort);
			}
			else if (srcVendor.equalsIgnoreCase(Constants.informix))
			{
				textfieldSrcPortNum.setText(Constants.informixPort);
			}
			else if (srcVendor.equalsIgnoreCase(Constants.netezza))
			{
				textfieldSrcPortNum.setText(Constants.netezzaPort);
			}
			
			if (srcVendor.equalsIgnoreCase(Constants.informix) || srcVendor.equalsIgnoreCase(Constants.mssql))
			{
				setSrcServerBox(1, srcVendor);				
			} else
			{
				setSrcServerBox(0, srcVendor);
			}
			btnSrcTestConn.setText("Connect to " + srcVendor.toUpperCase());
			if (srcVendor.equals("access"))
			{
				SetLabelMessage(lblMessage,"Type Access file name in Server name field", false);
				textfieldSrcPortNum.setText(Constants.accessPort);
				textfieldSrcJDBC.setText("");
				textfieldSrcDatabase.setText("access");
				textfieldSrcUserID.setText("null");
				textfieldSrcPassword.setText("null");	
				if (cfg.getDstSchName() == null || cfg.getDstSchName().length() == 0)
				{
				   cfg.setDstSchName("ADMIN");
				}
				if (cfg.getSrcSchName() == null || cfg.getSrcSchName().length() == 0)
				{
				   cfg.setSrcSchName("ADMIN");
				}
				if (cfg.getSelectSchemaName() == null || cfg.getSelectSchemaName().length() == 0)
				{
				   cfg.setSelectSchemaName("ADMIN");
				}
				checkboxDDL.setSelected(false);
			} else if (srcVendor.equals(Constants.zdb2))
			{
				lblDatabaseName.setText("Location Name:");
				textfieldSrcJDBC.setText("");
				textfieldSrcDatabase.setText("");
				textfieldSrcUserID.setText("");
				textfieldSrcPassword.setText("");				
			}
			else if (srcVendor.equals(Constants.idb2))
			{
				textfieldSrcDatabase.setText("SYSBAS");
				textfieldSrcJDBC.setText("");
				textfieldSrcUserID.setText("");
				textfieldSrcPassword.setText("");				
			} else if (srcVendor.equals(Constants.mssql))
			{
				lblServerName.setText("Host | Instance Name");
				textfieldSrcJDBC.setText("");
				textfieldSrcDatabase.setText("");
				textfieldSrcUserID.setText("");
				textfieldSrcPassword.setText("");								
			} else if (srcVendor.equals(Constants.informix))
			{
				lblServerName.setText("Host | Informix Server Name");
				textfieldSrcJDBC.setText("");
				textfieldSrcDatabase.setText("");
				textfieldSrcUserID.setText("");
				textfieldSrcPassword.setText("");								
			}
			else
			{
				textfieldSrcJDBC.setText("");
				textfieldSrcDatabase.setText("");
				textfieldSrcUserID.setText("");
				textfieldSrcPassword.setText("");				
			}
			maingui.Enable(srcVendor);
			setAccessFields(srcVendor);
			setSybaseParams(srcVendor);
		}
		else if (e.getSource().equals(comboDstVendor))
		{
			String tmpVendor = getDstVendor();
			if (tmpVendor.equalsIgnoreCase(Constants.netezza))
			{
				btnDstTestConn.setText("Connect to Netezza");
			} else if (tmpVendor.equalsIgnoreCase(Constants.zdb2))
			{
				btnDstTestConn.setText("Connect to DB2 on z/OS");				
			}
			else
			{
				btnDstTestConn.setText("Connect to DB2");
			}
			if (dstVendor.equalsIgnoreCase(Constants.netezza))
			{
			    textfieldDstDatabase.setText("SYSTEM");				
				textfieldDstDatabase.setEnabled(false);
				textfieldDstPortNum.setText(Constants.netezzaPort);
				checkboxDB2.setText("Netezza is not installed");
				btnDB2Script.setText("Execute Netezza Script");
			} else
			{
			    textfieldDstDatabase.setText("");				
				textfieldDstDatabase.setEnabled(true);
				textfieldDstPortNum.setText(Constants.db2luwPort);
				checkboxDB2.setText("DB2 is not installed");
				btnDB2Script.setText("Execute DB2 Script");
			}
		    textfieldDstJDBC.setText("");
		    textfieldDstUserID.setText("");
		    textfieldDstPassword.setText("");
		    lblDB2Instance.setText("");
		    lblDB2VarcharCompat.setText("");
		    lblDB2DateCompat.setText("");
		    lblDB2NumberCompat.setText("");
		    lblDB2Decflt_rounding.setText("");
			setSybaseParams(srcVendor);
		}
		else if (e.getSource().equals(btnOutputDir))
		{
			try
			{
		       File f = new File(new File(".").getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select "+srcVendor+" output directory");
			   fc.setFileSelectionMode( javax.swing.JFileChooser.DIRECTORIES_ONLY);
			
			   int result = fc.showOpenDialog(null);

			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }

			   File fileSelected = null;
			   fileSelected = fc.getSelectedFile();
			   if (!(fileSelected == null || fileSelected.getName().equals(""))) 
			   {				
				   cfg.setOutputDirectory(fileSelected.getAbsolutePath());
				   textfieldOutputDir.setText(cfg.getOutputDirectory());
				   cfg.loadIDMTConfigFile();
				   cfg.getParamValues();
				   SetCFGValues();
				   schemaTableChangeListener.actionPerformed(null);
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}			
		}
		else if (e.getSource().equals(btnSrcJDBC)) 
		{
			if (srcVendor.equals(Constants.oracle))
			{
			   JOptionPane.showMessageDialog(StateExtractTab.this,
					    "For Oracle 9i and up, you need\n"+cfg.getJDBCList(srcVendor)+"\nin order to connect to "+srcVendor+
					    ".\nThe first file is mandatory and others are optional." +
					    "\nYou will need all of the above if you have XML data type." +
					    "\n" +
					    "\nFor Oracle 8i or lower, you can still use above mentioned driver but if you get " +
					    "\nerror, you should consider using classes12.jar or classes111.jar as the case may be." +
					    "");
			} else
			{
				   JOptionPane.showMessageDialog(StateExtractTab.this,
						    "You need\n"+cfg.getJDBCList(srcVendor)+"\nin order to connect to "+srcVendor+
						    "\nPlease locate these files and include them.");				
			}			
			try
			{
			   String tmpDir = ".";
			   tmpDir = IBMExtractUtilities.targetJDBCHome(srcVendor);
		       File f = new File(new File(tmpDir).getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select "+srcVendor+"'s JDBC Driver(s)");
			   fc.setMultiSelectionEnabled(true);
			   fc.setFileSelectionMode( javax.swing.JFileChooser.FILES_AND_DIRECTORIES);
			
			   int result = fc.showOpenDialog(null);

			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }

			   File[] fileSelected = null;
			   fileSelected = fc.getSelectedFiles();
			   if (fileSelected != null)
			   {
				   for (int i = 0; i < fileSelected.length; ++i)
				   {
					   if (!fileSelected[i].getName().equals(""))
					   {
						  String tmp = fileSelected[i].getAbsolutePath();
						  String tmp2 = textfieldSrcJDBC.getText();
						  if (tmp2 == null || tmp2.equals(""))
							 textfieldSrcJDBC.setText(tmp);					  
						  else
						  {
							  if (!tmp2.contains(tmp))
							     textfieldSrcJDBC.setText(tmp2+sep+tmp);
						  }	
					   }
				   }				   
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}		
			if (srcVendor.equals(dstVendor) || (srcVendor.equals(Constants.db2luw) && dstVendor.equals(Constants.db2luw_compatibility)))
			   textfieldDstJDBC.setText(textfieldSrcJDBC.getText());
		}
		else if (e.getSource().equals(btnDstJDBC)) 
		{
			if (srcVendor.equalsIgnoreCase(Constants.sybase) && compatibility && !dstVendor.equalsIgnoreCase(Constants.netezza))
			{
				    JOptionPane.showMessageDialog(StateExtractTab.this,
						    "You need\n"+cfg.getJDBCList(Constants.ants)+"\nin order to connect to " + dstVendor +
						    "\nPlease locate these files and include them.");
			} else
			{
			    JOptionPane.showMessageDialog(StateExtractTab.this,
					    "You need\n"+cfg.getJDBCList(dstVendor)+"\nin order to connect to " + dstVendor +
					    "\nPlease locate these files and include them.");
			}
			try
			{
			   String tmpDir = ".";
			   tmpDir = IBMExtractUtilities.targetJDBCHome(dstVendor);				   
			   File f = new File(new File(tmpDir).getCanonicalPath());				
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select DB2 JDBC Driver(s)");
			   fc.setMultiSelectionEnabled(true);

			   fc.setFileSelectionMode( javax.swing.JFileChooser.FILES_AND_DIRECTORIES);
			
			   int result = fc.showOpenDialog(null);

			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }

			   File[] fileSelected = null;
			   fileSelected = fc.getSelectedFiles();
			   if (fileSelected != null)
			   {
				   for (int i = 0; i < fileSelected.length; ++i)
				   {
					   if (!fileSelected[i].getName().equals(""))
					   {
						  String tmp = fileSelected[i].getAbsolutePath();
						  String tmp2 = textfieldDstJDBC.getText();
						  if (tmp2 == null || tmp2.equals(""))
							 textfieldDstJDBC.setText(tmp);					  
						  else
						  {
							  if (!tmp2.contains(tmp))
							     textfieldDstJDBC.setText(tmp2+sep+tmp);
						  }					   
					   }
				   }				   
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}
		}
		else if (e.getSource().equals(btnSelectAll)) 
		{
			if (btnSelectAll.getText().trim().equalsIgnoreCase("Select All"))
			{
				btnSelectAll.setText("Deselect All");
				ToggleSchemaCheckBoxes(true);
			}
			else
			{
				btnSelectAll.setText("Select All");
				ToggleSchemaCheckBoxes(false);
			}
		}
		else if (e.getSource().equals(btnSrcTestConn)) 
		{
			String schemaList = "";
			if (!validateSrcFields())
			{
				SetLabelMessage(lblMessage,txtMessage, true);
				return;
			}
			if (!IBMExtractUtilities.isJDBCLicenseAdded(srcVendor, textfieldSrcJDBC.getText()))
			{				
				SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
				return;
			}		
			if (textfieldSrcJDBC.getText().contains("db2java.zip"))
			{				
				SetLabelMessage(lblMessage,"You selected db2java.zip. This is not the right JAR", true);
				return;
			}			
			setValues();
			if (cfg.pingJDBCDrivers(cfg.getSrcJDBC()))
			{
				AddJarsToClasspath(cfg.getSrcJDBC());
				String extraParam = (srcVendor.equals("sybase")) ? cfg.getSybaseConvClass() : "";
				if (IBMExtractUtilities.TestConnection(false, false, cfg.getSrcVendor(), cfg.getSrcServerName(), 
						cfg.getSrcPort(), cfg.getSrcDBName(), cfg.getSrcUid(), 
						cfg.getSrcPwd(), extraParam))
				{
					cfg.setSrcDB2Instance(IBMExtractUtilities.InstanceName);
					cfg.setSrcDB2Home(IBMExtractUtilities.DB2Path);
					schemaList = IBMExtractUtilities.GetSchemaList(cfg.getSrcVendor(), cfg.getSrcServerName(), 
						cfg.getSrcPort(), cfg.getSrcDBName(), cfg.getSrcUid(), 
						cfg.getSrcPwd());
					if (!schemaList.equals(""))
					{
						if (IBMExtractUtilities.Message.equals(""))
						{ 
							cfg.setSrcSchName(schemaList);
							setSchemaCheckBoxes();
							cfg.setSrcSchName(schemaList);							
							btnExtract.setEnabled(true);
							if (cfg.getUsePipe().equalsIgnoreCase("true") && 
									cfg.getSyncLoad().equalsIgnoreCase("false"))	
							{
								btnDeploy.setEnabled(false);
								btnExtract.setText("Extract / Deploy through Pipe Load");
							}
							else if (cfg.getUsePipe().equalsIgnoreCase("false") && 
									cfg.getSyncLoad().equalsIgnoreCase("true"))	
							{
								btnDeploy.setEnabled(false);
								btnExtract.setText("Sync Unload / Load");
							}
							else
							{
								SetDeployButton(true);
								setExtractButtonTitle();
								setDeployButtonTitle();
								//btnExtract.setText("Extract DDL/Data");
							}
							checkboxPipe.setEnabled(true);
							checkboxSyncLoad.setEnabled(true);
							btnCreateScript.setEnabled(true);
							if (srcVendor.equalsIgnoreCase("oracle") || srcVendor.equalsIgnoreCase(Constants.sybase) || srcVendor.equalsIgnoreCase(Constants.mssql))
								btnMeetScript.setEnabled(true);
							else
								btnMeetScript.setEnabled(false);
							SetLabelMessage(lblMessage,"Connect to " + srcVendor + " succeeded and schema information obtained.", false);						
						}
						else
						   SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
					} else
						SetLabelMessage(lblMessage,"No user schema found in your database", true);
				} else
				{
					SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
				}
			} else
			{
				SetLabelMessage(lblMessage,cfg.Message, true);
			}
		}
		else if (e.getSource().equals(btnDstTestConn)) 
		{
			if (!validateDstFields())
			{
				SetLabelMessage(lblMessage,txtMessage, true);
				return;
			}
			if (!IBMExtractUtilities.isJDBCLicenseAdded(dstVendor, textfieldDstJDBC.getText()))
			{				
				SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
				return;
			}
			if (srcVendor.equalsIgnoreCase(Constants.sybase) && compatibility && !dstVendor.equalsIgnoreCase(Constants.netezza))
			{
				if (!textfieldDstJDBC.getText().matches("(?i).*antsjconn.\\.jar.*"))
				{				
					SetLabelMessage(lblMessage,"For SKIN feature, include antsjconn2.jar(with db2jcc.jar) or antsjconn3.jar(with dbjcc4.jar)", true);
					return;
				}							
			}
			setValues();
		    cfg.writeConfigFile();		
	    	cfg.getParamValues();		
			boolean remote = Boolean.valueOf(cfg.getRemoteLoad());
			/*if (!remote &&  !IBMExtractUtilities.isDB2COMMSet())
			{
				SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
				return;
			}*/
			if (cfg.isTargetInstalled)
			{
				if (cfg.pingJDBCDrivers(cfg.getDstJDBC()))
				{
					AddJarsToClasspath(cfg.getDstJDBC());
					boolean compatibilityMode = comboDstVendor.getSelectedIndex() == 0;
					if (IBMExtractUtilities.TestConnection(remote, compatibilityMode, cfg.getDstVendor(), 
							cfg.getDstServer(), 
							cfg.getDstPort(), cfg.getDstDBName(), cfg.getDstUid(), 
							cfg.getDstPwd(), ""))
					{
						boolean isCompatibleMode = IBMExtractUtilities.DB2Compatibility;
						cfg.setDstDB2Instance(IBMExtractUtilities.InstanceName);
						cfg.setDstDB2Home(IBMExtractUtilities.DB2Path);
						cfg.setDstDBRelease(IBMExtractUtilities.ReleaseLevel);
						cfg.setDstDB2FixPack(IBMExtractUtilities.fixPack);
						if (comboDstVendor.getSelectedIndex() == 0)
						{
							if (!isCompatibleMode)
							{
							    SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
								return;
							}
							if (srcVendor.equalsIgnoreCase("oracle"))
							{
								if (IBMExtractUtilities.param01.equalsIgnoreCase("on") ||
									IBMExtractUtilities.param02.equalsIgnoreCase("on") ||
									IBMExtractUtilities.param03.equalsIgnoreCase("on"))
								{
								   SetDeployButton(true);
								   btnDropObjs.setEnabled(true);
								   btnDB2Script.setEnabled(true);
								   SetLabelMessage(lblDB2Instance," Instance Name " + IBMExtractUtilities.InstanceName + " (" + IBMExtractUtilities.ReleaseLevel + ")", false);
								   SetLabelMessage(lblDB2VarcharCompat," varchar2_compat " + IBMExtractUtilities.param01, false);
								   SetLabelMessage(lblDB2DateCompat," date_compat " + IBMExtractUtilities.param02, false);
								   SetLabelMessage(lblDB2NumberCompat," number_compat " + IBMExtractUtilities.param03, false);
								   SetLabelMessage(lblMessage,IBMExtractUtilities.Message, false);	
								} else
								{
									SetLabelMessage(lblDB2Instance," Instance Name " + IBMExtractUtilities.InstanceName + " (" + IBMExtractUtilities.ReleaseLevel + ")", false);
									SetLabelMessage(lblDB2VarcharCompat," varchar2_compat " + IBMExtractUtilities.param01, true);
									SetLabelMessage(lblDB2DateCompat," date_compat " + IBMExtractUtilities.param02, true);
									SetLabelMessage(lblDB2NumberCompat," number_compat " + IBMExtractUtilities.param03, true);
									SetLabelMessage(lblMessage,"*WARNING* Database is not in Oracle compatibility mode. Drop and re-create it.", true);
								}
								if (IBMExtractUtilities.param04.equalsIgnoreCase("round_half_up"))
								{
									SetLabelMessage(lblDB2Decflt_rounding," decflt_rounding " + IBMExtractUtilities.param04, false);
								} else
								{
									SetLabelMessage(lblDB2Decflt_rounding," *Warning* decflt_rounding is not ROUND_HALF_UP", true);
								}
							} else
							{
								SetLabelMessage(lblDB2Instance," Instance Name " + IBMExtractUtilities.InstanceName + " (" + IBMExtractUtilities.ReleaseLevel + ")", false);
								SetDeployButton(true);
								btnDropObjs.setEnabled(true);
								btnDB2Script.setEnabled(true);
								SetLabelMessage(lblMessage,IBMExtractUtilities.Message, false);															
							}
						} else if (comboDstVendor.getSelectedIndex() == 2)
						{
							   SetLabelMessage(lblDB2Instance," Instance Name " + IBMExtractUtilities.InstanceName + " (" + IBMExtractUtilities.ReleaseLevel + ")", false);
							   SetLabelMessage(lblDB2VarcharCompat, IBMExtractUtilities.param01, false);
							   SetLabelMessage(lblDB2DateCompat, IBMExtractUtilities.param02, false);
							   SetLabelMessage(lblDB2NumberCompat, IBMExtractUtilities.param03, false);
							   SetLabelMessage(lblMessage,IBMExtractUtilities.Message, false);	
						} else
						{
							SetLabelMessage(lblDB2Instance," Instance Name " + IBMExtractUtilities.InstanceName + " (" + IBMExtractUtilities.ReleaseLevel + ")", false);
							SetDeployButton(true);
							btnDropObjs.setEnabled(true);
							btnDB2Script.setEnabled(true);
							SetLabelMessage(lblMessage,IBMExtractUtilities.Message, false);							
						}
					} else
					{
						SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
					}
				} else
				{
					SetLabelMessage(lblMessage,cfg.Message, true);
				}
			}
			else
			{
				SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
			}
			setValues();
		    cfg.writeConfigFile();		
	    	cfg.getParamValues();		
		}
		else if (e.getSource().equals(checkboxDDL)) 			
		{
			setDeployButtonTitle();
			if (checkboxPipe.isSelected() && checkboxPipe.isEnabled())
				btnDeploy.setEnabled(false);
			else
				SetDeployButton(true);
		/*	if (!checkboxDDL.isSelected())
			{
				if (checkboxData.isSelected())
				   btnDeploy.setText("Deploy Data");
				else
				{
				   btnDeploy.setText("Deploy");
				   btnDeploy.setEnabled(false);
				}
			} */
			setExtractButtonTitle();
		}
		else if (e.getSource().equals(checkboxData)) 			
		{
			setDeployButtonTitle();
			if (checkboxPipe.isSelected() && checkboxPipe.isEnabled())
				btnDeploy.setEnabled(false);
			else
				SetDeployButton(true);
			if (checkboxData.isSelected())
			{
				textLimitExtractRows.setEnabled(true);
				textLimitLoadRows.setEnabled(true);
			} else
			{
				if (!checkboxDDL.isSelected())
				  btnDeploy.setEnabled(false);
				
				textLimitExtractRows.setEnabled(false);
				textLimitLoadRows.setEnabled(false);				
			}
			setExtractButtonTitle();
		}
		else if (e.getSource().equals(checkboxObjects)) 			
		{
			if (checkboxObjects.isSelected())
			{			
				setSyncPipeCheckBoxes(false);
			} else
			{
				setSyncPipeCheckBoxes(true);
			}
			setDeployButtonTitle();
			setExtractButtonTitle();
		}
		else if (e.getSource().equals(checkboxPipe)) 			
		{
			if (checkboxPipe.isSelected())
			{				
				checkboxDDL.setEnabled(false);
				checkboxData.setEnabled(false);
				checkboxObjects.setEnabled(false);
				checkboxSyncLoad.setEnabled(false);
		        if (validatePipeLoad())
		        {
		        	btnDeploy.setEnabled(false);
		        	btnExtract.setText("Unload / Load through Pipe");
		        }
			} else
			{
				SetDeployButton(true);
				checkboxDDL.setEnabled(true);
				checkboxData.setEnabled(true);
				checkboxObjects.setEnabled(true);
				checkboxSyncLoad.setEnabled(true);
				setExtractButtonTitle();
			}
		}
		else if (e.getSource().equals(checkboxSyncLoad)) 			
		{
			if (checkboxSyncLoad.isSelected())
			{				
				checkboxDDL.setEnabled(false);
				checkboxData.setEnabled(false);
				checkboxPipe.setEnabled(false);
		        if (validatePipeLoad())
		        {
		        	btnDeploy.setEnabled(false);
		        	btnExtract.setText("Sync Unload / Load");
		        }
			} else
			{
				SetDeployButton(true);
				checkboxDDL.setEnabled(true);
				checkboxData.setEnabled(true);
				checkboxPipe.setEnabled(true);
				setExtractButtonTitle();
			}
		}
		else if (e.getSource().equals(btnExtract)) 			
		{
			int n = JOptionPane.showConfirmDialog(
					StateExtractTab.this,
				    "Are you sure to run the extract?", 
				    "Confirm before extract",
				    JOptionPane.YES_NO_OPTION);
			if (n == JOptionPane.NO_OPTION)
				return;
			String runningProcess = IBMExtractUtilities.CheckUnixProcesses();
			if (runningProcess != null && runningProcess.length() > 0)
			{
				n = JOptionPane.showConfirmDialog(
						StateExtractTab.this,
					    "Unloads are running in # (" + runningProcess + ")" + 
					    "\nPress Yes to continue and No to cancel",
					    "Running Processes",
					    JOptionPane.YES_NO_CANCEL_OPTION);
				if (n == JOptionPane.NO_OPTION)
					return;
			}

			boolean dataExtract = checkboxData.isSelected();
			boolean ddlextract = checkboxDDL.isSelected();
			boolean objectsExtract = checkboxObjects.isSelected();
			boolean usePipe = checkboxPipe.isSelected();
			boolean syncLoad = checkboxSyncLoad.isSelected();
			String limitExtractRows = textLimitExtractRows.getText();
			if (limitExtractRows == null || limitExtractRows.length() == 0)
			{
				SetLabelMessage(lblMessage,"Specify Limit # of extract rows to be either ALL or a number > 0", true);
				return;				
			} else
			{
				try
				{
					if (!limitExtractRows.equalsIgnoreCase("ALL"))
					{
				        int x = Integer.parseInt(limitExtractRows);
				        if (x < 0)
				        {
							SetLabelMessage(lblMessage,"Specify Limit # of extract rows > 0", true);
							return;				        	
				        }
					}
				} catch (Exception ex)
				{
					SetLabelMessage(lblMessage,"Specify Limit # of extract rows to be either ALL or a number > 0", true);
					return;					
				}
			}
			String limitLoadRows = textLimitLoadRows.getText();
			if (limitLoadRows == null || limitLoadRows.length() == 0)
			{
				SetLabelMessage(lblMessage,"Specify Limit # of load rows to be either ALL or a number > 0", true);
				return;				
			} else
			{
				try
				{
					if (!limitLoadRows.equalsIgnoreCase("ALL"))
					{
				        int x = Integer.parseInt(limitLoadRows);
				        if (x < 0)
				        {
							SetLabelMessage(lblMessage,"Specify Limit # of load rows > 0", true);
							return;				        	
				        }
					}
				} catch (Exception ex)
				{
					SetLabelMessage(lblMessage,"Specify Limit # of load rows to be either ALL or a number > 0", true);
					return;					
				}
			}
			String outputDir = textfieldOutputDir.getText();
			if (outputDir == null || outputDir.equals(""))
			{
				SetLabelMessage(lblMessage,"Specify Output Directory", true);
				return;
			} 
			if (!cfg.isOutputDirectorySameAsLoadDirectory())
			{
				n = JOptionPane.showConfirmDialog(
						StateExtractTab.this,
					    "Output directory is different than the Load directory."+
					    "\nPress YES to continue " +  
					    "\nPress No to cancel. Go to Set Params and change LOAD directory",
					    "Confirm LOAD directory location",
					    JOptionPane.YES_NO_OPTION);
				if (n == JOptionPane.NO_OPTION)
					return;				
			}
			if (!(dataExtract || ddlextract))
			{
				SetLabelMessage(lblMessage,"Select DDL or DATA extraction or both.", true);
				return;
			}
			if (objectsExtract && (usePipe && !syncLoad))
			{
				SetLabelMessage(lblMessage,"Select either Objects extraction or use (Pipe or sync Load", true);
				return;
			}
			else if (objectsExtract && (!usePipe && syncLoad))
			{
				SetLabelMessage(lblMessage,"Select either Objects extraction or use (Pipe or sync Load", true);
				return;
			}
			if (usePipe && syncLoad)
			{
				SetLabelMessage(lblMessage,"Select either pipe or sync load", true);
				return;
			}
			if (cfg.getDstDB2Instance() == null || cfg.getDstDB2Instance().equals("") || 
					cfg.getDstDB2Instance().equals("null"))
			{
				if (Constants.netezza())
				    SetLabelMessage(lblMessage,"Please connect to Netezza first.", true);
				else
				    SetLabelMessage(lblMessage,"Please connect to DB2 first.", true);
				return;				
			}
            if (generateScripts(false))
            {				
    			if (((usePipe && !syncLoad) || (!usePipe && syncLoad)) && !validatePipeLoad())
    			{
    		        return;
    			}
			    maingui.setOutputTab();
    			if (cfg.getNumJVM() > 1)
    			{    				
					executingScriptName = (Constants.win()) ? "unload.cmd" : "unload";
					RunGenerateExtract task = null;
				    s = java.util.concurrent.Executors.newFixedThreadPool(cfg.getNumJVM());				
				    lblSplash.setVisible(true);	
				    for (int i = 0; i < cfg.getNumJVM(); ++i)
				    {
				    	String output = outputDir + IBMExtractUtilities.filesep + "work" +
					         IBMExtractUtilities.pad((i+1), 2, "0");
					    task = new RunGenerateExtract(maingui.getTextArea(), output, executingScriptName);				   
					    s.execute(task);
				    }
				    s.shutdown();	
				    SetLabelMessage(lblMessage,"Extract started using '"+(new File(executingScriptName).getName())+"' ...", false);
    			} else
    			{
					executingScriptName = (Constants.win()) ? "unload.cmd" : "unload";
					RunGenerateExtract task = null;
				    s = java.util.concurrent.Executors.newFixedThreadPool(1);				
				    lblSplash.setVisible(true);	
				    task = new RunGenerateExtract(maingui.getTextArea(), outputDir, executingScriptName);	
				    s.execute(task);
				    s.shutdown();		
				    SetLabelMessage(lblMessage,"Extract started using '"+(new File(executingScriptName).getName())+"' ...", false);
    			}
            }
		}
		else if (e.getSource().equals(btnDeploy)) 
		{
			try
			{
			   int choice = 0; 
			   if (lblDB2Instance.getText().equals(""))
			   {
					choice = JOptionPane.showConfirmDialog(
							StateExtractTab.this,
						    "Connect to "+Constants.getDbTargetName()+" first",
						    "Connect to "+Constants.getDbTargetName()+" first",
						    JOptionPane.DEFAULT_OPTION);
					return;
			   }
			   if (cfg.getNumJVM() > 1)
			   {
				   s = java.util.concurrent.Executors.newFixedThreadPool(cfg.getNumJVM());				
				   String wd = textfieldOutputDir.getText();
				   for (int i = 0; i < cfg.getNumJVM(); ++i)
				   {
					   executingScriptName = wd + System.getProperty("file.separator") + "work" +
						 IBMExtractUtilities.pad((i+1), 2, "0") + System.getProperty("file.separator") +
				               cfg.getDB2RutimeShellScriptName();
				       if (IBMExtractUtilities.FileExists(executingScriptName))
				       {		    	    
							choice = JOptionPane.showConfirmDialog(
									StateExtractTab.this,
								    "Ready to deploy ... \n"+executingScriptName+"\nDo you want to run this?",
								    "Confirm running of a script",
								    JOptionPane.YES_NO_OPTION);
				       } else
				    	   executingScriptName = "";
					   if (!executingScriptName.equals(""))
					   {	
						   RunScript task = null;
						   lblSplash.setVisible(true);							
						   task = new RunScript(maingui.getTextArea(), cfg.getDstDB2Instance(), cfg.getDstDB2Home(), executingScriptName, i);
						   maingui.setOutputTab();
						   s.execute(task);
					   }				   
				   }				   
				   s.shutdown();							   
			   } else
			   {
				   choice = 0;
				   String wd = textfieldOutputDir.getText();			   
				   if (wd == null || wd.equals(""))
					   wd = ".";
				   if (!(IBMExtractUtilities.FileExists(wd)))
					   wd = ".";
			       executingScriptName = wd + System.getProperty("file.separator") + cfg.getDB2RutimeShellScriptName();
			       if (IBMExtractUtilities.FileExists(executingScriptName))
			       {		    	    
						choice = JOptionPane.showConfirmDialog(
								StateExtractTab.this,
							    "Ready to deploy ... \n"+executingScriptName+"\nDo you want to run this?",
							    "Confirm running of a script",
							    JOptionPane.YES_NO_OPTION);
			       } else
			    	   executingScriptName = "";
			       
			       if (choice == 1)
			       {
					   JFileChooser fc = new JFileChooser();
					   File f = new File(new File(executingScriptName).getCanonicalPath());
					   fc.setCurrentDirectory(f);
					   fc.setDialogTitle("Select Shell Script to run.");
					   fc.setFileSelectionMode( javax.swing.JFileChooser.FILES_AND_DIRECTORIES);
					   fc.addChoosableFileFilter(new FileFilter()
					   {
						   public boolean accept(File f)
						   {
								if (f.isDirectory())
								{
									return true;
								}
		
								String s = f.getName();
								int pos = s.lastIndexOf('.');
								if (pos > 0)
								{
									String ext = s.substring(pos);
									if (ext.equalsIgnoreCase(".sh")
										|| ext.equalsIgnoreCase(".cmd"))
									{
										return true;
									} else
									{
										return false;
									}
								}
		
								return false;
						   }
		
						   public String getDescription()
						   {
								if (Constants.win())
								   return "*.cmd";
								else
								   return "*.sh";
						   }
					   });
					   fc.setAcceptAllFileFilterUsed(false);
					   int result = fc.showOpenDialog(null);
		
					   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
					   {
						  return;
					   }
		
					   File fileSelected = null;
					   fileSelected = fc.getSelectedFile();
					   if (fileSelected == null || fileSelected.getName().equals(""))
					   {
						   executingScriptName = "";
					   } else
					   {
						   executingScriptName = fileSelected.getAbsolutePath();
					   }
			       }
			       
				   if (!executingScriptName.equals(""))
				   {	
					   RunScript task = null;
					   s = java.util.concurrent.Executors.newFixedThreadPool(1);				
					   lblSplash.setVisible(true);							
					   task = new RunScript(maingui.getTextArea(), cfg.getDstDB2Instance(), cfg.getDstDB2Home(), executingScriptName, 0);
					   maingui.setOutputTab();
					   s.execute(task);
					   s.shutdown();		
					   SetLabelMessage(lblMessage,"Script '"+(new File(executingScriptName).getName())+"' started ...", false);						
				   }				   
			   }
			} catch (Exception ex)
			{
				ex.printStackTrace();
			}			
		}
		else if (e.getSource().equals(btnDropObjs)) 
		{
			int choice = 0;
			if (cfg.getNumJVM() > 1)
			{
			   s = java.util.concurrent.Executors.newFixedThreadPool(cfg.getNumJVM());				
			   String wd = textfieldOutputDir.getText();
			   for (int i = 0; i < cfg.getNumJVM(); ++i)
			   {
				   executingScriptName = wd + System.getProperty("file.separator") + "work" +
				   IBMExtractUtilities.pad((i+1), 2, "0") + System.getProperty("file.separator") +
				   Constants.dropscripts;
			       if (IBMExtractUtilities.FileExists(executingScriptName))
			       {		    	    
						choice = JOptionPane.showConfirmDialog(
								StateExtractTab.this,
							    "Ready to deploy ... \n"+executingScriptName+"\nDo you want to run this?",
							    "Confirm running of a script",
							    JOptionPane.YES_NO_OPTION);
			       } else
			    	   executingScriptName = "";
				   if (choice == 0)
				   {	
					   RunScript task = null;
					   lblSplash.setVisible(true);							
					   task = new RunScript(maingui.getTextArea(), cfg.getDstDB2Instance(), cfg.getDstDB2Home(), executingScriptName, i);
					   maingui.setOutputTab();
					   s.execute(task);
				   }				   
			   }				   
			   s.shutdown();							   
			} else
			{
				try
				{
				   String wd = textfieldOutputDir.getText();			   
				   if (wd == null || wd.equals(""))
					   wd = ".";
				   if (!(IBMExtractUtilities.FileExists(wd)))
					   wd = ".";
			       executingScriptName = wd + System.getProperty("file.separator") + Constants.dropscripts;
			       if (IBMExtractUtilities.FileExists(executingScriptName))
			       {		    	    
						choice = JOptionPane.showConfirmDialog(
								StateExtractTab.this,
							    "Ready to run ... \n"+executingScriptName+"\nDo you want to run this?",
							    "Confirm running of a script",
							    JOptionPane.YES_NO_OPTION);
			       } else
			    	   executingScriptName = "";
				   if (choice == 0)
				   {	
					   RunScript task = null;
					   s = java.util.concurrent.Executors.newFixedThreadPool(1);				
					   lblSplash.setVisible(true);							
					   task = new RunScript(maingui.getTextArea(), cfg.getDstDB2Instance(), cfg.getDstDB2Home(), executingScriptName, 0);
					   maingui.setOutputTab();
					   s.execute(task);
					   s.shutdown();		
					   SetLabelMessage(lblMessage,"Script '"+(new File(executingScriptName).getName())+"' started ...", false);
				   }
				} catch (Exception ex)
				{
					ex.printStackTrace();
				}				
			}
		}
		else if (e.getSource().equals(btnCreateScript))
		{
			if (!validateSrcFields() || !validateDstFields())
			{
				SetLabelMessage(lblMessage,txtMessage, true);
				return;
			}
			SetLabelMessage(lblMessage, "Counting rows. Please wait ....", false);
			if (generateScripts(true))
			{
			    SetLabelMessage(lblMessage, "Scripts generated", false);
			}
		}		
		else if (e.getSource().equals(btnMeetScript))
		{
			int choice = 0;
			if (!validateSrcFields())
			{
				SetLabelMessage(lblMessage,txtMessage, true);
				return;
			}
			if (generateMeetScript(srcVendor))
			{
			    String wd = textfieldOutputDir.getText();			   
			    if (wd == null || wd.equals(""))
				   wd = ".";
				if (!(IBMExtractUtilities.FileExists(wd)))
				   wd = ".";
				executingScriptName = wd + System.getProperty("file.separator") + cfg.getMeetScriptName();
				if (IBMExtractUtilities.FileExists(executingScriptName))
			       {		    	    
						choice = JOptionPane.showConfirmDialog(
								StateExtractTab.this,
							    "Ready to run ... \n"+executingScriptName+"\nDo you want to run this?",
							    "Confirm running of a script",
							    JOptionPane.YES_NO_OPTION);
			       } else
			    	   executingScriptName = "";
				if (choice == 0)
				{
					RunScript task = null;
					s = java.util.concurrent.Executors.newFixedThreadPool(1);				
					lblSplash.setVisible(true);							
					task = new RunScript(maingui.getTextArea(), "", "", executingScriptName, 0);
					maingui.setOutputTab();
					s.execute(task);
					s.shutdown();		
					SetLabelMessage(lblMessage,"Script '"+(new File(executingScriptName).getName())+"' started ...", false);
				}
			}
		}		
		else if (e.getSource().equals(btnDB2Script))
		{
			try
			{
			   String wd = textfieldOutputDir.getText();
			   if (wd == null || wd.equals(""))
				   wd = ".";
			   if (!(IBMExtractUtilities.FileExists(wd)))
				   wd = ".";
		       File f = new File(new File(wd).getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
		       fc.setDialogTitle("Select "+Constants.getDbTargetName()+" Script to run.");
			   fc.setFileSelectionMode( javax.swing.JFileChooser.FILES_ONLY);
			   fc.addChoosableFileFilter(new FileFilter()
			   {
				   public boolean accept(File f)
				   {
						if (f.isDirectory())
						{
							return false;
						}

						String s = f.getName();
						int pos = s.lastIndexOf('.');
						if (pos > 0)
						{
							String ext = s.substring(pos);
							if (ext.equalsIgnoreCase(Constants.netezza() ? ".nz" : ".db2"))
							{
								return true;
							} else
							{
								return false;
							}
						}

						return false;
				   }

				   public String getDescription()
				   {
					   return Constants.netezza() ? "*.nz" : "*.db2";
				   }
			   });
			   fc.setAcceptAllFileFilterUsed(false);
			   int result = fc.showOpenDialog(null);

			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }

			   File fileSelected = null;
			   fileSelected = fc.getSelectedFile();
			   if (!(fileSelected == null || fileSelected.getName().equals(""))) 
			   {
				   RunDB2Script task = null;
				   executingScriptName = fileSelected.getAbsolutePath();

				   s = java.util.concurrent.Executors.newFixedThreadPool(1);				
				   lblSplash.setVisible(true);							
				   task = new RunDB2Script(maingui.getTextArea(), 
						   cfg.getDstDB2Home(), executingScriptName, false);
				   maingui.setOutputTab();
				   s.execute(task);
				   s.shutdown();									   
				   SetLabelMessage(lblMessage,"Script '"+fileSelected.getName()+"' started ...", false);
			   }
			} catch (Exception ex)
			{
				ex.printStackTrace();
			}						
		}
		/*else if (e.getSource().equals(btnView)) 
		{
			String wd = textfieldOutputDir.getText();
			new FileViewer(wd, null);
		}*/
		else if (e.getSource().equals(checkboxDB2))
		{
			if (checkboxDB2.isSelected())
			{
				setDB2NotAvailableFields(false);
			} else
			{
				setDB2NotAvailableFields(true);
			}
		}
	}
	
	private void setDB2NotAvailableFields(boolean on)
	{
		textfieldDstServer.setEnabled(on);
		textfieldDstPortNum.setEnabled(on);
		textfieldDstDatabase.setEnabled(on);
		textfieldDstJDBC.setEnabled(on);
		textfieldDstPassword.setEnabled(on);
		textfieldDstUserID.setEnabled(on);
		btnDstJDBC.setEnabled(on);
		btnDstTestConn.setEnabled(on);
		btnDeploy.setEnabled(on);
		btnDropObjs.setEnabled(on);
		btnView.setEnabled(on);
		btnDB2Script.setEnabled(on);
		if (!on)
		{
			cfg.setDstServer("localhost");
			if (Constants.win())
			{
				if (Constants.netezza())
				{
					cfg.setDstDB2Instance("NETEZZA");
					cfg.setDstJDBC("C:\\Program Files\\NZ\\nzjdbc.jar");
					cfg.setDstUid("admin");
					cfg.setDstDB2Home("C:\\Program Files\\NZ");
				} else
				{
					cfg.setDstDB2Instance("DB2");
					cfg.setDstJDBC("C:\\Program Files\\IBM\\SQLLIB\\java\\db2jcc.jar;C:\\Program Files\\IBM\\SQLLIB\\java\\db2jcc_license_cu.jar");
					cfg.setDstUid("db2admin");
					cfg.setDstDB2Home("C:\\Program Files\\IBM\\SQLLIB");
				}
			} else
			{
				if (Constants.netezza())
				{
					cfg.setDstDB2Instance("nz");				
					cfg.setDstJDBC("/nz/kit/sbin/nzjdbc.jar");
					cfg.setDstUid("admin");
					cfg.setDstDB2Home("/home/nz");
				} else
				{
					cfg.setDstDB2Instance("db2inst1");				
					cfg.setDstJDBC("/home/db2inst1/sqllib/java/db2jcc.jar;/home/db2inst1/sqllib/java/db2jcc_license_cu.jar");
					cfg.setDstUid("db2inst1");
					cfg.setDstDB2Home("/home/db2inst1/sqllib");					
				}
			}
			if (Constants.netezza())
			{
				cfg.setDstDBName("SYSTEM");
				cfg.setDstDB2FixPack("0");
				cfg.setDstDBRelease("6.0");
				cfg.setDstPort("5480");
				cfg.setDstPwd("password");				
			} else
			{
				cfg.setDstDBName("sample");
				cfg.setDstDB2FixPack("3");
				cfg.setDstDBRelease("9.7");
				cfg.setDstPort("50000");
				cfg.setDstPwd("password");
			}
		} else
		{
			cfg.setDstServer("");
			cfg.setDstDB2Instance("");
			cfg.setDstJDBC("");
			cfg.setDstUid("");
			cfg.setDstDBName("");
			cfg.setDstDB2FixPack("");
			cfg.setDstDBRelease("");
			cfg.setDstPort("");
			cfg.setDstPwd("");			
		}
	}
	
	private void clearSrcServerBox()
	{
        for (int i = srcServerBox.getComponentCount() - 1; i >= 0; i--)
		{
			String name = (String) srcServerBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JTextField")
			{
				Component cont = srcServerBox.getComponent(i);	
				srcServerBox.remove(cont);
			}        	
		}		
	}
	
	private void clearSchemaCheckBoxes()
	{
        for (int i = panelCheckBox.getComponentCount() - 1; i >= 0; i--)
		{
			String name = (String) panelCheckBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JCheckBox")
			{
				Component cont = panelCheckBox.getComponent(i);	
				panelCheckBox.remove(cont);
			}
		}
	}

	public String getSchemaList()
	{
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		if (srcVendor.equalsIgnoreCase("access"))
		{
			if (cfg.getSelectSchemaName() == null || cfg.getSelectSchemaName().length() == 0)
			{
				cfg.setSelectSchemaName("ADMIN");
			}
			return cfg.getSelectSchemaName();
		}
		String schemaList = "";
		int component = panelCheckBox.getComponentCount(); 
        for (int i = 0; i < component; i++)
		{
			String name = (String) panelCheckBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JCheckBox")
			{
				if (schemaList.length() > 0)
					schemaList += ":";
				JCheckBox nameCheckBox = (JCheckBox) panelCheckBox.getComponent(i);
				schemaList += nameCheckBox.getText();
			}
		}      
        return schemaList;		
	}
	
	private String getSelectedSchemaList()
	{
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		if (srcVendor.equalsIgnoreCase("access"))
		{
			if (cfg.getSelectSchemaName() == null || cfg.getSelectSchemaName().length() == 0)
			{
				cfg.setSelectSchemaName("ADMIN");
			}
			return cfg.getSelectSchemaName();
		}
		String schemaList = "";
		int component = panelCheckBox.getComponentCount(); 
        for (int i = 0; i < component; i++)
		{
			String name = (String) panelCheckBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JCheckBox")
			{
				JCheckBox nameCheckBox = (JCheckBox) panelCheckBox.getComponent(i);
				if (nameCheckBox.isSelected())
				{
					if (!schemaList.equals(""))
						schemaList += ":";
					schemaList += nameCheckBox.getText();
				}
			}
		}   
        return schemaList;
	}
	
	private void ToggleSchemaCheckBoxes(boolean selected)
	{
		int component = panelCheckBox.getComponentCount(); 
        for (int i = 0; i < component; i++)
		{
			String name = (String) panelCheckBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JCheckBox")
			{
				JCheckBox nameCheckBox = (JCheckBox) panelCheckBox.getComponent(i);
			    nameCheckBox.setSelected(selected);
			}
		}      
	}
	
	private void setSrcServerBox(int pos, String vendor)
	{
		clearSrcServerBox();
		if (pos == 0)
		{
			textfieldSrcServer.setColumns(40);
			srcServerBox.add(textfieldSrcServer);
		} else
		{
			textfieldSrcServer.setColumns(20);
			textfieldInstanceName.setColumns(20);
			srcServerBox.add(textfieldSrcServer);
			srcServerBox.add(Box.createHorizontalGlue());
			srcServerBox.add(textfieldInstanceName);	
			if (vendor.equalsIgnoreCase("informix"))
			{
				textfieldInstanceName.setToolTipText("Specify Informix Server Name here if using ifxjdbc driver otherwise keep this blank");
			} else
			{
				textfieldInstanceName.setToolTipText("Specify SQL Server Named Instance Name here otherwise keep this blank for default SQL Server");
			}
		}
		srcServerBox.revalidate();
	}
	
	private void setSchemaCheckBoxes()
	{
		clearSchemaCheckBoxes();
		boolean checkFlag = false;
		String srcSchemaList = cfg.getSrcSchName();
		String[] tmp = cfg.getSelectSchemaName().split(":");
		String[] tmp2 = srcSchemaList.split(":");
		if (srcSchemaList != null && srcSchemaList.length() > 0)
		{
			panelCheckBox.add(btnSelectAll);
			for (int i = 0; i < tmp2.length; ++i)
			{
				if (tmp != null)
				{
					checkFlag = false;
					for (int j = 0; j < tmp.length; ++j)
						if (tmp2[i].equals(tmp[j]))
							checkFlag = true;
				}
				if (!tmp2[i].equals(""))
				{
				   JCheckBox  cb = new JCheckBox(tmp2[i], checkFlag);
				   panelCheckBox.add(cb);
				}
			}
		    panelCheckBox.revalidate();
		}
	}
	
	private void setSybaseParams(String vendor)
	{
		if (vendor.equalsIgnoreCase("sybase"))
		{
			cfg.setCaseSensitiveTabColName(cfg.getCaseSensitiveTabColName());
			cfg.setMapTimeToTimestamp(cfg.getMapTimeToTimestamp());
		}
	}
	
	private void setAccessFields(String vendor)
	{
		if (vendor.equals("access"))
		{
			btnSrcTestConn.setEnabled(false);
			btnSrcJDBC.setEnabled(false);
			btnExtract.setEnabled(true);
			checkboxPipe.setEnabled(false);
			checkboxSyncLoad.setEnabled(false);
			btnCreateScript.setEnabled(true);
			btnMeetScript.setEnabled(false);
			textfieldSrcPortNum.setEnabled(false);
			textfieldSrcDatabase.setEnabled(false);
			textfieldSrcUserID.setEnabled(false);
			textfieldSrcPassword.setEnabled(false);
			textfieldSrcJDBC.setEnabled(false);
			cfg.setFetchSize("0");
			checkboxDDL.setEnabled(false);
			checkboxObjects.setEnabled(false);
		} else
		{
			btnSrcTestConn.setEnabled(true);
			btnSrcJDBC.setEnabled(true);
			btnExtract.setEnabled(false);
			checkboxPipe.setEnabled(false);
			checkboxPipe.setEnabled(false);
			btnCreateScript.setEnabled(false);
			btnMeetScript.setEnabled(false);
			textfieldSrcPortNum.setEnabled(true);
			textfieldSrcDatabase.setEnabled(true);
			textfieldSrcUserID.setEnabled(true);
			textfieldSrcPassword.setEnabled(true);
			textfieldSrcJDBC.setEnabled(true);
			checkboxDDL.setEnabled(true);
			checkboxObjects.setEnabled(true);
			cfg.setFetchSize("100");
		}
		if (cfg.isDataExtracted())
		{
		   btnDeploy.setEnabled(true);
		   btnDropObjs.setEnabled(true);
		   btnDB2Script.setEnabled(true);
		} else
		{
		   btnDeploy.setEnabled(false);
		   btnDropObjs.setEnabled(false);
		   btnDB2Script.setEnabled(false);				
		}
		clearSchemaCheckBoxes();
	}

	private boolean createTableScript(boolean flag, String outputDir, String selectedSchemaList, String prevSelectedSchemaList)
	{
		String tableScriptFile = outputDir + IBMExtractUtilities.filesep + cfg.getSrcDBName()+".tables";
		
		
		if (flag)
		{
			if (IBMExtractUtilities.FileExists(tableScriptFile))
			{
				int n = JOptionPane.showConfirmDialog(
						StateExtractTab.this,
					    "File " + tableScriptFile + " exists. Press YES to overwrite it." + 
					    "\nPress No to use existing file",
					    "Option to overwrite table script",
					    JOptionPane.YES_NO_OPTION);
				if (n == JOptionPane.YES_OPTION)
				{
				    System.setProperty("OUTPUT_DIR", outputDir);
			    	numTables = IBMExtractUtilities.CreateTableScript(cfg.getSrcVendor(), cfg.getDstSchName(), cfg.getSrcSchName(), selectedSchemaList,
			    			cfg.getSrcServerName(), cfg.getSrcPort(), cfg.getSrcDBName(), 
			    			cfg.getSrcUid(), cfg.getSrcPwd(), getDstCompatibility(), cfg.getCaseSensitiveTabColName());
					
				}
			} else
			{
			    System.setProperty("OUTPUT_DIR", outputDir);
			    numTables = IBMExtractUtilities.CreateTableScript(cfg.getSrcVendor(), cfg.getDstSchName(), cfg.getSrcSchName(), selectedSchemaList,
		    			cfg.getSrcServerName(), cfg.getSrcPort(), cfg.getSrcDBName(), 
		    			cfg.getSrcUid(), cfg.getSrcPwd(), getDstCompatibility(), cfg.caseSensitiveTabColName);			
			}			
		} else
		{
			if (!IBMExtractUtilities.FileExists(tableScriptFile))
			{
				    System.setProperty("OUTPUT_DIR", outputDir);
			    	numTables = IBMExtractUtilities.CreateTableScript(cfg.getSrcVendor(), cfg.getDstSchName(), cfg.getSrcSchName(), selectedSchemaList,
			    			cfg.getSrcServerName(), cfg.getSrcPort(), cfg.getSrcDBName(), 
			    			cfg.getSrcUid(), cfg.getSrcPwd(), getDstCompatibility(), cfg.getCaseSensitiveTabColName());
					
			}
			else
			{
		     if(!prevSelectedSchemaList.equalsIgnoreCase(selectedSchemaList))
		     {
		    	 System.setProperty("OUTPUT_DIR", outputDir);
			    	numTables = IBMExtractUtilities.CreateTableScript(cfg.getSrcVendor(), cfg.getDstSchName(), cfg.getSrcSchName(), selectedSchemaList,
			    			cfg.getSrcServerName(), cfg.getSrcPort(), cfg.getSrcDBName(), 
			    			cfg.getSrcUid(), cfg.getSrcPwd(), getDstCompatibility(), cfg.getCaseSensitiveTabColName());
					
		     }
			}
		}
		int numFileLimit = IBMExtractUtilities.GetFileNumLimit();
		if (numFileLimit != Integer.MAX_VALUE)
		{
        	if (numTables > (numFileLimit/4))
        	{
        		int n = JOptionPane.showConfirmDialog(
    					StateExtractTab.this,
    				    "The output of 'ulimit -n' reported is " + numFileLimit +
    				    "\nPlease ask your system administrator to set this limit to unlimited or a high number." +
    				    "\nThe IDMT behavior may become unpredicatble due to this limit." +
    				    "\n\nDo you still want to continue",
    				    "Check File Num Limit",
    				    JOptionPane.YES_NO_OPTION);
    			if (n == JOptionPane.NO_OPTION)
    			{
    				return false;
    			}
        	}
		}
    	if (cfg.getNumJVM() > 1)
    	{
    		boolean optimize = true;
    		int n = JOptionPane.showConfirmDialog(
					StateExtractTab.this,
				    "Do you want to count number of rows to optimize unload",
				    "Optimize unload process",
				    JOptionPane.YES_NO_OPTION);
			if (n == JOptionPane.NO_OPTION)
				optimize = false;
    		GenerateParallelScripts.GenParallelScripts(outputDir, cfg.getSrcVendor(), tableScriptFile, cfg.getNumJVM(), 
    				cfg.getNumThreads(), cfg.getSrcServerName(), cfg.getSrcDBName(), cfg.getSrcPort(), cfg.getSrcUid(), cfg.getSrcPwd(), optimize);
			SetLabelMessage(lblMessage,cfg.getSrcDBName()+".tables file created for extract for " + cfg.getNumJVM() + " JVM", false);
    	} else
    	{
			if (IBMExtractUtilities.Message.equals(""))
			{
				SetLabelMessage(lblMessage,cfg.getSrcDBName()+".tables file created for extract", false);
				SetLabelMessage(lblMessage, "Scripts generated successfully", false);
			} else
			{
				SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
			}		
    	}		
    	return true;
	}
	
	private boolean generateMeetScript(String Vendor)
	{
		String schemaList;
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		
        String outputDir = textfieldOutputDir.getText();
		if (outputDir == null || outputDir.equals(""))
		{
			SetLabelMessage(lblMessage,"Specify Output Directory", true);
			return false;
		}
	    System.setProperty("OUTPUT_DIR", outputDir);
		schemaList = getSelectedSchemaList();
        if (srcVendor.equalsIgnoreCase("access"))
        	schemaList = "ADMIN";
        if (schemaList.equals(""))
        {
        	SetLabelMessage(lblMessage,"No schema selected.", true);
        	return false; 
        }
		cfg.setSelectSchemaName(schemaList);
		if (Vendor.equalsIgnoreCase("mssql"))
		{
			String s = (String)JOptionPane.showInputDialog(
					StateExtractTab.this,
                    "Type-in MTK Home Directory Name to create scripts that will invoke MTK in batch mode\n"
                    + "to convert SQL Server Procedures",
                    "MTK Home Directory Name",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    cfg.getMtkHome());
			if (s != null && s.length() > 0)
				cfg.setMtkHome(s);
		}
        setValues();
        if (cfg.getSelectSchemaName().equals(""))
        {
        	SetLabelMessage(lblMessage, "No schema Selected.", true);
        	return false;
        }
    	try 
    	{
    		cfg.writeMeetScript(textfieldOutputDir.getText(), Vendor);    		
	    } catch (IOException e1) {
			e1.printStackTrace();
		}	    
	    return true;				
	}
	
	private boolean generateScripts(boolean flag)
	{
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		String selectedSchemaList;
		
		if (!checkboxDB2.isSelected())
		{
	        if (textfieldDstDatabase.getText().equals(""))
	        {
	        	SetLabelMessage(lblMessage,"Please specify DB2 database name.", true);
	        	textfieldDstDatabase.requestFocusInWindow();
	        	return false;
	        }
	
	        if (textfieldDstJDBC.getText().equals(""))
	        {
	        	SetLabelMessage(lblMessage,"Please specify DB2 JDBC name.", true);
	        	textfieldDstJDBC.requestFocusInWindow();
	        	return false;
	        }
		}

        if (!outputDirandThread())
        	return false;

        selectedSchemaList = getSelectedSchemaList();
        if (srcVendor.equalsIgnoreCase("access"))
        	selectedSchemaList = "ADMIN";
        if (selectedSchemaList.equals(""))
        {
        	SetLabelMessage(lblMessage,"No schema selected for migration.", true);
        	return false; 
        }
		String prevSelectedSchemaList = cfg.getSelectSchemaName();
        cfg.setSelectSchemaName(selectedSchemaList);
		
        setValues();
        if (cfg.getSelectSchemaName().equals(""))
        {
        	SetLabelMessage(lblMessage, "No schema Selected.", true);
        	return false;
        }
	    
        cfg.writeConfigFile();		
    	cfg.getParamValues();
    	
		boolean retValue = createTableScript(flag, cfg.getOutputDirectory(), selectedSchemaList,prevSelectedSchemaList);
    	try 
    	{
	    	if (cfg.getNumJVM() > 1)
	    	{
	    		for (int i = 0; i < cfg.getNumJVM(); ++i)
	    		{
	    			String dirName = cfg.getOutputDirectory() + IBMExtractUtilities.filesep + "work" +
					    IBMExtractUtilities.pad((i+1), 2, "0");
		    		cfg.writeUnload(dirName, cfg.unload, i+1);
		    		cfg.writeRowCount(dirName);
		    		if (Constants.db2())
		    		{
			    		if (cfg.getLoadException().equalsIgnoreCase("true"))
			    			cfg.writeExceptRowCount(dirName);
			    		cfg.writeFixCheck(dirName);
			    		cfg.writeDataCheck(dirName);
		    		}
	    		}
	    	} else
	    	{
				cfg.writeGeninput();
	    		cfg.writeUnload(cfg.getOutputDirectory(), cfg.unload, 0);
	    		cfg.writeRowCount(cfg.getOutputDirectory());
	    		if (Constants.db2())
	    		{	
		    		if (cfg.getLoadException().equalsIgnoreCase("true"))
		    			cfg.writeExceptRowCount(cfg.getOutputDirectory());
		    		cfg.writeFixCheck(cfg.getOutputDirectory());
		    		cfg.writeDataCheck(cfg.getOutputDirectory());
	    		}
	    	}
	    } catch (IOException e1) {
			e1.printStackTrace();
		}
	    schemaTableChangeListener.actionPerformed(null);
	    return retValue;
	}
	
	private boolean outputDirandThread()
	{
        String outputDir = textfieldOutputDir.getText();
		if (outputDir == null || outputDir.equals(""))
		{
			SetLabelMessage(lblMessage,"Specify Output Directory", true);
			return false;
		}
		
        String numThreads = textfieldNumTreads.getText();
		if (numThreads == null || numThreads.equals(""))
		{
			SetLabelMessage(lblMessage,"Specify Number of threads > 0", true);
			return false;
		}
		try
		{
			int num = Integer.valueOf(numThreads);
			if (num < 0)
			{
				SetLabelMessage(lblMessage,"Specify Number of threads > 0", true);
				return false;
			}
		} catch (Exception e)
		{
			SetLabelMessage(lblMessage,"Invalid value specified for number of threads.", true);
			return false;
		}
        String numJVM = textfieldNumJVM.getText();
		if (numJVM == null || numJVM.equals(""))
		{
			SetLabelMessage(lblMessage,"Specify Number of JVM > 0", true);
			return false;
		}
		try
		{
			int num = Integer.valueOf(numJVM);
			if (num < 0)
			{
				SetLabelMessage(lblMessage,"Specify Number of JVM > 0", true);
				return false;
			}
		} catch (Exception e)
		{
			SetLabelMessage(lblMessage,"Invalid value specified for number of JVM.", true);
			return false;
		}
		return true;
	}
	
	private boolean validatePipeLoad()
	{
		if (validateSrcFields() && validateDstFields())
		{
			setValues();
			return true;
		} 
		SetLabelMessage(lblMessage,txtMessage, true);
		return false;
	}
	
	private boolean validateDstFields()
	{
		txtMessage = "";
		if (textfieldDstJDBC.getText().equals(""))
		{
			txtMessage = "Please specify "+getDstVendor()+"  JDBC Drivers";			
			return false;
		}
		if (textfieldDstServer.getText().equals(""))
		{
			txtMessage = "Please specify destination server";			
			return false;
		}
		if (textfieldDstPortNum.getText().equals(""))
		{
			txtMessage = "Please specify destination port number";			
			return false;
		} else
		{
			try
			{
			   Integer.parseInt(textfieldDstPortNum.getText());
			} catch (Exception e)
			{
				txtMessage = "Invalid number. Please specify destination port number";
				return false;
			}
		}
		if (textfieldDstDatabase.getText().equals(""))
		{
			txtMessage = "Please specify destination database";			
			return false;
		}
		if (textfieldDstUserID.getText().equals(""))
		{
			txtMessage = "Please specify destination user id";			
			return false;
		}
		if (textfieldDstPassword.getText().equals(""))
		{
			txtMessage = "Please specify destination password";			
			return false;
		}
		return true;
	}
	
	public void setValues()
	{
		cfg.setSrcServer(textfieldSrcServer.getText());
		cfg.setSrcPort(textfieldSrcPortNum.getText());
		if (((String)comboSrcVendor.getSelectedItem()).equalsIgnoreCase(Constants.db2luw) || ((String)comboSrcVendor.getSelectedItem()).equals(Constants.zdb2))
		{
			cfg.setSrcDBName(textfieldSrcDatabase.getText().toUpperCase());
		} else
		{
			cfg.setSrcDBName(textfieldSrcDatabase.getText());
		}
		if (((String)comboSrcVendor.getSelectedItem()).equals("mssql"))
		{
			btnMeetScript.setText("Generate Input for MTK");
		} else
		{
			btnMeetScript.setText("Generate Input for MEET");			
		}
		cfg.setSrcPwd(textfieldSrcPassword.getText());
		cfg.setSrcUid(textfieldSrcUserID.getText());
		cfg.setSrcVendor((String)comboSrcVendor.getSelectedItem());		
		cfg.setSrcJDBC(textfieldSrcJDBC.getText());
		cfg.setExtractDDL(String.valueOf(checkboxDDL.isSelected()));
		cfg.setDB2Installed(String.valueOf(!checkboxDB2.isSelected()));
		cfg.setExtractData(String.valueOf(checkboxData.isSelected()));
		cfg.setExtractObjects(String.valueOf(checkboxObjects.isSelected()));
		cfg.setUsePipe(String.valueOf(checkboxPipe.isSelected()));
		cfg.setSyncLoad(String.valueOf(checkboxSyncLoad.isSelected()));
		cfg.setLimitExtractRows(textLimitExtractRows.getText());
		cfg.setLimitLoadRows(textLimitLoadRows.getText());
		cfg.setNumThreads(textfieldNumTreads.getText());
		cfg.setNumJVM(textfieldNumJVM.getText());
		if (!checkboxDB2.isSelected())
		{
			cfg.setDstServer(textfieldDstServer.getText());
			cfg.setDstPort(textfieldDstPortNum.getText());
			cfg.setDstDBName(textfieldDstDatabase.getText().toUpperCase());
			cfg.setDstPwd(textfieldDstPassword.getText());
			cfg.setDstUid(textfieldDstUserID.getText());
			cfg.setDstVendor(getDstVendor());
			cfg.setDstJDBC(textfieldDstJDBC.getText());
		}
		cfg.setOutputDirectory(textfieldOutputDir.getText());		
		cfg.setSrcSchName(getSchemaList());
		cfg.setSelectSchemaName(getSelectedSchemaList());
		cfg.setDbclob(optionCodes[MenuBarView.OPTION_DBCLOBS][1]);
		cfg.setTrimTrailingSpaces(optionCodes[MenuBarView.OPTION_TRAILING_BLANKS][1]);
		cfg.setGraphic(optionCodes[MenuBarView.OPTION_GRAPHICS][1]);
		cfg.setRegenerateTriggers(optionCodes[MenuBarView.OPTION_SPLIT_TRIGGER][1]);
		boolean isRemote = IBMExtractUtilities.isIPLocal(textfieldDstServer.getText());
		cfg.setRemoteLoad((isRemote) ? "false" : "true");
		cfg.setCompressTable(optionCodes[MenuBarView.OPTION_COMPRESS_TABLE][1]);
		cfg.setCompressIndex(optionCodes[MenuBarView.OPTION_COMPRESS_INDEX][1]);		
		cfg.setExtractPartitions(optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS][1]);
		cfg.setExtractHashPartitions(optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS][1]);
		cfg.setValidObjects(optionCodes[MenuBarView.OPTION_MEET_VALID_OBJECTS][1]);
		cfg.setLoadStats(optionCodes[MenuBarView.OPTION_LOAD_STATS][1]);
		cfg.setNorowwarnings(optionCodes[MenuBarView.OPTION_NOROWWARNINGS][1]);
		cfg.setInstanceName(textfieldInstanceName.getText());
		cfg.setDB2Compatibility(getDstCompatibility());
	}
	
	private boolean validateSrcFields()
	{
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		txtMessage = "";
		if (textfieldSrcJDBC.getText().equals(""))
		{
			if (!srcVendor.equalsIgnoreCase("access"))
			{
			   txtMessage = "Please specify source JDBC Driver name";			
			   return false;
			}
		}
		if (textfieldSrcServer.getText().equals(""))
		{
			txtMessage = "Please specify source server";			
			return false;
		}
		if (textfieldSrcPortNum.getText().equals(""))
		{
			txtMessage = "Please specify source port number";			
			return false;
		} else
		{
			try
			{
			   Integer.parseInt(textfieldSrcPortNum.getText());
			} catch (Exception e)
			{
				txtMessage = "Invalid number. Please specify source port number";
				return false;
			}
		}
		if (textfieldSrcDatabase.getText().equals(""))
		{
			txtMessage = "Please specify source database";			
			return false;
		}
		if (textfieldSrcUserID.getText().equals(""))
		{
			txtMessage = "Please specify source user id";			
			return false;
		}
		if (textfieldSrcPassword.getText().equals(""))
		{
			txtMessage = "Please specify source password";			
			return false;
		}
		if (srcVendor.equalsIgnoreCase("informix") && textfieldSrcJDBC.getText().matches(".*ifxjdbc.jar.*"))
		{
			if (textfieldInstanceName.getText().equals(""))
			{
				txtMessage = "Please specify informix server name. This field is next to the server name";			
				return false;				
			}
		}
		return true;
	}
	
	public void SetCFGValues()
	{
		String tmpSrcVendor = "", cfgSrcVendor = cfg.getSrcVendor(), cfgDstVendor = cfg.getDstVendor();
		textfieldDstDatabase.setText(cfg.getDstDBName());
		comboSrcVendor.setSelectedItem(cfg.getSrcVendor());
		tmpSrcVendor = (String) comboSrcVendor.getSelectedItem();
	    comboDstVendor.setEnabled(true);
	    textfieldDstDatabase.setEnabled(true);
	    
		if (cfgDstVendor != null && cfgDstVendor.length() > 0)
	    {
	    	if (cfgDstVendor.equalsIgnoreCase(Constants.netezza))
	    	{
	 		   comboDstVendor.setSelectedIndex(2);
	 		   textfieldDstDatabase.setEnabled(false);
	    	}
	    	else if (cfgDstVendor.equalsIgnoreCase(Constants.zdb2))
	    	{
	 	 	   comboDstVendor.setSelectedIndex(3);	    		
	    	}
	 		else if (cfgDstVendor.equalsIgnoreCase(Constants.db2luw))
	 		{
	 	 	    comboDstVendor.setSelectedIndex(1);	 			
	 		}
	 		else
	 		{
	 	 	    comboDstVendor.setSelectedIndex(0);	 			
	 		}
	    } else
	    {
			if (cfgSrcVendor.equals("oracle"))
			{
			    comboDstVendor.setSelectedIndex(1);
			} 
			else if (cfgSrcVendor.equals(Constants.sybase))
			{
				comboDstVendor.setSelectedIndex(1);			
			}
			else if (cfgSrcVendor.equals(Constants.mssql))
			{
				if (!cfg.getCustomMapping().equalsIgnoreCase("false"))
				{
					comboDstVendor.setSelectedIndex(1);
				    comboDstVendor.setEnabled(false);
				}
			}
		}
		if (tmpSrcVendor.equals(Constants.informix) || tmpSrcVendor.equals(Constants.mssql))
		{
			textfieldInstanceName.setText(cfg.getInstanceName());
			setSrcServerBox(1, tmpSrcVendor);
		} else
		{
			textfieldInstanceName.setText("");
			setSrcServerBox(0, tmpSrcVendor);
		}
		textfieldSrcServer.setText(cfg.getSrcServer());
		textfieldDstServer.setText(cfg.getDstServer());
		if (cfg.getSrcPort() == 0)
			textfieldSrcPortNum.setText("" + cfg.getDefaultSrcVendorPort(tmpSrcVendor));
		else
		    textfieldSrcPortNum.setText("" + cfg.getSrcPort());
		if (cfg.getDstPort() == 0)
		   textfieldDstPortNum.setText("" + cfg.getDefaultSrcVendorPort(Constants.db2luw));
		else
		   textfieldDstPortNum.setText("" + cfg.getDstPort());
		textfieldSrcDatabase.setText(cfg.getSrcDBName());
		textfieldSrcPassword.setText(cfg.getSrcPwd());
		textfieldDstDatabase.setText(cfg.getDstDBName());
		textfieldDstPassword.setText(cfg.getDstPwd());
		textfieldSrcUserID.setText(cfg.getSrcUid());
		textfieldDstUserID.setText(cfg.getDstUid());
		btnSrcTestConn.setText("Connect to " + tmpSrcVendor.toUpperCase());
		checkboxDB2.setSelected(!Boolean.valueOf(cfg.getDB2Installed()));
		checkboxDDL.setSelected(Boolean.valueOf(cfg.getExtractDDL()));
		checkboxData.setSelected(Boolean.valueOf(cfg.getExtractData()));
		checkboxObjects.setSelected(Boolean.valueOf(cfg.getExtractObjects()));
		setExtractButtonTitle();
		setDeployButtonTitle();
		checkboxPipe.setSelected(Boolean.valueOf(cfg.getUsePipe()));
		checkboxSyncLoad.setSelected(Boolean.valueOf(cfg.getSyncLoad()));
		textLimitExtractRows.setText(cfg.getLimitExtractRows());
		textLimitLoadRows.setText(cfg.getLimitLoadRows());
		textfieldNumTreads.setText(""+cfg.getNumThreads());
		textfieldNumJVM.setText(""+cfg.getNumJVM());
		textfieldSrcJDBC.setText(cfg.getSrcJDBC());
		textfieldDstJDBC.setText(cfg.getDstJDBC());
		textfieldOutputDir.setText(cfg.getOutputDirectory());	
		
		if (checkboxObjects.isSelected())
		{				
			setSyncPipeCheckBoxes(false);
		} else
		{
			setSyncPipeCheckBoxes(true);
		}
		
		optionCodes[MenuBarView.OPTION_DBCLOBS][1] = cfg.getDbclob();
		optionCodes[MenuBarView.OPTION_TRAILING_BLANKS][1] = cfg.getTrimTrailingSpaces();
		optionCodes[MenuBarView.OPTION_GRAPHICS][1] = cfg.getGraphic();
		optionCodes[MenuBarView.OPTION_SPLIT_TRIGGER][1] = cfg.getRegenerateTriggers();
		optionCodes[MenuBarView.OPTION_COMPRESS_TABLE][1] = cfg.getCompressTable();
		optionCodes[MenuBarView.OPTION_COMPRESS_INDEX][1] = cfg.getCompressIndex();		
		optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS][1] = cfg.getExtractPartitions();	
		optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS][1] = cfg.getExtractHashPartitions();	
		optionCodes[MenuBarView.OPTION_MEET_VALID_OBJECTS][1] = cfg.getValidObjects();	
		optionCodes[MenuBarView.OPTION_LOAD_STATS][1] = cfg.getLoadStats();	
		optionCodes[MenuBarView.OPTION_NOROWWARNINGS][1] = cfg.getNorowwarnings();			
		setAccessFields(tmpSrcVendor);
		setSchemaCheckBoxes();
		
		if (checkboxDB2.isSelected())
		{
			setDB2NotAvailableFields(false); 
		} else
		{
			setDB2NotAvailableFields(true);
		}
	}

    public String getDB2InstanceName()
    {
    	return lblDB2Instance.getText();
    }
    
	public String getOutputDir()
	{
		return textfieldOutputDir.getText();
	}	
	
	public String getDBName()
	{
		return textfieldSrcDatabase.getText();
	}
	
	public String getVendor()
	{
		return (String) comboSrcVendor.getSelectedItem();
	}
}
