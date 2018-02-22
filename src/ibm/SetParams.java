package ibm;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import com.jgoodies.forms.builder.DefaultFormBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class SetParams  extends JFrame implements ActionListener
{
	private static String helpSetParams = "<html>You can use Set Params to <br>1. Choose an "+IBMExtractUtilities.getConfigFile()+" file to load " +
		"<br>2. Modify some of the values in "+IBMExtractUtilities.getConfigFile()+" file.<br>3. Save changes to the "+IBMExtractUtilities.getConfigFile()+" file" +
		"</html>";
	private static final long serialVersionUID = 5166280186048343402L;
	private String[] truefalse = { "true", "false"};
	private JLabel lblMessage = new JLabel("Ready");
	private JLabel lblHelpSetParams = new JLabel("");
	private JButton btnSaveParams = new JButton("Save Params");
	private IBMExtractConfig cfg;
	private JButton btnParamFile = new JButton("...");
	private JButton btnLoadParam = new JButton("Load");
	private JButton btnJavaHome = new JButton("...");
	private JTextField textfieldParamFile = new JTextField(35);
	private JTextField textfieldJavaHome = new JTextField(35);
	private JTextField textfieldDataEncoding = new JTextField(35);
	private JTextField textfieldSQLEncoding = new JTextField(35);
	private JTextField textfieldVarcharLimit = new JTextField(35);
	private JTextField textfieldExtentSize = new JTextField(35);
	private JTextField textfieldLoadDirectory = new JTextField(35);
	private JTextField textfieldBatchSize = new JTextField(35);
	private JComboBox comboDebug = new JComboBox(truefalse);
	private JComboBox comboGraphic = new JComboBox(truefalse);
	private JComboBox comboDBClob = new JComboBox(truefalse);
	private JComboBox comboChar2Varchar = new JComboBox(truefalse);
	private JComboBox comboTime2Timestamp = new JComboBox(truefalse);
	private JComboBox comboTMZ2Varchar = new JComboBox(truefalse);
	private String currJavaHome = ".", currParamHome = ".";
	private StateExtractTab tab1;
	private IBMExtractGUI2 maingui;
	private ActionListener schemaTableChangeListener;

	public SetParams(IBMExtractGUI2 maingui, StateExtractTab tab1, ActionListener schemaTableChangeListener)
	{
		this.maingui = maingui;
		this.tab1 = tab1;
		this.schemaTableChangeListener = schemaTableChangeListener;
		this.cfg = tab1.getCfg();
		SetParamValues();
	}

	JComponent build()
	{
		FormLayout layout = new FormLayout(
			    "right:max(50dlu;pref), 3dlu, pref", // columns
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu," +
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 5dlu, p, 2dlu, p," +
			    "p, 2dlu, p");   // rows
		DefaultFormBuilder builder = new DefaultFormBuilder(layout);
		builder.setDefaultDialogBorder();
		builder.setOpaque(false);
		
		btnSaveParams.setMinimumSize(new Dimension(210,35));
		btnSaveParams.setPreferredSize(new Dimension(210,35));
		btnSaveParams.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		
		
		Box saveBox = Box.createHorizontalBox();
		saveBox.add(btnSaveParams);
		saveBox.add(Box.createHorizontalGlue());

		Box paramFileBox = Box.createHorizontalBox();
		paramFileBox.add(textfieldParamFile);
		paramFileBox.add(btnParamFile);
		paramFileBox.add(btnLoadParam);

		Box javaHomeBox = Box.createHorizontalBox();
		javaHomeBox.add(textfieldJavaHome);
		javaHomeBox.add(btnJavaHome);

		CellConstraints cc = new CellConstraints();

		builder.addLabel("        Param File:",     cc.xy (1,  5));
		builder.add(paramFileBox,                   cc.xyw(3,  5, 1));
		builder.addLabel("         Java Home:",     cc.xy (1,  7));
		builder.add(javaHomeBox,                    cc.xyw(3,  7, 1));
		builder.addLabel("             Debug:",     cc.xy (1,  9));
		builder.add(comboDebug,                     cc.xyw(3,  9, 1));
		builder.addLabel("     Data Encoding:",     cc.xy (1,  11));
		builder.add(textfieldDataEncoding,          cc.xy (3,  11));
		builder.addLabel("      SQL Encoding:",     cc.xy (1,  13));
		builder.add(textfieldSQLEncoding,           cc.xy (3,  13));
		builder.addLabel("           Graphic:",     cc.xy (1,  15));
		builder.add(comboGraphic,                   cc.xyw(3,  15, 1));
		builder.addLabel("            DBCLOB:",     cc.xy (1,  17));
		builder.add(comboDBClob,                    cc.xyw(3,  17, 1));
		builder.addLabel("   CHAR to VARCHAR:",     cc.xy (1,  19));
		builder.add(comboChar2Varchar,              cc.xyw(3,  19, 1));
		builder.addLabel(" TIME to TIMESTAMP:",     cc.xy (1,  21));
		builder.add(comboTime2Timestamp,            cc.xyw(3,  21, 1));
		builder.addLabel("     VARCHAR Limit:",     cc.xy (1,  23));
		builder.add(textfieldVarcharLimit,          cc.xyw(3,  23, 1));
		builder.addLabel("   EXTENT in Bytes:",     cc.xy (1,  25));
		builder.add(textfieldExtentSize,            cc.xyw(3,  25, 1));
		builder.addLabel("    Load Directory:",     cc.xy (1,  27));
		builder.add(textfieldLoadDirectory,         cc.xyw(3,  27, 1));
		builder.addLabel("Display Batch Size:",     cc.xy (1,  29));
		builder.add(textfieldBatchSize,             cc.xyw(3,  29, 1));
		builder.addLabel("Ora TMZ to Varchar:",     cc.xy (1,  31));
		builder.add(comboTMZ2Varchar,               cc.xyw(3,  31, 1));
		builder.addSeparator("",                    cc.xyw(1,  33, 3));
		builder.add(saveBox,                        cc.xy (3, 35));
		builder.add(lblHelpSetParams,               cc.xy (3, 37));
		builder.add(lblMessage,                     cc.xyw(1, 37, 3));

		addActionListeners();
		
	    SetLabelMessage(lblMessage,"", false);
		SetLabelMessage(lblHelpSetParams, helpSetParams, false);
		return builder.getPanel();
	}
	
	private void SetParamValues()
	{
		boolean maptime = (cfg.getMapTimeToTimestamp() == null ? false : Boolean.valueOf(cfg.getMapTimeToTimestamp()));
		textfieldParamFile.setText((new File(IBMExtractUtilities.getConfigFile())).getAbsolutePath());
		comboDebug.setSelectedIndex(cfg.getDebug().equalsIgnoreCase("true") ? 0 : 1);
		comboGraphic.setSelectedIndex(cfg.getGraphic().equalsIgnoreCase("true") ? 0 : 1);
		comboDBClob.setSelectedIndex(cfg.getDbclob().equalsIgnoreCase("true") ? 0 : 1);
		comboChar2Varchar.setSelectedIndex(cfg.getMapCharToVarchar().equalsIgnoreCase("true") ? 0 : 1);
		comboTime2Timestamp.setSelectedIndex(maptime ? 0 : 1);
		comboTMZ2Varchar.setSelectedIndex(cfg.getConvertOracleTimeStampWithTimeZone2Varchar().equalsIgnoreCase("true") ? 0 : 1);
		textfieldDataEncoding.setText(cfg.getEncoding());
		textfieldSQLEncoding.setText(cfg.getSqlFileEncoding());
		textfieldJavaHome.setText(cfg.getJavaHome());
		textfieldExtentSize.setText(cfg.getExtentSizeinBytes());
		textfieldVarcharLimit.setText(cfg.getVarcharLimit());
		textfieldLoadDirectory.setText(cfg.getLoadDirectory());
		textfieldBatchSize.setText(cfg.getBatchSizeDisplay());
		textfieldLoadDirectory.setToolTipText("Specify the load directory on DB2 server if you unload data on the source server.");
		textfieldBatchSize.setToolTipText("Specify a number that will display time elapsed to unload given # of rows. Specify 0 to turn the display off.");
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

	private void addActionListeners() 
	{
		btnSaveParams.addActionListener(this);
		btnParamFile.addActionListener(this);
		btnJavaHome.addActionListener(this);
		btnLoadParam.addActionListener(this);
	}

	private boolean ValidateValues()
	{
		if (textfieldParamFile.getText().length() == 0)
		{
			SetLabelMessage(lblMessage, "Specify location of Param File", true);
			return false;						
		} else
		{
			String paramName = textfieldParamFile.getText();
			if (!paramName.matches("(?i).*"+Constants.IDMT_CONFIG_FILE))
			{
				SetLabelMessage(lblMessage, "Please select "+IBMExtractUtilities.getConfigFile()+" file", true);
				return false;
			}

		}
		if (textfieldJavaHome.getText().equals(""))
		{
			SetLabelMessage(lblMessage, "Specify Java Home directory name", true);
			return false;			
		} else
		{
			if (!IBMExtractUtilities.CheckValidJavaHome(textfieldJavaHome.getText()))
			{
				SetLabelMessage(lblMessage, "Specified Java Home does not seem right.", true);
				return false;
			}
		}
		if (!IBMExtractUtilities.CheckValidEncoding(textfieldDataEncoding.getText()))
		{
			SetLabelMessage(lblMessage, "Invalid Encoding specified", true);
			return false;
		}
		if (!IBMExtractUtilities.CheckValidEncoding(textfieldSQLEncoding.getText()))
		{
			SetLabelMessage(lblMessage, "Invalid Encoding speified", true);
			return false;
		}
		if (!IBMExtractUtilities.CheckValidInteger(textfieldVarcharLimit.getText()))
		{
			SetLabelMessage(lblMessage, "Invalid varchar limit number specified", true);
			return false;
		}		
		if (!IBMExtractUtilities.CheckValidInteger(textfieldExtentSize.getText()))
		{
			SetLabelMessage(lblMessage, "Invalid extent size number specified", true);
			return false;
		}		
		if (!IBMExtractUtilities.CheckValidInteger(textfieldBatchSize.getText()))
		{
			SetLabelMessage(lblMessage, "Invalid display batch size specified", true);
			return false;
		}		
		return true;
	}
	
	private void SetCfgValues()
	{
		cfg.setDebug(comboDebug.getSelectedIndex() == 0 ? "true" : "false");
		cfg.setDbclob(comboDBClob.getSelectedIndex() == 0 ? "true" : "false");
		cfg.setVarcharLimit(textfieldVarcharLimit.getText());
		cfg.setMapTimeToTimestamp(comboTime2Timestamp.getSelectedIndex() == 0 ? "true" : "false");
		cfg.setMapCharToVarchar(comboChar2Varchar.getSelectedIndex() == 0 ? "true" : "false");
		cfg.setConvertOracleTimeStampWithTimeZone2Varchar(comboTMZ2Varchar.getSelectedIndex() == 0 ? "true" : "false");
		cfg.setExtentSizeinBytes(textfieldExtentSize.getText());
		cfg.setJavaHome(textfieldJavaHome.getText());
		cfg.setEncoding(textfieldDataEncoding.getText());
		cfg.setSqlFileEncoding(textfieldSQLEncoding.getText());
		cfg.setGraphic(comboGraphic.getSelectedIndex() == 0 ? "true" : "false");
		cfg.setBatchSizeDisplay(textfieldBatchSize.getText());
		cfg.setLoadDirectory(textfieldLoadDirectory.getText());
	}
	
	public void actionPerformed(ActionEvent e)
	{
		SetLabelMessage(lblMessage,"", false);
		if (e.getSource().equals(btnLoadParam)) 			
		{
			if (textfieldParamFile.getText().length() == 0)
			{
				SetLabelMessage(lblMessage, "Specify location of Param File", true);
				return;						
			}
			System.setProperty("IDMTConfigFile", textfieldParamFile.getText().trim());
			cfg.loadConfigFile();
			cfg.getParamValues();
			SetParamValues();
			maingui.Enable(cfg.getSrcVendor());
			tab1.SetCFGValues();
			schemaTableChangeListener.actionPerformed(null);
			SetLabelMessage(lblMessage, "Parameters loaded", false);			
		} else if (e.getSource().equals(btnSaveParams)) 			
		{
			if (!ValidateValues())
				return;
			SetCfgValues();
			
			int choice = 0;
					choice = JOptionPane.showConfirmDialog(
							SetParams.this,
						    "Do you want to save?",
						    "Confirm save params to "+Constants.IDMT_CONFIG_FILE+" file",
						    JOptionPane.YES_NO_OPTION);
			if (choice == 0)
			{
				System.setProperty("IDMTConfigFile", textfieldParamFile.getText().trim());
				cfg.writeConfigFile();
				cfg.loadConfigFile();
				cfg.getParamValues();
				tab1.SetCFGValues();
				SetLabelMessage(lblMessage, "Parameters saved in the file", false);
			}
		} else if (e.getSource().equals(btnParamFile)) 
		{
		   try
		   {
			   if (textfieldParamFile.getText().length() > 0)
			   {
				   File ff = new File(textfieldParamFile.getText());
				   if (ff.getParent() != null)
				   {
					   currParamHome = ff.getParent();
				   }
			   }
		       File f = new File(new File(currParamHome).getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select " + Constants.IDMT_CONFIG_FILE);
			   fc.setMultiSelectionEnabled(false);
			   fc.setFileSelectionMode( javax.swing.JFileChooser.FILES_ONLY);
			
			   int result = fc.showOpenDialog(null);

			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }

			   File fileSelected = fc.getSelectedFile();
			   if (fileSelected != null)
			   {
				   currParamHome = fileSelected.getParent();
				   if (currParamHome == null)
					   currParamHome = ".";
				   String name = fileSelected.getAbsolutePath();
				   if (name.matches("(?i).*"+Constants.IDMT_CONFIG_FILE))
				      textfieldParamFile.setText(name);
				   else
				   {
					  SetLabelMessage(lblMessage, "Please select "+Constants.IDMT_CONFIG_FILE+" file", true);
				   }
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}		
		} else if (e.getSource().equals(btnJavaHome)) 
		{
		   try
		   {
			   if (textfieldJavaHome.getText().length() > 0)
			   {
				   File ff = new File(textfieldJavaHome.getText());
				   if (ff.getParent() != null)
				   {
					   currJavaHome = ff.getParent();
				   }
			   }
		       File f = new File(new File(currJavaHome).getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select Java Home");
			   fc.setMultiSelectionEnabled(false);
			   fc.setFileSelectionMode( javax.swing.JFileChooser.DIRECTORIES_ONLY);
			
			   int result = fc.showOpenDialog(null);

			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }

			   File fileSelected = fc.getSelectedFile();
			   if (fileSelected != null)
			   {
				   currJavaHome = fileSelected.getParent();
				   if (currJavaHome == null)
					   currJavaHome = ".";
				   String name = fileSelected.getAbsolutePath();
				   if (IBMExtractUtilities.CheckValidJavaHome(name))
				   {
				      textfieldJavaHome.setText(name);
				   }
				   else
				   {
					  SetLabelMessage(lblMessage, "Specified Java Home does not seem right.", true);
				   }
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}		
		}
	}
}
