/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.Timer;

import com.jgoodies.forms.builder.DefaultFormBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class TurboFix  extends JFrame implements ActionListener
{
	private static String helpAutoFix = "<html>You can use Auto Fix to<br>1. Convert Oracle SQL*Plus scripts to " +
			"DB2 CLPPlus Scripts.<br>2. Convert Oracle DDLs to DB2.<br>3. Make auto fixes to PL/SQL code to make it compatible with DB2" +
			"<br>Click help button on the toolbar to see details.</html>";
	private static final long serialVersionUID = 416948361685257241L;
	private String sep = Constants.win() ? ";" : ":";
	private JPanel panelCheckBox = new JPanel();	
	private JLabel lblSplash = IBMExtractGUI2.lblSplash;
	private JLabel lbloutputFile = new JLabel();
	private JLabel lblMessage = new JLabel("Ready");
	private JLabel lblHelpAutoFix = new JLabel("");
	private JScrollPane scrollpane = new JScrollPane(panelCheckBox);
	private JComboBox comboSrcVendor;
	private JTextField textfieldOutputDir = new JTextField(40);
	private JButton btnOutputDir = new JButton("...");
	private JButton btnTurboFix = new JButton("Run Auto Fix");
	private JButton btnView = new JButton("View Output");
	private JButton btnDeploy = new JButton("Deploy to DB2");
	private Timer busy = null;
	private IBMExtractConfig cfg;
	private String executingScriptName = "";
	private JButton btnSrcScript = new JButton("...");
	private JTextField textfieldSrcScript = new JTextField(35);

	private JTextArea buffer;
	private ActionListener fillTextAreaTab3ActionListener;
	private ActionListener fillTextAreaTurboFixFileActionListener;

	public TurboFix(IBMExtractConfig cfg,	
			JTextArea buffer,
			ActionListener fillTextAreaTab3ActionListener,
			ActionListener fillTextAreaTurboFixFileActionListener)
	{
		this.cfg = cfg;
		this.buffer = buffer;
		this.fillTextAreaTab3ActionListener = fillTextAreaTab3ActionListener;
		this.fillTextAreaTurboFixFileActionListener = fillTextAreaTurboFixFileActionListener;
	}
	
	public String getOutputFileName()
	{
		return lbloutputFile.getText();
	}	

	JComponent build()
	{
		FormLayout layout = new FormLayout(
			    "right:max(50dlu;pref), 3dlu, pref", // columns
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu," +
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 5dlu, p, 2dlu, p");   // rows
		DefaultFormBuilder builder = new DefaultFormBuilder(layout);
		builder.setDefaultDialogBorder();
		builder.setOpaque(false);
		
		String[] srcChoices = { "oracle"};
				
		scrollpane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		scrollpane.setPreferredSize(new Dimension(300,100));
				
		comboSrcVendor = new JComboBox(srcChoices);
		
		Box srcFileBox = Box.createHorizontalBox();
		srcFileBox.add(textfieldSrcScript);
		srcFileBox.add(btnSrcScript);

		btnTurboFix.setMinimumSize(new Dimension(210,25));
		btnTurboFix.setPreferredSize(new Dimension(210,25));
		btnTurboFix.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		Box turboFixBox = Box.createHorizontalBox();
		turboFixBox.add(btnTurboFix);
		turboFixBox.add(Box.createHorizontalGlue());

		Box browseBox = Box.createHorizontalBox();
		browseBox.add(textfieldOutputDir);
		browseBox.add(btnOutputDir);

		btnView.setMinimumSize(new Dimension(210,25));
		btnView.setPreferredSize(new Dimension(210,25));
		btnView.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		Box scriptBox = Box.createHorizontalBox();
		scriptBox.add(btnView);
		scriptBox.add(Box.createHorizontalGlue());
		
		btnDeploy.setMinimumSize(new Dimension(210,25));
		btnDeploy.setPreferredSize(new Dimension(210,25));
		btnDeploy.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		Box deployBox = Box.createHorizontalBox();
		deployBox.add(btnDeploy);
		deployBox.add(Box.createHorizontalGlue());
		
		CellConstraints cc = new CellConstraints();

		builder.addLabel("Source Database:",   cc.xy (1,  1));
		builder.add(comboSrcVendor,            cc.xy (3,  1));
		builder.addSeparator("",               cc.xyw(1,  3, 3));
		builder.addLabel("Source File:",       cc.xy (1,  5));
		builder.add(srcFileBox,                cc.xyw(3,  5, 1));
		builder.addLabel("Output Directory:",  cc.xy (1,  7));
		builder.add(browseBox,                 cc.xyw(3,  7, 1));
		builder.addLabel("Output Script:",     cc.xy (1,  9));
		builder.add(lbloutputFile,             cc.xy (3,  9));
		builder.addSeparator("",               cc.xyw(1, 11, 3));
		builder.add(turboFixBox,               cc.xy (3, 13));
		builder.addSeparator("",               cc.xyw(1, 15, 3));
		builder.add(scriptBox,                 cc.xy (3, 17));
		builder.add(deployBox,                 cc.xy (3, 19));
		builder.addSeparator("",               cc.xyw(1, 21, 3));
		builder.add(lblHelpAutoFix,            cc.xy (3, 23));
		builder.add(lblMessage,                cc.xyw(1, 37, 3));

		addActionListeners();
		
    	boolean isRemote = Boolean.valueOf(cfg.getRemoteLoad());
		if (IBMExtractUtilities.isDB2Installed(isRemote))
		{
			if (!isRemote)
			   SetLabelMessage(lblMessage,"DB2 was detected.", false);
		} else
		{
			SetLabelMessage(lblMessage,IBMExtractUtilities.Message, true);
		}		
		SetTimer();		
		getValues();
		SetLabelMessage(lblHelpAutoFix, helpAutoFix, false);
		return builder.getPanel();
	}
	
	private void SetTimer()
	{
		if (busy == null)
		{
			busy = new Timer(500, new ActionListener()
			{
				public void actionPerformed(ActionEvent a)
				{
					if (IBMExtractUtilities.ScriptExecutionCompleted)
					{
						IBMExtractUtilities.ScriptExecutionCompleted = false;
						SetLabelMessage(lblMessage,"Script Execution completeted ...", false);
						lblSplash.setVisible(false);
						btnView.setEnabled(true);
						btnDeploy.setEnabled(true);		
						ActionEvent e = new ActionEvent(this,0,executingScriptName);
						fillTextAreaTab3ActionListener.actionPerformed(e);
					}
					if (IBMExtractUtilities.db2ScriptCompleted)
					{
						IBMExtractUtilities.db2ScriptCompleted = false;
						SetLabelMessage(lblMessage,"db2 script Execution completeted ...", false);
						lblSplash.setVisible(false);
					}
				}
			});
			busy.start();
		}
	}
		
	private void getValues()
	{
		textfieldOutputDir.setText(cfg.getOutputDirectory());	
		btnView.setEnabled(false);
		btnDeploy.setEnabled(false);		
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
		btnTurboFix.addActionListener(this);
		btnSrcScript.addActionListener(this);
		btnDeploy.addActionListener(this);
		comboSrcVendor.addActionListener(this);
		btnView.addActionListener(fillTextAreaTurboFixFileActionListener);
		btnOutputDir.addActionListener(this);
	}

	private String generateTurboFixScript(String outputDir, String inputFileName, String outputFileName)
	{
    	try 
    	{
    		return cfg.writeAutoFixScript(outputDir, inputFileName, outputFileName);    		
	    } catch (IOException e1) {
			e1.printStackTrace();
	    	return null;
		}	    
	}
	
	public void actionPerformed(ActionEvent e)
	{
		String srcVendor = (String) comboSrcVendor.getSelectedItem();
		SetLabelMessage(lblMessage,"", false);
		if (e.getSource().equals(comboSrcVendor)) 
		{
			
		}
		else if (e.getSource().equals(btnOutputDir)) 
		{
			try
			{
		       File f = new File(new File(".").getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select output directory");
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
				   textfieldOutputDir.setText(fileSelected.getAbsolutePath());
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}						
		}
		else if (e.getSource().equals(btnSrcScript)) 
		{
		   JOptionPane.showMessageDialog(TurboFix.this,
				    "Select script name to Auto Fix ");
		   try
		   {
			   String tmpDir = textfieldOutputDir.getText();
			   if (tmpDir == null || tmpDir.trim().length() == 0)
				   tmpDir = ".";
		       File f = new File(new File(tmpDir).getCanonicalPath());
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select "+srcVendor+"'s script to fix for DB2");
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
						  String tmp2 = textfieldSrcScript.getText();
						  if (tmp2 == null || tmp2.equals(""))
							  textfieldSrcScript.setText(tmp);					  
						  else
						  {
							  if (!tmp2.contains(tmp))
								  textfieldSrcScript.setText(tmp2+sep+tmp);
						  }	
					   }
				   }				   
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}		
		}
		else if (e.getSource().equals(btnTurboFix)) 			
		{
			String outputDir = textfieldOutputDir.getText();
			if (outputDir == null || outputDir.trim().equals(""))
			{
				SetLabelMessage(lblMessage,"Specify Output Directory", true);
				return;
			}
			String inputFileName;
			inputFileName = textfieldSrcScript.getText();
			if (inputFileName == null || inputFileName.trim().equals(""))
			{
				SetLabelMessage(lblMessage,"Specify Script to Fix", true);
				return;
			}			
			File f = new File(inputFileName);
			String outputScript = f.getName();
			int pos = outputScript.lastIndexOf(".");
			outputScript = outputScript.substring(0, pos) + "_FIXED" + outputScript.substring(pos);
			outputScript = outputDir + System.getProperty("file.separator") + outputScript;
			lbloutputFile.setText(outputScript);
			if (srcVendor.equalsIgnoreCase("oracle"))
			{
				executingScriptName = generateTurboFixScript(outputDir, inputFileName, outputScript);
				if (executingScriptName != null)
				{
					int choice = 0;
					if (IBMExtractUtilities.FileExists(executingScriptName))
				    {		    	    
							choice = JOptionPane.showConfirmDialog(
									TurboFix.this,
								    "Ready to run ... \n"+executingScriptName+"\nDo you want to run this?",
								    "Confirm running of a script",
								    JOptionPane.YES_NO_OPTION);
				    } else
				    	   executingScriptName = "";
					if (choice == 0 && executingScriptName.length() > 0)
					{
						RunScript task = null;
						java.util.concurrent.ExecutorService s = java.util.concurrent.Executors.newFixedThreadPool(1);				
						lblSplash.setVisible(true);							
						task = new RunScript(buffer, "", "", executingScriptName, 0);				   
						s.execute(task);
						s.shutdown();		
						SetLabelMessage(lblMessage,"Script '"+(new File(executingScriptName).getName())+"' started ...", false);
					}
				}
			}
		}
		else if (e.getSource().equals(btnDeploy)) 
		{
			int choice = 0;
			if (cfg.getDstDB2Instance() == null || cfg.getDstDB2Instance().equals("") || 
					cfg.getDstDB2Instance().equals("null"))
			{
				SetLabelMessage(lblMessage,"Please connect to DB2 first.", true);
				return;				
			}
			executingScriptName = lbloutputFile.getText();
			if (IBMExtractUtilities.FileExists(executingScriptName))
		    {		    	    
				choice = JOptionPane.showConfirmDialog(
							TurboFix.this,
						    "Ready to run ... \n"+executingScriptName+"\nDo you want to run this?",
						    "Confirm running of a script",
						    JOptionPane.YES_NO_OPTION);
		    } else
		    	executingScriptName = "";
			if (choice == 0)
			{
			    RunDB2Script task = null;
			    java.util.concurrent.ExecutorService s = java.util.concurrent.Executors.newFixedThreadPool(1);				
			    lblSplash.setVisible(true);							
			    task = new RunDB2Script(buffer,  
					   cfg.getDstDB2Home(), executingScriptName, false);
			    s.execute(task);
			    s.shutdown();									   
			    SetLabelMessage(lblMessage,"Script '"+executingScriptName+"' started ...", false);
			}
		}
	}
}
