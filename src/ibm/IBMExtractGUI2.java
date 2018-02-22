/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.File;
import java.net.URL;
import java.util.Hashtable;

import javax.swing.AbstractButton;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JToggleButton;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.LookAndFeel;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.plaf.metal.DefaultMetalTheme;
import javax.swing.plaf.metal.MetalLookAndFeel;

import com.jgoodies.looks.LookUtils;
import com.jgoodies.looks.Options;
import com.jgoodies.looks.plastic.PlasticLookAndFeel;
import com.jgoodies.looks.windows.WindowsLookAndFeel;

public class IBMExtractGUI2 extends JFrame
{	
	private static final long serialVersionUID = -7554199705627733295L;

	protected static final Dimension PREFERRED_SIZE = LookUtils.IS_LOW_RESOLUTION ? new Dimension(
			1100, 750) : new Dimension(1100, 750);

	private static final String COPYRIGHT = "\u00a9 IBM Corporation All Rights Reserved.";
	
	private final Settings settings;
	// The order of this should match with menubarview deploycodes
	private String[][] optionCodes = new String[][] {{"trimTrailingSpaces","false"},{"dbclob","false"},{"graphic","false"},
			{"regenerateTriggers","false"},{"compressTable","false"},{"compressIndex","false"},
			{"extractPartitions","true"},{"extractHashPartitions","true"}, {"retainConstraintsName","false"},{"useBestPracticeTSNames","true"},
			{"validObjects","false"},{"caseSensitiveTabColName","true"},{"loadstats","false"},{"norowwarnings","true"}};
	// The order of this should match with menubarview deploycodes
	private String[][] deployCodes = new String[][] {{"LOGIN","false"},{"GROUP","false"},{"BPTS","false"},{"ROLE","false"},{"TABLE","false"},{"SEQUENCE","false"},{"DEFAULT","false"},{"CHECK_CONSTRAINTS","false"},
		{"PRIMARY_KEY","false"},{"UNIQUE_INDEX","false"},{"INDEX","false"},{"FOREIGN_KEYS","false"},{"TYPE","false"},
		{"FUNCTION","false"},{"VIEW","false"},{"MQT","false"},{"TRIGGER","false"},{"PROCEDURE","false"},{"PACKAGE","false"},{"PACKAGE_BODY","false"},
		{"DIRECTORY","false"},{"GRANT","false"},{"SYNONYM","false"}};
	
	private AbstractButton btnExecuteAll;
	private AbstractButton btnRevalidateAll;
	private AbstractButton btnExecute;
	private AbstractButton btnRevalidate;
	private AbstractButton btnDiscard;
	private AbstractButton btnWhatsNew;
	private AbstractButton btnRefresh;
	private IBMExtractConfig cfg;
	private AbstractButton btnDB2ScriptDeploy;
	private String srcVendor = "oracle";
	
	public static JLabel lblSplash = new JLabel("");	
	public String outputDirectory = null;
	private JTabbedPane tabbedPane;
	public StringBuffer buffer = new StringBuffer(); 
	
	StateExtractTab tab1;
	SplitExtractTab tab2;
	OutputFileTab tab3;
	TurboFix tab4;
	SelectTables tab5;
	SetParams tab6;
	
	MenuBarView menu = null;
	
	DB2FileOpenActionListener db2FileOpenActionListener = new DB2FileOpenActionListener();
	RefreshActionListener refreshActionListener = new RefreshActionListener();

	ExecuteAllActionListener executeAllActionListener = new ExecuteAllActionListener();	
	ExecuteActionListener executeActionListener = new ExecuteActionListener();
	
	RevalidateAllActionListener revalidateAllActionListener = new RevalidateAllActionListener();
	RevalidateActionListener revalidateActionListener = new RevalidateActionListener();
	
	DiscardActionListener discardActionListener = new DiscardActionListener ();
	
	WhatisNewActionListener whatisNewActionListener = new WhatisNewActionListener();
	
	AboutActionListener aboutActionListener = new AboutActionListener();
	CreateHelpActionListener createHelpActionListener = new CreateHelpActionListener();
	
	GetNewVersionActionListener getNewVersionActionListener = new GetNewVersionActionListener();
	
	OutputFileActionListener outputFileActionListener = new OutputFileActionListener(); 
	
	FillTextAreaTab3ActionListener fillTextAreaTab3ActionListener = new FillTextAreaTab3ActionListener();
	
	FillTextAreaTurboFixFileActionListener fillTextAreaTurboFixFileActionListener = new FillTextAreaTurboFixFileActionListener();
	
	FillTextAreaWithFileActionListener fillTextAreaWithFileActionListener = new FillTextAreaWithFileActionListener();
	
	TailOutputActionListener tailOutputActionListener = new TailOutputActionListener(); 
	
	SchemaTableChangeListener schemaTableChangeListener = new SchemaTableChangeListener();

	protected IBMExtractGUI2(Settings settings)
	{
		this.settings = settings;		

		ImageIcon icon = readImageIcon("waiting.gif");
		lblSplash.setIcon(icon);
		lblSplash.setSize(icon.getIconWidth(), icon.getIconHeight());
		lblSplash.setVisible(false);
		
		configureUI();
		build();		
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}
		
    private static Settings createDefaultSettings() 
    {
        Settings settings = Settings.createDefault();
        return settings;
    }

    final class OutputFileActionListener implements ActionListener
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tabbedPane.setSelectedIndex(2);
        	//new RefreshActionListener().actionPerformed(e);
        }    	    	
    }
    
    final class DB2FileOpenActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
			try
			{			  
			   if (tab1.getDB2InstanceName().equals(""))
			   {
						JOptionPane.showConfirmDialog(
							IBMExtractGUI2.this,
							    "Connect to "+Constants.getDbTargetName()+" first",
							    "Connect to "+Constants.getDbTargetName()+" first",
							    JOptionPane.DEFAULT_OPTION);
				   return;
			   }
			   outputDirectory = tab1.getOutputDir();
			   File f = new File(new File(outputDirectory).getCanonicalPath());				
			   JFileChooser fc = new JFileChooser();
			   fc.setCurrentDirectory(f);
			   fc.setDialogTitle("Select output directory");
			   fc.setMultiSelectionEnabled(false);
			   fc.setFileSelectionMode( javax.swing.JFileChooser.DIRECTORIES_ONLY);
			
			   int result = fc.showOpenDialog(null);
			   if( result == javax.swing.JFileChooser.CANCEL_OPTION) 
			   {
				  return;
			   }
			   File fileSelected = null;
			   fileSelected = fc.getSelectedFile();
			   if (fileSelected != null)
			   {
				   if (!fileSelected.getName().equals(""))
				   {
					  String outputDir = fileSelected.getAbsolutePath();		
					  if (!cfg.getOutputDirectory().equals(outputDir))
					  {
						  outputDirectory = outputDir;
						  cfg.setOutputDirectory(outputDir);
					      cfg.writeConfigFile();
					  }
				   }
			   }
			} catch (Exception ex)
			{
			   ex.printStackTrace();
			}
        	tabbedPane.setSelectedIndex(1);
        	new RefreshActionListener().actionPerformed(e);
        }    	
    }

    final class RefreshActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	Hashtable<String, String> hashTree = new Hashtable<String, String>();
        	Hashtable<String, PLSQLInfo> hashPLSQLSource = 
        		new Hashtable<String, PLSQLInfo>();
        	/*hash.put("View", "1my_view:2your_view:3her_view");
        	hash.put("Function", "1my_func1:2your_func2:3her_func3");
        	hash.put("Procedure", "1my_proc1:2your_proc2:3her_proc3");
        	hash.put("Package", "1my_pkg1:2your_pkg2:3her_pkg3");
        	hash.put("Package Body", "1my_pkgbody1:2your_pkgbody2:3her_pkgbody3");*/
        	if (outputDirectory == null)
        		outputDirectory = tab1.getOutputDir();
        	BuildPLSQLObjects pl = new BuildPLSQLObjects(cfg.getDB2Compatibility(), srcVendor, outputDirectory);
        	hashTree = pl.getTreeHash();
        	hashPLSQLSource = pl.getPLSQLHash();
        	tab2.refreshStatementsTree(hashTree, hashPLSQLSource, outputDirectory, deployCodes);
        }    	
    }
    
    final class ExecuteAllActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tab2.executeAllObjects();        
        }    	
    }

    final class FillTextAreaTab3ActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	String scriptName = e.getActionCommand();
        	tab3.FillTextAreaFromOutput(scriptName);   
        	tabbedPane.setSelectedIndex(2);        	
        }        
    }
    
    final class TailOutputActionListener  implements ActionListener 
    {
		public void actionPerformed(ActionEvent e)
		{
        	tabbedPane.setSelectedIndex(2);   
        	int len = buffer.toString().length();
			tab3.getTextArea().setText(buffer.toString());
			try { tab3.getTextArea().setCaretPosition(len); } catch (Exception ex) {}
		}
    }

    final class FillTextAreaTurboFixFileActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tab3.FillTextAreaFromFile(tab4.getOutputFileName());        
        	outputFileActionListener.actionPerformed(e);
        }    	
    }

    final class FillTextAreaWithFileActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tab3.FillTextAreaFromFile(tab1.getOutputDir());        
        	outputFileActionListener.actionPerformed(e);
        }    	
    }

    final class ExecuteActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tab2.executeSelectedObjects();
        }    	
    }


    final class RevalidateAllActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	JOptionPane.showMessageDialog(
                	IBMExtractGUI2.this,
                    "Revalidate All statement\n\n"
                        + COPYRIGHT + "\n\n");
        }    	
    }

    final class RevalidateActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) {
            JOptionPane.showMessageDialog(
            	IBMExtractGUI2.this,
                "Revalidate statement\n\n"
                    + COPYRIGHT + "\n\n");
        }    	
    }

    final class DiscardActionListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tab2.discardSelectedObjects();
        }   	
    }

    final class SchemaTableChangeListener implements ActionListener 
    {
        public void actionPerformed(ActionEvent e) 
        {
        	tab5.refreshTables();
        }   	
    }

    final class CreateHelpActionListener implements ActionListener 
    {
		public void actionPerformed(ActionEvent e)
		{
		}
    }
 
    final class GetNewVersionActionListener extends JDialog 
    implements ActionListener 
	{    	
		private static final long serialVersionUID = -9010340696935497145L;
	
		public void actionPerformed(ActionEvent e) 
	    {
	    	String message = new RunIDMTVersion().GetIDMTVersion();
	    	GetIDMTUpdate upd = new GetIDMTUpdate(IBMExtractGUI2.this, 
	    			"IBM Data Movement Tool", message);
	    }
	}

    final class WhatisNewActionListener extends JDialog 
        implements ActionListener 
    {    	
		private static final long serialVersionUID = -9083793275953495751L;
		public void actionPerformed(ActionEvent e) 
        {
        	tabbedPane.setSelectedIndex(2);
        	String whatisNewString = IBMExtractUtilities.GetWhatisNew();
			tab3.getTextArea().setText(whatisNewString);
			tab3.getTextArea().setCaretPosition(0);
        }
    }
    
    final class AboutActionListener implements ActionListener 
    {
    	String currentVersion = IBMExtractGUI2.class.getPackage().getImplementationVersion();
        public void actionPerformed(ActionEvent e) 
        {
            JOptionPane.showMessageDialog(IBMExtractGUI2.this
            	, "IBM Data Movement Tool " +
            	((currentVersion != null) ? currentVersion : "") + "\n\n" + COPYRIGHT);
        }
    }
        
    public JTextArea getTextArea()
    {
    	return tab3.getTextArea();
    }
    
    public void setOutputTab()
    {
    	tabbedPane.setSelectedIndex(2);
    }
    
    private Component buildToolBar() 
    {
    	String version = IBMExtractGUI2.class.getPackage().getImplementationVersion();
    	JLabel lblVersion = new JLabel();
        JToolBar toolBar = new JToolBar();
        toolBar.setFloatable(true);
        toolBar.putClientProperty("JToolBar.isRollover", Boolean.TRUE);
        
        toolBar.putClientProperty(
            Options.HEADER_STYLE_KEY,
            settings.getToolBarHeaderStyle());
        toolBar.putClientProperty(
            PlasticLookAndFeel.BORDER_STYLE_KEY,
            settings.getToolBarPlasticBorderStyle());
        toolBar.putClientProperty(
            WindowsLookAndFeel.BORDER_STYLE_KEY,
            settings.getToolBarWindowsBorderStyle());
        toolBar.putClientProperty(
            PlasticLookAndFeel.IS_3D_KEY,
            settings.getToolBar3DHint());
                        
        btnDB2ScriptDeploy = createToolBarButton("open.gif", "Select directory having objects to be deployed", db2FileOpenActionListener, KeyStroke.getKeyStroke(KeyEvent.VK_D, KeyEvent.CTRL_DOWN_MASK)); 
        btnDB2ScriptDeploy.addActionListener(db2FileOpenActionListener);
        toolBar.add(btnDB2ScriptDeploy);
        
        btnRefresh = createToolBarButton("refresh.gif", "Refresh objects to be deployed");
        btnRefresh.addActionListener(refreshActionListener);
        toolBar.add(btnRefresh);
        
        toolBar.addSeparator();

        btnExecuteAll = createToolBarButton("srcdb.png", "Deploy All objects");
        btnExecuteAll.addActionListener(executeAllActionListener);
        toolBar.add(btnExecuteAll);
        
        btnExecute = createToolBarButton("dstdb.png", "Deploy Selected Objects");
        btnExecute.addActionListener(executeActionListener);
        toolBar.add(btnExecute);
        
        toolBar.addSeparator();

        btnRevalidateAll = createToolBarButton("revalidate.png", "Revalidate All objects");
        btnRevalidateAll.addActionListener(revalidateAllActionListener);
        toolBar.add(btnRevalidateAll);
        
        btnRevalidate = createToolBarButton("valid.gif", "Revalidate selected objects");
        btnRevalidate.addActionListener(revalidateActionListener);
        toolBar.add(btnRevalidate);

        toolBar.addSeparator();

        btnDiscard = createToolBarButton("remove.gif", "Do not deploy selected objects");
        btnDiscard.addActionListener(discardActionListener);
        toolBar.add(btnDiscard);

        toolBar.addSeparator();

        toolBar.add(lblSplash);
        
        toolBar.addSeparator();

        btnWhatsNew = createToolBarButton("help.gif", "Click to see what is new");
        btnWhatsNew.addActionListener(whatisNewActionListener);
        toolBar.add(btnWhatsNew);

        if (version != null)
        {
        	lblVersion.setText("             " + version);
        	toolBar.add(lblVersion);
        }
       
        return toolBar;
    }

    protected AbstractButton createToolBarButton(String iconName, 
    		String toolTipText) 
    {
        JButton button = new JButton(readImageIcon(iconName));
        button.setToolTipText(toolTipText);
        button.setFocusable(false);
        return button;
    }

    private AbstractButton createToolBarButton(String iconName, String toolTipText, 
    		ActionListener action, KeyStroke keyStroke) 
    {
        AbstractButton button = createToolBarButton(iconName, toolTipText);
        button.registerKeyboardAction(action, keyStroke, JComponent.WHEN_IN_FOCUSED_WINDOW);
        return button;
    }
    
    private JComponent buildContentPane() 
    {
        JPanel panel = new JPanel(new BorderLayout());
        panel.add(buildToolBar(), BorderLayout.NORTH);
        panel.add(buildMainPanel(), BorderLayout.CENTER);
        return panel;
    }
    
    protected MenuBarView createMenuBuilder() 
    {
        return (menu = new MenuBarView());
    }
    
    private void build()
    {
        setContentPane(buildContentPane());
        setTitle(getWindowTitle());
        //setResizable(false);
        setJMenuBar(
            createMenuBuilder().buildMenuBar(
                settings,
                cfg,
                optionCodes,
                deployCodes,
                createHelpActionListener,
                aboutActionListener,
                getNewVersionActionListener,
                executeAllActionListener,
                executeActionListener,
                revalidateActionListener,
                db2FileOpenActionListener,
                revalidateAllActionListener,
                refreshActionListener,
                discardActionListener)); 
    	Enable(srcVendor);
        setIconImage(readImageIcon("db2.gif").getImage());
    }
    
    protected AbstractButton createToolBarRadioButton(String iconName, 
    		String toolTipText) 
    {
        JToggleButton button = new JToggleButton(readImageIcon(iconName));
        button.setToolTipText(toolTipText);
        button.setFocusable(false);
        return button;
    }

	public void Enable(String srcVendor)
	{
		if (menu == null)
			return;
		
		if (Constants.netezza())
		{
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setVisible(true);						

			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setSelected(false);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setSelected(false);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setSelected(false);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setSelected(false);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setSelected(false);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setSelected(false);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setSelected(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setSelected(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setSelected(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setSelected(true);	

			deployCodes[MenuBarView.DEPLOY_TSBP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TABLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DEFAULT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PRIMARY_KEY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_UNIQUE_INDEX][1] = "false";
			deployCodes[MenuBarView.DEPLOY_INDEX][1] = "false";
			deployCodes[MenuBarView.DEPLOY_FOREIGN_KEYS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TYPE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_FUNCTION][1] = "false";
			deployCodes[MenuBarView.DEPLOY_VIEW][1] = "true";
			deployCodes[MenuBarView.DEPLOY_MQT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TRIGGER][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PROCEDURE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE_BODY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_DIRECTORY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GRANT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GROUP][1] = "false";
			deployCodes[MenuBarView.DEPLOY_LOGIN][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "true";

			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_MEET_VALID_OBJECTS].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_CASE_SENSITIVE_TAB_COL_NAME].setVisible(false);
			return;			
		}
		
		if (srcVendor.equals(Constants.oracle))
		{
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setEnabled(true);						
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setVisible(true);						

			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setSelected(true);	

			deployCodes[MenuBarView.DEPLOY_TSBP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TABLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DEFAULT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PRIMARY_KEY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_UNIQUE_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_FOREIGN_KEYS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TYPE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_FUNCTION][1] = "true";
			deployCodes[MenuBarView.DEPLOY_VIEW][1] = "true";
			deployCodes[MenuBarView.DEPLOY_MQT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TRIGGER][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PROCEDURE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PACKAGE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PACKAGE_BODY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DIRECTORY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_GRANT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GROUP][1] = "false";
			deployCodes[MenuBarView.DEPLOY_LOGIN][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "true";

			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(true);
			menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES].setVisible(true);
			menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES].setVisible(true);
			menu.optionMenu[MenuBarView.OPTION_MEET_VALID_OBJECTS].setVisible(true);
			menu.optionMenu[MenuBarView.OPTION_CASE_SENSITIVE_TAB_COL_NAME].setVisible(true);
			
		} else if (srcVendor.equals(Constants.sybase))
		{
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setVisible(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setVisible(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setVisible(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setVisible(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setVisible(true);						
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setVisible(true);						
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setVisible(true);		
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setVisible(false);						
			
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setSelected(false);				
			
			deployCodes[MenuBarView.DEPLOY_TSBP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TABLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DEFAULT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PRIMARY_KEY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_UNIQUE_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_FOREIGN_KEYS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TYPE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_FUNCTION][1] = "false";
			deployCodes[MenuBarView.DEPLOY_VIEW][1] = "true";
			deployCodes[MenuBarView.DEPLOY_MQT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TRIGGER][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PROCEDURE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PACKAGE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE_BODY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_DIRECTORY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GRANT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_GROUP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_LOGIN][1] = "true";
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "false";

			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_MEET_VALID_OBJECTS].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_CASE_SENSITIVE_TAB_COL_NAME].setVisible(true);

			/*optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS][1] = "true";
			optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_MEET_VALID_OBJECTS][1] = "false";*/
		} else if (srcVendor.equals(Constants.zdb2))
		{
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setVisible(false);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setVisible(true);						
			
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setSelected(true);	
			
			deployCodes[MenuBarView.DEPLOY_TSBP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TABLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DEFAULT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PRIMARY_KEY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_UNIQUE_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_FOREIGN_KEYS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TYPE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_FUNCTION][1] = "false";
			deployCodes[MenuBarView.DEPLOY_VIEW][1] = "false";
			deployCodes[MenuBarView.DEPLOY_MQT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TRIGGER][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PROCEDURE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE_BODY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_DIRECTORY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GRANT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GROUP][1] = "false";
			deployCodes[MenuBarView.DEPLOY_LOGIN][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "true";

			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES].setVisible(true);
			menu.optionMenu[MenuBarView.OPTION_MEET_VALID_OBJECTS].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_CASE_SENSITIVE_TAB_COL_NAME].setVisible(true);

			/*optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_MEET_VALID_OBJECTS][1] = "false";*/			
		} else if (srcVendor.equals(Constants.db2luw) ||  srcVendor.equals(Constants.mssql) || 
				srcVendor.equals(Constants.postgres) || srcVendor.equals(Constants.mysql)
				|| srcVendor.equals(Constants.teradata) || srcVendor.equals(Constants.informix))
		{
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setVisible(false);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setVisible(false);		
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setVisible(true);						

			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setSelected(true);			
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setSelected(false);				
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setSelected(true);				
			
			deployCodes[MenuBarView.DEPLOY_TSBP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TABLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DEFAULT][1] = "true";
			deployCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_PRIMARY_KEY][1] = "true";
			deployCodes[MenuBarView.DEPLOY_UNIQUE_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_FOREIGN_KEYS][1] = "true";
			deployCodes[MenuBarView.DEPLOY_TYPE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_FUNCTION][1] = "false";
			deployCodes[MenuBarView.DEPLOY_VIEW][1] = "false";
			deployCodes[MenuBarView.DEPLOY_MQT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TRIGGER][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PROCEDURE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE_BODY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_DIRECTORY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GRANT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GROUP][1] = "false";
			deployCodes[MenuBarView.DEPLOY_LOGIN][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "true";

			if(srcVendor.equals(Constants.mssql))
			{
				menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setVisible(true);
				menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
				deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "true";
			}
			else
			{
				menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setVisible(false);
				menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
				deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "false";
			}

			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_GENERATE_CONS_NAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_USE_BESTPRACTICE_TSNAMES].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_MEET_VALID_OBJECTS].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_CASE_SENSITIVE_TAB_COL_NAME].setVisible(true);

			/*optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_MEET_VALID_OBJECTS][1] = "false";*/
		} else if (srcVendor.equals(Constants.access))
		{
			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setEnabled(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setVisible(false);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setVisible(false);	
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setVisible(false);					
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setVisible(false);					
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setVisible(false);					
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setVisible(false);						
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setVisible(false);				
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setVisible(false);
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setVisible(true);

			menu.deployMenu[MenuBarView.DEPLOY_TSBP].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_ROLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_SEQUENCE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TABLE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_DEFAULT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_CHECK_CONSTRAINTS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_PRIMARY_KEY].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_UNIQUE_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_INDEX].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FOREIGN_KEYS].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TYPE].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_FUNCTION].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_VIEW].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_MQT].setSelected(true);
			menu.deployMenu[MenuBarView.DEPLOY_TRIGGER].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PROCEDURE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE].setSelected(true);						
			menu.deployMenu[MenuBarView.DEPLOY_PACKAGE_BODY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_DIRECTORY].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GRANT].setSelected(true);	
			menu.deployMenu[MenuBarView.DEPLOY_GROUP].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_LOGIN].setSelected(false);	
			menu.deployMenu[MenuBarView.DEPLOY_SYNONYM].setSelected(true);	
			
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "false";			
			deployCodes[MenuBarView.DEPLOY_TSBP][1] = "true";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SEQUENCE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TABLE][1] = "true";
			deployCodes[MenuBarView.DEPLOY_DEFAULT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_CHECK_CONSTRAINTS][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PRIMARY_KEY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_UNIQUE_INDEX][1] = "false";
			deployCodes[MenuBarView.DEPLOY_INDEX][1] = "true";
			deployCodes[MenuBarView.DEPLOY_FOREIGN_KEYS][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TYPE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_FUNCTION][1] = "false";
			deployCodes[MenuBarView.DEPLOY_VIEW][1] = "false";
			deployCodes[MenuBarView.DEPLOY_MQT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_TRIGGER][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PROCEDURE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_ROLE][1] = "false";
			deployCodes[MenuBarView.DEPLOY_PACKAGE_BODY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_DIRECTORY][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GRANT][1] = "false";
			deployCodes[MenuBarView.DEPLOY_GROUP][1] = "false";
			deployCodes[MenuBarView.DEPLOY_LOGIN][1] = "false";
			deployCodes[MenuBarView.DEPLOY_SYNONYM][1] = "false";
			
			menu.optionMenu[MenuBarView.OPTION_TRAILING_BLANKS].setSelected(true);
			/*menu.optionMenu[MenuBarView.OPTION_DBCLOBS].setSelected(false);
			menu.optionMenu[MenuBarView.OPTION_DBCLOBS].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_GRAPHICS].setSelected(false);
			menu.optionMenu[MenuBarView.OPTION_GRAPHICS].setVisible(false);*/
			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_MEET_VALID_OBJECTS].setVisible(false);
			menu.optionMenu[MenuBarView.OPTION_CASE_SENSITIVE_TAB_COL_NAME].setVisible(true);
			/*menu.optionMenu[MenuBarView.OPTION_SPLIT_TRIGGER].setSelected(false);

			optionCodes[MenuBarView.OPTION_TRAILING_BLANKS][1] = "true";
			optionCodes[MenuBarView.OPTION_DBCLOBS][1] = "false";
			optionCodes[MenuBarView.OPTION_GRAPHICS][1] = "false";
			optionCodes[MenuBarView.OPTION_SPLIT_TRIGGER][1] = "false";*/
			optionCodes[MenuBarView.OPTION_EXTRACT_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_EXTRACT_HASH_PARTITIONS][1] = "false";
			optionCodes[MenuBarView.OPTION_MEET_VALID_OBJECTS][1] = "false";
		}
	}	

    private Component buildMainPanel() 
    {
        tabbedPane = new JTabbedPane(SwingConstants.TOP);
        tabbedPane.addChangeListener(new ChangeListener() 
        {
            public void stateChanged(ChangeEvent e) 
            {
        		if (e.getSource().equals(tabbedPane)) 
        		{
        			if (tabbedPane.getSelectedIndex() == 0)
        			{
        				btnExecuteAll.setEnabled(false);
        				btnExecute.setEnabled(false);
        				btnRevalidate.setEnabled(false);
        				btnRevalidateAll.setEnabled(false);
        				btnRefresh.setEnabled(false);
        				btnDiscard.setEnabled(false);
        				if (menu != null)
        				{
	        				menu.menuExecuteAll.setEnabled(false);
	        				menu.menuExecute.setEnabled(false);
	        				menu.menuRevalidate.setEnabled(false);
	        				menu.menuRevalidateAll.setEnabled(false);
	        				menu.menuRefresh.setEnabled(false);
	        				menu.menuDiscard.setEnabled(false);	        				
        				}
        			} else
        			{
        				btnExecuteAll.setEnabled(true);
        				btnExecute.setEnabled(true);
        				btnRevalidate.setEnabled(true);
        				btnRevalidateAll.setEnabled(true);
        				btnRefresh.setEnabled(true);
        				btnDiscard.setEnabled(true);
        				if (menu != null)
        				{
	        				menu.menuExecuteAll.setEnabled(true);
	        				menu.menuExecute.setEnabled(true);
	        				menu.menuRevalidate.setEnabled(true);
	        				menu.menuRevalidateAll.setEnabled(true);
	        				menu.menuRefresh.setEnabled(true);
	        				menu.menuDiscard.setEnabled(true);
        				}
        			}
        		}        
            }
        });
        //tabbedPane.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);

        addTabs(tabbedPane);

        tabbedPane.setBorder(new EmptyBorder(10, 10, 10, 10));
        return tabbedPane;
    }

    protected String getWindowTitle() 
    {
        return "IBM Data Movement Tool";
    }

    protected static ImageIcon readImageIcon(String filename) 
    {    	
    	URL url = IBMExtractGUI2.class.getResource("resources/images/" + filename);
        return new ImageIcon(url);
    }
    
    private void addTabs(JTabbedPane tabbedPane) 
    {
    	tabbedPane.addTab(" Extract / Deploy ", (tab1 = new StateExtractTab(this,
    			fillTextAreaTab3ActionListener,
    			fillTextAreaWithFileActionListener,
    			schemaTableChangeListener,
    			optionCodes)).build());
    	this.cfg = tab1.getCfg();
    	srcVendor = cfg.getSrcVendor();
    	tabbedPane.addTab(" Interactive Deploy ", (tab2 = new SplitExtractTab(tab1.getCfg(),
    			executeAllActionListener,
    			revalidateActionListener,
    			executeActionListener, 
    			revalidateAllActionListener,
    			discardActionListener)).build());
    	tabbedPane.addTab(" View File ", (tab3 = new OutputFileTab(buffer)).build());
    	tabbedPane.addTab(" Auto Fix ", (tab4 = new TurboFix(tab1.getCfg(),getTextArea(),
    			fillTextAreaTab3ActionListener,
    			fillTextAreaTurboFixFileActionListener
    			)).build());
        tabbedPane.addTab(" Select Tables ", (tab5 = new SelectTables(tab1)).build());    		
    	tabbedPane.addTab(" Set Params ", (tab6 = new SetParams(this, tab1, schemaTableChangeListener)).build());
    }
            
    protected void locateOnScreen(Component component) 
    {
        Dimension paneSize = component.getSize();
        Dimension screenSize = component.getToolkit().getScreenSize();
        component.setLocation(
            (screenSize.width  - paneSize.width)  / 2,
            (screenSize.height - paneSize.height) / 2);
    }
              
    private void configureUI() 
    {
        // UIManager.put("ToolTip.hideAccelerator", Boolean.FALSE);

        Options.setDefaultIconSize(new Dimension(18, 18));

        Options.setUseNarrowButtons(settings.isUseNarrowButtons());

        // Global options
        Options.setTabIconsEnabled(settings.isTabIconsEnabled());
        UIManager.put(Options.POPUP_DROP_SHADOW_ENABLED_KEY,
                settings.isPopupDropShadowEnabled());

        // Swing Settings
        LookAndFeel selectedLaf = settings.getSelectedLookAndFeel();
        if (selectedLaf instanceof PlasticLookAndFeel) {
            PlasticLookAndFeel.setPlasticTheme(settings.getSelectedTheme());
            PlasticLookAndFeel.setTabStyle(settings.getPlasticTabStyle());
            PlasticLookAndFeel.setHighContrastFocusColorsEnabled(
                settings.isPlasticHighContrastFocusEnabled());
        } else if (selectedLaf.getClass() == MetalLookAndFeel.class) {
            MetalLookAndFeel.setCurrentTheme(new DefaultMetalTheme());
        }

        // Work around caching in MetalRadioButtonUI
        JRadioButton radio = new JRadioButton();
        radio.getUI().uninstallUI(radio);
        JCheckBox checkBox = new JCheckBox();
        checkBox.getUI().uninstallUI(checkBox);

        try {
            UIManager.setLookAndFeel(selectedLaf);
        } catch (Exception e) {
            System.out.println("Can't change L&F: " + e);
        }
    }    
    
    public static void main(String[] args) 
    {
       	Settings settings = createDefaultSettings();
       	if (args.length > 0)
       	{
        	String lafClassName;
       		String laf = args[0];
	       	if (laf != null)
	       	{
	            if ("Windows".equalsIgnoreCase(laf)) {
	                lafClassName = Options.JGOODIES_WINDOWS_NAME;
	            } else if ("Plastic".equalsIgnoreCase(laf)) {
	                lafClassName = Options.PLASTIC_NAME;
	            } else if ("Plastic3D".equalsIgnoreCase(laf)) {
	                lafClassName = Options.PLASTIC3D_NAME;
	            } else if ("PlasticXP".equalsIgnoreCase(laf)) {
	                lafClassName = Options.PLASTICXP_NAME;
	            } else {
	                lafClassName = laf;
	            }
	            IBMExtractUtilities.log("L&f chosen: " + lafClassName);
	            settings.setSelectedLookAndFeel(lafClassName);
	       	}
       	}
        IBMExtractGUI2 instance = new IBMExtractGUI2(settings);        
        instance.setSize(PREFERRED_SIZE);
        instance.locateOnScreen(instance);
        instance.setVisible(true);
    }   
}