/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.net.URL;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.jgoodies.looks.Options;
import com.jgoodies.looks.plastic.PlasticLookAndFeel;
import com.jgoodies.looks.windows.WindowsLookAndFeel;

public class MenuBarView
{
	public JMenuItem menuExecuteAll;
	public JMenuItem menuExecute;
	public JMenuItem menuRevalidate;
	public JMenuItem menuRevalidateAll;
	public JMenuItem menuRefresh;
	public JMenuItem menuDiscard;
	
	public static final Integer OPTION_TRAILING_BLANKS = new Integer(0);
	public static final Integer OPTION_DBCLOBS = new Integer(1);
	public static final Integer OPTION_GRAPHICS = new Integer(2);
	public static final Integer OPTION_SPLIT_TRIGGER = new Integer(3);
	public static final Integer OPTION_COMPRESS_TABLE = new Integer(4);
	public static final Integer OPTION_COMPRESS_INDEX = new Integer(5);
	public static final Integer OPTION_EXTRACT_PARTITIONS = new Integer(6);
	public static final Integer OPTION_EXTRACT_HASH_PARTITIONS = new Integer(7);
	public static final Integer OPTION_GENERATE_CONS_NAMES = new Integer(8);
	public static final Integer OPTION_USE_BESTPRACTICE_TSNAMES = new Integer(9);
	public static final Integer OPTION_MEET_VALID_OBJECTS = new Integer(10);
	public static final Integer OPTION_CASE_SENSITIVE_TAB_COL_NAME = new Integer(11);
	public static final Integer OPTION_LOAD_STATS = new Integer(12);
	public static final Integer OPTION_NOROWWARNINGS = new Integer(13);
	
	public static final Integer DEPLOY_LOGIN = new Integer(0);	
	public static final Integer DEPLOY_GROUP = new Integer(1);
	public static final Integer DEPLOY_TSBP = new Integer(2);
	public static final Integer DEPLOY_ROLE = new Integer(3);
	public static final Integer DEPLOY_TABLE = new Integer(4);
	public static final Integer DEPLOY_SEQUENCE = new Integer(5);
	public static final Integer DEPLOY_DEFAULT = new Integer(6);
	public static final Integer DEPLOY_CHECK_CONSTRAINTS = new Integer(7);
	public static final Integer DEPLOY_PRIMARY_KEY = new Integer(8);
	public static final Integer DEPLOY_UNIQUE_INDEX = new Integer(9);
	public static final Integer DEPLOY_INDEX = new Integer(10);
	public static final Integer DEPLOY_FOREIGN_KEYS = new Integer(11);
	public static final Integer DEPLOY_TYPE = new Integer(12);
	public static final Integer DEPLOY_FUNCTION = new Integer(13);
	public static final Integer DEPLOY_VIEW = new Integer(14);
	public static final Integer DEPLOY_MQT = new Integer(15);
	public static final Integer DEPLOY_TRIGGER = new Integer(16);
	public static final Integer DEPLOY_PROCEDURE = new Integer(17);
	public static final Integer DEPLOY_PACKAGE = new Integer(18);
	public static final Integer DEPLOY_PACKAGE_BODY = new Integer(19);
	public static final Integer DEPLOY_DIRECTORY = new Integer(20);
	public static final Integer DEPLOY_GRANT = new Integer(21);
	public static final Integer DEPLOY_SYNONYM = new Integer(22);
			
	public JCheckBoxMenuItem[] optionMenu = new JCheckBoxMenuItem[14];
	public JCheckBoxMenuItem[] deployMenu = new JCheckBoxMenuItem[23];
	
	private String[][] deployCodes;
	private String[][] optionCodes;
	private IBMExtractConfig cfg = null;

	JMenuBar buildMenuBar(Settings settings,
		IBMExtractConfig cfg,
		String[][] optionCodes,
		String[][] deployCodes,	
		ActionListener helpActionListener,
        ActionListener aboutActionListener,
        ActionListener getNewVersionActionListener,
        ActionListener executeAllListener,
        ActionListener executeListener, 
        ActionListener revalidateListener,
        ActionListener db2FileOpenActionListener, 
        ActionListener revalidateAllListener,
        ActionListener refreshListener, 
        ActionListener discardListener        
		) 
	{
		this.cfg = cfg;
		this.optionCodes = optionCodes;
		this.deployCodes = deployCodes;
		JMenuBar bar = new JMenuBar();
		bar.putClientProperty(Options.HEADER_STYLE_KEY,
							  settings.getMenuBarHeaderStyle());
		bar.putClientProperty(PlasticLookAndFeel.BORDER_STYLE_KEY,
							  settings.getMenuBarPlasticBorderStyle());
		bar.putClientProperty(WindowsLookAndFeel.BORDER_STYLE_KEY,
							  settings.getMenuBarWindowsBorderStyle());
		bar.putClientProperty(PlasticLookAndFeel.IS_3D_KEY,
							  settings.getMenuBar3DHint());

		bar.add(buildFileMenu(
				executeAllListener, 
				executeListener, 
				revalidateListener, 
				db2FileOpenActionListener, 
				revalidateAllListener, 
				refreshListener, 
				discardListener));
		bar.add(buildOptionMenu());
		bar.add(buildDeployMenu());
		bar.add(buildHelpMenu(helpActionListener, aboutActionListener, getNewVersionActionListener));
		return bar;
	}

	/**
	 * Builds and returns the file menu.
	 */
	private JMenu buildFileMenu(
			ActionListener executeAllListener, 
			ActionListener executeListener, 
			ActionListener revalidateListener,
			ActionListener db2FileOpenActionListener,
			ActionListener revalidateAllListener,
	        ActionListener refreshListener, 
	        ActionListener discardListener) 
	{
		JMenuItem item;
		
		JMenu menu = createMenu("File", 'F');
		
		item = createMenuItem("Select DB2 Objects Directory",
                readImageIcon("open.gif"),
                'O',
                KeyStroke.getKeyStroke("ctrl O"));
		
        if (db2FileOpenActionListener != null) 
        {
    		item.addActionListener(db2FileOpenActionListener);
        }		
        menu.add(item); 
		
        menuRefresh = createMenuItem("Refresh objects\u2026",
                readImageIcon("valid.gif"),
                'L',
                KeyStroke.getKeyStroke("ctrl L"));
        if (refreshListener != null) 
        {
        	menuRefresh.addActionListener(refreshListener);
        }		
        menuRefresh.setEnabled(false);
        menu.add(menuRefresh);
        
        menu.addSeparator();
        
		menuExecuteAll = createMenuItem("Execute All Statements\u2026",
                              readImageIcon("srcdb.png"),
                              'A',
                              KeyStroke.getKeyStroke("ctrl A"));
        if (executeAllListener != null) 
        {
        	menuExecuteAll.addActionListener(executeAllListener);
        }
        menuExecuteAll.setEnabled(false);
		menu.add(menuExecuteAll);
		
		menuExecute = createMenuItem("Execute Selected Statements\u2026",
                readImageIcon("dstdb.png"),
                'E',
                KeyStroke.getKeyStroke("ctrl E"));
        if (executeListener != null) 
        {
        	menuExecute.addActionListener(executeListener);
        }		
        menuExecute.setEnabled(false);
        menu.add(menuExecute);
        
        menu.addSeparator();

        menuRevalidateAll = createMenuItem("Revalidate All Statements\u2026",
                readImageIcon("revalidate.png"),
                'R',
                KeyStroke.getKeyStroke("ctrl R"));
        if (revalidateAllListener != null) 
        {
        	menuRevalidateAll.addActionListener(revalidateAllListener);
        }		
        menuRevalidateAll.setEnabled(false);
        menu.add(menuRevalidateAll);

        menuRevalidate = createMenuItem("Revalidate Selected Statements\u2026",
                readImageIcon("valid.gif"),
                'H',
                KeyStroke.getKeyStroke("ctrl H"));
        if (revalidateListener != null) 
        {
        	menuRevalidate.addActionListener(revalidateListener);
        }		
        menuRevalidate.setEnabled(false);
        menu.add(menuRevalidate);
        
        menu.addSeparator();

        menuDiscard = createMenuItem("Do not deploy these objects\u2026",
                readImageIcon("valid.gif"),
                'C',
                KeyStroke.getKeyStroke("ctrl C"));
        if (discardListener != null) 
        {
        	menuDiscard.addActionListener(discardListener);
        }		
        menuDiscard.setEnabled(false);
        menu.add(menuDiscard);
               
        if (!isQuitInOSMenu()) {
            menu.addSeparator();
            item = createMenuItem("Exit", 'x');
            menu.add(item);
            item.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e)
				{					
				   System.exit(1);					
				}
            });
        }
		return menu;
	}

	private void setSubMenu(String cbName, final int idx, final String menuHeading, JMenu menu, final JCheckBoxMenuItem[] subMenu, final String[][] codes)
	{
		subMenu[idx] = createCheckBoxMenuItem(menuHeading, false);
		subMenu[idx].setName(cbName);
		subMenu[idx].setEnabled(true);		   
		subMenu[idx].addChangeListener(new ChangeListener() {
				public void stateChanged(ChangeEvent e) 
				{
					JCheckBoxMenuItem source = (JCheckBoxMenuItem) e.getSource();
					if (source.isEnabled())
						source.setText(menuHeading);
				}
		});
		boolean caseSenstivity = (cfg.getCaseSensitiveTabColName() == null ? false : Boolean.valueOf(cfg.getCaseSensitiveTabColName()));
		subMenu[idx].setIcon(readImageIcon("check.gif"));
		subMenu[idx].setSelectedIcon(readImageIcon("check_selected.gif"));
		if (cbName.equals("OPTION_TRAILING_BLANKS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getTrimTrailingSpaces()));
		else if (cbName.equals("OPTION_DBCLOBS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getDbclob()));
		else if (cbName.equals("OPTION_GRAPHICS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getGraphic()));
		else if (cbName.equals("OPTION_SPLIT_TRIGGER"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getRegenerateTriggers()));			
		else if (cbName.equals("OPTION_COMPRESS_TABLE"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getCompressTable()));		
		else if (cbName.equals("OPTION_COMPRESS_INDEX"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getCompressIndex()));		
		else if (cbName.equals("OPTION_EXTRACT_PARTITIONS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getExtractPartitions()));		
		else if (cbName.equals("OPTION_EXTRACT_HASH_PARTITIONS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getExtractHashPartitions()));		
		else if (cbName.equals("OPTION_GENERATE_CONS_NAMES"))
			subMenu[idx].setSelected(!Boolean.valueOf(cfg.getRetainConstraintsName()));		
		else if (cbName.equals("OPTION_USE_BESTPRACTICE_TSNAMES"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getUseBestPracticeTSNames()));		
		else if (cbName.equals("OPTION_MEET_VALID_OBJECTS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getValidObjects()));		
		else if (cbName.equals("OPTION_CASE_SENSITIVE_TAB_COL_NAME"))
			subMenu[idx].setSelected(!caseSenstivity);		
		else if (cbName.equals("OPTION_LOAD_STATS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getLoadStats()));		
		else if (cbName.equals("OPTION_NOROWWARNINGS"))
			subMenu[idx].setSelected(Boolean.valueOf(cfg.getNorowwarnings()));		
		//optionsMenu[idx].addActionListener(action);
		subMenu[idx].addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e) 
			{
				boolean value = subMenu[idx].isSelected();
				codes[idx][1] = Boolean.toString(value);
				JCheckBoxMenuItem o = (JCheckBoxMenuItem) e.getSource();
				if (o.getName().equals("OPTION_TRAILING_BLANKS"))
				{
					cfg.setTrimTrailingSpaces(Boolean.toString(value));
					codes[OPTION_TRAILING_BLANKS][1] = Boolean.toString(value);
				} 
				else if (o.getName().equals("OPTION_DBCLOBS"))
				{
					cfg.setDbclob(Boolean.toString(value));
					codes[OPTION_DBCLOBS][1] = Boolean.toString(value);
				} 
				else if (o.getName().equals("OPTION_GRAPHICS"))
				{
					cfg.setGraphic(Boolean.toString(value));
					codes[OPTION_GRAPHICS][1] = Boolean.toString(value);
				} 
				else if (o.getName().equals("OPTION_SPLIT_TRIGGER"))
				{
					cfg.setRegenerateTriggers(Boolean.toString(value));
					codes[OPTION_SPLIT_TRIGGER][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_COMPRESS_TABLE"))
				{
					cfg.setCompressTable(Boolean.toString(value));
					codes[OPTION_COMPRESS_TABLE][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_COMPRESS_INDEX"))
				{
					cfg.setCompressTable(Boolean.toString(value));
					codes[OPTION_COMPRESS_INDEX][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_EXTRACT_PARTITIONS"))
				{
					cfg.setExtractPartitions(Boolean.toString(value));
					codes[OPTION_EXTRACT_PARTITIONS][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_EXTRACT_HASH_PARTITIONS"))
				{
					cfg.setExtractHashPartitions(Boolean.toString(value));
					codes[OPTION_EXTRACT_HASH_PARTITIONS][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_GENERATE_CONS_NAMES"))
				{
					cfg.setRetainConstraintsName(Boolean.toString(!value));
					codes[OPTION_GENERATE_CONS_NAMES][1] = Boolean.toString(!value);
				}
				else if (o.getName().equals("OPTION_USE_BESTPRACTICE_TSNAMES"))
				{
					cfg.setUseBestPracticeTSNames(Boolean.toString(value));
					codes[OPTION_USE_BESTPRACTICE_TSNAMES][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_MEET_VALID_OBJECTS"))
				{
					cfg.setValidObjects(Boolean.toString(value));
					codes[OPTION_MEET_VALID_OBJECTS][1] = Boolean.toString(value);
				}
				else if (o.getName().equals("OPTION_CASE_SENSITIVE_TAB_COL_NAME"))
				{
					cfg.setCaseSensitiveTabColName(Boolean.toString(!value));
					codes[OPTION_CASE_SENSITIVE_TAB_COL_NAME][1] = Boolean.toString(!value);
				}
				else if (o.getName().equals("OPTION_LOAD_STATS"))
				{
					cfg.setLoadStats(Boolean.toString(value));
					codes[OPTION_LOAD_STATS][1] = Boolean.toString(value);
				}				
				else if (o.getName().equals("OPTION_NOROWWARNINGS"))
				{
					cfg.setNorowwarnings(Boolean.toString(value));
					codes[OPTION_NOROWWARNINGS][1] = Boolean.toString(value);
				}				
			}			
		});
		menu.add(subMenu[idx]);
	}
	
	
	/**
	 * Builds and returns a menu with different JCheckBoxMenuItems.
	 */
	private JMenu buildOptionMenu()	
	{
		JMenu menu = createMenu("Options", 'O');

		setSubMenu("OPTION_TRAILING_BLANKS",  OPTION_TRAILING_BLANKS, "Trim trailing blanks during unload", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_DBCLOBS",  OPTION_DBCLOBS, "Turn DB CLOB to varchar during unload", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_GRAPHICS",  OPTION_GRAPHICS, "Turn graphics char to normal char", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_SPLIT_TRIGGER",  OPTION_SPLIT_TRIGGER, "Split multiple action Triggers", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_COMPRESS_TABLE",  OPTION_COMPRESS_TABLE, "Compress Tables", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_COMPRESS_INDEX",  OPTION_COMPRESS_INDEX, "Compress Index", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_EXTRACT_PARTITIONS",  OPTION_EXTRACT_PARTITIONS, "Extract Partitions", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_EXTRACT_HASH_PARTITIONS",  OPTION_EXTRACT_HASH_PARTITIONS, "Extract Hash Partitions", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_GENERATE_CONS_NAMES",  OPTION_GENERATE_CONS_NAMES, "Use Generated Constraints Names", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_USE_BESTPRACTICE_TSNAMES",  OPTION_USE_BESTPRACTICE_TSNAMES, "Use Best Practice Tablespace Definitions", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_MEET_VALID_OBJECTS",  OPTION_MEET_VALID_OBJECTS, "Extract Valid Objects for MEET input", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_CASE_SENSITIVE_TAB_COL_NAME",  OPTION_CASE_SENSITIVE_TAB_COL_NAME, "Convert Tab/Col Names to Upper Case from Source", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_LOAD_STATS",  OPTION_LOAD_STATS, "Generate Statistics in DB2 Load", menu, optionMenu, optionCodes);
		setSubMenu("OPTION_NOROWWARNINGS",  OPTION_NOROWWARNINGS, "NOROWWARNINGS is added to the DB2 LOAD statement", menu, optionMenu, optionCodes);
		
		return menu;
	}

	/** 
	 * Builds and returns a menu with different JCheckBoxMenuItems.
	 */
	private JMenu buildDeployMenu()	
	{
		JMenu menu = createMenu("Deploy", 'Y');

		setSubMenu("DEPLOY_LOGIN",  DEPLOY_LOGIN, "Include Login in interactive Deploy", menu, deployMenu, deployCodes);
		if (Constants.netezza())
		   setSubMenu("DEPLOY_TSBP",  DEPLOY_TSBP, "Include CREATE DATABASE in interactive Deploy", menu, deployMenu, deployCodes);
		else
		   setSubMenu("DEPLOY_TSBP",  DEPLOY_TSBP, "Include BUFFER POOL/TABLE SPACE in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_ROLE",  DEPLOY_ROLE, "Include ROLE in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_SEQUENCE",  DEPLOY_SEQUENCE, "Include SEQUENCES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_TABLE",  DEPLOY_TABLE, "Include TABLES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_DEFAULT",  DEPLOY_DEFAULT, "Include DEFAULTS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_CHECK_CONSTRAINTS",  DEPLOY_CHECK_CONSTRAINTS, "Include CHECK CONSTRAINTS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_PRIMARY_KEY",  DEPLOY_PRIMARY_KEY, "Include PRIMARY KEYS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_UNIQUE_INDEX",  DEPLOY_UNIQUE_INDEX, "Include UNIQUE INDEXES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_INDEX",  DEPLOY_INDEX, "Include INDEXES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_FOREIGN_KEYS",  DEPLOY_FOREIGN_KEYS, "Include FOREIGN KEYS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_TYPE",  DEPLOY_TYPE, "Include TYPE in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_FUNCTION",  DEPLOY_FUNCTION, "Include FUNCTIONS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_VIEW",  DEPLOY_VIEW, "Include VIEWS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_MQT",  DEPLOY_MQT, "Include MQT in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_TRIGGER",  DEPLOY_TRIGGER, "Include TRIGGERS in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_PROCEDURE",  DEPLOY_PROCEDURE, "Include PROCEDURES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_PACKAGE",  DEPLOY_PACKAGE, "Include PACKAGES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_PACKAGE_BODY",  DEPLOY_PACKAGE_BODY, "Include PACKAGE BODIES in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_DIRECTORY",  DEPLOY_DIRECTORY, "Include Directory in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_GRANT",  DEPLOY_GRANT, "Include Grants in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_GROUP",  DEPLOY_GROUP, "Include Users in interactive Deploy", menu, deployMenu, deployCodes);
		setSubMenu("DEPLOY_GROUP",  DEPLOY_SYNONYM, "Include Synonyms in interactive Deploy", menu, deployMenu, deployCodes);
		return menu;
	}
	
	/**
	 * Builds and returns the help menu.
	 */
	private JMenu buildHelpMenu(
        ActionListener helpActionListener,
        ActionListener aboutActionListener,
        ActionListener getNewVersionActionListener) {

		JMenu menu = createMenu("Help", 'H');

		JMenuItem item;
        item = createMenuItem("Help Contents", readImageIcon("help.gif"), 'H');
        if (helpActionListener != null) {
    		item.addActionListener(helpActionListener);
        }
        item = createMenuItem("Check New Version", readImageIcon("check_selected.gif"), 'C');
        if (getNewVersionActionListener != null) {
    		item.addActionListener(getNewVersionActionListener);
        }
        menu.add(item);
        if (!isAboutInOSMenu()) {
            menu.addSeparator();
            item = createMenuItem("About", 'a');
            item.addActionListener(aboutActionListener);
            menu.add(item);
        }

		return menu;
	}


    // Factory Methods ********************************************************

    protected JMenu createMenu(String text, char mnemonic) {
        JMenu menu = new JMenu(text);
        menu.setMnemonic(mnemonic);
        return menu;
    }


    protected JMenuItem createMenuItem(String text) {
        return new JMenuItem(text);
    }


    protected JMenuItem createMenuItem(String text, char mnemonic) {
        return new JMenuItem(text, mnemonic);
    }


    protected JMenuItem createMenuItem(String text, char mnemonic, KeyStroke key) {
        JMenuItem menuItem = new JMenuItem(text, mnemonic);
        menuItem.setAccelerator(key);
        return menuItem;
    }


    protected JMenuItem createMenuItem(String text, Icon icon) {
        return new JMenuItem(text, icon);
    }


    protected JMenuItem createMenuItem(String text, Icon icon, char mnemonic) {
        JMenuItem menuItem = new JMenuItem(text, icon);
        menuItem.setMnemonic(mnemonic);
        return menuItem;
    }


    protected JMenuItem createMenuItem(String text, Icon icon, char mnemonic, KeyStroke key) {
        JMenuItem menuItem = createMenuItem(text, icon, mnemonic);
        menuItem.setAccelerator(key);
        return menuItem;
    }


    protected JRadioButtonMenuItem createRadioButtonMenuItem(String text, boolean selected) {
        return new JRadioButtonMenuItem(text, selected);
    }


    protected JCheckBoxMenuItem createCheckBoxMenuItem(String text, boolean selected) {
        return new JCheckBoxMenuItem(text, selected);
    }


    // Subclass will override the following methods ***************************

    /**
     * Checks and answers whether the quit action has been moved to an
     * operating system specific menu, e.g. the OS X application menu.
     *
     * @return true if the quit action is in an OS-specific menu
     */
    protected boolean isQuitInOSMenu() {
        return false;
    }


    /**
     * Checks and answers whether the about action has been moved to an
     * operating system specific menu, e.g. the OS X application menu.
     *
     * @return true if the about action is in an OS-specific menu
     */
    protected boolean isAboutInOSMenu() {
        return false;
    }


    // Higher Level Factory Methods *****************************************

	/**
	 * Creates and returns a JCheckBoxMenuItem
	 * with the given enablement and selection state.
	 */
	private JCheckBoxMenuItem createCheckItem(boolean enabled, boolean selected) {
		JCheckBoxMenuItem item = createCheckBoxMenuItem(
			getToggleLabel(enabled, selected),
			selected);
		item.setEnabled(enabled);
		item.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				JCheckBoxMenuItem source = (JCheckBoxMenuItem) e.getSource();
				source.setText(getToggleLabel(source.isEnabled(), source.isSelected()));
			}
		});
		return item;
	}


	/**
	 *  Returns an appropriate label for the given enablement and selection state.
	 */
	protected String getToggleLabel(boolean enabled, boolean selected) {
		String prefix = enabled  ? "Enabled" : "Disabled";
		String suffix = selected ? "Selected" : "Deselected";
		return prefix + " and " + suffix;
	}


    // Helper Code ************************************************************

    /**
     * Looks up and returns an icon for the specified filename suffix.
     */
    private ImageIcon readImageIcon(String filename) {
        URL url = getClass().getResource("resources/images/" + filename);
        return new ImageIcon(url);
    }
}
