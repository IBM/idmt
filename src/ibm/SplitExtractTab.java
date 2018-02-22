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
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.net.URL;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTree;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import jsyntaxpane.DefaultSyntaxKit;
import jsyntaxpane.SyntaxDocument;
import jsyntaxpane.actions.ActionUtils;

import com.jgoodies.forms.factories.Borders;
import com.jgoodies.looks.LookUtils;
import com.jgoodies.looks.Options;
import com.jgoodies.uif_lite.component.Factory;
import com.jgoodies.uif_lite.component.UIFSplitPane;
import com.jgoodies.uif_lite.panel.SimpleInternalFrame;

public class SplitExtractTab extends JFrame implements TreeSelectionListener
{
	private static final long serialVersionUID = 1876263871408702989L;
    public static String linesep = System.getProperty("line.separator");
	private IBMExtractConfig cfg;
	private Connection mainConn = null;
	private String outputDirectory, sqlTerminator;
	private JLabel lblSplash = IBMExtractGUI2.lblSplash;
	
	private ActionListener executeActionListener;
	private ActionListener revalidateActionListener;
	private ActionListener executeAllActionListener;
	private ActionListener revalidateAllActionListener;
	private ActionListener discardActionListener;
	
	//private JTextArea topArea = new JTextArea();
	//private JEditorPane topArea;
	private JTree treeStatements;
	private JTable table = new JTable();
	
	private CustomTreeCellRenderer ctcr = new CustomTreeCellRenderer();	
	private JTabbedPane tabbedPane;
	private DefaultMutableTreeNode rootStatementTree;
	private JComponent lowerRight;
	private Timer busy = null;
	private RunDeployObjects task = null;
	
	private javax.swing.JEditorPane topArea;
	private JPopupMenu popup;

	
	private Hashtable<String, PLSQLInfo> hashPLSQLSource = 
		new Hashtable<String, PLSQLInfo>();
	
	ImageIcon errorIcon = readImageIcon("error.png");
    ImageIcon passedIcon = readImageIcon("passed.gif");
    ImageIcon failedIcon = readImageIcon("valid.gif");
    ImageIcon removeIcon = readImageIcon("remove.gif");

	public SplitExtractTab(IBMExtractConfig cfg,
			ActionListener executeAllActionListener,
			ActionListener revalidateActionListener,
			ActionListener executeActionListener, 
			ActionListener revalidateAllActionListener,
			ActionListener discardActionListener)
	{
		this.cfg = cfg;
		sqlTerminator = IBMExtractUtilities.sqlTerminator;
		this.executeAllActionListener = executeAllActionListener;
		this.revalidateActionListener = revalidateActionListener;
		this.executeActionListener = executeActionListener;
		this.revalidateAllActionListener = revalidateAllActionListener;
		this.discardActionListener = discardActionListener;
		
		popup = new JPopupMenu();
		JMenuItem mi;
		mi = new JMenuItem("Get Source");
		mi.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				getSource();
			}
		});
		mi.setActionCommand("Source");		
		popup.add(mi);
		
		mi = new JMenuItem("Get Detailed Error Message");
		mi.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				getDetailedErrorMessage();
			}
		});
		mi.setActionCommand("ErrorMessage");		
		popup.add(mi);				
		
		mi = new JMenuItem("Auto Fix Code");
		mi.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				getTurboFix();
			}
		});
		mi.setActionCommand("AutoFix");		
		popup.add(mi);				

		mi = new JMenuItem("Get Original Source");
		mi.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				getOriginalSource();
			}
		});
		mi.setActionCommand("GetOriginalSource");		
		popup.add(mi);				

		SetTimer();
	}
	
	private void getDetailedErrorMessage()
	{
		int rowIndex = table.getSelectedRow();
        
        String type = table.getValueAt(rowIndex, 0).toString();
        if (type.equalsIgnoreCase("connection"))
        	return;
        String schema = table.getValueAt(rowIndex, 1).toString();
        String objectName = table.getValueAt(rowIndex, 2).toString();
        String errorCode = table.getValueAt(rowIndex, 4).toString();
        String lineNum = table.getValueAt(rowIndex, 5).toString();
        String message = table.getValueAt(rowIndex, 6).toString();
        topArea.setText("SQL Error Code = " + errorCode + linesep +
        		        "Object Type    = " + type + linesep +
        		        "Object Name    = " + objectName + linesep +
        		        "Schema         = " + schema + linesep +
        		        "Line Num       = " + lineNum + linesep +
        		        "Message        = " + message + linesep);
    	topArea.setCaretPosition(0);		
	}
	
	private void getTurboFix()
	{
		int rowIndex = table.getSelectedRow();
        String type = table.getValueAt(rowIndex, 0).toString();
        if (type.equalsIgnoreCase("connection"))
        	return;
        String schema = table.getValueAt(rowIndex, 1).toString();
        String object = table.getValueAt(rowIndex, 2).toString();
        String code = table.getValueAt(rowIndex, 3).toString();
        String lineNum = table.getValueAt(rowIndex, 5).toString();
        String oraSQL = getNode(type, schema,  code+object);
        String outputOraSQL = TinyMig.TurboFixOraSQL(oraSQL, false, true, false, "--IDMT:", true);  
    	topArea.setText(outputOraSQL);
        if (!(lineNum == null || lineNum.equals("")))
        {
            int pos = ActionUtils.getDocumentPosition(topArea, Integer.parseInt(lineNum), 0);
            topArea.setCaretPosition(pos);
        } else
        {
        	topArea.setCaretPosition(0);
        }				
	}
	
	private void getSource()
	{
		int rowIndex = table.getSelectedRow();
        
        String type = table.getValueAt(rowIndex, 0).toString();
        if (type.equalsIgnoreCase("connection"))
        	return;
        String schema = table.getValueAt(rowIndex, 1).toString();
        String object = table.getValueAt(rowIndex, 2).toString();
        String code = table.getValueAt(rowIndex, 3).toString();
        String lineNum = table.getValueAt(rowIndex, 5).toString();
    	topArea.setText(getNode(type, schema,  code+object));
        //System.out.println("Type=" + type + " schema = " + schema + " object " + code+object + " line at " + lineNum);
        if (!(lineNum == null || lineNum.equals("")))
        {
            int pos = ActionUtils.getDocumentPosition(topArea, Integer.parseInt(lineNum), 0);
            topArea.setCaretPosition(pos);
        } else
        {
        	topArea.setCaretPosition(0);
        }		
	}
	
	private void getOriginalSource()
	{
		int rowIndex = table.getSelectedRow();
        
        String type = table.getValueAt(rowIndex, 0).toString();
        if (type.equalsIgnoreCase("connection"))
        	return;
        String schema = table.getValueAt(rowIndex, 1).toString();
        String object = table.getValueAt(rowIndex, 2).toString();
        String code = table.getValueAt(rowIndex, 3).toString();
        String lineNum = table.getValueAt(rowIndex, 5).toString();
    	topArea.setText(getOriginalNode(type, schema,  code+object));
        //System.out.println("Type=" + type + " schema = " + schema + " object " + code+object + " line at " + lineNum);
        if (!(lineNum == null || lineNum.equals("")))
        {
            int pos = ActionUtils.getDocumentPosition(topArea, Integer.parseInt(lineNum), 0);
            topArea.setCaretPosition(pos);
        } else
        {
        	topArea.setCaretPosition(0);
        }		
	}
	
    JComponent build() 
    {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setOpaque(false);
        panel.setBorder(Borders.DIALOG_BORDER);
        panel.add(buildHorizontalSplit());                        
        return panel;
    }
        
	/*private void jEdtTestCaretUpdate(javax.swing.event.CaretEvent evt)
	{
		if (topArea.getDocument() instanceof SyntaxDocument)
		{
			SyntaxDocument sDoc = (SyntaxDocument) topArea.getDocument();
			Token t = sDoc.getTokenAt(evt.getDot());
			if (t != null)
			{
				try
				{
					String tData = sDoc
							.getText(t.start, Math.min(t.length, 40));
					if (t.length > 40)
					{
						tData += "...";
					}
					lblToken.setText(t.toString() + ": " + tData);
				} catch (BadLocationException ex)
				{
					// should not happen.. and if it does, just ignore it
					System.err.println(ex);
					ex.printStackTrace();
				}
			}
		}
	}*/
	
    
	private void SetTimer()
	{
		if (busy == null)
		{
			busy = new Timer(1000, new ActionListener()
			{
				public void actionPerformed(ActionEvent a)
				{
					if (IBMExtractUtilities.DeployCompleted)
					{
						IBMExtractUtilities.DeployCompleted = false;
						lblSplash.setVisible(false);
						refreshTable(task.getTabData());
					} 					
				}
			});
			busy.start();
		}
	}

	private void connectToDB2()
	{
    	mainConn = IBMExtractUtilities.OpenConnection(cfg.getDstVendor(), 
				cfg.getDstServer(), 
				cfg.getDstPort(), cfg.getDstDBName(), cfg.getDstUid(), 
				cfg.getDstPwd()); 
		Object[][] tabData = new Object[1][7];
		if (mainConn == null)
		{
			tabData[0][0] = "Connection";
			tabData[0][1] = "Failure";
			tabData[0][2] = "";
			tabData[0][3] = "0";
			tabData[0][4] = "";
			tabData[0][5] = "";
			tabData[0][6] = IBMExtractUtilities.Message;
		} else
		{
			tabData[0][0] = "Connection";
			tabData[0][1] = "Success";
			tabData[0][2] = "";
			tabData[0][3] = "1";
			tabData[0][4] = "";
			tabData[0][5] = "";
			if (Constants.netezza())
			   tabData[0][6] = "Connection to Netezza succeeded.";
			else
			   tabData[0][6] = "Connection to DB2 succeeded.";			
		}
		refreshTable(tabData);
	}
	
    private JComponent buildHorizontalSplit() 
    {
    	JComponent left = new JScrollPane(buildMainLeftPanel());
        left.setPreferredSize(new Dimension(200, 100));

        DefaultSyntaxKit.initKit();
		topArea = new javax.swing.JEditorPane();
        JComponent upperRight = new JScrollPane(topArea);        
        upperRight.setPreferredSize(new Dimension(100, 400));
        ((JScrollPane)upperRight).setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        ((JScrollPane)upperRight).setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        topArea.setContentType("text/sql");
		topArea.setFont(new java.awt.Font("Monospaced", 0, 13));
		((JScrollPane)upperRight).setViewportView(topArea);
		SyntaxDocument sDoc = (SyntaxDocument) topArea.getDocument();
		sDoc.clearUndos();
		
        lowerRight = new JScrollPane(refreshTable(new Object[][] {{"","","","","","",""}}));
        lowerRight.setPreferredSize(new Dimension(100, 40));
    	
        JSplitPane verticalSplit = UIFSplitPane.createStrippedSplitPane(
                JSplitPane.VERTICAL_SPLIT,
                upperRight,
                lowerRight);
	    verticalSplit.setOpaque(false);
	    JSplitPane horizontalSplit = UIFSplitPane.createStrippedSplitPane(
	        JSplitPane.HORIZONTAL_SPLIT,
	        left,
	        verticalSplit);
	    horizontalSplit.setOpaque(false);
	    
	    topArea.setText("In order to use the interactive deploy. Please follow these steps.\n\n" +
	    		"1. Extract objects from Extract / Deploy tab.\n" +
	    		"2. From the Extract/Deploy tab, hit Connect button to connect to the target database " +
	    		"   such as DB2 or Netezza or DB2 on z/OS.\n" +
	    		"3. From the Extract/Deploy tab, click on the Open Directory button on the tool bar or Click Menu \n" +
	    		"   option File > Open or hit CTRL-O.\n" +
	    		"4. Select the directory where objects were extracted. The default directory is the output directory.\n" +
	    		"5. You can deploy objects contained in any other directory other than the default output directory.\n");
	    
	    return horizontalSplit;
    }
    
    private static final class SampleTableModel extends AbstractTableModel 
    {
		private static final long serialVersionUID = 3789421824163250805L;
		private final String[] columnNames;
        private final Object[][] rowData;

        SampleTableModel(Object[][] rowData, String[] columnNames) 
        {
            this.columnNames = columnNames;
            this.rowData = rowData;
        }
        
        public String getColumnName(int column) { return columnNames[column].toString(); }
        public int getRowCount() { return rowData.length; }
        public int getColumnCount() { return columnNames.length; }
        public Class getColumnClass(int column) {
            return column == 3 ? super.getColumnClass(column) : 
            	super.getColumnClass(column);
        }
        public Object getValueAt(int row, int col) { 
        	return rowData[row][col]; 
        }

        public boolean isCellEditable(int row, int column) { return false; }
        public void setValueAt(Object value, int row, int col) {
            rowData[row][col] = value;
            fireTableCellUpdated(row, col);
        }
    }

    private JTable refreshTable(Object[][] TableData) 
    {
        TableModel model = new SampleTableModel(TableData,
                new String[] { "Type", "Schema", "Object Name", "Status", "SQL Code", "Line #", "Message" });
    
        table.setModel(model);       
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        table.getColumnModel().getColumn(0).setPreferredWidth(65);
        table.getColumnModel().getColumn(1).setPreferredWidth(65);
        table.getColumnModel().getColumn(2).setPreferredWidth(115);
        table.getColumnModel().getColumn(3).setPreferredWidth(50);
        table.getColumnModel().getColumn(3).setCellRenderer(new DefaultTableCellRenderer()
        {
        	public Component getTableCellRendererComponent(
    				JTable table, Object value, boolean isSelected,
    				boolean hasFocus, int row, int column)
    		{
	        	if (value.equals("0"))
					setIcon(errorIcon);
				else if (value.equals("1"))
					setIcon(passedIcon);
				else if (value.equals("2"))
					setIcon(failedIcon);
				else
					setIcon(removeIcon);
	        	setHorizontalAlignment(JLabel.CENTER);
	        	return this;
    		}
        });
        table.getColumnModel().getColumn(4).setPreferredWidth(40);
        table.getColumnModel().getColumn(5).setPreferredWidth(40);
        table.getColumnModel().getColumn(6).setPreferredWidth(220);
        table.getColumnModel().getColumn(6).setCellRenderer(new DefaultTableCellRenderer()
        {
        	public Component getTableCellRendererComponent(
    				JTable table, Object value, boolean isSelected,
    				boolean hasFocus, int row, int column)
    		{
        		setText(value.toString());
        	    //setToolTipText(value.toString());
    			return this;
    		}
        });
        int tableFontSize    = table.getFont().getSize();
        int minimumRowHeight = tableFontSize + 6;
        int defaultRowHeight = LookUtils.IS_LOW_RESOLUTION ? 17 : 18;
        table.setRowHeight(Math.max(minimumRowHeight, defaultRowHeight));
        table.addMouseListener(new MouseAdapter()
        {
        	public void mouseClicked(MouseEvent e)
			{
				JTable target = (JTable) e.getSource();
        		if(SwingUtilities.isRightMouseButton(e) == true && e.getClickCount() == 1)
        		{
	        		int row = target.rowAtPoint(e.getPoint());
	        		target.clearSelection();
	        		target.addRowSelectionInterval(row,row);
	        		
	        		/*int rowIndex = target.getSelectedRow();
	                
	                String type = target.getValueAt(rowIndex, 0).toString();
	                String schema = target.getValueAt(rowIndex, 1).toString();
	                String object = target.getValueAt(rowIndex, 2).toString();
	                String code = target.getValueAt(rowIndex, 3).toString();
	                String lineeNum = target.getValueAt(rowIndex, 5).toString();
	                System.out.println("Type=" + type + " schema = " + schema + " object " + code+object + " line at " + lineeNum);*/	
	                popup.show(e.getComponent(), e.getX(), e.getY());
        		} else if (e.getClickCount() == 2)
        		{
        			getSource();
        		}
			}
        });
        table.updateUI();
        return table;
    }

    public void refreshTable()
    {
        Object[][] obj = new Object[][] {{"FUNCTION","CIGWMS","CIG_ROUND_QTY_FN","0", "100", "-1", "Ok Compile"}};
        refreshTable(obj);
    }
    
    private JComponent buildMainLeftPanel() 
    {    	
        final SimpleInternalFrame sif = new SimpleInternalFrame("Select DB2 Objects");
        sif.setPreferredSize(new Dimension(150, 100));
        
        tabbedPane = new JTabbedPane(SwingConstants.BOTTOM);
        tabbedPane.putClientProperty(Options.EMBEDDED_TABS_KEY, Boolean.TRUE);
        tabbedPane.addTab("DB2 Objects", Factory.createStrippedScrollPane(buildStatementsTree()));
        /*tabbedPane.addChangeListener(new ChangeListener()
        {
			public void stateChanged(ChangeEvent e)
			{
				JTabbedPane pane = (JTabbedPane)e.getSource();
				if (pane.getSelectedIndex() == 0)
				{
					sif.setTitle("Select file to execute");
				} else
				{
					sif.setTitle("Select statement(s) to execute");					
				}
			}        
        });*/
        sif.add(tabbedPane);
        return sif;
    }
                   
    class CustomTreeCellRenderer extends DefaultTreeCellRenderer
    {
		private static final long serialVersionUID = -8502054967098094234L;
    	
        public Component getTreeCellRendererComponent(JTree tree,
				Object value, boolean sel, boolean expanded, boolean leaf,
				int row, boolean hasFocus)
		{

			Component c = super.getTreeCellRendererComponent(tree, value, sel, expanded,
					leaf, row, hasFocus);
			Object nodeObj = ((DefaultMutableTreeNode) value).getUserObject();
			if (leaf)
			{
				if (c instanceof JLabel)
				{
					String str = nodeObj.toString();
					((JLabel) c).setText(str.substring(1));
					if (str.charAt(0) == '0')
					{
						setIcon(errorIcon);
					} else if (str.charAt(0) == '1')
					{
						setIcon(passedIcon);
					} else if (str.charAt(0) == '2')
					{
						setIcon(failedIcon);
					} else
					{
						setIcon(removeIcon);
					}
				}				
			}
			return this;
		}            
    }
    
    private JTree buildStatementsTree() 
    {    	
    	String str = (Constants.netezza() ? " Deploy Objects in Netezza" : " Deploy Objects in DB2");
    	rootStatementTree = new DefaultMutableTreeNode(str);
        treeStatements = new CustomJTree(rootStatementTree,
        		executeAllActionListener,
    			revalidateAllActionListener,
    			executeActionListener, 
        		revalidateActionListener,
        		discardActionListener);      
        treeStatements.addTreeSelectionListener(this);
        
        treeStatements.setCellRenderer(ctcr);
        return treeStatements;
    }
 	
    public void turboFixObject()
    {
    	
    }
    
    public void executeAllObjects()
    {    	    	
    	java.util.concurrent.ExecutorService s = java.util.concurrent.Executors.newFixedThreadPool(1);				
		lblSplash.setVisible(true);							
		task = new RunDeployObjects(sqlTerminator, outputDirectory, treeStatements, true, null);
		lblSplash.setVisible(true);
		s.execute(task);
		s.shutdown();
    }
    
    public void executeSelectedObjects()
    {    	    	
    	java.util.concurrent.ExecutorService s = java.util.concurrent.Executors.newFixedThreadPool(1);				
		lblSplash.setVisible(true);							
		task = new RunDeployObjects(sqlTerminator, outputDirectory, treeStatements, false, topArea);
		lblSplash.setVisible(true);
		s.execute(task);
		s.shutdown();
    }
    
    public void discardSelectedObjects()
    {
    	DefaultMutableTreeNode node;
    	TreePath[] paths;
    	int count;
    	
		paths = treeStatements.getSelectionPaths();
		if (paths == null) return;
		
		count = 0;
		for (int i = 0; i < paths.length; ++i)
		{
			node = (DefaultMutableTreeNode) paths[i].getLastPathComponent();    	    
	    	if (node != null)
	    	{		    	
				if (node.isLeaf())
				{
					++count;
				}
	    	}
		}
		Object[][] tabData = new Object[count][7];
		count = 0;
		for (int i = 0; i < paths.length; ++i)
		{
			node = (DefaultMutableTreeNode) paths[i].getLastPathComponent();    	    
	    	if (node == null) return;
	    	
	    	Object nodeInfo = node.getUserObject();
			if (node.isLeaf())
			{
				PLSQLInfo plsql = (PLSQLInfo) nodeInfo;			
				plsql.codeStatus = "3";
				node.setUserObject(plsql);				
				((DefaultTreeModel )treeStatements.getModel()).nodeStructureChanged((TreeNode)node);
				tabData[count][0] = plsql.type;
				tabData[count][1] = plsql.schema;
				tabData[count][2] = plsql.object;
				tabData[count][3] = plsql.codeStatus;
				tabData[count][4] = "-1";
				tabData[count][5] = "";
				tabData[count][6] = "Object was not chosen to be deployed.";
				++count;
			}
		}
		if (count > 0)
		  refreshTable(tabData);
	}
    
    public void refreshStatementsTree(Hashtable<String, String> hashTree, 
    		Hashtable<String, PLSQLInfo> hashPLSQLSource, String outputDirectory,
    		String[][] typeCodes)
    {
    	this.hashPLSQLSource = hashPLSQLSource;
    	this.outputDirectory = outputDirectory;
    	DefaultTreeModel model = (DefaultTreeModel)treeStatements.getModel();
        //model.setRoot(null);
        //treeStatements = buildStatementsTree();
    	rootStatementTree.removeAllChildren();
    	model.reload();
    	rootStatementTree.add(processHashTable(hashTree, typeCodes));
    	int row = 0;
    	while (row < treeStatements.getRowCount())
    	{
    		treeStatements.expandRow(row);
    		row++;
    	}
    	connectToDB2();
    }
    
    private TreePath find2(JTree tree, TreePath parent, Object[] nodes,
			int depth, boolean byName)
	{
		TreeNode node = (TreeNode) parent.getLastPathComponent();
		Object o = node;

		if (byName)
		{
			o = o.toString();
		}

		if (o.equals(nodes[depth]))
		{
			if (depth == nodes.length - 1)
			{
				return parent;
			}

			if (node.getChildCount() >= 0)
			{
				for (Enumeration e = node.children(); e.hasMoreElements();)
				{
					TreeNode n = (TreeNode) e.nextElement();
					TreePath path = parent.pathByAddingChild(n);
					TreePath result = find2(tree, path, nodes, depth + 1, byName);
					if (result != null)
					{
						return result;
					}
				}
			}
		}
		return null;
	}
    
    private TreePath find(JTree tree, Object[] nodes)
	{
		TreeNode root = (TreeNode) tree.getModel().getRoot();
		return find2(tree, new TreePath(root), nodes, 0, false);
	}
    
    private TreePath findByName(JTree tree, String[] names)
	{
		TreeNode root = (TreeNode) tree.getModel().getRoot();
		return find2(tree, new TreePath(root), names, 0, true);
	}
    
    private String getNode(String type, String schema, String object)
    {
    	String str = (Constants.netezza() ? " Deploy Objects in Netezza" : " Deploy Objects in DB2");
    	TreePath path = findByName(treeStatements, new String[]{str, "Objects",type,schema,object});
    	if (path == null) return "";    	
		treeStatements.setSelectionPath(path);
		treeStatements.scrollPathToVisible(path);
    	Object obj = path.getLastPathComponent();
    	DefaultMutableTreeNode node = (DefaultMutableTreeNode) obj;
		Object nodeInfo = node.getUserObject();
    	PLSQLInfo plsql = (PLSQLInfo) nodeInfo;
    	return plsql.plSQLCode;
    }
    
    private String getOriginalNode(String type, String schema, String object)
    {
    	String str = (Constants.netezza() ? " Deploy Objects in Netezza" : " Deploy Objects in DB2");
    	TreePath path = findByName(treeStatements, new String[]{str, "Objects",type,schema,object});
    	if (path == null) return "";    	
		treeStatements.setSelectionPath(path);
		treeStatements.scrollPathToVisible(path);
    	Object obj = path.getLastPathComponent();
    	DefaultMutableTreeNode node = (DefaultMutableTreeNode) obj;
		Object nodeInfo = node.getUserObject();
    	PLSQLInfo plsql = (PLSQLInfo) nodeInfo;
    	return (plsql.oldPLSQLCode == null) ? plsql.plSQLCode : plsql.oldPLSQLCode;
    }
    
    private DefaultMutableTreeNode processHashTable(Hashtable<String, String> hash,
    		String[][] typeCodes) 
    {
    	String[] nameArray;
    	String oldSchema, newSchema;
    	PLSQLInfo plsql;    	
        DefaultMutableTreeNode node = new DefaultMutableTreeNode("Objects");
        DefaultMutableTreeNode parent, child = null;;
        for (int idx = 0; idx < typeCodes.length; ++idx)        	
		{
        	if (hash.containsKey(typeCodes[idx][0]))
        	{
        		if (typeCodes[idx][1].equals("true"))
        		{
					oldSchema = "";
					String key = typeCodes[idx][0];			
					String[] vals = hash.get(key).split(":");
					//parent = new DefaultMutableTreeNode(key  + "(" + vals.length + ")");
					parent = new DefaultMutableTreeNode(key);
					node.add(parent);
					Arrays.sort(vals);
					for (int j = 0; j < vals.length; ++j)
					{
						nameArray = vals[j].split("\\.");
						newSchema = nameArray[0];
						if (!oldSchema.equalsIgnoreCase(newSchema))
						{
							child = new DefaultMutableTreeNode(nameArray[0]);
							parent.add(child);
							oldSchema = newSchema;
						}
				    	plsql = (PLSQLInfo)hashPLSQLSource.get(key + ":" + vals[j].replace('.', ':'));
				    	if (plsql == null)
				    		IBMExtractUtilities.log("plsql is null, Check " + key + " for " + vals[j]);
				    	else
						    child.add(new DefaultMutableTreeNode(new PLSQLInfo(
								plsql.codeStatus, key, newSchema, nameArray[1], plsql.lineNumber, plsql.plSQLCode, plsql.skin)));
					}
        		}
        	}
		}
        return(node);
     }
    
    protected static ImageIcon readImageIcon(String filename) {    	
    	URL url = IBMExtractGUI2.class.getResource("resources/images/" + filename);
        return new ImageIcon(url);
    }

	public void valueChanged(TreeSelectionEvent e)
	{
		DefaultMutableTreeNode node;
		Object obj = treeStatements.getLastSelectedPathComponent();

		if (obj == null)
		{
			return;
		} else
		{
			node = (DefaultMutableTreeNode) obj;
			Object nodeInfo = node.getUserObject();
			if (node.isLeaf())
			{
				if (nodeInfo instanceof PLSQLInfo)
				{
					PLSQLInfo plsql = (PLSQLInfo) nodeInfo;
					topArea.setText(plsql.plSQLCode);	
					//topArea.setWrapStyleWord(true);					
				}
			}
		}
	}    
}

class CustomJTree extends JTree implements ActionListener
{
	private static final long serialVersionUID = 1273279053102663185L;
	JPopupMenu popup, popup2;
	JMenuItem mi;

	public CustomJTree(DefaultMutableTreeNode node, 
			ActionListener executeAllActionListener,
			ActionListener revalidateAllActionListener,
			ActionListener executeActionListener, 
			ActionListener revalidateActionListener,
			ActionListener discardActionListener)
	{
		super(node);
    	String str = (Constants.netezza() ? "Netezza" : "DB2");
		popup = new JPopupMenu();
		
		mi = new JMenuItem("Deploy selected objects in " + str);
		mi.addActionListener(executeActionListener);
		mi.setActionCommand("deploy");
		popup.add(mi);
		mi = new JMenuItem("Revalidate selected objects in " + str);
		mi.addActionListener(revalidateActionListener);
		mi.setActionCommand("revalidate");
		popup.add(mi);
		mi = new JMenuItem("Do not deploy selected objects in " + str);
		mi.addActionListener(discardActionListener);
		mi.setActionCommand("discard");		
		popup.add(mi);
		popup.setOpaque(true);
		popup.setLightWeightPopupEnabled(true);
		
		popup2 =  new JPopupMenu();
		mi = new JMenuItem("Expand All");
		mi.addActionListener(this);
		mi.setActionCommand("expand");
		popup2.add(mi);
		
		mi = new JMenuItem("Collapse All");
		mi.addActionListener(this);
		mi.setActionCommand("collapse");
		popup2.add(mi);
		
		popup2.addSeparator();
		
		mi = new JMenuItem("Deploy All objects in " + str);
		mi.addActionListener(executeAllActionListener);
		mi.setActionCommand("deployAll");
		popup2.add(mi);
		mi = new JMenuItem("Revalidate All objects in " + str);
		mi.addActionListener(revalidateAllActionListener);
		mi.setActionCommand("revalidateAll");
		popup2.add(mi);
		popup2.setOpaque(true);
		popup2.setLightWeightPopupEnabled(true);

		addMouseListener(new MouseAdapter() {
			public void mousePressed(MouseEvent e) 
            {
                showPopup(e);
            }
        	public void mouseReleased(MouseEvent e)
			{
        		showPopup(e);
			}        	
    		private void showPopup(MouseEvent e) 
    		{
    			if (e.isPopupTrigger())
				{
					DefaultMutableTreeNode node;
					TreePath path = getSelectionPath();
					if (path != null)
					{
						node = (DefaultMutableTreeNode) path.getLastPathComponent();
						if (node.isLeaf())
						{
						   popup.show((JComponent) e.getSource(), e.getX(), e.getY());
						} else
						{
						   popup2.show((JComponent) e.getSource(), e.getX(), e.getY());
						}
					}
				}
    		}
		});
	}
	
	public void actionPerformed(ActionEvent e)
	{
		Object node;
		TreeModel data = getModel();
	    node = data.getRoot();
	    if (node == null) return;

		if (e.getActionCommand().equals("expand"))
		{
			int row = 0;
		    while (row < getRowCount()) 
		    {
		      expandRow(row);
		      row++;
		    }
		}
		if (e.getActionCommand().equals("collapse"))
		{
			int row = getRowCount() - 1;
		    while (row >= 0) 
		    {
		      collapseRow(row);
		      row--;
		    }
			/*node = (DefaultMutableTreeNode) dmtn.getParent();
			node.removeAllChildren();
			((DefaultTreeModel) this.getModel())
					.nodeStructureChanged((TreeNode) dmtn);*/
		}
	}
}	

