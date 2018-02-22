package ibm;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.EventObject;

import javax.swing.Box;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.ScrollPaneLayout;
import javax.swing.UIManager;
import javax.swing.plaf.ColorUIResource;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellEditor;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeCellEditor;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import com.jgoodies.forms.builder.DefaultFormBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class SelectTables extends JFrame implements ActionListener
{
	private static String helpSelectTables = "<html>You can use Select Tables to <br>1. Select tables that you need from the given list." +
		"<br>2. Hit Save button to save new selection.<br>3. From Extract tab, extract tables again for the tables you selected." +
		"</html>";
	private static final long serialVersionUID = 9024514242728221583L;

	private JLabel lblHelpSelectTables = new JLabel("");
	private JButton btnExpand = new JButton("Collapse All");
	private JButton btnSelectAll = new JButton("Select All");
	private JButton btnInverse = new JButton("Inverse");
	private JButton btnSaveTables = new JButton("Save");
	private JTree tableTree = null;
	private JPanel panelSchemaTextFieldBox = new JPanel(new GridBagLayout());
	private JScrollPane leftPane = null, rightPane = null;
	private CheckNode rootTablesNode;
	
	TableList tlist = null;

	private JLabel lblMessage = new JLabel("Ready");
	private String[] srcSchemaNames, dstSchemaNames, tableNames;
	private String tableFileName;
	private StateExtractTab tab1;
	private IBMExtractConfig cfg;
	
	public SelectTables(StateExtractTab tab1)
	{
		this.tab1 = tab1;
		this.tableFileName = tab1.getOutputDir() + IBMExtractUtilities.filesep + tab1.getDBName() + ".tables";	
		this.cfg = tab1.getCfg();
		refreshSchemaNames();
	}

	JComponent build()
	{
		FormLayout layout = new FormLayout(
			    "right:max(50dlu;pref), 3dlu, pref, right:max(50dlu;pref), 3dlu, pref", // columns
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu," +
			    "p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 2dlu, p, 5dlu, p, 2dlu, p");   // rows
		DefaultFormBuilder builder = new DefaultFormBuilder(layout);
		builder.setDefaultDialogBorder();
		builder.setOpaque(false);

		int width = 75, height = 30;
		btnExpand.setMinimumSize(new Dimension(width, height));
		btnExpand.setPreferredSize(new Dimension(width, height));
		btnExpand.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		
		btnSelectAll.setMinimumSize(new Dimension(width, height));
		btnSelectAll.setPreferredSize(new Dimension(width, height));
		btnSelectAll.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));

		btnInverse.setMinimumSize(new Dimension(width, height));
		btnInverse.setPreferredSize(new Dimension(width, height));
		btnInverse.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));

		btnSaveTables.setMinimumSize(new Dimension(width, height));
		btnSaveTables.setPreferredSize(new Dimension(width, height));
		btnSaveTables.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
		
		Box tableTreeBox = Box.createHorizontalBox();
		tableTreeBox.add(btnSaveTables);
		tableTreeBox.add(Box.createHorizontalGlue());

		Box buttonBox = Box.createHorizontalBox();
		buttonBox.add(btnExpand);
		buttonBox.add(Box.createHorizontalGlue());
		buttonBox.add(btnSelectAll);
		buttonBox.add(Box.createHorizontalGlue());
		buttonBox.add(btnInverse);
		buttonBox.add(Box.createHorizontalGlue());
		buttonBox.add(btnSaveTables);
		buttonBox.add(Box.createHorizontalGlue());

		leftPane = new JScrollPane(panelSchemaTextFieldBox);
		leftPane.setLayout(new ScrollPaneLayout());
		leftPane.setMinimumSize(new Dimension(300, 300));
		leftPane.setPreferredSize(new Dimension(300, 300));
		leftPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		leftPane.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));	
		
		rootTablesNode = new CheckNode("Select tables", true, true, "");		
		tableTree = new JTree(rootTablesNode);
		tableTree.setCellRenderer(new CheckRenderer());
		tableTree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
		tableTree.putClientProperty("JTree.lineStyle", "Angled");
		tableTree.addMouseListener(new NodeSelectionListener(tableTree));
		tableTree.setEditable(false);	
		
		rightPane = new JScrollPane(tableTree);
		rightPane.setMinimumSize(new Dimension(300, 300));
		rightPane.setPreferredSize(new Dimension(300, 300));
		rightPane.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));	
		
		CellConstraints cc = new CellConstraints();
		builder.addLabel("Map Schema:",     cc.xy (1,  5));
		builder.add(leftPane,                  cc.xyw(3,  5, 1));
		builder.addLabel("Select Tables:",     cc.xy (4,  5));
		builder.add(rightPane,                  cc.xyw(6,  5, 1));
		builder.addSeparator("",               cc.xyw(1,  7, 3));
		builder.add(buttonBox,                 cc.xy (6, 9));
		builder.add(lblHelpSelectTables,       cc.xy (6, 11));
		builder.add(lblMessage,                cc.xyw(1, 13, 3));

		addActionListeners();
		
	    setLabelMessage(lblMessage,"", false);
		setLabelMessage(lblHelpSelectTables, helpSelectTables, false);
		
		buildMappedSchemaForm();
		refreshTableTree();
		
		
		return builder.getPanel();
	}

	private void refreshSchemaNames()
	{
		String list;
		if (cfg.getSrcSchName() != null)
			   srcSchemaNames = cfg.getSrcSchName().split(":");
		if (cfg.getDstSchName() != null)
		   dstSchemaNames = cfg.getDstSchName().split(":");
		if (srcSchemaNames.length == 0)
		{
			list = tab1.getSchemaList();
			srcSchemaNames = list.split(":");
			dstSchemaNames = list.split(":");
		}
	}

	private void clearSchemaTextFields()
	{
        for (int i = panelSchemaTextFieldBox.getComponentCount() - 1; i >= 0; i--)
		{
			String name = (String) panelSchemaTextFieldBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JTextField")
			{
				Component cont = panelSchemaTextFieldBox.getComponent(i);	
				panelSchemaTextFieldBox.remove(cont);
			} else if (name == "javax.swing.JLabel")
			{
				Component cont = panelSchemaTextFieldBox.getComponent(i);	
				panelSchemaTextFieldBox.remove(cont);
			}
		}
	}
	
	private String[] getDstSchemaList()
	{
		int j = 0, component = panelSchemaTextFieldBox.getComponentCount(); 
        for (int i = 0; i < component; i++)
		{
			String name = (String) panelSchemaTextFieldBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JTextField")
			{
				JTextField tf = (JTextField) panelSchemaTextFieldBox.getComponent(i);
				dstSchemaNames[j++] = tf.getText();
			}
		}      
        return dstSchemaNames;		
	}

	private boolean validateSchemaList()
	{
		String str;
		int component = panelSchemaTextFieldBox.getComponentCount(); 
        for (int i = 0; i < component; i++)
		{
			String name = (String) panelSchemaTextFieldBox.getComponent(i).getClass().getName();
			if (name == "javax.swing.JTextField")
			{
				JTextField tf = (JTextField) panelSchemaTextFieldBox.getComponent(i);
				str = tf.getText();
				if (str == null || str.length() == 0)
					return false;
			}
		}      
        return true;		
	}

	private void buildMappedSchemaForm()
	{
		clearSchemaTextFields();

    	String srcSchema, dstSchema;
    	tlist = new TableList(tableFileName);		
		GridBagConstraints c = new GridBagConstraints();

		if (srcSchemaNames != null)
		{
			for (int i = 0; i < srcSchemaNames.length; i++)
			{
				srcSchema = IBMExtractUtilities.removeQuote(srcSchemaNames[i]);
				dstSchema = IBMExtractUtilities.removeQuote(dstSchemaNames[i]);
				JLabel  l = new JLabel(srcSchema);		
				JTextField  tf = new JTextField(10);	
				tf.setText(dstSchema);
				c.fill = GridBagConstraints.HORIZONTAL;
				c.insets = new Insets(2,2,2,2);
				c.gridx = 0;
				c.gridy = i;
				panelSchemaTextFieldBox.add(l, c);
				c.weightx = 0.5;
				c.gridx = 1;
				c.gridy = i;
				panelSchemaTextFieldBox.add(tf, c);
			}
			panelSchemaTextFieldBox.revalidate();
		}
	}
	
    private CheckNode processTables() 
    {
    	String schema, table;
    	tlist = new TableList(tableFileName);		
		String[] schemaNames = tlist.getSchemaList();
    	    	  
		CheckNode schemaNode, tableNode = null;
        
		rootTablesNode.removeAllChildren();
		if (schemaNames != null)
		{
			for (int i = 0; i < schemaNames.length; i++)
			{
				schema = IBMExtractUtilities.removeQuote(schemaNames[i]);
				schemaNode = new CheckNode(schema, true, true, "");
				rootTablesNode.add(schemaNode);		
				tableNames = tlist.getTableList(schemaNames[i]);
				for (int j = 0; j < tableNames.length; ++j)
				{
					String[] tabs =  tableNames[j].split("~");
					boolean isSelected = tabs[1].equals("1") ? true : false;
					table = IBMExtractUtilities.removeQuote(tabs[0]);
					tableNode = new CheckNode(table, false, isSelected, schema);
					schemaNode.add(tableNode);
				}
			}
		}
		return rootTablesNode;
    }

    private void expandTables()
    {
    	int row = 0;
    	while (row < tableTree.getRowCount())
    	{
    		tableTree.expandRow(row);
    		row++;
    	}    	    	
    }
    
    private void collapseTables()
    {
    	int row = 0;
    	while (row < tableTree.getRowCount())
    	{
    		tableTree.collapseRow(row);
    		row++;
    	}    	    	
    }

    private void selectAllTables(boolean select) 
    { 
		Enumeration e = rootTablesNode.breadthFirstEnumeration();
		while (e.hasMoreElements())
		{
			CheckNode node = (CheckNode) e.nextElement();
			boolean isSelected = !(node.isSelected());
			node.setSelected(select);
		}
		tableTree.revalidate();
		tableTree.repaint();
    } 
    
    private void inverseTables() 
    { 
		Enumeration e = rootTablesNode.breadthFirstEnumeration();
		while (e.hasMoreElements())
		{
			CheckNode node = (CheckNode) e.nextElement();
			boolean isSelected = !(node.isSelected());
			node.SetSelect(isSelected);
		}
		tableTree.revalidate();
		tableTree.repaint();
    } 
    
    private ArrayList getTables()
    {
    	String[] schemaTableArray;
    	boolean isSelected;
    	String schema, table;
    	ArrayList al = new ArrayList();
		Enumeration e = rootTablesNode.breadthFirstEnumeration();
		while (e.hasMoreElements())
		{
			CheckNode node = (CheckNode) e.nextElement();
			schema = IBMExtractUtilities.putQuote(node.schemaName);
			table = IBMExtractUtilities.putQuote(node.toString());
			if (node.schemaName.length() > 0)
			{
				al.add(schema+"."+table + "." + (node.isSelected() ? "1" : "0"));
			}
		}
        return al;
    }

    private void refreshTableTree()
	{
		DefaultTreeModel model = (DefaultTreeModel) tableTree.getModel();		
		rootTablesNode.removeAllChildren();
    	model.reload();

    	refreshSchemaNames();
    	buildMappedSchemaForm();
    	processTables();
    	expandTables();
	}
    
    public void refreshTables()
    {
		this.tableFileName = tab1.getOutputDir() + IBMExtractUtilities.filesep + tab1.getDBName() + ".tables";
		refreshTableTree();
    }
	
    private void setLabelMessage(JLabel label, String message, boolean warning)
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
		btnSaveTables.addActionListener(this);
		btnExpand.addActionListener(this);
		btnSelectAll.addActionListener(this);
		btnInverse.addActionListener(this);
	}
	
	public void actionPerformed(ActionEvent e)
	{
		setLabelMessage(lblMessage,"", false);
		if (e.getSource().equals(btnExpand)) 			
		{
			if (btnExpand.getText().equals("Expand All"))
			{
				btnExpand.setText("Collapse All");
				expandTables();
			} else
			{
				btnExpand.setText("Expand All");
				collapseTables();
			}			
		}
		else if (e.getSource().equals(btnSelectAll)) 			
		{
			if (btnSelectAll.getText().equals("Select All"))
			{
				btnSelectAll.setText("Deselect All");
				selectAllTables(true);
			} else
			{
				btnSelectAll.setText("Select All");
				selectAllTables(false);
			}			
		} else if (e.getSource().equals(btnInverse)) 			
		{
			inverseTables();
		} else if (e.getSource().equals(btnSaveTables)) 			
		{
			if (!validateSchemaList())
			{
				JOptionPane.showConfirmDialog(
						SelectTables.this,
					    "Schema field can not be blank",
					    "Please check",
					    JOptionPane.DEFAULT_OPTION);
				return;

			}
			dstSchemaNames = getDstSchemaList();
			cfg.setSrcSchName(srcSchemaNames);
			cfg.setDstSchName(dstSchemaNames);
			tab1.setValues();
			cfg.writeConfigFile();
			tlist.WriteTableFile(new SchemaData(dstSchemaNames, srcSchemaNames, cfg, getTables()));
			if (tlist.Message.length() > 0)
			{
				setLabelMessage(lblMessage, tlist.Message, false);
			}
		}
	}

	class SchemaCellEditor extends DefaultTreeCellEditor 
	{
		public SchemaCellEditor(JTree tree, DefaultTreeCellRenderer renderer)
		{
			super(tree, renderer);
		}
		
		public SchemaCellEditor(JTree tree, DefaultTreeCellRenderer renderer,
				TreeCellEditor editor)
		{
			super(tree, renderer, editor);
		}
		
		public boolean isCellEditable(EventObject event) 
		{
		    boolean returnValue = super.isCellEditable(event);
		    if (returnValue) 
		    {
		       CheckNode node = (CheckNode) tree.getLastSelectedPathComponent();
		       returnValue = node.isSchema;
		    }
		    return returnValue;
		  }
	}
	
	class NodeSelectionListener extends MouseAdapter
	{
		JTree tree;

		NodeSelectionListener(JTree tree)
		{
			this.tree = tree;
		}

		public void mouseClicked(MouseEvent e)
		{
			int x = e.getX();
			int y = e.getY();
			int row = tree.getRowForLocation(x, y);
			TreePath path = tree.getPathForRow(row);
			// TreePath path = tree.getSelectionPath();
			if (path != null)
			{
				CheckNode node = (CheckNode) path.getLastPathComponent();
				boolean isSchema = node.isSchema;
				boolean isSelected = !(node.isSelected());
				node.setSelected(isSelected);
				((DefaultTreeModel) tree.getModel()).nodeChanged(node);
				if (row == 0)
				{
					tree.revalidate();
					tree.repaint();
				} else
				{
					if (isSelected)
					{
						tree.expandPath(path);
					} else
					{
						tree.collapsePath(path);
					}
				}
			}
		}
	}

	class ButtonActionListener implements ActionListener
	{
		CheckNode root;
		JTextArea textArea;

		ButtonActionListener(final CheckNode root, final JTextArea textArea)
		{
			this.root = root;
			this.textArea = textArea;
		}

		public void actionPerformed(ActionEvent ev)
		{
			Enumeration e = root.breadthFirstEnumeration();
			while (e.hasMoreElements())
			{
				CheckNode node = (CheckNode) e.nextElement();
				if (node.isSelected())
				{
					TreeNode[] nodes = node.getPath();
					textArea.append("\n" + nodes[0].toString());
					for (int i = 1; i < nodes.length; i++)
					{
						textArea.append("/" + nodes[i].toString());
					}
				}
			}
		}
	}	
}

class CheckRenderer extends JPanel implements TreeCellRenderer
{
	private static final long serialVersionUID = -6480987832872670766L;
	protected JCheckBox check;
	protected TreeLabel label;
	
	public CheckRenderer()
	{
		setLayout(null);
		add(check = new JCheckBox());
		add(label = new TreeLabel());
		check.setBackground(UIManager.getColor("Tree.textBackground"));
		label.setForeground(UIManager.getColor("Tree.textForeground"));
	}

	public Component getTreeCellRendererComponent(JTree tree, Object value,
			boolean isSelected, boolean expanded, boolean leaf, int row,
			boolean hasFocus)
	{
		String stringValue = tree.convertValueToText(value, isSelected,
				expanded, leaf, row, hasFocus);
		setEnabled(tree.isEnabled());
		check.setSelected(((CheckNode) value).isSelected());
		label.setFont(tree.getFont());
		label.setText(stringValue);
		//label.setSelected(isSelected);
		label.setFocus(hasFocus);
		if (leaf)
		{
			label.setIcon(UIManager.getIcon("Tree.leafIcon"));
		} else if (expanded)
		{
			label.setIcon(UIManager.getIcon("Tree.openIcon"));
		} else
		{
			label.setIcon(UIManager.getIcon("Tree.closedIcon"));
		}
		return this;
	}

	public Dimension getPreferredSize()
	{
		Dimension d_check = check.getPreferredSize();
		Dimension d_label = label.getPreferredSize();
		return new Dimension(d_check.width + d_label.width,
				(d_check.height < d_label.height ? d_label.height
						: d_check.height));
	}
	
	public void doLayout()
	{
		Dimension d_check = check.getPreferredSize();
		Dimension d_label = label.getPreferredSize();
		int y_check = 0;
		int y_label = 0;
		if (d_check.height < d_label.height)
		{
			y_check = (d_label.height - d_check.height) / 2;
		} else
		{
			y_label = (d_check.height - d_label.height) / 2;
		}
		check.setLocation(0, y_check);
		check.setBounds(0, y_check, d_check.width, d_check.height);
		label.setLocation(d_check.width, y_label);
		label.setBounds(d_check.width, y_label, d_label.width, d_label.height);
	}

	public void setBackground(Color color)
	{
		if (color instanceof ColorUIResource)
			color = null;
		super.setBackground(color);
	}

	public class TreeLabel extends JLabel
	{
		private static final long serialVersionUID = 6475377056252523350L;
		boolean isSelected;
		boolean hasFocus;

		public TreeLabel()
		{
		}

		public void setBackground(Color color)
		{
			if (color instanceof ColorUIResource)
				color = null;
			super.setBackground(color);
		}

		public void paint(Graphics g)
		{
			String str;
			if ((str = getText()) != null)
			{
				if (0 < str.length())
				{
					if (isSelected)
					{
						g.setColor(UIManager.getColor("Tree.selectionBackground"));
					} else
					{
						g.setColor(UIManager.getColor("Tree.textBackground"));
					}
					Dimension d = getPreferredSize();
					int imageOffset = 0;
					Icon currentI = getIcon();
					if (currentI != null)
					{
						imageOffset = currentI.getIconWidth()
								+ Math.max(0, getIconTextGap() - 1);
					}
					g.fillRect(imageOffset, 0, d.width - 1 - imageOffset,
							d.height);
					if (hasFocus)
					{
						g.setColor(UIManager
								.getColor("Tree.selectionBorderColor"));
						g.drawRect(imageOffset, 0, d.width - 1 - imageOffset,
								d.height - 1);
					}
				}
			}
			super.paint(g);
		}

		public Dimension getPreferredSize()
		{
			Dimension retDimension = super.getPreferredSize();
			if (retDimension != null)
			{
				retDimension = new Dimension(retDimension.width + 3,
						retDimension.height);
			}
			return retDimension;
		}

		public void setSelected(boolean isSelected)
		{
			this.isSelected = isSelected;
		}

		public void setFocus(boolean hasFocus)
		{
			this.hasFocus = hasFocus;
		}
	}
}

class CheckNode extends DefaultMutableTreeNode
{
	private static final long serialVersionUID = -4705352818639484027L;
	protected boolean isSelected;
	protected boolean isSchema;
	protected String schemaName; 

	public CheckNode()
	{
		this(null, false);
	}

	public CheckNode(Object userObject, boolean isSchema)
	{
		this(userObject, true, false, "");
		this.isSchema = isSchema;
	}

	public CheckNode(Object userObject, boolean allowsChildren,
			boolean isSelected, String schemaName)
	{
		super(userObject, allowsChildren);
		this.isSelected = isSelected;
		this.schemaName = schemaName;
	}

	public void SetSelect(boolean isSelected)
	{
		this.isSelected = isSelected;
	}
	
	public void setSelected(boolean isSelected)
	{
		this.isSelected = isSelected;

		if (children != null)
		{
			Enumeration e = children.elements();
			while (e.hasMoreElements())
			{
				CheckNode node = (CheckNode) e.nextElement();
				node.setSelected(isSelected);
			}
		}
	}

	public boolean isSelected()
	{
		return isSelected;
	}

	// If you want to change "isSelected" by CellEditor,
	/*
	 * public void setUserObject(Object obj) { if (obj instanceof Boolean) {
	 * setSelected(((Boolean)obj).booleanValue()); } else {
	 * super.setUserObject(obj); } }
	 */
}