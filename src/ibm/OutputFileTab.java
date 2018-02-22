/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;

import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;
import javax.swing.filechooser.FileFilter;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class OutputFileTab extends JFrame implements ActionListener
{
	private static final long serialVersionUID = 301580586163894447L;
	private String fileName;
	private String dirName;
	private JTextArea textArea = new JTextArea(); 
	private JPopupMenu popup;
	private StringBuffer outputBuffer;
	//private ActionListener fillOutputActionListener;
	
	public JTextArea getTextArea()
	{
		return textArea;
	}

	public void setTextArea(JTextArea textArea)
	{
		this.textArea = textArea;
	}

	public OutputFileTab(StringBuffer outputBuffer)
	{
		this.outputBuffer = outputBuffer;
		//this.fillOutputActionListener = fillOutputActionListener;
	}
		
	private JScrollPane createArea(
            String text,
            boolean lineWrap,
            int columns,
            Dimension minimumSize) {
		textArea.setText(text);
		textArea.setBorder(new CompoundBorder(
                new LineBorder(Color.GRAY),
                new EmptyBorder(1, 3, 1, 1)));
		textArea.setLineWrap(lineWrap);
		textArea.setWrapStyleWord(true);
		textArea.setColumns(columns);
        
        JScrollPane scrollPane = new JScrollPane(textArea);
		scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
		scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        
        Font font = new Font("monospaced", Font.BOLD, 15);        
        textArea.setFont(font);
        textArea.setForeground(Color.BLUE);
        
        if (minimumSize != null) {
        	textArea.setMinimumSize(new Dimension(100, 32));
        }
        
		popup = new JPopupMenu();
		JMenuItem mi;
		mi = new JMenuItem("Clear output");
		mi.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				outputBuffer.setLength(0);
				textArea.setText("");
			}
		});
		mi.setActionCommand("Source");		
		popup.add(mi);
		
		textArea.addMouseListener(new MouseAdapter()
        {
        	public void mouseClicked(MouseEvent e)
			{
        		if(SwingUtilities.isRightMouseButton(e) == true && 
        				e.getClickCount() == 1)
        		{
	                popup.show(e.getComponent(), e.getX(), e.getY());
        		} 
        	    else if (e.getClickCount() == 2)
        		{
    				outputBuffer.setLength(0);
        			textArea.setText("");
        		}
			}
        });

        return scrollPane;
    }
	
	private JComponent buildTab(JScrollPane area) 
	{
	        FormLayout layout = new FormLayout(
	        		"fill:200dlu:grow",
	                "fill:default:grow");
	        PanelBuilder builder = new PanelBuilder(layout);
	        builder.setDefaultDialogBorder();
	        CellConstraints cc = new CellConstraints();
	        builder.add(area,        cc.xy(1, 1));
	        return builder.getPanel();
	}

	JComponent build()
	{
		String str = "";		
		//textArea = createArea(str, true, 0, null);
		return buildTab(createArea(str, true, 0, null));	
	}

	static private class MyFilter extends FileFilter
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
				if (ext.equalsIgnoreCase(".sql")
						|| ext.equalsIgnoreCase(".db2") 
						|| ext.equalsIgnoreCase(".nz")
						|| ext.equalsIgnoreCase(".sh")
						|| ext.equals(".TXT")
						|| ext.equals(".log")
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
			   return "*.db2;*.sql;*.TXT;*.cmd;*.nz";
			else
			   return "*.db2;*.sql;*.log;*.sh;*.nz";
		}
	}
	
	private void openDialog(boolean isDir)
	{
		File fp = null;
		if (isDir)
		{
			JFileChooser fc = new JFileChooser();
			fc.setFileSelectionMode(javax.swing.JFileChooser.FILES_AND_DIRECTORIES);
			fc.addChoosableFileFilter(new MyFilter());
			fc.setCurrentDirectory(new File(dirName));
			fc.setAcceptAllFileFilterUsed(false);
	
			int result = fc.showOpenDialog(OutputFileTab.this);
	
			if (result == javax.swing.JFileChooser.CANCEL_OPTION)
			{
				return;
			}
	
			fp = fc.getSelectedFile();
			if (fp == null || fp.getName().equals(""))
			{
				javax.swing.JOptionPane.showMessageDialog(null, "Error", "Error", javax.swing.JOptionPane.ERROR_MESSAGE);
			}
		} else
		{
			fp = new File(dirName);
		}
		try {
			setTitle(fp.getAbsolutePath());
			textArea.setText(IBMExtractUtilities.FileContents(fp.getAbsolutePath()));
			textArea.setCaretPosition(0);
			textArea.setEditable(false);
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void FillTextAreaFromOutput(String scriptName)
	{
	    fileName = scriptName;
		if (Constants.win())
			fileName = fileName.replaceAll("\\.cmd", "_OUTPUT.TXT");
		else
			fileName = fileName + ".log";
		
		if (IBMExtractUtilities.FileExists(fileName))
		{
			int n = JOptionPane.showConfirmDialog(
					    OutputFileTab.this,
					    "Do you want to view the output log file?",
					    "View output log file",
					    JOptionPane.YES_NO_OPTION);
			if (n == JOptionPane.YES_OPTION)
			{
				try {
					setTitle(fileName);
					textArea.setText(IBMExtractUtilities.FileContents(fileName));
					textArea.setCaretPosition(0);
				} catch (IOException e1) {
					e1.printStackTrace();
				}

			}
		}
	}
	
	public void FillTextAreaFromFile(String dirName)
	{
		if (dirName == null || dirName.equals(""))
			this.dirName = ".";
		else
		    this.dirName = dirName;
		
		File f = new File(this.dirName);
		openDialog(f.isDirectory());		
	}
	
	public void actionPerformed(ActionEvent e)
	{	
	}
}
