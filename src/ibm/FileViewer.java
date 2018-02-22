/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Container;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.filechooser.FileFilter;

public class FileViewer extends JFrame
{

	private static final long serialVersionUID = -2670041685718816055L;
	JTextArea fileViewerTextArea;
	String dirName = ".";

	JButton btnOpen = new JButton("Open");
	JButton btnCancel = new JButton("Cancel");
	
	public FileViewer(String dirName, String fileName)
	{
		super("View Files");
		if (dirName == null || dirName.equals(""))
			this.dirName = ".";
		else
		    this.dirName = dirName;
		
		Container container = getContentPane();

		container.setLayout(new BorderLayout());

		Listener listener = new Listener();
		btnOpen.addActionListener(listener);		
		btnCancel.addActionListener(listener);

		fileViewerTextArea = new JTextArea(25, 100);
		Font font = new Font("monospaced", Font.BOLD, 15);
		fileViewerTextArea.setFont(font);
		fileViewerTextArea.setForeground(Color.BLUE);
		JScrollPane scrollPane = new JScrollPane(fileViewerTextArea);
		scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
		scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);

		JPanel middlePanel = new JPanel();
		middlePanel.setLayout(new FlowLayout(FlowLayout.CENTER, 0, 0));
		middlePanel.add(scrollPane);

		JPanel topPanel = new JPanel();
		
		topPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		topPanel.add(btnOpen);
		topPanel.add(btnCancel);

		container.add(BorderLayout.NORTH, topPanel);
		container.add(BorderLayout.CENTER, middlePanel);

		pack();
		setVisible(true);
		setResizable(true);
		setLocationRelativeTo(null);
		if (fileName == null || fileName.equals(""))
		{
			openDialog();
		} else
		{
			try {
				setTitle(fileName);
				fileViewerTextArea.setText(IBMExtractUtilities.FileContents(fileName));
				fileViewerTextArea.setCaretPosition(0);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	private void openDialog()
	{
		JFileChooser fc = new JFileChooser();
		fc.setFileSelectionMode(javax.swing.JFileChooser.FILES_AND_DIRECTORIES);
		fc.addChoosableFileFilter(new MyFilter());
		fc.setCurrentDirectory(new File(dirName));
		fc.setAcceptAllFileFilterUsed(false);

		int result = fc.showOpenDialog(FileViewer.this);

		if (result == javax.swing.JFileChooser.CANCEL_OPTION)
		{
			return;
		}

		File fp = fc.getSelectedFile();
		if (fp == null || fp.getName().equals(""))
		{
			javax.swing.JOptionPane.showMessageDialog(null, "Error", "Error", javax.swing.JOptionPane.ERROR_MESSAGE);
		} else
		{
			try {
				setTitle(fp.getAbsolutePath());
				fileViewerTextArea.setText(IBMExtractUtilities.FileContents(fp.getAbsolutePath()));
				fileViewerTextArea.setCaretPosition(0);
				fileViewerTextArea.setEditable(false);
				
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	private class Listener implements ActionListener
	{
		public void actionPerformed(ActionEvent e)
		{
			Object source = e.getSource();

			if (source.equals(btnOpen))
			{
				openDialog();
			}

			if (source.equals(btnCancel))
			{
				setVisible(false);
				dispose();				
			}
		}
	}

	public class MyFilter extends FileFilter
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
			   return "*.db2;*.sql;*.TXT;*.cmd";
			else
			   return "*.db2;*.sql;*.log;*.sh";
		}
	}
}