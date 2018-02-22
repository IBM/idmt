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

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class About extends JFrame
{
	private static final long serialVersionUID = -3117414075377274935L;

	JTextArea fileViewerTextArea;
	JButton OkBtn = new JButton("Ok");
	
	public About()
	{
		super("About");

				
		Container container = getContentPane();
		container.setLayout(new BorderLayout());
		Listener listener = new Listener();
		OkBtn.addActionListener(listener);
		
		fileViewerTextArea = new JTextArea(25, 100);
		Font font = new Font("monospaced", Font.BOLD, 15);
		fileViewerTextArea.setFont(font);
		fileViewerTextArea.setForeground(Color.BLUE);
		fileViewerTextArea.setLineWrap(true);
		fileViewerTextArea.setText(IBMExtractUtilities.readJarFile("About.txt"));
		fileViewerTextArea.setCaretPosition(0);
		JScrollPane scrollPane = new JScrollPane(fileViewerTextArea);
		scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
		scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);

		JPanel middlePanel = new JPanel();
		middlePanel.setLayout(new FlowLayout(FlowLayout.CENTER, 0, 0));
		middlePanel.add(scrollPane);

		JPanel topPanel = new JPanel();
		
		topPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
		topPanel.add(OkBtn);

		container.add(BorderLayout.NORTH, topPanel);
		container.add(BorderLayout.CENTER, middlePanel);

		pack();
		setVisible(true);
		setResizable(false);
		setLocationRelativeTo(null);
	}
	
	private class Listener implements ActionListener
	{
		public void actionPerformed(ActionEvent e)
		{
			Object source = e.getSource();
			if (source.equals(OkBtn))
			{
				setVisible(false);
				dispose();				
			}
		}
	}
}
