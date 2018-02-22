/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.UIManager;

public class GetIDMTUpdate extends JDialog implements ActionListener 
{
	private JTextArea area = new JTextArea();
	
	public GetIDMTUpdate(JFrame parent, String title, String message)
	{
		super(parent, title, true);
		
		area.setEditable(false);
		area.setBorder(null);
		area.setForeground(UIManager.getColor("Label.foreground"));
		area.setFont(UIManager.getFont("Label.font"));    	
		area.setText(message);
    	Dimension parentSize = parent.getSize(); 
	    Point p = parent.getLocation(); 
	    setLocation(p.x + parentSize.width / 4, p.y + parentSize.height / 4);
	    JPanel messagePane = new JPanel();
	    messagePane.add(area);
	    getContentPane().add(messagePane);
	    JPanel buttonPane = new JPanel();
	    JButton button = new JButton("OK"); 
	    buttonPane.add(button); 
	    button.addActionListener(this);
	    getContentPane().add(buttonPane, BorderLayout.SOUTH);
	    setDefaultCloseOperation(DISPOSE_ON_CLOSE);
	    pack(); 
	    setVisible(true);    	      
	}
	
	public void actionPerformed(ActionEvent e)
	{
    	setVisible(false); 
        dispose();
	}
}
