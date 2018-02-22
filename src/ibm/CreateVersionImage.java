/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.font.FontRenderContext;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;

public class CreateVersionImage
{
	public static void main(String[] args)
    {
		String currentVersion = IBMExtractUtilities.class.getPackage().getImplementationVersion();
        Font font = new Font("Verdana", Font.BOLD, 12);
        String str = "The current version available for download is " + currentVersion;
       
        BufferedImage bufferedImage = new BufferedImage(1, 1, BufferedImage.TYPE_INT_RGB);
        Graphics2D g2d = bufferedImage.createGraphics();  
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        FontRenderContext fc = g2d.getFontRenderContext();
        Rectangle2D bounds = font.getStringBounds(str,fc);
        
        int width = (int) bounds.getWidth();
        int height = (int) bounds.getHeight();
        
        bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        g2d = bufferedImage.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2d.setFont(font);


        g2d.setColor(Color.white);
        g2d.fillRect(0, 0, width, height);
        g2d.setColor(Color.black);
        g2d.drawString(str, 0, (int)-bounds.getY());
        g2d.dispose();

        try
        {
       	   File file = new File("IBMDatamovementToolVersion.jpg");
       	   ImageIO.write(bufferedImage, "jpg", file);
        } catch (Exception e)
        {
       	   e.printStackTrace();
        }
    }
}
