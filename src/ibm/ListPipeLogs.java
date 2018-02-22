/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.NoSuchElementException;

public class ListPipeLogs implements Enumeration
{
	private String outputDirectory;
	private String[] pipeLogs;
	private int current = 0;
	
	public ListPipeLogs(String outputDirectory, final String extension)
	{
		this.outputDirectory = outputDirectory;
		File dir = new File (outputDirectory);
		FilenameFilter filter = new FilenameFilter() 
		{ 
			public boolean accept(File dir, String name) 
			{ 
				return name.endsWith(extension); 
			} 
		}; 
		pipeLogs = dir.list(filter);
	}
	
	public boolean hasMoreElements()
	{
		if (current < pipeLogs.length)
            return true;
        else
            return false;
	}

	public Object nextElement()
	{
		InputStream in = null;
		if (!hasMoreElements())
            throw new NoSuchElementException("No more files.");
        else {
            String nextElement = outputDirectory + pipeLogs[current];
            current++;
            try {
                in = new FileInputStream(nextElement);
            } catch (FileNotFoundException e) {
                IBMExtractUtilities.log("ListPipeLogs: Can't open " + nextElement);
            }
        }
        return in;
	}
}
