package ibm;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class RunIDMTVersion implements Runnable
{
	public String message = "";
	
	public RunIDMTVersion()
	{
	}
	
	public String GetIDMTVersion()
	{
		boolean getUpdatedVersion = false;
		int curMajor, curMinor, curBuild, updMajor, updMinor, updBuild;
		String currentVersion = IBMExtractUtilities.class.getPackage().getImplementationVersion();
		String updatedVersion = "", outcome = "";
		int readCount = 0;
		byte[] buffer = new byte[1024];

		curMajor = (currentVersion == null) ? -1 : Integer.valueOf(currentVersion.substring(0,currentVersion.indexOf('.'))); 
		curMinor = (currentVersion == null) ? -1 : Integer.valueOf(currentVersion.substring(currentVersion.indexOf('.')+1,currentVersion.indexOf('-'))); 
		curBuild = (currentVersion == null) ? -1 : Integer.valueOf(currentVersion.substring(currentVersion.indexOf('b')+1)); 
		
		try
		{
			URL url = new URL("ftp://public.dhe.ibm.com/education/db2pot/demos/UploadedVersion.txt");
			URLConnection con = url.openConnection();
			InputStream is = con.getInputStream();
			BufferedInputStream bis = new BufferedInputStream(is);

			while( (readCount = bis.read(buffer)) > 0)
		    {
				updatedVersion = new String(buffer, 0, readCount);
		    }
		    bis.close();
	        is.close();
	        
			updMajor = (updatedVersion == null) ? -1 : Integer.valueOf(updatedVersion.substring(0,updatedVersion.indexOf('.'))); 
			updMinor = (updatedVersion == null) ? -1 : Integer.valueOf(updatedVersion.substring(updatedVersion.indexOf('.')+1,updatedVersion.indexOf('-'))); 
			updBuild = (updatedVersion == null) ? -1 : Integer.valueOf(updatedVersion.substring(updatedVersion.indexOf('b')+1)); 
			
			IBMExtractUtilities.log("Current Version         = " + currentVersion);			
			IBMExtractUtilities.log("New Available Version   = " + updatedVersion);			
			IBMExtractUtilities.log("Download latest version = http://www.ibm.com/services/forms/preLogin.do?lang=en_US&source=idmt");			
			if (updMajor >= curMajor && updMinor >= curMinor && updBuild >= curBuild)
			{
				if (!(updMajor == curMajor && updMinor == curMinor && updBuild == curBuild))
					getUpdatedVersion = true;
			}
			if (getUpdatedVersion)
			{
				outcome = "Current version is " + ((currentVersion == null) ? " not detected. " : currentVersion) + "\n" + 
					      "New Version " + updatedVersion + " is available for download from\n" +
						"http://www.ibm.com/services/forms/preLogin.do?lang=en_US&source=idmt"; 
			} else
			{
				outcome =  "Your version " + currentVersion + " is current";
			}
		} catch (Exception e)
		{
			outcome =  "The new version details could not be obtained.\n" +
			   "Download latest version from this link\n" +
			   "http://www.ibm.com/services/forms/preLogin.do?lang=en_US&source=idmt";
		}
		return outcome;
	}
	
    public void run()
    {
	   message = GetIDMTVersion();
    }
}
