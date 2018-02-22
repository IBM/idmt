/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Version
{
	public static void shortVersion()
	{
		BufferedWriter writer;
		try
		{
			String version = Version.class.getPackage().getImplementationVersion();
			writer = new BufferedWriter(new FileWriter("./UploadedVersion.txt", false));
			writer.write(version);
			writer.close();
		} catch (IOException e)
		{
		}
	}
	
	public static void main(String[] args)
	{
		System.out.println("IBMDataMovementTool Version " + Version.class.getPackage().getImplementationVersion());
		System.out.println("Author: Vikram S Khatri vikram.khatri@us.ibm.com");
		System.out.println("This software is for data migration of following databases to DB2");
		System.out.println("Oracle        ==> DB2");
		System.out.println("SQL Server    ==> DB2");
		System.out.println("Sybase ASE    ==> DB2");
		System.out.println("MySQL         ==> DB2");
		System.out.println("PostgreSQL    ==> DB2");
		System.out.println("MS Access     ==> DB2");
		System.out.println("DB2           ==> DB2");
	}
}