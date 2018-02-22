/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

public class Cleanup
{
   public static void main(String[] args) throws Exception 
   {
      ZFileProcess.cleanupPSFiles(args[0]);
   }
}
