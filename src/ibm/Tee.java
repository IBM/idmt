/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */

package ibm;

import java.io.PrintStream;

public class Tee extends PrintStream
{
	protected PrintStream out;

	public Tee (PrintStream out1, PrintStream out2)
	{
		super(out1);
        this.out = out2;
	}
	
	public void write(byte buf[], int off, int len) {
        try {
            super.write(buf, off, len);
            out.write(buf, off, len); 
        } catch (Exception e) {
        }
    }
	
    public void flush() {
        super.flush();
        out.flush();
    }
}