package ibm;

public class BinData
{
	public int length, offset, curpos;
	public byte[] buffer;
	
	public BinData(int length, byte[] byteBuffer)
	{
		this.length = length;
		this.buffer = byteBuffer;
	}

	public BinData(int length)
	{
		this.length = length;
		this.buffer = new byte[length];
		this.curpos = 0;
	}

	public void putByte(byte b)
	{
		buffer[curpos++] = b;
	}
	
	public byte[] trimQuote(byte colsep)
	{
		byte[] tmp = null;
    	byte b1, b2, b3;
    	byte[] newBytes = new byte[curpos];
    	int j = 0;
    	
    	b1 = buffer[0];

    	if (curpos == 1 && (b1 == Constants.CR || b1 == Constants.LF || b1 == colsep))
    		return null;
    	
    	b2 = buffer[curpos-1];
    	b3 = buffer[curpos-2];
    	if (b1 != Constants.dbquote && (b2 == Constants.CR || b2 == Constants.LF || b2 == colsep))
    	{
    		tmp = new byte[curpos-1];
    		System.arraycopy(buffer, 0, tmp, 0, curpos-1);
    	} else if (b1 == Constants.dbquote && (b2 == Constants.CR || b2 == Constants.LF || b2 == colsep) && b3 == Constants.dbquote)
    	{
        	for (int i = 1; i < curpos-2; ++i)
        	{
        		b1 = buffer[i];
    			b2 = (i+1 < curpos) ? buffer[i+1] : 0;
        		if (b1 == Constants.dbquote && b2 == Constants.dbquote)
        		{
        			newBytes[j++] = buffer[i];
        			++i;
        		} else
        		{
        		    newBytes[j++] = buffer[i];
        		}
        	}    		
    		tmp = new byte[j];
    		System.arraycopy(newBytes, 0, tmp, 0, j);
    	}    	    	
    	return tmp;
	}
}
