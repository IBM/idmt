package ibm;

public class BlobVal 
{
    public boolean[] isBLOB, isCLOB, isXML;
    boolean isBlob, isClob, isXml;
    public int blobOffset, xmlOffset, clobOffset, dbclobOffset, blobFileCounter, clobFileCounter, xmlFileCounter;
    
    public BlobVal()
    {
        blobOffset = 0;
        clobOffset = 0;
        xmlOffset = 0;
        dbclobOffset = 0;
        blobFileCounter = 0;
        clobFileCounter = 0;
        xmlFileCounter = 0;
        isBlob = false;
        isXml = false;
        isClob = false;
    }
    
    public int getNZLOBEstimatedLength()
    {
    	int lobsColCount = 0, n =  (isBLOB == null ? 0 : isBLOB.length);

    	for (int i = 0; i < n; ++i)
    	{
    		if (isBLOB[i])
    			++lobsColCount;
    		if (isCLOB[i])
    			++lobsColCount;
    		if (isXML[i])
    			++lobsColCount;
    	}
    	if (lobsColCount == 0)
    		return Constants.netezzaMaxlength;
    	else
    		return Constants.netezzaMaxlength / lobsColCount;        		
    }
}

