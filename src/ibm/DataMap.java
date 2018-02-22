package ibm;

public class DataMap 
{

	public boolean varlength = false;
    public String targetDataType = "", sourceDataType = "";
    public int defaultlength, precision, scale;    

    public DataMap()
	{
	}

    public DataMap(String sourceDataType)
	{
		this.sourceDataType = sourceDataType;
	}

    void init()
    {
    	varlength = false;
    	targetDataType = "";
    	sourceDataType = "";
    	defaultlength = 0;
    	precision = 0;
    	scale = 0;
    }
    
    public String toString()
    {
    	return "{targetDataType = " + targetDataType + " sourceDataType = " + sourceDataType + " defaultlength = " + defaultlength + " precision = " + precision + " scale = " + scale + "}";
    }
}
