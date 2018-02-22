package ibm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

public class IOBuffer
{
	Object empty = new Object();
	private byte[] buffer;
	private char[] cuffer;
	private int rPos = 0, wPos = 0;
	boolean inClosed, outClosed, rinClosed, routClosed; 
	public InputStream in;
	public OutputStream out;
	public Reader rin;
	public Writer rout;
	private boolean binary;
	
	public IOBuffer(boolean binary)
	{
		this.binary = binary;
		if (binary)
		{
		   buffer = new byte[Constants.readLobBuffer];
		   in = new IStream();
		   out = new OStream();
		} else
		{
		   cuffer = new char[Constants.readLobBuffer];
		   rin = new RIStream();
		   rout = new ROStream();
		}			
	}
	
	private void resizeBuffer()
	{
		if (binary)
		{
			byte[] newBuffer = new byte[buffer.length * 2];
			System.arraycopy(buffer, 0, newBuffer, 0, wPos);
			buffer = newBuffer;
			rPos = 0;
			wPos = availableBufferLength();
		} else
		{
			char[] newBuffer = new char[cuffer.length * 2];
			System.arraycopy(cuffer, 0, newBuffer, 0, wPos);
			cuffer = newBuffer;
			rPos = 0;
			wPos = availableBufferLength();			
		}
	}
	
	private int availableBufferLength()
	{
		if (rPos <= wPos)
		{
			return wPos - rPos;
		}
		return (binary) ? (buffer.length - (rPos - wPos)) : (cuffer.length - (rPos - wPos));
	}
	
	private int remainingBufferLength()
	{
		return (binary) ? buffer.length - 1 - wPos : cuffer.length - 1 - wPos;
	}
	
	public void clear()
	{
		synchronized (empty)
		{
			rPos = wPos = 0;
			outClosed = false;
			inClosed = false;
			rinClosed = false;
			routClosed = false;
		}
	}
	
	protected class RIStream extends Reader
	{

		public int available() throws IOException
		{
			synchronized (empty)
			{
				if (rinClosed)
					throw new IOException("Reader closed.");
				return availableBufferLength();
			}
		}
		
		@Override 
		public void close()  throws IOException
		{
			synchronized (empty)
			{
				rinClosed = true;
			}
		}
		
		@Override 
		public int read() throws IOException 
		{
			while (true)
			{
				synchronized (empty)
				{
					if (rinClosed) 
						throw new IOException("Cannot read from a closed Reader.");
					int available = availableBufferLength();
					if (available > 0)
					{
						int result = cuffer[rPos] & 0xff;
						rPos++;
						if (rPos == cuffer.length)
						{
							rPos = 0;
						}
						return result;
					} else if (routClosed)
					{
						return -1;
					}
				}
				try 
				{
					Thread.sleep(100);
				} catch(Exception x)
				{
					throw new IOException("Read operation interrupted.");
				}
			}
		}
		
		@Override 
		public int read(char[] cbuf, int off, int len) throws IOException 
		{
			while (true)
			{
				synchronized (empty)
				{
					if (rinClosed) 
						throw new IOException("Cannot read from a closed InputStream.");
					int available = availableBufferLength();
					if (available > 0)
					{
						int readLength = Math.min(len, available);
						int initialLength = Math.min(readLength, cuffer.length - rPos);
						int remainingLength = readLength - initialLength;
						System.arraycopy(cuffer, rPos, cbuf, off, initialLength);
						if (remainingLength > 0)
						{
							System.arraycopy(cuffer, 0, cbuf, off+initialLength,  remainingLength);
							rPos = remainingLength;
						} 
						else 
						{
							rPos += readLength;
						}
						if (rPos == cuffer.length) 
						{
							rPos = 0;
						}
						return readLength;
					} else if (routClosed)
					{
						return -1;
					}
				}
				try 
				{
					Thread.sleep(100);
				} catch(Exception x)
				{
					throw new IOException("Read operation interrupted.");
				}
			}
		}
		
		@Override 
		public int read(char[] cbuf) throws IOException 
		{
			return read(cbuf, 0, cbuf.length);
		}
		
		@Override public void reset() throws IOException 
		{
			synchronized (empty)
			{
				if (rinClosed) 
					throw new IOException("Cannot reset a closed Reader.");
				rPos = 0;
			}
		}
	}
		
	protected class ROStream extends Writer
	{

		@Override
		public void close() throws IOException
		{
			synchronized (empty)
			{
				routClosed = true;
			}
		}

		@Override
		public void flush() throws IOException
		{
			if (routClosed) 
				throw new IOException("Cannot flush a closed Writer.");
			if (rinClosed) 
				throw new IOException("Cannot flush a closed Reader.");
		}

		@Override
		public void write(char[] cbuf, int off, int len) throws IOException
		{
			while (len > 0)
			{
				synchronized (empty)
				{
					if (routClosed) 
						throw new IOException("Cannot write to a closed Writer.");
					if (rinClosed) 
						throw new IOException("Cannot write to a closed Reader");
					int rSpace = remainingBufferLength();
					while (rSpace < len)
					{
						resizeBuffer();
						rSpace = remainingBufferLength();
					}
					int writeLength = Math.min(len, rSpace);
					int initialLength = Math.min(writeLength, cuffer.length - wPos);
					int remainingLength = Math.min(writeLength - initialLength, cuffer.length - 1);
					int charsWritten = initialLength + remainingLength;
					if (initialLength > 0){
						System.arraycopy(cbuf, off, cuffer, wPos, initialLength);
					}
					if (remainingLength > 0){
						System.arraycopy(cbuf, off+initialLength, cuffer, 0, remainingLength);
						wPos = remainingLength;
					} else {
						wPos += charsWritten;
					}
					if (wPos == cuffer.length) {
						wPos = 0;
					}
					off += charsWritten;
					len -= charsWritten;
				}
				if (len > 0)
				{
					try {
						Thread.sleep(100);
					} catch(Exception x){
						throw new IOException("Waiting for available space in buffer interrupted.");
					}
				}
			}
		}		
	}
	
	protected class IStream extends InputStream
	{

		@Override
		public int available() throws IOException
		{
			synchronized (empty)
			{
				if (inClosed)
					throw new IOException("InputStream closed.");
				return availableBufferLength();
			}
		}
		
		@Override 
		public void close()  throws IOException
		{
			synchronized (empty)
			{
				inClosed = true;
			}
		}
		
		@Override 
		public int read() throws IOException 
		{
			while (true)
			{
				synchronized (empty)
				{
					if (inClosed) 
						throw new IOException("Cannot read from a closed InputStream.");
					int available = availableBufferLength();
					if (available > 0)
					{
						int result = buffer[rPos] & 0xff;
						rPos++;
						if (rPos == buffer.length)
						{
							rPos = 0;
						}
						return result;
					} else if (outClosed)
					{
						return -1;
					}
				}
				try 
				{
					Thread.sleep(100);
				} catch(Exception x)
				{
					throw new IOException("Read operation interrupted.");
				}
			}
		}
		
		@Override 
		public int read(byte[] buf, int off, int len) throws IOException 
		{
			while (true)
			{
				synchronized (empty)
				{
					if (inClosed) 
						throw new IOException("Cannot read from a closed InputStream.");
					int available = availableBufferLength();
					if (available > 0)
					{
						int readLength = Math.min(len, available);
						int initialLength = Math.min(readLength, buffer.length - rPos);
						int remainingLength = readLength - initialLength;
						System.arraycopy(buffer, rPos, buf, off, initialLength);
						if (remainingLength > 0)
						{
							System.arraycopy(buffer, 0, buf, off+initialLength,  remainingLength);
							rPos = remainingLength;
						} 
						else 
						{
							rPos += readLength;
						}
						if (rPos == buffer.length) 
						{
							rPos = 0;
						}
						return readLength;
					} else if (outClosed)
					{
						return -1;
					}
				}
				try 
				{
					Thread.sleep(100);
				} catch(Exception x)
				{
					throw new IOException("Read operation interrupted.");
				}
			}
		}
		
		@Override 
		public int read(byte[] buf) throws IOException 
		{
			return read(buf, 0, buf.length);
		}
		
		@Override public void reset() throws IOException 
		{
			synchronized (empty)
			{
				if (inClosed) 
					throw new IOException("Cannot reset a closed InputStream.");
				rPos = 0;
			}
		}
	}
		
	protected class OStream extends OutputStream
	{

		@Override 
		public void close() throws IOException 
		{
			synchronized (empty)
			{
				if (!outClosed){
					flush();
				}
				outClosed = true;
			}
		}
		
		@Override 
		public void flush() throws IOException 
		{
			if (outClosed) 
				throw new IOException("Cannot flush a closed OutputStream.");
			if (inClosed) 
				throw new IOException("Cannot flush a closed InputStream.");
		}
		
		@Override 
		public void write(byte[] buf, int off, int len) throws IOException 
		{
			while (len > 0)
			{
				synchronized (empty)
				{
					if (outClosed) 
						throw new IOException("Cannot write to a closed OutputStream.");
					if (inClosed) 
						throw new IOException("Cannot write to a closed InputStream");
					int rSpace = remainingBufferLength();
					while (rSpace < len)
					{
						resizeBuffer();
						rSpace = remainingBufferLength();
					}
					int writeLength = Math.min(len, rSpace);
					int initialLength = Math.min(writeLength, buffer.length - wPos);
					int remainingLength = Math.min(writeLength - initialLength, buffer.length - 1);
					int bytesWritten = initialLength + remainingLength;
					if (initialLength > 0){
						System.arraycopy(buf, off, buffer, wPos, initialLength);
					}
					if (remainingLength > 0){
						System.arraycopy(buf, off+initialLength, buffer, 0, remainingLength);
						wPos = remainingLength;
					} else {
						wPos += bytesWritten;
					}
					if (wPos == buffer.length) {
						wPos = 0;
					}
					off += bytesWritten;
					len -= bytesWritten;
				}
				if (len > 0)
				{
					try {
						Thread.sleep(100);
					} catch(Exception x){
						throw new IOException("Waiting for available space in buffer interrupted.");
					}
				}
			}
		}
		
		@Override 
		public void write(byte[] buf) throws IOException 
		{
			write(buf, 0, buf.length);
		}
		
		@Override 
		public void write(int c) throws IOException 
		{
			boolean written = false;
			while (!written){
				synchronized (empty)
				{
					if (outClosed) 
						throw new IOException("Cannot write to a closed OutputStream.");
					if (inClosed) 
						throw new IOException("Cannot write to a closed InputStream.");
					int rSpace = remainingBufferLength();
					while (rSpace < 1)
					{
						resizeBuffer();
						rSpace = remainingBufferLength();
					}
					if (rSpace > 0)
					{
						buffer[wPos] = (byte)(c & 0xff);
						wPos++;
						if (wPos == buffer.length) {
							wPos = 0;
						}
						written = true;
					}
				}
				if (!written)
				{
					try {
						Thread.sleep(100);
					} catch(Exception x){
						throw new IOException("Waiting for available space in buffer interrupted.");
					}
				}
			}
		}
	}
}
