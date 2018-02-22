/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser
{
	char[] charsBuffer;
	private int bufferLength = 0;
	Lexer lexer;
	List<Token> tokens;

	public Parser(Lexer lexer)
	{
		this.lexer = lexer;
	}

	/**
	 * @return the charsBuffer
	 */
	public char[] getCharsBuffer()
	{
		return charsBuffer;
	}

	public String getText(int start)
	{
		return String.valueOf(charsBuffer, start, charsBuffer.length - start);
	}
	
	public String getText(int start, int length)
	{
		return String.valueOf(charsBuffer, start, length);
	}
	
	public void parse(String buffer)
	{
		charsBuffer = buffer.toCharArray();
		CharArrayReader reader = new CharArrayReader(charsBuffer);
		parse(reader);
	}
	
	public void parse(Reader reader)
	{
		// if we have no lexer, then we must have no tokens...
		if (lexer == null || reader == null)
		{
			tokens = null;
			return;
		}
		List<Token> toks = new ArrayList<Token>(bufferLength / 10);
		long ts = System.nanoTime();
		int len = bufferLength;
		try
		{
			lexer.yyreset(reader);
			Token token;
			while ((token = lexer.yylex()) != null)
			{
				toks.add(token);
			}
		} catch (IOException ex)
		{
			// This will not be thrown from the Lexer
			ex.printStackTrace();
		} finally
		{
			//IBMExtractUtilities.log(String.format(
			//		"Parsed %d in %d ms, giving %d tokens\n", len, (System
			//				.nanoTime() - ts) / 1000000, toks.size()));
			tokens = toks;
		}
	}

	public void replaceToken(Token token, String replacement)
	{
		;
	}

	/**
	 * Return a matcher that matches the given pattern on the entire document
	 * 
	 * @param pattern
	 * @return matcher object
	 */
	public Matcher getMatcher(Pattern pattern)
	{
		return getMatcher(pattern, 0, bufferLength);
	}

	/**
	 * Return a matcher that matches the given pattern in the part of the
	 * document starting at offset start. Note that the matcher will have offset
	 * starting from <code>start</code>
	 * 
	 * @param pattern
	 * @param start
	 * @return matcher that <b>MUST</b> be offset by start to get the proper
	 *         location within the document
	 */
	public Matcher getMatcher(Pattern pattern, int start)
	{
		return getMatcher(pattern, start, bufferLength - start);
	}

	/**
	 * Return a matcher that matches the given pattern in the part of the
	 * document starting at offset start and ending at start + length. Note that
	 * the matcher will have offset starting from <code>start</code>
	 * 
	 * @param pattern
	 * @param start
	 * @param length
	 * @return matcher that <b>MUST</b> be offset by start to get the proper
	 *         location within the document
	 */
	public Matcher getMatcher(Pattern pattern, int start, int length)
	{
		Matcher matcher = null;
		if (bufferLength == 0)
		{
			return null;
		}
		matcher = pattern.matcher(getText(start, length));
		return matcher;
	}

	/**
	 * Find the token at a given position. May return null if no token is found
	 * (whitespace skipped) or if the position is out of range:
	 * 
	 * @param pos
	 * @return
	 */
	public Token getTokenAt(int pos)
	{
		if (tokens == null || tokens.isEmpty() || pos > bufferLength)
		{
			return null;
		}
		Token tok = null;
		Token tKey = new Token(TokenType.DEFAULT, pos, 1);
		@SuppressWarnings("unchecked")
		int ndx = Collections.binarySearch((List) tokens, tKey);
		if (ndx < 0)
		{
			// so, start from one before the token where we should be...
			// -1 to get the location, and another -1 to go back..
			ndx = (-ndx - 1 - 1 < 0) ? 0 : (-ndx - 1 - 1);
			Token t = tokens.get(ndx);
			if ((t.start <= pos) && (pos <= t.end()))
			{
				tok = t;
			}
		} else
		{
			tok = tokens.get(ndx);
		}
		return tok;
	}

	/**
	 * This is used to return the other part of a paired token in the document.
	 * A paired part has token.pairValue <> 0, and the paired token will have
	 * the negative of t.pairValue. This method properly handles nestings of
	 * same pairValues, but overlaps are not checked. if The document does not
	 * contain a paired
	 * 
	 * @param t
	 * @return the other pair's token, or null if nothing is found.
	 */
	public Token getPairFor(Token t)
	{
		if (t == null || t.pairValue == 0)
		{
			return null;
		}
		Token p = null;
		int ndx = tokens.indexOf(t);
		// w will be similar to a stack. The openners weght is added to it
		// and the closers are subtracted from it (closers are already negative)
		int w = t.pairValue;
		int direction = (t.pairValue > 0) ? 1 : -1;
		boolean done = false;
		int v = Math.abs(t.pairValue);
		while (!done)
		{
			ndx += direction;
			if (ndx < 0 || ndx >= tokens.size())
			{
				break;
			}
			Token current = tokens.get(ndx);
			if (Math.abs(current.pairValue) == v)
			{
				w += current.pairValue;
				if (w == 0)
				{
					p = current;
					done = true;
				}
			}
		}
		return p;
	}

	public String getBufferContents(String buffer)
	{
		if (buffer == null || buffer.length() == 0)
		{
			bufferLength = 0;
		    return null;
		} else
		{
			bufferLength = buffer.length();
			return buffer;
		}
	}
	
	public void getUncommentedText(int aStart, int anEnd)
	{
		StringBuilder result = new StringBuilder();
        Iterator<Token> iter = getTokens(aStart, anEnd);
        while (iter.hasNext()) 
        {
            Token t = iter.next();
            if (TokenType.COMMENT != t.type && TokenType.COMMENT2 != t.type) 
            {
                result.append(t.getText(charsBuffer));
            }
        }
	}
	
	public String getFileContents(String fileName)
	{
		BufferedReader br;
		StringBuilder sb = new StringBuilder();

		char[] buffer;
		try
		{
			br = new BufferedReader(new FileReader(fileName));
			buffer = new char[30000];
			int n = bufferLength = 0;
			while (n >= 0)
			{
				n = br.read(buffer, 0, buffer.length);
				if (n > 0)
				{
					sb.append(buffer, 0, n);
					bufferLength += n;
				}
			}
			if (br != null)
				br.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		return sb.toString();
	}

	public TokenIterator getTokens()
	{
		return getTokens(0, charsBuffer.length);
	}
	
	/**
     * Return an iterator of tokens between p0 and p1.
     * @param start start position for getting tokens
     * @param end position for last token
     * @return Iterator for tokens that overal with range from start to end
     */
    public TokenIterator getTokens(int start, int end) 
    {
        return new TokenIterator(start, end);
    }
    
	class TokenIterator implements ListIterator<Token>
	{

		int start;
		int end;
		int ndx = 0;

		@SuppressWarnings("unchecked")
		private TokenIterator(int start, int end)
		{
			this.start = start;
			this.end = end;
			if (tokens != null && !tokens.isEmpty())
			{
				Token token = new Token(TokenType.COMMENT, start, end - start);
				ndx = Collections.binarySearch((List) tokens, token);
				// we will probably not find the exact token...
				if (ndx < 0)
				{
					// so, start from one before the token where we should be...
					// -1 to get the location, and another -1 to go back..
					ndx = (-ndx - 1 - 1 < 0) ? 0 : (-ndx - 1 - 1);
					Token t = tokens.get(ndx);
					// if the prev token does not overlap, then advance one
					if (t.end() <= start)
					{
						ndx++;
					}
				}
			}
		}

		// @Override
		public boolean hasNext()
		{
			if (tokens == null)
			{
				return false;
			}
			if (ndx >= tokens.size())
			{
				return false;
			}
			Token t = tokens.get(ndx);
			if (t.start >= end)
			{
				return false;
			}
			return true;
		}

		// @Override
		public Token next()
		{
			return tokens.get(ndx++);
		}
		
		public Token nextToken()
		{
			++ndx;
			while ((((Token)tokens.get(ndx)).type == TokenType.WHITESPACE) || 
				   (((Token)tokens.get(ndx)).type == TokenType.LINE))
			{
				ndx++;
			}	
			return tokens.get(ndx);
		}

		public Token next(TokenType type)
		{
			++ndx;
			while (((Token)tokens.get(ndx)).type != type)
			{
				ndx++;
			}	
			return tokens.get(ndx);
		}

		// @Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

		public boolean hasPrevious()
		{
			if (tokens == null)
			{
				return false;
			}
			if (ndx <= 0)
			{
				return false;
			}
			Token t = tokens.get(ndx);
			if (t.end() <= start)
			{
				return false;
			}
			return true;
		}

		// @Override
		public Token previous()
		{
			return tokens.get(ndx--);
		}

		// @Override
		public int nextIndex()
		{
			return ndx + 1;
		}

		// @Override
		public int previousIndex()
		{
			return ndx - 1;
		}

		// @Override
		public void set(Token e)
		{
			throw new UnsupportedOperationException();
		}

		// @Override
		public void add(Token e)
		{
			throw new UnsupportedOperationException();
		}
	}
}
