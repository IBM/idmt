/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

import java.io.Serializable;

public class Token implements Serializable, Comparable
{

	public final TokenType type;
	public final int start;
	public final int length;
	public String text;
	/**
	 * the pair value to use if this token is one of a pair: This is how it is
	 * used: The openning part will have a positive number X The closing part
	 * will have a negative number X X should be unique for a pair: e.g. for [
	 * pairValue = +1 for ] pairValue = -1
	 */
	public final byte pairValue;

	/**
	 * Constructs a new token
	 * 
	 * @param type
	 * @param start
	 * @param length
	 */
	public Token(TokenType type, int start, int length)
	{
		this.type = type;
		this.start = start;
		this.length = length;
		this.pairValue = 0;
	}

	/**
	 * Constructs a new token
	 * 
	 * @param type
	 * @param start
	 * @param length
	 */
	public Token(TokenType type, int start, int length, String text)
	{
		this.type = type;
		this.start = start;
		this.length = length;
		this.text = text;
		this.pairValue = 0;
	}

	/**
	 * Construct a new part of pair token
	 * 
	 * @param type
	 * @param start
	 * @param length
	 * @param pairValue
	 */
	public Token(TokenType type, int start, int length, String text, byte pairValue)
	{
		this.type = type;
		this.start = start;
		this.length = length;
		this.text = text;
		this.pairValue = pairValue;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof Object)
		{
			Token token = (Token) obj;
			return ((this.start == token.start)
					&& (this.length == token.length) && (this.type
					.equals(token.type)));
		} else
		{
			return false;
		}
	}

	@Override
	public int hashCode()
	{
		return start;
	}

	@Override
	public String toString()
	{
		return String
				.format("%s (%d, %d) (%d)", type, start, length, pairValue);
	}

	// @Override
	public int compareTo(Object o)
	{
		Token t = (Token) o;
		if (this.start != t.start)
		{
			return (this.start - t.start);
		} else if (this.length != t.length)
		{
			return (this.length - t.length);
		} else
		{
			return this.type.compareTo(t.type);
		}
	}

	/**
	 * return the end position of the token.
	 * 
	 * @return start + length
	 */
	public int end()
	{
		return start + length;
	}

	/**
	 * Get the text of the token from this document
	 * 
	 * @param doc
	 * @return
	 */
	public String getText(char[] doc)
	{
		String text = null;
		try
		{
			text = String.valueOf(doc, start, length);
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return text;
	}
	
	public String getText()
	{
		return text;
	}
}
