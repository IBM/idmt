/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

import java.io.Reader;

public interface Lexer {

    /**
     * This will be called to reset the the lexer, generally whenever a
     * document is changed
     * @param reader
     */
    public void yyreset(Reader reader);

    /**
     * This is called to return the next Token from the Input Reader
     * @return next token, or null if no more tokens.
     * @throws java.io.IOException
     */
    public Token yylex() throws java.io.IOException;

    /**
     * Returns the character at position <tt>pos</tt> from the
     * matched text.
     *
     * It is equivalent to yytext().charAt(pos), but faster
     *
     * @param pos the position of the character to fetch.
     *            A value from 0 to yylength()-1.
     *
     * @return the character at position pos
     */
    public char yycharat(int pos);

    /**
     * Returns the length of the matched text region.
     */
    public int yylength();

    /**
     * Returns the text matched by the current regular expression.
     */
    public String yytext();
}