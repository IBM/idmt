/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

public abstract class DefaultLexer implements Lexer {
    
    protected int tokenStart;
    protected int tokenLength;

    /**
     * Helper method to create and return a new Token from of TokenType
     * @param type
     * @param tStart
     * @param tLength
     * @param newStart
     * @param newLength
     * @return
     */
    protected Token token(TokenType type, int tStart, int tLength,
            int newStart, int newLength) {
        tokenStart = newStart;
        tokenLength = newLength;
        return new Token(type, tStart, tLength);
    }

    /**
     * Return the current matched token as a string.  This is <b>expensive</b>
     * as it creates a new String object for the token.  Use with care.
     *
     * @return
     */
    protected CharSequence getTokenSrring() {
        return yytext();
    }
}
