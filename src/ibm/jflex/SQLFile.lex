/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

%%  

%class SQLFileLexer
%extends DefaultLexer
%final
%unicode
%char
%type Token
%caseless
%states YYINITIAL
%init{
    
%init}

%{
    public String useDatabase = "";
    public SQLFileLexer() {
        super();
    }

    private Token token(TokenType type) {
        return new Token(type, yychar, yylength(), yytext());
    }

    private Token token(TokenType type, String txt) {
        return new Token(type, yychar, yylength(), txt);
    }
    
    private void getUseDatabase(String text)
    {
        text = text.replaceAll("\\r|\\n|\\r\\n","");
        //System.out.println("from getUseDatabase="+text);
        if (text.matches("(?i)^\\s*use\\s+.*"))
        {
           useDatabase = text;
           //System.out.println("********* Yes I matched ="+text);
        }
    }
%}

/* main character classes */
LineTerminator = \r|\n|\r\n
InputCharacter = [^\r\n]

WhiteSpace = [ \t\f]
SpaceOrNewline = {WhiteSpace} | {LineTerminator}

useDatabase = "use" {WhiteSpace} {Identifier}  {LineTerminator} 

/* comments */
Comment = {TraditionalComment} | {EndOfLineComment} | {DocumentationComment}

TraditionalComment = "/*" [^*] ~"*/" | "/*" "*"+ "/"
DocumentationComment = "/**" {CommentContent} "*"+ "/"
CommentContent       = ( [^*] | \*+ [^/*] )*
EndOfLineComment = "--" {InputCharacter}* {LineTerminator}?

/* identifiers */
Identifier = [:jletter:][:jletterdigit:]*

%%

<YYINITIAL> {
 
  /* Get use database command */
  
  {useDatabase}					{  getUseDatabase(yytext());
  								   return token(TokenType.IDENTIFIER); }
  
  /* Get SQL Terminator */
  {EndOfLineComment}            {  return token(TokenType.COMMENT); }
   /* SQL Terminator */
  ^@ {SpaceOrNewline}*          { return token(TokenType.SQLTERMINATOR); } 
  
  /* SQL Terminator */
  ^\/ {SpaceOrNewline}*         { return token(TokenType.SQLTERMINATOR); }
  
  /* SQL Terminator */
  go {SpaceOrNewline}*         { return token(TokenType.SQLTERMINATOR); }

  /* SQL Terminator */
  ;				                { return token(TokenType.SQLTERMINATOR); }

  /* comments */
  {Comment}                      { return token(TokenType.COMMENT); }

  /* end of line */
  {LineTerminator}				 { return token(TokenType.LINE); }

  /* whitespace */
  {WhiteSpace}+                  { return token(TokenType.WHITESPACE); }

  /* identifiers */ 
  {Identifier}                   { return token(TokenType.IDENTIFIER); }
  
}

/* error fallback */
.|\n                             { return token(TokenType.DEFAULT); }
<<EOF>>                          { return null; }


