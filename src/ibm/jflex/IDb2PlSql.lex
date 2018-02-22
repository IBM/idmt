/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

%% 

%class IDb2PlSqlLexer
%extends DefaultLexer
%final
%unicode
%char
%type Token
%caseless

%init{
   
%init}

%{
    public IDb2PlSqlLexer() {
        super();
    }

    private Token token(TokenType type) {
        return new Token(type, yychar, yylength(), yytext());
    }

   /*private void error()
   throws IOException
   {
      throw new IOException("illegal text at line = "+yyline+", column = "+yycolumn+", text = '"+yytext()+"'");
   }*/
%}

/* main character classes */
LineTerminator = \r|\n|\r\n
InputCharacter = [^\r\n]

WhiteSpace = [ \t\f]
SpaceOrNewline = {WhiteSpace} | {LineTerminator}

/* comments */
Comment = {TraditionalComment} | {EndOfLineComment} | {DocumentationComment}

TraditionalComment = "/*" [^*] ~"*/" | "/*" "*"+ "/"
DocumentationComment = "/**" {CommentContent} "*"+ "/"
CommentContent       = ( [^*] | \*+ [^/*] )*
EndOfLineComment = "--" {InputCharacter}* {LineTerminator}?

/* identifiers */
Identifier = [:jletter:][:jletterdigit:]*

/* integer literals */
DecIntegerLiteral = 0 | [1-9][0-9]*
    
/* floating point literals */        
FloatLiteral  = ({FLit1}|{FLit2}|{FLit3}) {Exponent}? [fF]

FLit1    = [0-9]+ \. [0-9]* 
FLit2    = \. [0-9]+ 
FLit3    = [0-9]+ 
Exponent = [eE] [+-]? [0-9]+

/* string and character literals */
StringCharacter = [^\r\n\"\\]
SingleCharacter = [^\r\n\'\\]

iDB2Words =
	"CHARACTER_LENGTH" |
	"CHAR_LENGTH" |
	"SUBSTRING" |
	"IFNULL" |
	"DECLARE" ~";" |
	"NOW" {SpaceOrNewline}* "(" {SpaceOrNewline}* ")" |
	"GET" {SpaceOrNewline}* "DIAGNOSTICS" {SpaceOrNewline}* "EXCEPTION" {SpaceOrNewline}* ~ "MESSAGE_LENGTH" {SpaceOrNewline}* ";" |
	"POSITION" {SpaceOrNewline}* "(" .* ")" |
	"CHAR" {SpaceOrNewline}* "(" ~ ")"

Reserved = 
   "TRIGGER"                |
   "BEFORE"                 |
   "AFTER"                  |
   "INSTEAD OF"             |
   "OR"                     |
   "ON"                     |
   "INSERT"                 |
   "UPDATE"                 |
   "DELETE"                 |
   "CREATE"                 |
   "REPLACE"                |
   "VIEW"                   |
   ROLLBACK {SpaceOrNewline}* TO {SpaceOrNewline}* SAVEPOINT
       
%%

<YYINITIAL> {
 
  /* keywords */
  {Reserved}                     { return token(TokenType.KEYWORD);}
  
  /* iDB2 words */
  {iDB2Words}					 { return token(TokenType.IDB2WORDS);}

 /* SQL Terminator */
  ^\/	             { return token(TokenType.SQLTERMINATOR); } 

  /* string literal */
  \"{StringCharacter}+\"         { return token(TokenType.STRING); } 

  \'{SingleCharacter}*\'         { return token(TokenType.CHARLITERAL); } 

  /* operators */
  "#"                            |
  "'"                            |
  "|"                            |
  "-"                            |
  "+"                            |
  "*"                            |
  "%"                            |
  "/"                            |
  "("                            |
  ")"                            |
  "{"                            | 
  "}"                            | 
  "["                            | 
  "]"                            | 
  ";"                            | 
  ","                            | 
  "."                            | 
  "@"                            | 
  "="                            | 
  ">"                            | 
  "<"                            |
  "!"                            | 
  "~"                            | 
  "?"                            | 
  ":"                            { return token(TokenType.OPERATOR); } 


  /* numeric literals */

  {DecIntegerLiteral}            |
 
  {FloatLiteral}                 { return token(TokenType.NUMBER); }
  
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
.|\n                             {  }
<<EOF>>                          { return null; }


