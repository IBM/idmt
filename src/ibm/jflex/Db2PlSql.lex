/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

%%  

%class Db2PlSqlLexer
%extends DefaultLexer
%final
%unicode
%char
%type Token
%caseless
%states YYINITIAL,TRIGGER
%init{
   
%init}

%{
    private boolean isRowTrigger = false;
    public Db2PlSqlLexer() {
        super();
    }

    private Token token(TokenType type) {
        return new Token(type, yychar, yylength(), yytext());
    }

    private Token token(TokenType type, String txt) {
        return new Token(type, yychar, yylength(), txt);
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
EnableTrigger = "ALTER" {SpaceOrNewline}* "TRIGGER" .* "ENABLE" {SpaceOrNewline}* ;?
rowTrigger = FOR {SpaceOrNewline}* EACH {SpaceOrNewline}* ROW

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

CreateTrigger =
   "CREATE" {SpaceOrNewline}* "OR REPLACE"? {SpaceOrNewline}* "TRIGGER"
   
/* Must be matched so that only rollback missing the savepoint has savepoint added to it  */
Reserved = 
   "TRIGGER"                |
   "BEFORE"                 |
   "AFTER"                  |
   "INSTEAD" {SpaceOrNewline}* "OF"              |
   "OR"                     |
   "ON"                     |
   "INSERT"                 |
   "INSERTING"              |
   "UPDATE"                 |
   "UPDATING"               |
   "DELETE"                 |
   "DELETING"               |
   "CREATE"                 |
   "REPLACE"                |
   "VIEW"                   |
   ROLLBACK {SpaceOrNewline}* TO {SpaceOrNewline}* SAVEPOINT
       
CommentText = 
  {EnableTrigger} |
  AUTHID {SpaceOrNewline}* (CURRENT_USER | DEFINER)  |
  PRAGMA {SpaceOrNewline}* RESTRICT_REFERENCES ~";" |
  SET {SpaceOrNewline}* TRANSACTION {SpaceOrNewline}* USE {SpaceOrNewline}* ROLLBACK ~";"
  
DeleteText = 
  "FORCE" |
  WITH {SpaceOrNewline}* READ {SpaceOrNewline}* ONLY
     
%%

<TRIGGER> {
 
 
 {rowTrigger}                    { isRowTrigger = true; 
                                   return token(TokenType.KEYWORD);}
                                    
 DECLARE | BEGIN 
                                 { 
                                   yybegin(YYINITIAL);
                                   if (isRowTrigger == false)
                                   {
                                      return token(TokenType.KEYWORD, "\nFOR EACH STATEMENT\n" + yytext()); 
                                   }
                                   return token(TokenType.KEYWORD);
                                 }
}

<YYINITIAL,TRIGGER> {
 
  {CreateTrigger}                { yybegin(TRIGGER);
                                   return token(TokenType.KEYWORD);}
  /* Comment out text, since user may want to address the removal */
  {CommentText}                  { return token(TokenType.COMMENTEDTEXT, "/* " + yytext() + " */"); }

  /* Remove text, definitely understood that not required */
  {DeleteText}                   { /* Do nothing and skip writing token */ }
   
  /* keywords */
  {Reserved}                     { return token(TokenType.KEYWORD);}
  
 /* SQL Terminator */
  ^@	             { return token(TokenType.SQLTERMINATOR); } 
  
 /* begin body */
  "BEGIN" 						 { return token(TokenType.BODY); }

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
  
  /* Only if missing word SAVEPOINT in rollback, add it */
  ROLLBACK {SpaceOrNewline}* "WORK"? {SpaceOrNewline}* TO     
                                 { return token(TokenType.MODIFIED,
                                         yytext() + " SAVEPOINT "); }
  
  /* To any new savepoint add the option for db2 */
  "SAVEPOINT" {SpaceOrNewline}* {Identifier}                  
                                 { return token(TokenType.MODIFIED,
                                         yytext() + " ON ROLLBACK RETAIN CURSORS"); }
 
}

/* error fallback */
.|\n                             {  }
<<EOF>>                          { return null; }


