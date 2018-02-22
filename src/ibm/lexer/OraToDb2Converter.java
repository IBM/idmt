/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ibm.IBMExtractUtilities;
import ibm.PLSQLInfo;
import ibm.lexer.Parser.TokenIterator;


/**
 * Functions to modify Oracle's PL/SQL to a version that performs essentailly the same
 * task and is DB2 compliant 
 * @author isiljanovski
 * @author Vikram S Khatri vikram.khatri@us.ibm.com (Original author)
 *
 */
public class OraToDb2Converter
{
   private static Parser pt;
   
   private static void initializePlSqlParsedTokens(String oraSql)
   {
      pt = new Parser(new Db2PlSqlLexer());
      pt.parse(pt.getBufferContents(oraSql));
   }
   
   private static String buildTrigger(String triggerName, boolean inTrigger, Token start, Token end, String type, Token[] pos)
   {
      Token t;
      
      TokenIterator iter = pt.getTokens(start.start+1, end.start+1);
      StringBuffer sb = new StringBuffer();
      boolean once = true;
      String newTriggerName = IBMExtractUtilities.getStringName(triggerName, "_"+type);
      
      while (iter.hasNext()) 
      {
         t = iter.next();
         
         if (t.start == pos[0].start)
         {				
            sb.append(newTriggerName + " ");
         } else if (t.start > pos[1].start && t.start < pos[2].start)
         {
            if (once)
            {
               if (t.text.equalsIgnoreCase(type))
               {
                  // append the type of trigger
                  sb.append(" ");
                  sb.append(type);
                  sb.append(" ");
                  
                  /* 
                   * skip the OR part but include the OF portion only for the update
                   * the OF can appear right after the update or after the delete
                   */
                  boolean skip = false;
                  if (t.text.equalsIgnoreCase("UPDATE"))
                  {
                     t = iter.next();
                     while(!t.text.equalsIgnoreCase("ON"))
                     {
                        // if delete skip everything up until the ON
                        if (t.text.equalsIgnoreCase("OR"))
                        {
                           skip = true;
                        }
                        else if ( t.text.equalsIgnoreCase("OF"))
                        {
                           skip = false;
                        }
                        if (!skip && t.type != TokenType.LINE && t.type != TokenType.WHITESPACE )
                        {
                           sb.append(t.text + " ");
                        }     
                        t = iter.next();
                     }
                  }
                  else // INSERT OR DELETE skip everything until the ON
                  {
                     while(!t.text.equalsIgnoreCase("ON"))
                     {
                        t = iter.next(); 
                     }   
                  }
                  
                  // append ON
                  sb.append(t.text);
                  once = false;
               }
            }
         }
         else if (t.type == TokenType.DELETEDTEXT)
         {
            continue;
         }
         else if (t.type == TokenType.COMMENT)
         {
            if (t.text.startsWith("--#SET :"))
            {
               if (!inTrigger)
               {
                  String comment = t.text;
                  String trigName = t.text.substring(t.text.lastIndexOf(":")+1);
                  comment = comment.replaceFirst(trigName, newTriggerName.replaceAll("\"", ""));
                  sb.append(comment+IBMExtractUtilities.linesep);
               } 
               else
               {
                  sb.append(t.text);
               }
            }
            else
            {
               sb.append(t.text);
            }
         }
         else if (t.type == TokenType.LINE)
         {
            sb.append(IBMExtractUtilities.linesep);
         }
         else
         {
            sb.append(t.text);
         }
      }
      sb.append(IBMExtractUtilities.linesep);	
      return sb.toString();
   }
   
   
   
   /**
    * Splits a compound trigger into single action triggers
    * @param oraSql Oracle SQL trigger ddl
    * @return       Separate trigger SQL, or null if oraSql is null or empty
    */
   public static String getSplitTriggers(String oraSql)
   {
      
      boolean beginTrigger = true, insertTrigger = false, updateTrigger = false, deleteTrigger = false; 
      Token startToken = null, endToken = null, beginToken = null;
      boolean inTrigger;
      String triggerName = "";
      Token t;
      // tokens (positions) of interest 0 - trigger name, 1 - "BEFORE/AFTER/INSTEAD OF"  2 - "ON"
      Token[] pos = new Token[3];
      StringBuffer sb = new StringBuffer();
      
      if (oraSql == null || oraSql.length() == 0)
      {
         return oraSql;
      }
      try
      {
         initializePlSqlParsedTokens(oraSql);
         TokenIterator iter = pt.getTokens();
         
         inTrigger = false;
         while (iter.hasNext()) 
         {
            if (beginTrigger)
            {
               startToken = iter.next();
               beginTrigger = false;
               insertTrigger = false;
               updateTrigger = false;
               deleteTrigger = false;
               triggerName = "";
            }
            t = iter.next();
            if (t.type == TokenType.KEYWORD && (t.getText().matches("(?i).*TRIGGER")))
            {
               inTrigger = true;		        	
               t = iter.nextToken();
               if (t.type == TokenType.STRING || t.type == TokenType.IDENTIFIER)
               {
                  triggerName = t.text;
                  pos[0] = t;
                  t = iter.nextToken();
                  if (t.type == TokenType.OPERATOR && t.getText().equals("."))
                  {
                     t = iter.nextToken();
                     if (t.type == TokenType.STRING || t.type == TokenType.IDENTIFIER)
                     {
                        pos[0] = t;
                        triggerName = t.getText();
                     }
                  }
               }
            }
            
            if (inTrigger && t.type == TokenType.KEYWORD &&
                  (t.getText().equalsIgnoreCase("BEFORE") ||
                        t.getText().equalsIgnoreCase("AFTER") ||
                        t.getText().equalsIgnoreCase("INSTEAD OF")))
            {
               pos[1] = t;
               do
               {
                  t = iter.next(TokenType.KEYWORD);
                  if (t.type == TokenType.KEYWORD)
                  {
                     if (t.getText().equalsIgnoreCase("INSERT"))
                     {
                        insertTrigger = true;
                     }
                     if (t.getText().equalsIgnoreCase("UPDATE"))
                     {
                        updateTrigger = true;
                     }
                     if (t.getText().equalsIgnoreCase("DELETE"))
                     {
                        deleteTrigger = true;
                     }
                  }
               } 
               while (t.type == TokenType.KEYWORD &&
                     (!(t.getText().equalsIgnoreCase("ON"))));
               pos[2] = t;
               endToken = iter.next(TokenType.SQLTERMINATOR);
               inTrigger = false;
               beginTrigger = true;
               if (insertTrigger)
               {
                  sb.append(buildTrigger(triggerName, inTrigger, startToken, endToken, "INSERT", pos));
               }
               if (updateTrigger)
               {
                  sb.append(buildTrigger(triggerName, inTrigger, startToken, endToken, "UPDATE", pos));
               }
               if (deleteTrigger)
               {
                  sb.append(buildTrigger(triggerName, inTrigger, startToken, endToken, "DELETE", pos));
               }              
            }
         }			
      } catch (Exception e)
      {
         e.printStackTrace();
      }
      return sb.toString();
   }
   
   /**
    * Returns the db2 version of the Oracle Pl/SQL
    * @param oraSql   Oracle Pl/SQL
    * @return         "Equivalent" Db2 compliant Pl/SQL
    */
   public static String getDb2PlSql(String oraSql)
   {
      Token t;
      StringBuffer sb = new StringBuffer();
      
      if (oraSql == null || oraSql.length() == 0)
      {
         return oraSql;
      }

      initializePlSqlParsedTokens(oraSql);
      TokenIterator iter = pt.getTokens();
      /* Since jflex has already matched the tokens most of the work will be done in the
       * the parser, and tokens will be modified or commented out accordingly.
       */
      while (iter.hasNext()) 
      {
         t = iter.next();
         sb.append(t.getText());
      }   
      return sb.toString();
   }
   
   private static boolean checkOpenCursor(Parser p, String cursorName)
   {
	   boolean found = false;
	   Token t;
	   TokenIterator iter = p.getTokens();
	   
	   while (iter.hasNext())
	   {
		   t = iter.next(); 
		   if (t.type != TokenType.WHITESPACE && t.text.equalsIgnoreCase("OPEN"))
		   {
			   t = iter.next();
			   if (t.type == TokenType.WHITESPACE)
				   t = iter.next();
			   if (t.text.equalsIgnoreCase(cursorName))
               {
			      found = true;
               }
		   }
		   if (t.type != TokenType.WHITESPACE && (t.text.equalsIgnoreCase("CLOSE") ||
				   t.text.equalsIgnoreCase("FETCH")))
		   {
			   t = iter.next();
			   if (t.type == TokenType.WHITESPACE)
				   t = iter.next();
			   if (t.text.equalsIgnoreCase(cursorName))
               {
			      found = false;
               }
		   }
	   }
	   return found;
   }
   
   /**
    * We should run this method on individual SP body and not to the whole file
    * since we are looking for the OPEN token to determine if we need to 
    * add WITH RETURN clause in DECLARE CURSOR statement or not.
    * @param iDB2SQL
    * @return modified body of the individual SP
    */
   public static String fixiDB2CursorForReturn(String iDB2SQL)
   {
	   String word = "", uword = "";
	   String tok1;
	   Token t;
	   StringBuffer sb = new StringBuffer();

       if (iDB2SQL == null || iDB2SQL.length() == 0)
       {
           return iDB2SQL;
       }
       try
       {
          pt = new Parser(new IDb2PlSqlLexer());
          pt.parse(pt.getBufferContents(iDB2SQL));
          TokenIterator iter = pt.getTokens();
          while (iter.hasNext()) 
          {
        	 tok1 = "";
             t = iter.next();
             word = t.text.toUpperCase();
             if (t.type == TokenType.LINE)
             {
                 sb.append(IBMExtractUtilities.linesep);
             }
             else if (t.type == TokenType.IDB2WORDS)
             {
            	 word = IBMExtractUtilities.removeLineChar(word);
            	 if (word.matches("DECLARE\\s+\\w+\\s+CURSOR\\s+WITH\\s+RETURN\\s+.*"))
            	 {
            		 sb.append(t.text); 
            	 } else if (word.matches("DECLARE\\s+\\w+\\s+CURSOR\\s+.*\\;\\s*"))
            	 {
            		 String regex = "DECLARE\\s+(\\w+)\\s+CURSOR\\s+.*";
				     Pattern pattern;
				     Matcher matcher = null;
				     pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
				     matcher = pattern.matcher(word);
				     if (matcher.matches()) 
				     {
				       tok1 = matcher.group(1);
				       if (checkOpenCursor(pt, tok1))
				       {
				    	   uword = t.text.toUpperCase();
				    	   int pos2 = word.indexOf(tok1);
				    	   int pos = uword.indexOf("CURSOR", pos2+tok1.length());
				    	   word = t.text.substring(0,pos + 6) + " WITH RETURN " + t.text.substring(pos+7);
				    	   sb.append(word);
				       } else
				    	   sb.append(t.text);
				     }				     
            	 } else
            		 sb.append(t.text);
             } else
             {
            	 sb.append(t.text); 
             }
          }   
       } catch (Exception e)
       {
          e.printStackTrace();
       }
       return sb.toString();
   }
   
   /**
    * @param iDB2SQL iDB2 Stored procedure source code
    * @return Fixed Stored Procedure code for DB2 LUW
    */
   public static String fixiDB2Procedures(String iDB2SQL)
   {
	   String word = "";
	   String tok1, tok2, tok3;
	   Token t;
	   StringBuffer sb = new StringBuffer();

       if (iDB2SQL == null || iDB2SQL.length() == 0)
       {
           return iDB2SQL;
       }
       try
       {
          pt = new Parser(new IDb2PlSqlLexer());
          pt.parse(pt.getBufferContents(iDB2SQL));
          TokenIterator iter = pt.getTokens();
          // Remove the unsupported tokens
          while (iter.hasNext()) 
          {
        	  tok1 = tok2 = tok3 = "";
             t = iter.next();
             word = t.text.toUpperCase();
             if (t.type == TokenType.COMMENT)
             {
            	 //if (t.text.startsWith("--")) Commented this line since we figured out why comments were not going in DB2 LUW.
                 {
            		 sb.append(t.text);
                 }
             } else if (t.type == TokenType.LINE)
             {
                 sb.append(IBMExtractUtilities.linesep);
             }
             else if (t.type == TokenType.IDB2WORDS)
             {
            	 word = IBMExtractUtilities.removeLineChar(word);
            	 if (word.equals("CHARACTER_LENGTH") || word.equals("CHAR_LENGTH"))
            		 word = "LENGTH";
            	 else if (word.startsWith("NOW"))
            	 {
            		 word = "CURRENT_TIMESTAMP";
            	 } 
            	 else if (word.matches(".*(NOW\\s+\\(\\s+\\)).*"))
            	 {
            		 String regex = "(.*)(NOW\\s+\\(\\s+\\))(.*)";
				     Pattern pattern;
				     Matcher matcher = null;
				     pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
				     matcher = pattern.matcher(word);
				     if (matcher.matches()) 
				     {
				       tok1 = matcher.group(1);
				       tok2 = matcher.group(2);
				       tok3 = matcher.group(3);
				       word = tok1 + "CURRENT_TIMESTAMP" + tok3;
				     }            		 
            	 }
            	 else if (word.equals("SUBSTRING"))
            		 word = "SUBSTR";
            	 else if (word.equals("IFNULL"))
            		 word = "COALESCE";
            	 else if (word.startsWith("POSITION"))
            	 {
            		  String regex = "POSITION\\s*\\(\\s*(\\'.*\\'|\\w*)\\s+(IN)\\s+(\\w+)\\s*\\)\\s*";				      
				      Pattern pattern;
				      Matcher matcher = null;
				      pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
				      matcher = pattern.matcher(word);
				      if (matcher.matches()) 
				      {
				    	  tok1 = matcher.group(1);
				    	  tok2 = matcher.group(3);
				    	  word = "INSTR(" + tok2 + "," + tok1 + ")";
				      }
            	 }
            	 else if (word.toUpperCase().startsWith("CHAR"))
            	 {
            		 int val;
            		 String regex = "CHAR\\s*\\(\\s*(\\d+)\\s*\\)\\s*";				     
				     Pattern pattern;
				     Matcher matcher = null;
				     pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
				     matcher = pattern.matcher(word);
				     if (matcher.matches()) 
				     {
				       tok1 = matcher.group(1);
				       try 
				       { 
				    	   val = Integer.valueOf(tok1);
				    	   if (val > 254)
				    		   word = "VARCHAR (" + val + ")";
				       } catch (Exception e) { ; }
				     }
            	 }
            	 else if (word.matches("GET\\s+\\DIAGNOSTICS.*\\;"))
            	 {
            		 String regex = "GET\\s+DIAGNOSTICS\\s+EXCEPTION\\s+1\\s+(\\w+)\\s*=\\s*MESSAGE_TEXT\\s*\\,\\s*(\\w+)\\s*=\\s*MESSAGE_LENGTH\\s*\\;";
				     Pattern pattern;
				     Matcher matcher = null;
				     pattern = Pattern.compile(regex,Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
				     matcher = pattern.matcher(word);
				     if (matcher.matches()) 
				     {
				       tok1 = matcher.group(1);
				       tok2 = matcher.group(2);
				       word = "GET DIAGNOSTICS EXCEPTION 1 " + tok1 + " = MESSAGE_TEXT; SET " + tok2 + " = LENGTH("+tok1+");";
				     }
            	 }
                 sb.append(word);
             } else
             {
            	 sb.append(t.text); 
             }
          }   
       } catch (Exception e)
       {
          e.printStackTrace();
       }
       return sb.toString();
   }
   
   private static void processiDB2ProceduresForCursors(String outputDirectory, String fileName)
   {
		String linesep = IBMExtractUtilities.linesep;
		String inFileName = outputDirectory + fileName;
	   	String origFileName;
	   	
	   	origFileName =  outputDirectory + fileName.substring(0, fileName.lastIndexOf('.')) + 
	   	    "_Original" + fileName.substring(fileName.lastIndexOf('.'));
	   	
		
		StringBuffer buffer = new StringBuffer();
		
		try
		{
		   	IBMExtractUtilities.copyFile(new File(inFileName), new File(origFileName));
		   	BufferedWriter out =  new BufferedWriter(new FileWriter(inFileName, false));		   	
		   	BufferedReader in = new BufferedReader(new FileReader(origFileName));
		   			   			   	
			String line;
			boolean collectCode = false;	
			
			while ((line = IBMExtractUtilities.readLine(in)) != null)
			{
				if (line.startsWith("--#SET :"))
				{
					collectCode = true;
				}
				if (collectCode)
				{
					if (line.startsWith("--#SET :"))
					{
						collectCode = true;
						out.write(line+linesep);
					}
				    else if (line.equals("@"))
					{
				    	String text = OraToDb2Converter.fixiDB2CursorForReturn(buffer.toString());
				    	out.write(text);
				        out.write(line+linesep);
						collectCode = false;
						buffer.setLength(0);
					}
					else
					    buffer.append(line+linesep);					
				} else
		           out.write(line+linesep);
				
			}
			in.close();
	        out.close();
	        IBMExtractUtilities.log("File " + inFileName + " saved as " + origFileName);
		} 					
		catch (IOException ex)
		{
			IBMExtractUtilities.log("File "+inFileName+" was not found. Skipping it");
			ex.printStackTrace();
		}						   
   }
   
   public static void main(String[] args)
   {
	    /*String fileName = "db2procedure2.db2";
	    String OUTPUT_DIR = "C:\\Vikram\\Prospects\\FirstAmerican\\";
	   	processiDB2ProceduresForCursors(OUTPUT_DIR, fileName);*/

	   	/*try			
	   	{    		
	   		String inFileName = OUTPUT_DIR + fileName;
	   		String outFileName;
	   		outFileName =  OUTPUT_DIR + fileName.substring(0, fileName.lastIndexOf('.')) + "_Original" + fileName.substring(fileName.lastIndexOf('.'));
			IBMExtractUtilities.copyFile(new File(inFileName), new File(outFileName));
			String text = OraToDb2Converter.fixiDB2CursorForReturn(IBMExtractUtilities.FileContents(outFileName));
			BufferedWriter out =  new BufferedWriter(new FileWriter(inFileName, false));
	        out.write(text);
	        out.close();
	        IBMExtractUtilities.log("File " + inFileName + " saved as " + outFileName);
		} catch (IOException e)
		{
			e.printStackTrace();
		}*/
	   
	   /*String fileName = "C:\\IBMDataMovementTool\\PVMNAME.txt";
	   try
	   {
			String text = OraToDb2Converter.getDb2PlSql(IBMExtractUtilities.FileContents(fileName));
			IBMExtractUtilities.log(text);
	   } catch (IOException e)
	   {
			e.printStackTrace();
	   }*/
	   String fileName = "C:\\temp\\xe\\db2trigger.db2";
	   try
	   {
			String text = OraToDb2Converter.getSplitTriggers(IBMExtractUtilities.FileContents(fileName));
			IBMExtractUtilities.log(text);
	   } catch (IOException e)
	   {
			e.printStackTrace();
	   }
	   
   }
}
