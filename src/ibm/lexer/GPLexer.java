package ibm.lexer;

import ibm.IBMExtractUtilities;
import ibm.lexer.Parser.TokenIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class GPLexer
{
	   private static Parser pt;
	   private static SQLFileLexer sl = new SQLFileLexer();
	   
	   public static void InitSQLFileLexer(String sql)
	   {
		   pt = new Parser(sl);
		   pt.parse(pt.getBufferContents(sql));
	   }	   
	   
	   public static ArrayList parseTempTableFile(String fileName) throws IOException
	   {
	    	Token t;
	        StringBuffer sb = new StringBuffer();
	        ArrayList sqlStatements = new ArrayList();
	    	String fileContent = IBMExtractUtilities.FileContents(fileName);
	    	String sqlTerminator = ";";
	    	String tok1 = "--#SET TERMINATOR", tok;
	    	
	    	GPLexer.InitSQLFileLexer(fileContent);
	    	TokenIterator iter = pt.getTokens();
	    	
	        while (iter.hasNext()) 
	        {
	           t = iter.next();
	           tok = t.getText();
	           if (t.type == TokenType.COMMENT) 
	           {
	        	   String term = IBMExtractUtilities.trimLine(tok);
        		   int pos = term.indexOf(tok1);
	        	   if (pos >= 0)
	        	   {
	        		   sqlTerminator = term.substring(pos+tok1.length()+1);
	        	   }
	           } else
	           {
	        	   String term = IBMExtractUtilities.trimLine(tok);
	        	   if (term.trim().equals(sqlTerminator))
	        	   {
		        	   if (sb.length() > 0)
		        	      sqlStatements.add(sb.toString());
		        	   sb.setLength(0);
	        	   } else
		              sb.append(t.getText());
	           } 
	        }   
	        return sqlStatements;
	   }
	    
	   public static ArrayList ParseSQLFile(String fileName) throws IOException
	   {
	    	Token t;
	        StringBuffer sb = new StringBuffer();
	        ArrayList sqlStatements = new ArrayList();
	    	String fileContent = IBMExtractUtilities.FileContents(fileName);
	    	String sqlTerminator = ";", methodToProcess = "";
	    	boolean inCode = false;
	    	String tok1 = "--#SET TERMINATOR", tok2 = "--#SET :";
	    	
	    	GPLexer.InitSQLFileLexer(fileContent);
	    	TokenIterator iter = pt.getTokens();
	    	
	    	sqlStatements.add("useDatabase"+"~#@"+sl.useDatabase);
	    	
	        while (iter.hasNext()) 
	        {
	           t = iter.next();
	           if (t.type == TokenType.COMMENT) 
	           {
	        	   String term = IBMExtractUtilities.trimLine(t.getText());
        		   int pos = term.indexOf(tok1);
	        	   if (pos >= 0)
	        	   {
	        		   sqlTerminator = term.substring(pos+tok1.length()+1);
	        		   //System.out.println("sqlTerminator="+sqlTerminator);
	        	   }
	           }
	           //System.out.print(t.getText());
	           if (t.type == TokenType.SQLTERMINATOR)
	           {
	        	   String term = IBMExtractUtilities.trimLine(t.getText());
	        	   if (term.equals(sqlTerminator))
	        	   {
		        	   //System.out.println("Ends Here = " + t.getText());
		        	   inCode = false;
		        	   if (sb.length() > 0)
		        	      sqlStatements.add(methodToProcess+"~#@"+sb.toString());
		        	   sb.setLength(0);
	        	   }
	           } 
	           if (t.type == TokenType.COMMENT && t.getText().startsWith(tok2))
	           {
	        	   methodToProcess = IBMExtractUtilities.trimLine(t.getText());
        		   int pos = methodToProcess.indexOf(tok2);
	        	   if (pos >= 0)
	        	   {
	        		   methodToProcess = methodToProcess.substring(pos+tok2.length());
	        		   //System.out.println("Method to Process="+methodToProcess);
	        	   }
	        	   inCode = true;
	        	   continue;
	           }
	           if (inCode)
	           {
	              sb.append(t.getText());
	           }
	        }   
	        return sqlStatements;
	   }
	    
	   public static void main(String[] args)
	   {
	       ArrayList al;
	 	   String fileName = "C:\\Vikram\\Prospects\\nrc\\temp_tables.sp";
	 	   try
	 	   {
	 			al = GPLexer.parseTempTableFile(args[0]);
	 			Iterator iter = al.iterator();
	 			while (iter.hasNext()) 
	 			{
	 				String element = (String) iter.next();
	 			    System.out.print(element);
	 			}
	 	   } catch (IOException e)
	 	   {
	 			e.printStackTrace();
	 	   }
	   }
}
