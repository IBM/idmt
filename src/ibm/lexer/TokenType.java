/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm.lexer;

public enum TokenType
{
	    IDB2WORDS,    // iDB2 transformation
	    OPERATOR,     // Language operators
	    KEYWORD,      // language reserved keywords
	    KEYWORD2,     // Other language reserved keywords, like C #defines
	    IDENTIFIER,   // identifiers, variable names, class names
	    NUMBER,       // numbers in various formats
	    STRING,       // String
	    STRING2,      // For highlighting meta chars within a String
	    COMMENT,      // comments
	    COMMENT2,     // special stuff within comments
	    REGEX,        // regular expressions
	    REGEX2,       // special chars within regular expressions
	    TYPE,         // Types, usually not keywords, but supported by the language
	    TYPE2,        // Types from standard libraries
	    TYPE3,        // Types for users
	    DEFAULT,      // any other text
	    WARNING,      // Text that should be highlighted as a warning
	    ERROR,        // Text that signals an error 
	    BODY,         // Start of the body of the trigger
	    INSERTING,    // If trigger action is inserting
	    UPDATING,    // If trigger action is inserting
	    DELETING,    // If trigger action is inserting
	    LINE,		  // Line marker found in the text
	    WHITESPACE,   // Whitespace - To retain original space
	    CHARLITERAL,  // Single quote Literals  
	    SQLTERMINATOR,// SQL Terminator
	    DELETEDTEXT,  // Deleted text. Text that can definitely be left out
     COMMENTEDTEXT,  // Text that's been commented out since it may be relevant
	    // Syntax that must be changed to make it db2 compliant
     MODIFIED     // Text has been modified to be compatible with db2
}
