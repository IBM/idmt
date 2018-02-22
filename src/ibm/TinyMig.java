    /*
     * IBM Confidential
     * 
     * OCO Source Materials
     * (C) Copyright IBM Corp. 2008
     *
     * The source code for this program is not published or otherwise divested of its trade secrets,
     * irrespective of what has been deposited with the U.S. Copyright Office.
     *
     * Created on 2 Jul 2008
     *
     * Author: Patrick Dantressangle dantress@uk.ibm.com
     * for regression tests:
     *  C:\Devjava\sqlplus\regression_tests.sql C:\Devjava\sqlplus\regression_tests_mod.sql -MERGE_ALL_INCLUDES_INTO_ONE_FILE -MODIFIER=XX -MAX_LINES=300000 -IFS -fp=0 -MERGE_LINES=80 -sxqlplus_mode -NO_KEEP_WRAPPED  -NO_REMOVE_WRAPPED
     *   C:\MTK\projects\Avaloq\all_ddl.src x21.sql  -fp=1 -REMOVE_WRAPPED
     * C:\Devjava\sqlplus\regression_tests.sql C:\Devjava\sqlplus\regression_tests_mod.sql -MERGE_ALL_INCLUDES_INTO_ONE_FILE -MODIFIER=XX -MAX_LINES=300000 -dont_modify_comments 
     * C:\MTK\projects\Avaloq\all_ddl.src C:\MTK\projects\Avaloq\x21.sql -silent -fp=1 -MAX_LINES=2700000 -MERGE_LINES=80
     * 
     * usage examples:
     *  C:\MTK\projects\IFS\10tb83i.sql    C:\MTK\projects\IFS\x.db2 -silent
     *  C:\MTK\projects\infor2\83ins_db2\10tb83i.sql  C:\MTK\projects\infor2\83ins_db2\10tb83i.psql
     *  .\83ins_db2\50cs83i.sql  .\83ins_db2\50cs83i.psql 
     *  C:\Devjava\sqlplus\83ins_db2\50cs83i.sql  C:\Devjava\sqlplus\83ins_db2\50cs83i.psql
     *  C:\Devjava\sqlplus\83ins_db2\10tb83i.sql  C:\Devjava\sqlplus\83ins_db2\10tb83i.psql
     *  C:\MTK\projects\Lagan\scripts\oraclesetup.sql C:\MTK\projects\Lagan\scripts\lagan.psql [-MERGE_ALL_INCLUDES_INTO_ONE_FILE ] 
     *  C:\MTK\projects\IFS\Hursley\Database\Appsrv.api C:\MTK\projects\IFS\Appsrv.api.mod -MERGE_ALL_INCLUDES_INTO_ONE_FILE
     *  
     *  30 july 2008: fixed issue when 2 variables are included one into another like "&PROJ" and "&PROJBUD"
     *                replaced fixed array by LinkedHashMap to be dynamix and keep order of discovery of variables.
     *                Process include files recursively (START keyword), assuming that the includes are in  the  same directory as the processed file.
     *  31 july 2008: remove single quotes in variable values
     *                only ask for input from user for the variable value if the DEFINEd variable hasn't be found previously
     *                fixed conflict with START <includefile> and  START WITH <value> of a sequence is located on a new line
     *  1 august 2008              
     *                [-MERGE_ALL]_INCLUDES_INTO_ONE_FILE option to coalesce everything into a final plsql file
     *  2 august 2008 process @ and @@ includes
     *                Remove comments from output file with -NO_COMMENT option 
     *               
     */

package ibm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TinyMig {

    //  Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
    public static final String PROPRIETARY_NOTICE =
        "Licensed Materials - Property of IBM\n\n" +
        "(C) Copyright IBM Corp. 2008 All Rights Reserved.\n" +
        "US Government Users Restricted Rights - Use, duplication or\n" +
        "disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
    
    private final static String crlf = System.getProperty("line.separator");
    private final static String sep = System.getProperty("file.separator");
    private static final char dirSepChar = File.separatorChar;
    private static BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    private static int foundvars = 0;
    private static boolean in_create_table = false;
    private static boolean sqlplus_mode = false; //that means we are using DB2 CLP 
    
    private static boolean process_SQLPLUS_VARS=true;
    private static boolean outputallinone = false;
    private static boolean silent = false;
    private static int merge_lines=-1;
    private static int fp = 1;  //fixpack 1 is the default level of DB2 fixpack
    private static boolean stop = false;
    private static boolean removecomments = false;
    private static boolean dont_modify_comments=false;
    private static boolean Keep_wrapped_code=false;
    private static boolean Remove_wrapped_code=false;

    private static String  source_modifier=" --PDA ";
    private static int max_lines=1000000; //default amount of lines to  be processed
    private static long alllines = 0;
    private static long nb_changed = 0;
    private static int nb_includes = 0;
    private static int nbincludesnotfound = 0;
    private static boolean IFS = false;

    private static String[] SetCmd = {
            "AUTO", // SET AUTO[COMMIT] {ON | OFF | IMMEDIATE | statement_count}
            "BREAK", //
            "BTITLE", //
            "CLEAR", //
            "COMPUTE",//
            "EDIT", //
            "DISCONNECT",//
            "HOST",//
            "SHUTDOWN",//
            "RUNFORM",//
            "SQL>",
            "WHENEVER",//
            "PRINT",//
            "TIMING",
            "COLSEP",// column_seperator Column separator is the text that will appear between columns.It defaults to a single space.
            "ESCAPE", // ESCAPE / character for instance
            "ECHO",// {ON | OFF} Determines whether SQL & CLPPlus script statements are shown to the screen as they are executed. The default is OFF.
            "FEED",// [BACK] {ON | OFF | row_threshold} Controls the display of interactive information after a SQL statement is run. row_threshold must be an integer. Setting row_threshold equal 0 is same as OFF. Setting ON is same as setting row_threshold equal 1.
            "FLU",// [SH] {ON | OFF} Setting Flush to ON creates makes the output buffer accesible to external programs even if it is still being appended too. However, it adds quit a bit of overhead to the process.
            "HEAD",// [DING] {ON | OFF} Determines if column headings are displayed for SELECT statements HEADS[EP] Sets the new heading seperator character used by the COLUMN HEADING command. The default is '|'.
            "LIN",// [ESIZE] width_of_line Specify the width of a line in characters (defaults to 132).
            "NEWP",// [AGE] lines_per_page Controls how many blank lines are printed after a page break. The default is 1. Use
            // SET NEWAPGE 0 to print a formfeed at the start of each new page.
            "NULL \"",// SET NULL "null string" The string that is displayed when showing a null value for a column value being displayed in the output buffer.
            "PAGES",// [IZE] Set the number of printed lines that fit on a page
            "SQLC",// [ASE] {MIX[ED] | UP[PER] | LO[WER]} Controls whether SQL statements transmitted to the server are converted to upper or lower case.
            "PAU",// [SE] {ON | OFF} If set to ON, the message "Hit ENTER to continue" will be displayed before each page break and the output will pause accordingly.
            "SPACE ",// number_of_spaces Similar to COLSEP, but less flexible, this command sets the number of spaces to display between columns.
            "SQLP",// [ROMPT] "prompt" The interactive prompt defaults to "SQL> ".
            "TERM",// [OUT] {ON | OFF} Determines whether the output from a command is displayed to the terminal
            "TIMI",// [NG] {ON | OFF} Controls whether elapsed time is display statement after it is executed.
            "VARIABLE",// vname type :  create a host variable (refered to with :varname later in sql/plus scripts
            "VER"// [IFY] { ON | OFF } Determines whether the old & new values of a SQL statement are displayed when a
    // substitution variable is encountered.
    };
    
    //Generic whitespace tokens
    private final static String WS0 = "\\s*"; //zero or more whitespace character
    private final static String WS1 = "\\s+"; //at least one whitespace character

    //tokens for pattern matching expressions
    private final static String AnyChar_tok=".*[\r]*[\n]*.*";
    private final static String Any_integer_tok="[0-9]+";    
    private final static String Any_optional_integer_tok="[0-9]*";    
    private static final String Anykeyword_tok="[A-Za-z0-9._&#\"\"]+"; //any SQL keyword
    private final static String Any_real_tok="\\-*[0-9.]+";    
    private final static String Any_identifier_tok="[a-zA-Z_.0-9&#\"\"]+";
    private static final String Any_SQLtype_tok="[A-Za-z0-9&#._\\(\\)%]+";  //any definition of SQL type
    //type tokens
    private static final String Varchar2_tok = "(?i)varchar[2]*"; //[Vv][Aa][Rr][Cc][Hh][Aa][Rr]2";
    private static final String Float_tok = "(?i)float";       //[Ff][Ll][Oo][Aa][Tt]";
    private static final String Number_tok = "(?i)number";     //[Nn][Uu][Mm][Bb][Ee][Rr]";
    //SQL tokens
    private static final String Add_tok = "(?i)add";         //[Aa][Dd][Dd]";
    private static final String Alter_tok = "(?i)alter";     //[Aa][Ll][Tt][Ee][Rr]";
    private static final String As_tok = "[Aa][Ss]";    
    private static final String As_Is_tok = "[AaIi][Ss]";
    private static final String AuthID_tok = "(?i)authid";   //[Aa][Uu][Tt][Hh][Ii][Dd]";
    private static final String By_tok = "(?i)by";           //[Bb][Yy]";
    private static final String Check_tok = "(?i)check";     //[Cc][Hh][Ee][Cc][Kk]";
    private static final String Constraint_tok = "(?i)constraint"; //[Cc][oO][Nn][Ss][Tt][Rr][Aa][Ii][Nn][Tt]";
    private static final String Create_tok="(?i)create";     //[Cc][Rr][Ee][Aa][Tt][Ee]";
    private static final String Current_User_tok = "(?i)current_user";  //[Cc][Uu][Rr][Rr][Ee][Nn][Tt]_[Uu][Ss][Ee][rr]";
    private static final String Cursors_tok = "(?i)cursors";   //[Cc][Uu][Rr][Ss][Oo][Rr][Ss]";
    private static final String Default_tok = "(?i)default";   //[Dd][Ee][Ff][Aa][Uu][Ll][Tt]";
    private static final String DBMS_IS_OPEN_tok ="(?i)DBMS_SQL[.](?i)is_open";
    private static final String Enable_tok = "(?i)enable";     //[Ee][Nn][Aa][Bb][Ll][Ee]";
    private static final String Exec_tok = "(?i)exec";         //[Ee][Xx][Ee][Cc]";
    private static final String Execute_tok = "(?i)execute";   
    private static final String Final_tok = "(?i)final";       //[fF][Ii][Nn][Aa][Ll]";
    private static final String For_tok = "(?i)for";           //[Ff][Oo][Rr]";
    private static final String Force_tok = "(?i)force";       //[Ff][Oo][Rr][Cc][Ee]";
    private static final String Foreign_tok="(?i)foreign";     //[Ff][Oo][Rr][Ee][Ii][Gg][Nn]";
    private static final String From_tok = "(?i)from";         //[Ff][Rr][Oo][Mm]";
    private static final String Grant_tok = "(?i)grant";         
    private static final String IS_tok = "[Ii][Ss]";
    private static final String IF_tok = "[Ii][Ff]";
    private final static String index_tok="(?i)index";   //[Ii][Nn][Dd][Ee][Xx]";
    private static final String KEY_tok = "(?i)key";          //[Kk][Ee][Yy]";
    private final static String Long_tok="(?i)long";     //[lL][Oo][Nn][Gg]";
    private final static String null_tok="(?i)null";     //[Nn][uU][Ll][Ll]";
    private static final String Mod_tok = "(?i)mod";          //[Mm][Oo][Dd]";
    private static final String Module_tok = "(?i)module";  
    private static final String no_Compress_optional_tok = "[Nn]*[Oo]*[Cc]*[Oo]*[Mm]*[Pp]*[Rr]*[Ee]*[Ss]*[Ss]*";
    private static final String Not_tok = "(?i)not";          //[Nn][Oo][Tt]";
    private static final String Novalidate_optional_tok = "[Nn]*[Oo]*[Vv]*[Aa]*[Ll]*[Ii]*[Dd]*[Aa]*[Tt]*[Ee]*";
    private static final String Nowait_tok = "(?i)nowait";    //[Nn][Oo][Ww][Aa][Ii][Tt]";
    private final static String object_tok="(?i)object"; //[Oo][Bb][Jj][Ee][Cc][Tt]";
    private static final String Of_tok = "(?i)of";            //[Oo][Ff]";
    private static final String On_tok = "(?i)on";  
    private static final String Only_tok = "(?i)only";        //[Oo][Nn][Ll][Yy]";
    private final static String or_tok="(?i)or";         //[Oo][Rr]";
    private final static String organization_tok="(?i)organization"; //[Oo][Rr][Gg][Aa][Nn][Ii][Zz][Aa][Tt][Ii][Oo][Nn]";
    private static final String Partition_tok = "(?i)partition"; //[Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn]";
    private static final String Pragma_tok = "(?i)pragma";       //[Pp][Rr][Aa][Gg][Mm][Aa]";
    private static final String PRIMARY_tok = "(?i)primary";     //[Pp][Rr][Ii][Mm][Aa][Rr][Yy]";
    private final static String Raw_tok="(?i)raw";       //[rR][Aa][Ww]";
    private static final String Read_tok = "(?i)read";           //[Rr][Ee][Aa][Dd]";
    private static final String References_tok="(?i)references"; //[Rr][Ee][Ff][Ee][Rr][Ee][Nn][Cc][Ee][Ss]";
    private static final String Rely_optional_tok = "[Rr]*[Ee]*[Ll]*[Yy]*";
    private final static String Replace_tok="(?i)replace"; //[Rr][Ee][Pp][Ll][Aa][Cc][Ee]";
    private static final String Restrict_tok="(?i)restrict";     //[Rr][Ee][Ss][Tt][Rr][Ii][Cc][Tt]";
    private final static String Result_tok="(?i)result"; 
    private static final String Retain_tok = "(?i)retain";       //[Rr][Ee][Tt][Aa][Ii][Nn]";
    private final static String Return_tok="(?i)return";   //[Rr][Ee][Tt][Uu][Rr][Nn]";
    private final static String Rollback_tok = "(?i)rollback"; //[Rr][Oo][Ll][Ll][Bb][Aa][Cc][Kk]";
    private final static String Savepoint_tok="(?i)savepoint"; //[Ss][Aa][Vv][Ee][Pp][Oo][Ii][Nn][Tt]";
    private static final String Self_tok = "(?i)self";
    private static final String SubType_tok = "(?i)subtype";     //[Ss][Uu][Bb][Tt][Yy][Pp][Ee]";
    private static final String Table_tok = "(?i)table";         //[Tt][Aa][Bb][Ll][Ee]";
    private static final String Tablespace_tok = "(?i)tablespace"; //[Tt][Aa][Bb][Ll][Ee][Ss][Pp][Aa][Cc][Ee]";
    private final static String To_tok="(?i)to";             //[Tt][Oo]";
    private final static String Then_tok="(?i)then";     
    private final static String True_tok="(?i)true";     
    private static final String Trigger_tok = "(?i)trigger";     //[Tt][Rr][Ii][Gg][Gg][Ee][Rr]";
    private static final String Trim_tok = "(?i)trim";           //[Tt][Rr][Ii][Mm]";
    private final static String Type_tok="(?i)type";         //[Tt][Yy][Pp][Ee]";
    private static final String Unique_tok = "(?i)unique";       //[Uu][Nn][Ii][Qq][Uu][Ee]";
    private static final String Using_tok = "(?i)using";
    private static final String Update_tok = "(?i)update";       //[Uu][Pp][Dd][Aa][Tt][Ee]";
    private static final String Values_tok = "(?i)values";       //[Vv][Aa][Ll][Uu][Ee][Ss]";
    private static final String View_tok = "(?i)view";           //[Vv][Ii][Ee][Ww]";
    private static final String With_tok = "(?i)with";           //[Ww][Ii][Tt][Hh]";
    private static final String Wrapped_tok = "(?i)wrapped";

 
    //composed tokens
    private static final String Restrict_reference_tok =Restrict_tok+"_"+References_tok;
    private final static String Any_basic_expression=Any_identifier_tok+WS0+"[\\(]*"+WS0+"[a-zA-Z_.0-9&#\"\"]*"+WS0+"[\\)]*"+WS0+Any_identifier_tok+WS0+"[+\\-*/.]"+WS0+Any_identifier_tok; //like L_MsgRec(i).MSGNR -1      
    private static final String Create_or_Replace_tok=Create_tok+WS1+or_tok+WS1+Replace_tok;




    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    //static final private TreeMap<String, Pattern> matchers = new TreeMap<String, Pattern>();

    static final Pattern pattern_ADD_CONSTRAINT1         =Pattern.compile(AnyChar_tok+WS1+Add_tok+WS0+"\\("+WS0+Constraint_tok+WS1+AnyChar_tok+"\\)"+WS0+Anykeyword_tok+";"+AnyChar_tok); // add ( constraint
    static final Pattern pattern_ADD_CONSTRAINT2         =Pattern.compile(AnyChar_tok+WS1+Add_tok+WS0+"\\("+WS0+Constraint_tok+WS1+AnyChar_tok+"\\)"+WS0+";"+AnyChar_tok);
    static final Pattern pattern_ALTER_TRIGGER_ENABLE    =Pattern.compile(AnyChar_tok+WS0+Alter_tok+WS1+Trigger_tok+WS1+Any_identifier_tok+WS1+Enable_tok+WS0+"[;]*"+AnyChar_tok);    //ALTER TRIGGER "TR_CAGL_AIR" ENABLE;
    static final Pattern pattern_Authid_Current_User     =Pattern.compile(AnyChar_tok+WS0+AuthID_tok+WS1+Current_User_tok+AnyChar_tok); 
    static final Pattern pattern_CHECK_CONSTRAINT1       =Pattern.compile(AnyChar_tok+/*WS1+Add_tok+WS1+Constraint_tok+WS1+Any_identifier_tok*/ WS1+Check_tok+WS0+"\\("+AnyChar_tok+"\\)"+WS0+Enable_tok+AnyChar_tok); //ADD CONSTRAINT "CK_IERU_5" CHECK ( Str_State In ( 'on', 'off' ) ) ENABLE;
    static final Pattern pattern_Create_or_Replace_Force_View = Pattern.compile(AnyChar_tok+WS0+Create_or_Replace_tok+WS1+Force_tok+WS1+View_tok+WS1+Any_identifier_tok+AnyChar_tok); //CREATE OR REPLACE FORCE VIEW "PV_ADMIN"."ELMT_DESC_VW" 
    static final Pattern pattern_Create_Or_Replace_Type_Object_single_line=Pattern.compile(AnyChar_tok+Create_or_Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+As_Is_tok+WS1+object_tok+WS0+AnyChar_tok);  //or replace type <objname> as object syntax
    static final Pattern pattern_Create_Or_Replace_Type_Object_two_lines=Pattern.compile(AnyChar_tok+As_Is_tok+WS1+object_tok+WS0+AnyChar_tok );
    static final Pattern pattern_Create_Or_Replace_Type_Object_two_lines2=Pattern.compile(AnyChar_tok+WS1+object_tok+WS0+"\\("+AnyChar_tok );
    static final Pattern pattern_Create_Or_Replace_Type_Row=Pattern.compile(AnyChar_tok+Create_or_Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1); //only for Db2 v9.7GA  (ulow.contains(" as row") && ulow.contains("or replace type")
    static final Pattern pattern_Create_Or_Replace_Type_Table=Pattern.compile(AnyChar_tok+Create_or_Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+As_Is_tok+WS1+Table_tok+WS1+Of_tok+WS1+AnyChar_tok);
    static final Pattern pattern_Create_Or_Replace_Type_Table_line1=Pattern.compile(AnyChar_tok+Create_or_Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+As_Is_tok+WS0+AnyChar_tok);
    static final Pattern pattern_Create_Or_Replace_Type_Table_line2=Pattern.compile(AnyChar_tok+WS1+Table_tok+WS1+Of_tok+WS1+AnyChar_tok+WS0+AnyChar_tok);
    static final Pattern pattern_Create_Or_Replace_Type_as_scalar=Pattern.compile(AnyChar_tok+Create_or_Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+As_Is_tok+WS1+"(?![Tt][Aa][Bb][Ll][Ee])"+WS0+AnyChar_tok);
    static final Pattern pattern_Create_Or_Replace_Type  =Pattern.compile(AnyChar_tok+Create_or_Replace_tok+WS1+Type_tok+WS0+AnyChar_tok );
    static final Pattern pattern_Create_Type             =Pattern.compile(AnyChar_tok+Create_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+AnyChar_tok);
    static final Pattern pattern_Default_in_parenth      =Pattern.compile(AnyChar_tok+WS1+Default_tok+WS1+"\\("+AnyChar_tok+"\\)"+WS0+AnyChar_tok);
    static final Pattern pattern_Default_NULL            =Pattern.compile(AnyChar_tok+WS1+Default_tok+WS1+null_tok+WS0+"[,]*"+AnyChar_tok);
    static final Pattern pattern_Exec                    =Pattern.compile(AnyChar_tok+WS0+Exec_tok+WS1+AnyChar_tok);
    static final Pattern pattern_final_EOL               =Pattern.compile(AnyChar_tok+"\\)"+WS1+Final_tok); //final @ end of line
    static final Pattern pattern_FK_CONSTRAINT2          =Pattern.compile(AnyChar_tok+WS0+References_tok+WS1+Any_identifier_tok+WS0+"\\("+Any_identifier_tok+"\\)"+WS0+Enable_tok+WS0+";"+AnyChar_tok);
    static final Pattern pattern_float_with_precision    =Pattern.compile(AnyChar_tok+WS0+Float_tok+WS0+"\\("+WS0+"("+Any_integer_tok+")"+WS0+"\\)"+AnyChar_tok);
    static final Pattern pattern_For_Update              =Pattern.compile(AnyChar_tok+WS1+For_tok+WS1+Update_tok+WS0+";"+AnyChar_tok); // for update; 
    static final Pattern pattern_For_Update_no_Wait      =Pattern.compile(AnyChar_tok+WS0+For_tok+WS1+Update_tok+WS1+Nowait_tok+WS0+"[;]*"+AnyChar_tok); // for update nowait;
    static final Pattern pattern_Is_Table_Of             =Pattern.compile(WS0+Type_tok+WS1+Any_identifier_tok+WS1+IS_tok+WS1+Table_tok+WS1+Of_tok+WS1+Any_SQLtype_tok+WS0+";"+WS0+AnyChar_tok);
    static final Pattern pattern_Is_open                 =Pattern.compile(WS0+IF_tok+WS0+"\\(*"+WS0+DBMS_IS_OPEN_tok+WS0+"\\(*"+WS0+Any_identifier_tok+WS0+"\\)*"+WS0+"[=]*"+WS0+"["+True_tok+"]*"+"\\)*"+WS0+Then_tok+AnyChar_tok);
    static final Pattern pattern_Create_Or_Replace_Type_Table_pct_type=Pattern.compile(AnyChar_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+As_Is_tok+WS1+Table_tok+WS1+Of_tok+WS1+Any_identifier_tok+WS1+"%"+Type_tok+WS0+";"+AnyChar_tok);
    static final Pattern pattern_Grant_Execute           =Pattern.compile(WS0+Grant_tok+WS1+Execute_tok+WS1+On_tok+WS1+Any_identifier_tok+WS1+To_tok+WS1+Any_identifier_tok+WS0+AnyChar_tok);
    static final Pattern pattern_Long_ColType            =Pattern.compile(AnyChar_tok+WS1+Long_tok+"([ ,]*)"+AnyChar_tok); //and if LONg is stil lthere, replaces LONG with CLOB
    static final Pattern pattern_Long_raw                =Pattern.compile(AnyChar_tok+WS1+Long_tok+WS1+"raw[ ,]*"+AnyChar_tok);
    static final Pattern pattern_Mod_Complex             =Pattern.compile(AnyChar_tok+"\\("+WS0+Any_basic_expression+WS0+"\\)"+WS0+Mod_tok+WS1+Any_identifier_tok+WS0+"="+WS0+Any_integer_tok+AnyChar_tok);
    static final Pattern pattern_Mod_Complex2            =Pattern.compile(AnyChar_tok+"\\("+WS0+Any_basic_expression+WS1+Mod_tok+WS1+Any_identifier_tok+WS0+"\\)"+WS0+"="+WS0+Any_integer_tok+AnyChar_tok);
    static final Pattern pattern_Mod_simple              =Pattern.compile(AnyChar_tok+WS1+Any_identifier_tok+WS1+Mod_tok+WS1+Any_identifier_tok+WS0+"="+WS0+Any_integer_tok+AnyChar_tok);
    static final Pattern pattern_No_Wait                 =Pattern.compile(AnyChar_tok+WS0+Nowait_tok+WS0+";"+AnyChar_tok); // for  nowait;   
    static final Pattern pattern_not_final               =Pattern.compile(AnyChar_tok+"\\)"+WS1+Not_tok+WS1+Final_tok+AnyChar_tok); //) not final  
    static final Pattern pattern_NOT_NULL_ENABLE         =Pattern.compile(AnyChar_tok+WS1+null_tok+WS1+Enable_tok+"[ ,]*"+AnyChar_tok);
    static final Pattern pattern_NULL_Clause             =Pattern.compile(AnyChar_tok+WS1+"null[ ,]*"+AnyChar_tok);  //replaces "null" on that line
    static final Pattern pattern_Number_With_Star        =Pattern.compile(AnyChar_tok+WS0+Number_tok+WS0+"\\("+WS0+"([3*][2-9]*)"+WS0+"[,\\)]*"+AnyChar_tok);
    static final Pattern pattern_Organization_Index      =Pattern.compile(AnyChar_tok+WS1+organization_tok+WS1+index_tok+WS0+no_Compress_optional_tok+AnyChar_tok);
    static final Pattern pattern_Partition_by            =Pattern.compile(AnyChar_tok+WS0+Partition_tok+WS1+By_tok+WS1+Any_identifier_tok+WS0+AnyChar_tok);
    static final Pattern pattern_Partition_values        =Pattern.compile(AnyChar_tok+WS0+"[\\(]*"+WS0+Partition_tok+WS1+Any_identifier_tok+WS1+Values_tok+WS1+AnyChar_tok+"[,\\);]");
    static final Pattern pattern_PK_CONSTRAINT           =Pattern.compile(AnyChar_tok+WS1+Constraint_tok+WS1+Any_identifier_tok+WS1+PRIMARY_tok+WS1+KEY_tok+WS0+"\\("+AnyChar_tok+"\\)"+WS0+Rely_optional_tok+WS0+Enable_tok+WS0+Novalidate_optional_tok+WS0+"[;]*"+AnyChar_tok);// CONSTRAINT "CONTACT_GEOADDR_PK" PRIMARY KEY ("GEOADDR_ID") ENABLE;
    static final Pattern pattern_Pragma_Restrict_references=Pattern.compile(AnyChar_tok+WS0+Pragma_tok+WS1+Restrict_reference_tok+WS0+AnyChar_tok);
    static final Pattern pattern_return_value            =Pattern.compile(AnyChar_tok+WS1+Return_tok+WS1+Any_identifier_tok+WS0+","+WS0+AnyChar_tok);  //is this line looking like  ".... return <identifier>,"
    static final Pattern pattern_return_self_as_result   =Pattern.compile(AnyChar_tok+WS1+Any_identifier_tok+WS1+Return_tok+WS1+Self_tok+WS1+As_tok+WS1+Result_tok+WS0+","+AnyChar_tok);  //is this line looking like  ".... return self as result,"
    static final Pattern pattern_Rollback_DB2_Savepoint  =Pattern.compile(On_tok+WS1+Rollback_tok+WS1+Retain_tok+WS1+Cursors_tok);
    static final Pattern pattern_Rollback_to_Savepoint   =Pattern.compile(AnyChar_tok+WS0+Rollback_tok+WS0+"[WwOoRrKk]*"+WS1+To_tok+WS1+Any_identifier_tok+WS0+";"+AnyChar_tok );
    static final Pattern pattern_Rollback_to_Savepoints  =Pattern.compile(AnyChar_tok+WS0+Rollback_tok+WS1+To_tok+WS1+Savepoint_tok+WS1+Any_identifier_tok+WS0+";"+AnyChar_tok);
    static final Pattern pattern_Savepoint               =Pattern.compile(AnyChar_tok+"[[^Tt][^Oo]]"+WS0+Savepoint_tok+WS1+Any_identifier_tok+WS0+";"+AnyChar_tok);
    static final Pattern pattern_SubType                 =Pattern.compile(WS0+SubType_tok+WS1+Any_identifier_tok+WS1+IS_tok+WS1+Any_SQLtype_tok+WS0+";"+AnyChar_tok);
    static final Pattern pattern_Tablespace              =Pattern.compile(WS0+Tablespace_tok+WS1+Any_identifier_tok+"[\t ;]*"+AnyChar_tok);
    static final Pattern pattern_Trim_From               =Pattern.compile(AnyChar_tok+WS0+Trim_tok+WS0+"\\("+WS0+"' '"+WS1+From_tok+WS1+Any_identifier_tok+AnyChar_tok);
    static final Pattern pattern_UNIQUE_CONSTRAINT       =Pattern.compile(AnyChar_tok+WS1+Constraint_tok+WS1+Any_identifier_tok+WS1+Unique_tok+WS0+"\\("+AnyChar_tok+"\\)"+WS0+Enable_tok+WS0+"[;]*"+AnyChar_tok);// CONSTRAINT "CONTACT_GEOADDR_PK" PRIMARY KEY ("GEOADDR_ID") ENABLE;
    static final Pattern pattern_using_index             =Pattern.compile(AnyChar_tok+WS1+Using_tok+WS1+index_tok+WS1+AnyChar_tok); //(ulow.contains(" using index"))
    static final Pattern pattern_Varchar2_byte_char      =Pattern.compile(AnyChar_tok+WS0+Varchar2_tok+WS0+"\\("+WS0+"([0-9]+)"+WS0+"([CcBb]*[HhYy]*[AaTT]*[RrEe]*)"+WS0+"\\)"+AnyChar_tok); //(ulow.contains("varchar2(xxx byte/char"))
//    static final Pattern pattern_Varchar2_32k            =Pattern.compile(AnyChar_tok+WS0+Varchar2_tok+WS0+"\\("+WS0+"(32[67][0-9][0-9])"+WS0+"[CcBb]*[HhYy]*[AaTT]*[RrEe]*"+WS0+AnyChar_tok); //(ulow.contains("varchar2(32"))
    static final Pattern pattern_With_Read_only          =Pattern.compile(AnyChar_tok+With_tok+WS1+Read_tok+WS1+Only_tok+WS0+"[;]*"+AnyChar_tok);  //with read only
    static final Pattern pattern_Wrapped                 =Pattern.compile(AnyChar_tok+WS1+Wrapped_tok+WS0+AnyChar_tok);
    
    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    // static LinkedHashMap Variable = new LinkedHashMap();
    public static boolean in_SET_Command(String firsttoken)
    {
        boolean found = false;
        for (int ii = 0; ii < SetCmd.length && found == false; ii++)
        {
            if (firsttoken.toUpperCase().startsWith(SetCmd[ii]) == true)
                return  true;
        }// endfor
        return found;
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static void usage()
    {
        System.out.println("Parameters required or optional in [] : <SQLPLUSFileName_source> <SQLPLUSFileName_modified> [-silent] [-FP=1] [-NO_COMMENT] [-MAX_LINES=x] [--MODIFIER=xxx] [-MERGE_ALL_INCLUDES_INTO_ONE_FILE]");
        System.out.println("[[-MERGE_ALL]_INCLUDES_INTO_ONE_FILE]: it is an optional parameter to output all the lines of the includes into the outputsqlplusfileName_modified file");
        System.out.println("[-FP=[1|2|3|4]]: level of DB2:remove some of the changes that were done for GA,FP1,FP2 version.");
        System.out.println("[-MAX_LINES=x]: optional pre-allocation of x amount of lines ( default is 1000000. ");
        System.out.println("[-SILENT]: don't display any traces/output");
        System.out.println("[-NO_COMMENT]: don't include comment lines");
        System.out.println("[--MODIFIER=xxx]: prefix all modified lines with the \"--xxx\" string ");        
        System.out.println("[[-DONT_MODIFY_COMM]ENTS] ||[[-DONT_MOD_COMM] : don't modify C-style comment lines into SQL-like comments"); 
        System.out.println("[-SQLPLUS_MODE]: don't change any SQLPLUS commands as the output wil lbe for DB2 CLPPLUS instead of DB2 CLP (default mode)");
        System.out.println("[[-KEEP_WRAPPED]_CODE]: Keep wrapped code lines into source instead of automatically commenting them out");
        System.out.println("[[-REMOVE_WRAPPED]_CODE]: totally delete wrapped code lines from source instead of automatically commenting them out");
        System.exit(0);
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static void Log(String st)
    {
        if (!silent)
        {
            System.out.println(st);
        }
    }

    private static void Log(int l, String st)
    { // line/ string
        if (!silent)
        {
            System.out.println("===> Line " + l + " modified =" + st);
        }
    }


    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String Replace_Var_In_PLace(String InSt, String varname, String value, int curLine)
    {
        boolean foundvar = false;
        String vup = varname.toUpperCase();
        String Up = InSt.toUpperCase();
        String RInSt = InSt;
        // for debug only
        int vlength = varname.length();
        int p = Up.indexOf(vup);
        int posEndOfVar = p + vlength;
        boolean endofVarisDelimiter = ((posEndOfVar >= InSt.length()) ? true : !Character.isJavaLetterOrDigit(InSt.charAt(posEndOfVar)));

        while ((p > -1) && (endofVarisDelimiter)) // try to match the variable with && first.
        {
            foundvar = true;
            if (!(posEndOfVar == RInSt.length()) && (RInSt.charAt(posEndOfVar) == '.'))
                posEndOfVar += 1; // ( RInSt.charAt(posEndOfVar+1)=='.') ? 1 : 0; // posEndOfVar +=2;

            RInSt = RInSt.substring(0, p) + value + RInSt.substring(posEndOfVar);
            foundvars++;
            // now need to see if there is another one of this variable in the same line
            Up = RInSt.toUpperCase();
            p = Up.indexOf(vup);
            if (p > -1)
            {
                posEndOfVar = p + vlength;
                endofVarisDelimiter = ((posEndOfVar >= RInSt.length()) ? true : !Character.isJavaLetterOrDigit(RInSt.charAt(posEndOfVar)));
            }
        }
        if (foundvar && curLine > 0)
            Log(curLine, " replaced variable " + varname + " in===>" + InSt.trim() + "  ====>" + RInSt.trim());

        return RInSt;
    }

    /***************************************************************************************************************************
     * replace any variables (starting with &) in InSt string
     **************************************************************************************************************************/
    private static String Replace_Var_In_String(HashMap<String, String> V, String InSt, int curLine)
    {
        if (! process_SQLPLUS_VARS)
            return InSt;
        Iterator<Map.Entry<String,String>> i = V.entrySet().iterator();
        while ((InSt.indexOf('&') > -1) && i.hasNext())
        {
            Map.Entry<String,String> me = (Map.Entry<String,String>) i.next();
            String vv = "&" + (String) me.getKey();
            String vl = (String) me.getValue();
            if (!vv.equalsIgnoreCase(vl)) // make sure we don't infinitively loop on itself...
            {
                InSt = Replace_Var_In_PLace(InSt, "&" + vv, vl, curLine);
                if ((InSt.indexOf('&') > -1))
                    InSt = Replace_Var_In_PLace(InSt, vv, vl, curLine);
            }
        }
        return InSt;
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String InputFromUser(String Msg)
    {
        String inp = "";
        System.out.print(Msg);

        try
        {
            inp = stdin.readLine();
        } catch (Exception e)
        {
        }
        return inp;
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static void DumpVariableTable(HashMap<String, String> V )
    {
        Log("=================================================");
        Log("Found " + foundvars + " instances of variables, and " + V.size() + " variable declarations");
        Log("Dumping variable table...");
        Log("=================================================");
        Set<Map.Entry<String, String>> set = V.entrySet();
        Iterator<Entry<String, String>> i = set.iterator();
        for (int n = 1; (i.hasNext()); n++)
        {
            Map.Entry<String, String> me = (Map.Entry<String, String>) i.next();
            Log(" " + n + ":" + (String) me.getKey() + "=>[" + me.getValue() + "]");
        }
        Log("=================================================");
    }

    /***************************************************************************************************************************
     * remove comments from Variable value
     **************************************************************************************************************************/
    private static String CheckVariableValue(String val)
    {
        String vtmp = val.trim();
        int p = vtmp.indexOf("/*");
        int p2 = vtmp.lastIndexOf("*/");
        if (p2 > p ) // mean that we found at least one C-style comment in there, so remove it...
            vtmp = vtmp.substring(0, p).trim() + vtmp.substring(p2 + 2);
        else {
            int p3=vtmp.indexOf("--");
            if (p3>0)// mean that we found at least one SQL-style comment in there, so remove it...
               vtmp= vtmp.substring(0, p3).trim();
        }
        // boolean deb = (vtmp.startsWith("'")) ? true : false;
        // boolean fin = (vtmp.endsWith("'")) ? true : false;
        // if (deb || fin)
        // vtmp = vtmp.substring((deb) ? 1 : 0, vtmp.length() + ((fin) ? -1 : 0));
        return vtmp;
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static void InsertNewVariable(HashMap<String, String> V, String key, String value)
    {
        value = CheckVariableValue(value);
        value = Replace_Var_In_String(V, value, -1);
        V.put(key.trim(), value);
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String Process_Comment_On_Column( String[] tok2s, int cl, String inputLine)//, HashMap<String, String> V)
    {
        String[] toks = tok2s[1].trim().split(WS1); // find out what is in the first part of the statement
        int p = toks[2].indexOf("..");
        if (toks.length > 2)
            if (toks[0].equalsIgnoreCase("ON") && toks[1].equalsIgnoreCase("COLUMN") && (p > 0))
            {
                inputLine = inputLine.replace("..", ".");
            }
        // else
        return inputLine; 
    }

    /**************************************************************************************************************************
     * get indentation from string
     **************************************************************************************************************************/
    private static String get_indent(String st)
    {
      int lg=st.length();
      for (int i=0; i< lg; i++)
          if (st.charAt(i)!= ' ' && st.charAt(i)!= '\t')
              return st.substring(0,i);
      return "";
    }
    

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String  change_string(String line, String old, String newx, String comm, boolean newline)
    {
        return  line.replace(old, newx) + ((newline)? "\n":" ")+source_modifier+ comm;
    }

    /************************************************************************************************************
     * processing of IF/While implicit conditional boolean comparions
     ************************************************************************************************************/
    private static boolean Rewrite_IF_and_While(String[] fs,String[] toks, int cl, String line)
    {
        if (fp>0) //at least FP1
            return false;
        String ulow=line.toLowerCase(); String ltok1;
        boolean changed=false;
        toks = line.trim().split(WS1);
 //       if (cl > 59 && cl < 63)
 //            stop=true;
        String ltok0=toks[0].toLowerCase();
        // System.out.println("processing=>"+line+"<");
        if ((ltok0.equals("if") && (ulow.contains(" then")==false)) ||
                (ltok0.equals("elsif") && (ulow.contains(" then")==false)) ||
                (ltok0.equals("while") && (ulow.contains(" loop")==false))
                )
            { //is THEN or LOOP on the next line? if yes, bring it on this line and comment out the next one
                String nextLine=fs[cl+1].trim();
                String nxttmp=nextLine.toLowerCase();
                if (nxttmp.startsWith("then") ||nxttmp.endsWith(" then") || nxttmp.startsWith("loop") || nxttmp.endsWith(" loop"))                       
                {
                    String fstmp=line+" "+nextLine;
                    line=fstmp;  fs[cl]=fstmp;
                    fstmp=get_indent(fs[cl+1])+source_modifier+nextLine;
                    fs[cl+1]=fstmp;
                    toks = line.trim().split(WS1);
                    ulow = line.toLowerCase();            
                }     
            }
        //if (line.indexOf("'") == -1) // no strings found on that line, can remove spaces bewteen commas
        {
            line = line.replaceAll(", ", ",").replace("( ", "(").replace(" )",")");
            toks = line.trim().split(WS1);
            //need to add a space after if, elsif or while.
            String utok0=toks[0].toLowerCase();
            if (utok0.startsWith("while(") || utok0.contains("if(")|| utok0.contains("elsif("))
            {
                int p=line.indexOf("(");
                if (p>=0)
                    line=line.substring(0,p)+" "+line.substring(p); //replaceFirst("("," (");
                toks = line.trim().split(WS1);
                ulow = line.toLowerCase();            
            }
        }

        ltok1 = toks[1].toLowerCase();
        if ((toks.length == 2) && (toks[1].startsWith("(") && toks[1].endsWith(")") == true))
        { // found pattern similar to "if (G_TrdIsReversal)"
            String newtok = toks[1].substring(1, toks[1].length());
            String newline = line.replace(newtok, "true=" + newtok);
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if ((toks.length > 2) && (ltok1.contains("%isopen")==false) && (ltok1.contains("%notfound")==false)&& (ltok1.contains("%found")==false)
                && (toks[2].equalsIgnoreCase("then") || (toks[2].equalsIgnoreCase("and") || 
                       (toks[2].equalsIgnoreCase("loop")))) && (toks[1].contains("=") == false)
                && (toks[1].contains("<") == false) && (toks[1].contains(">") == false)&& (toks[1].contains("!=") == false))
        {
            String newline = "";
            if (toks[2].equalsIgnoreCase("then") || (toks[2].equalsIgnoreCase("and"))|| (toks[2].equalsIgnoreCase("loop")))
            {
                if (toks[1].startsWith("(") && toks[1].endsWith(")"))
                {
                    String newtok = toks[1].substring(1, toks[1].length() - 1);
                    newline = line.replace(newtok, "true=" + newtok);
                }
                else
                    newline = line.replace(toks[1], toks[1] + "=true ");
            }
            else
                newline = line.replace(" " + toks[2], "=true " + toks[2]);
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if (ltok1.equals("not") && toks.length == 3)
        { // no then here, need to add the true
            // immediately at the end of token 2 instead
            String newline = "";
            if (toks[2].startsWith("(") && toks[2].endsWith(")") == true)
            {
                String newtok = toks[2].substring(1, toks[2].length() - 1);
                newline = line.replace(newtok, newtok + "=true ");
            }
            else
                newline = line.replace(toks[2], toks[2] + "=true ");
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if (ltok1.equals("not") && (toks[3].contains("=") == false) && (toks[2].contains("=") == false))
        {
            changed = true;
            String newline = "";
            int thenpos = 0;
            for (int i = toks.length - 1; i > 1 && thenpos == 0; i--)
                // find the THEN token
                if (toks[i].equalsIgnoreCase("then") || (toks[i].equalsIgnoreCase("loop")))
                    thenpos = i;
            if (thenpos == 0)
                newline = line.replace(toks[2], "true=" + toks[2]);
            else if (toks[2].equals("("))
                newline = line.replace(toks[2], "(true=");
            else
                newline = line.replace(toks[thenpos], "=true " + toks[thenpos]);
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if ((toks.length == 5)
                && (toks[4].equalsIgnoreCase("then") == true || (toks[4].equalsIgnoreCase("and")) || (toks[4]
                        .equalsIgnoreCase("loop"))) && toks[1].startsWith("(") && toks[3].endsWith(")")
                && !toks[1].equalsIgnoreCase("(not") && (line.contains("=") == false)
                && (line.contains("<") == false) && (line.contains(">") == false))
        { // found pattern if ( G_TrdIsReversal ) then
            String newline;
            if (toks[1].equals("(") && toks[3].equals(")")) // pattern like IF ( xx(p1,p2 )) then
                newline = line.replace(toks[2], toks[2] + "=true ");
            else
                // pattern like IF (xx(p1, p2 )) then
                newline = line.replace(toks[1], "(true=" + toks[1].substring(1));
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if ((toks.length == 5)
                && (toks[4].equalsIgnoreCase("then") == true || (toks[4].equalsIgnoreCase("and")) || (toks[4]
                        .equalsIgnoreCase("loop"))) && toks[1].contains("(") && toks[3].endsWith(")")
                && !toks[1].equalsIgnoreCase("(not") && (line.contains("=") == false)
                && (line.contains("<") == false) && (line.contains(">") == false))
        { // found pattern if (G_TrdIsReversal( ppp )) then
            String newline;
            if (toks[1].equals("(") && toks[3].equals(")")) // pattern like IF ( xx(p1,p2 )) then
                newline = line.replace(toks[2], toks[2] + "=true ");
            else
                // pattern like IF xx(p1, p2 ) then
                newline = line.replace(toks[1], "true=" + toks[1]);
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if ((toks.length == 5)
                && (toks[4].equalsIgnoreCase("then") == true || (toks[4].equalsIgnoreCase("and")) || (toks[4]
                        .equalsIgnoreCase("loop"))) && toks[1].equalsIgnoreCase("(not") && toks[3].endsWith(")")
                && (line.contains("=") == false) && (line.contains("<") == false) && (line.contains(">") == false))
        { // found pattern if (not G_TrdIsReversal ) then
            String newline = line.replace(toks[1], toks[1] + " true= ");
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if ((toks.length >= 4) && toks[1].startsWith("(") && (toks[2].endsWith(")") == true)
                && toks[2].contains("%") == false && (line.contains("=") == false) && (line.contains("<") == false)
                && (line.contains(">") == false))
        {
            String newline;
            if (toks[1].equalsIgnoreCase("(not"))
                newline = line.replace(toks[1], toks[1] + " true=");
            else
                newline = line.replace(toks[1], "( true=" + toks[1].substring(1));
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        else if ((toks.length == 6)
                && (toks[5].equalsIgnoreCase("then") == true || (toks[5].equalsIgnoreCase("and")) || (toks[5]
                        .equalsIgnoreCase("loop"))) && toks[1].startsWith("(") && toks[4].endsWith(")")
                && (line.contains("=") == false) && (line.contains("<") == false) && (line.contains(">") == false))
        { // found pattern if (G_TrdIsReversal(ff, ff, ff)) then
            String newline;
            if (ltok1.equalsIgnoreCase("(not"))
                newline = line.replace(toks[1], toks[1] + " true=");
            else
                newline = line.replace(toks[1], "(true=" + toks[1].substring(1));
            changed = true;
            fs[cl]=change_string(line, line, newline, "",false);
        }
        return changed;
    }
    
    /********************************************************************************************************************
     * Basic simple rewrite
     ********************************************************************************************************************/
    private static boolean BasicSimplerewriteByReplaceString( String[] fs,String sttoreplace,String newstring, int cl, String line, String ulow,boolean newline, boolean distinct_type)
    {
        int p = ulow.indexOf(sttoreplace);
        if (p>-1)
        {
            String stmp="";
            stmp=change_string(line, line.substring(p, p + sttoreplace.length()),newstring, sttoreplace,newline);
            if (distinct_type)
            {
                int s=stmp.indexOf(";");                
                p=stmp.indexOf("--");
                if (s<p && s>0) p=s;
                if (p>-1)
                    stmp = stmp.substring(0,p).trim()+" WITH COMPARISONS "+stmp.substring(p);
                else stmp=stmp.trim()+" WITH COMPARISONS ";
            }
            fs[cl]=stmp;                    
            return true;
        }
        return false;
    }

    /***************************************************************************************************************************
     * OBSOLETE ? 
     **************************************************************************************************************************/
//    static final private HashMap<String, Pattern> patterns = new HashMap<String, Pattern>();
//
//    private static boolean matchesx(String patternst, String st) 
//    {
//        Pattern p;
//        if (patterns.containsKey(patternst)==false)
//        {
//           p = Pattern.compile(patternst);
//           patterns.put(patternst, p);
//        }
//        else
//            p=patterns.get(patternst);
//       return  p.matcher(st).matches();
//    }
 
    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static boolean Process_Object_type(String[] fs, int cl, int clrepl, String typeStr, int nblines)
    {
        boolean changed=false;
        String line=fs[cl]; String orgline=line;
        String ulow=line.toLowerCase();
        if (/*ulow.contains(typeStr) &&*/ cl != clrepl  ) // is or replace and as object on 2 lines? merge them            
        {
            int poscomment=fs[clrepl].indexOf("--");
            if (poscomment==-1)
                fs[clrepl]=fs[clrepl]+" "+fs[cl].trim();// merge the 2 lines above and comment out the current line
            else
            { //make sure we add the next line before the comment
                fs[clrepl]=fs[clrepl].substring(0,poscomment).trim()+" "+fs[cl].trim()+ fs[clrepl].substring(poscomment);
            }
            fs[cl]=source_modifier+fs[cl]; //commenting out the current line.
            //now process the previous line as if it is the current line.
            line=fs[clrepl];
            ulow=line.toLowerCase();
            cl=clrepl;
        }
        int p = ulow.indexOf("or replace type");
        if ((p>-1) && (fp==0)) //only for Db2 v9.7GA
        {
            line = line.replace(line.substring(p, p + "or replace type".length()), " TYPE ")+source_modifier+"removed OR REPLACE";
            fs[cl]=line;
            ulow = line.toLowerCase();
            changed=true;
        }
        p = ulow.indexOf(typeStr.toLowerCase());
        if (p>-1)
        {
            int plg=p + typeStr.length();
            String Type_to_subst=line.substring(p, plg);
            String Utype=typeStr.toUpperCase();
            fs[cl]=change_string(line,  Type_to_subst, " AS ROW ", " "+Utype,false);
            orgline=change_string(line, Type_to_subst, " AS "    , " "+Utype,false).replace(" type ", " package ").replace(" TYPE ", " package ");
            changed=true;
        }
        //now find if there is any static methods or members in this object up to / delimiter.
        boolean method_found=false;
        boolean end_of_pck_found=false;
        for ( int i=cl; (i < nblines) && (!end_of_pck_found) ; i++)
        {
            line=fs[i];
            String fslow=line.trim().toLowerCase();
            if (fslow.equals("/"))
            {
                if( method_found == true)
                    fs[i]=" end;"+crlf+"/"+crlf;
                end_of_pck_found=true;
                continue;
            }
               
            if (fslow.startsWith(","))
            {
                String fs_1=fs[i-1];
                int pcomment=fs_1.indexOf("--");
                if (pcomment>-1)
                     fs[i-1]=fs_1.replace("--", ", --");
                else 
                    fs[i-1]=fs_1+",";
                String s1=fs[i].substring(fs[i].indexOf(",")+1); //remove first comma
                fs[i]=s1;
                line=s1;
                fslow=s1.trim().toLowerCase();
            }
            if (fslow.indexOf("final")>-1 && pattern_not_final.matcher(fslow).matches()) //(matches(AnyChar_tok+"\\)"+WS1+"not"+WS1+"final"+AnyChar_tok,fsl))
            {
                String delim=((method_found==true)?";":")");
                String s1=fs[i].replaceFirst("\\)"+WS1+Not_tok+WS1+Final_tok,delim+source_modifier+"removing ) not final "); //remove not final
                fs[i]=s1;
                line=s1;
                fslow=s1.trim().toLowerCase();
            }
            else              
            if (fslow.indexOf("final")>-1 && pattern_final_EOL.matcher(fslow).matches()) // (matches(AnyChar_tok+"\\)"+WS1+"final",fsl)) //final @ end of line
            {
                String delim=((method_found==true)?";":")");
                String s1=fs[i].replaceFirst("\\)"+WS1+Final_tok,delim+source_modifier+"removing ) final "); //remove not final
                fs[i]=s1;
                line=s1;
                fslow=s1.trim().toLowerCase();
            }
            if (fslow.indexOf("return")>-1 && pattern_return_value.matcher(fslow).matches()) //(matches(AnyChar_tok+WS1+"return"+WS1+Any_identifier_tok+WS0+","+WS0+AnyChar_tok,fsl))  //is this line looking like  ".... return <identifier>,"
            {
                String s1=fs[i].replaceFirst(WS1+Return_tok+WS0+"("+Any_identifier_tok+")"+WS0+","," return $1;"); //remove comma after identifier
                fs[i]=s1;
                line=s1;
                fslow=s1.trim().toLowerCase();
            }
            if (fslow.indexOf("return")>-1 && fslow.indexOf("result")>-1 && pattern_return_self_as_result.matcher(fslow).matches()) 
            {
                String s1=fs[i].replaceFirst("("+Any_identifier_tok+")"+WS1+Return_tok+WS1+Self_tok+WS1+As_tok+WS1+Result_tok+WS0+","," $1 return $1;");
                fs[i]=s1;
                line=s1;
                fslow=s1.trim().toLowerCase();
            }            
            if(fslow.startsWith("static ") || fslow.startsWith("member ")|| fslow.startsWith("constructor "))
            {

                String[] stmptok=fs[i].trim().split(WS1);
                if (stmptok[0].equalsIgnoreCase("static") ||stmptok[0].equalsIgnoreCase("member")||stmptok[0].equalsIgnoreCase("constructor") )//ok foundit.. replace it with  nothing
                   line=change_string(line, stmptok[0], " ", "removing "+stmptok[0],false);
                fs[i]=line;
                if (method_found==false)
                {  //first method, close the object immediately, comma probably on previous line
                    {
                        String s=fs[i-1];
                        if (s.trim().endsWith(","))
                           fs[i-1]=s.substring(0,s.lastIndexOf(","))+")"+source_modifier+"finishing object here for now";
                        line="/"+crlf+orgline.replaceFirst("[Aa][Ss]"+WS1+"\\("+WS1+"--", "AS --")+crlf+fs[i];  //replace ending ( remaining from object declaration
                        fs[i]=line;
                    }
                }
                if (!silent)
                    System.out.print(line);
                method_found=true;
            }
        }
        return changed;
    }
    /***************************************************************************************************************************
     *   DBMS_SQL.BIND_VARIABLE(v_CursorID, ':d1', p_department1);
     *   DBMS_SQL.DEFINE_COLUMN(v_CursorID, 1, v_FirstName, 20); 
     **************************************************************************************************************************/
//NOTE to Patrick: Not sure I can really get this coded as I'm not sure I can find the real defintion of the type of the variable.
//    
//    private static final int DBMS_SQL_Bind_Variable = 1;
//    private static final int DBMS_SQL_COLUMN_VALUE = 2;
//    
//    private static boolean rewrite_dbms_sql(String[] fs, int cl,String ulow, int typeStr)
//    {
//        String line=fs[cl];
//        boolean changed=false;
//        int p=-1;
//        switch (typeStr) { 
//            case DBMS_SQL_Bind_Variable: p= ulow.indexOf("dbms_sql.bind_variable");  break;  
//            case DBMS_SQL_COLUMN_VALUE :  p= ulow.indexOf("dbms_sql.column_value");  break; 
//            default: return false;
//        } 
//        
//        int ppar=ulow.indexOf("(", p); //find first opening (
//        int p2ndpar=ulow.indexOf(")");
//        String parmstok[];
//        switch (typeStr) { 
//            case DBMS_SQL_Bind_Variable: parmstok = ulow.substring(ppar+1,p2ndpar).split(",",5);
//                break;  
//            case DBMS_SQL_COLUMN_VALUE : parmstok = ulow.substring(ppar+1,p2ndpar).split(",",5); 
//                break;
//            default: return false;
//        } 
//        if ((p>-1)&&(ppar>p)  && p2ndpar>ppar && parmstok.length >= 3)
//        {
//            String varsql="";
//            switch (typeStr) {
//                case DBMS_SQL_Bind_Variable: varsql =parmstok[2].trim(); break;
//                case DBMS_SQL_COLUMN_VALUE:  varsql =parmstok[2].trim(); break;
//            }
//            if (varsql.equals(""))
//                return false;
//            System.out.println("Found first variable name "+varsql);
//            int kk=cl; 
//            boolean found=false;
//            String varlobu=varsql.toLowerCase();
//            //trying to find the variable declarations above this line... 
//            for ( ;  kk>=0 && found==false; kk--)
//            {
//               String tmp=fs[kk].toLowerCase();
//               if ((tmp.indexOf(varlobu)>-1 && (tmp.indexOf(" clob")>-1 || tmp.indexOf(" blob")>-1))) 
//                {
//                   kk++;
//                   found=true;
//                }
//            }
//            //TODO ok  up to here...
//            if ( kk> 0 && found==true) //found the variable declaration
//            {                 
//                String tok3[] = fs[kk].trim().split(WS1);
//                //where is the variable
//                int vartok=-1;
//                int lobtok=-1;
//                System.out.println("Found dbms_sql variable name "+varsql);
//            }
//        }
//        return changed;
//    }
    
    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static boolean rewrite_dbms_lob(String[] fs, int cl,String ulow, String typeStr,String endofvar)
    {
        String line=fs[cl];
        boolean changed=false;
        int p = ulow.indexOf(typeStr);
        int ppar=ulow.indexOf("(", p); //find first opening (
        //String endofvar=(typeStr.equalsIgnoreCase("dbms_lob.close")? ")": ","); //find first closing ) after  variable for dbms_lob.close or comma for open
        int pendofvar=ulow.indexOf(endofvar, ppar); //find first comma after variable 
        if ((p>-1)&&(ppar>p)&&(pendofvar>ppar))
        {
            String varlob=line.substring(ppar+1,pendofvar).trim();
            //System.out.println("Found LOB variable name "+varlob);
            int kk=cl; 
            boolean found=false;
            String varlobu=varlob.toLowerCase();
            //trying to find the variable declarations above this line... 
            for ( ;  kk>=0 && found==false; kk--)
            {
               String tmp=fs[kk].toLowerCase();
               if ((tmp.indexOf(varlobu)>-1 && (tmp.indexOf(" clob")>-1 || tmp.indexOf(" blob")>-1))) 
                {
                   kk++;
                   found=true;
                }
            }
                  
            if ( kk> 0 && found==true) //found the variable declaration
            {                 
                String tok3[] = fs[kk].trim().split(WS1);
                //where is the variable
                int vartok=-1;
                int lobtok=-1;
                for (int t=0; t<tok3.length ; t++){
                    String tu=tok3[t].toLowerCase();
                    int plob=tu.indexOf(varlobu);
                    if (plob==0 || (plob>=1 && tu.charAt(plob-1) != '_'))
                    {
                       vartok=t;  
                    }
                    else {  //order of if then else is important, don't switch it around
                        plob=tu.indexOf("clob");
                        if (( plob==0 || (plob>=1 && tu.charAt(plob-1) != '_'  )) && !(tu.contains(varlobu) ))
                           lobtok=t;
                        else {
                            plob=tu.indexOf("blob");
                            if (( plob==0  ||(plob>=1 && tu.charAt(plob-1) != '_'  )) && !(tu.contains(varlobu) ))
                                lobtok=t;
                        }
                    }
                }
                if ((vartok>-1) && (lobtok>-1)) //tok3[vartok].equalsIgnoreCase(varlob))
                {
                    String orig_token=line.substring(p, p + typeStr.length());
                    String lob_type="";
                    String lob_typedef=tok3[lobtok].toLowerCase();
                    if (lob_typedef.startsWith("clob"))
                        lob_type="_CLOB";
                    else
                    if (lob_typedef.startsWith("blob"))  
                        lob_type="_BLOB";
                    
                    if (! lob_type.equals("")) //lob_type exists...do replacement now
                    {
                        if (typeStr.equals("dbms_lob.createtemporary"))
                            fs[cl]=fs[cl].replace(orig_token, varlob+":=EMPTY"+lob_type+"();"+source_modifier+"Replaced createtemporary() with DB2 equivalent."+orig_token);
                        else
                            fs[cl]=change_string(line, orig_token, orig_token+lob_type, "Replacing with DB2 "+typeStr+lob_type+" call",false);
                        changed=true;
                    }
//                    else  unknown lob type....for future enhancements
                }
            }                
        }
        return changed;
    }
    
    /***************************************************************************************************************************
     * there must be a dbms_lob function call in that line. find out which one...
     **************************************************************************************************************************/
    private static boolean Process_dbms_lobs(String[] fs, int cl,String ulow  )
    {
        boolean changed=false;
        if (ulow.contains("dbms_lob.append") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.append",",");
        else if (ulow.contains("dbms_lob.open") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.open",",");
        else if (ulow.contains("dbms_lob.close") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.close",")");
        else if (ulow.contains("dbms_lob.copy") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.copy",",");
        else if (ulow.contains("dbms_lob.read") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.read",",");
        else if (ulow.contains("dbms_lob.erase") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.erase",",");
        else if (ulow.contains("dbms_lob.trim") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.trim",",");
        else if (ulow.contains("dbms_lob.writeappend") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.writeappend",",");
        else if (ulow.contains("dbms_lob.write") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.write",",");
        else if (ulow.contains("dbms_lob.createtemporary") )
            changed=rewrite_dbms_lob(fs, cl, ulow,  "dbms_lob.createtemporary",",");
        return changed;
    }

    /***************************************************************************************************************************
     * this is used to extract a token(n)  out of a string follwing a pattern
     **************************************************************************************************************************/
    private static String ExtractTokenFromPattern(String source,int token_number,String patternst) 
    {
        String token="";
        Pattern pattern = Pattern.compile(patternst );
        Matcher matcher = pattern.matcher(source);
        boolean matchFound = matcher.find();                    
        if (matchFound && matcher.groupCount()>0) { // Get all groups for this match 
            token= matcher.group(token_number); //extract just  the is/as group of characters
        }
        return token;
    }
    
    /***************************************************************************************************************************
     *  Process_datatype_limitations for varchars2 above 32k, float( ) with precision and number() with * as precision
     **************************************************************************************************************************/
    private static boolean Process_datatype_limitations(String[] fs, String[] tok2s, int cl, String line, String ulow)
    {
        boolean changed=false;
       
        if (ulow.indexOf("varchar")>-1 && pattern_Varchar2_byte_char.matcher(line).matches()) //.matcher(line).matches())
        {
            Matcher matcher= pattern_Varchar2_byte_char.matcher(line); //pattern_Varchar2_32k.matcher(line);
            matcher.find();
            //int g=matcher.groupCount();
            if (matcher.groupCount()>0)
            {
                String varchar_sz=   matcher.group(1);
                String bytechar_qualifier=(matcher.groupCount()>=2)? matcher.group(2): "";
                changed = rewrite_Varchar2_32k(fs, cl, line, changed,varchar_sz,bytechar_qualifier);
            }
        }
        else if ( ulow.indexOf("float")>-1 && pattern_float_with_precision.matcher(line).matches())
        {
            Matcher matcher= pattern_float_with_precision.matcher(line);
            matcher.find();
            if (matcher.groupCount()>0)
            {
                String v= matcher.group(1);
                changed = rewrite_float_with_precision(fs, cl, line, ulow, v,changed);
            }
        }
        else if (ulow.indexOf("number")>-1  && pattern_Number_With_Star.matcher(line).matches())
        {
            Matcher matcher= pattern_Number_With_Star.matcher(line);
            matcher.find();
            if (matcher.groupCount()>0)
            {
                String v=   matcher.group(1);
                changed = rewrite_number_above_31(fs, cl, line, ulow, v,changed);                
            }
        }
        return changed;
    }

    /****************************************************************************************************************************
     * change number(39) where x > 38 into 31
     * @param fs
     * @param cl
     * @param line
     * @param ulow
     * @param changed
     * @return
     ***************************************************************************************************************************/
    private static boolean rewrite_number_above_31(String[] fs, int cl, String line, String ulow,String precision, boolean changed)
    { //(AnyChar_tok+WS0+Number_tok+WS0+"\\("+WS0+"[3*][2-9]*"+WS0+"[,\\)]*"+AnyChar_tok)
        if (precision.equals("*"))
        {
            fs[cl]=line.replaceFirst(Number_tok+WS0+"\\("+WS0+"\\*"+WS0+"([,\\)]*)"+WS0," number(10$1 ")+source_modifier+" removing (*) on number  definition";
            return true;
        }
        else
            try{
                if (Integer.valueOf(precision)>31)          
                {
                    fs[cl]=fs[cl].replaceFirst(Number_tok+WS0+"\\("+WS0+precision+WS0+"([\\),])"," number(31$1 ") +source_modifier+" removing precision "+precision+">31";
                    return true;
                }
            }catch( NumberFormatException e){
                //just ignore any numberformat exceptions here
            }
            return changed;
    }

    /***********************************************************************************************************************
     * Remove the precision from a float() declaration
     * @param fs
     * @param cl
     * @param line
     * @param ulow
     * @param changed
     * @return
     **********************************************************************************************************************/
    private static boolean rewrite_float_with_precision(String[] fs, int cl, String line, String ulow, String precision, boolean changed)
    {
        //      int poscomment=ulow.indexOf("--");
        try{
            if (Integer.valueOf(precision)>0); // ( poscomment > p || poscomment==-1))
            {
                fs[cl]=line.replaceFirst(Float_tok+WS0+"\\("+WS0+Any_integer_tok+WS0+"\\)"," float ") +source_modifier+" removing ("+precision+") for float";
                return true;
            }
        }catch( NumberFormatException e){
            //just ignore any numberformat exceptions here
        }
        return changed;    
    }

    /**************************************************************************************************************************
     * change varchar2( x ) where x > 32672. for instance "VARCHAR2(32730)"
     * @param fs
     * @param cl
     * @param line
     * @param changed
     * @return
     *************************************************************************************************************************/
    private static boolean rewrite_Varchar2_32k(String[] fs, int cl, String line, boolean changed, String varchar_sz, String byte_char_clause)
    {  
        try{
            if (Integer.valueOf(varchar_sz)>32672) // ( poscomment > p || poscomment==-1))
            {
                fs[cl]=line.replaceFirst("("+Varchar2_tok+")"+WS0+"\\("+WS0+"32[67][0-9][0-9]"+WS0+"[CcBb]*[HhYy]*[AaTT]*[RrEe]*"+WS0+"\\)","$1(32672)")+source_modifier+" varchar2 length of "+varchar_sz+" > 32672";
                return true;
            }
            else if (! byte_char_clause.equals(""))
            {
                fs[cl]=line.replaceFirst("("+Varchar2_tok+")"+WS0+"\\("+WS0+"([0-9]+)"+WS0+"[CcBb]*[HhYy]*[AaTT]*[RrEe]*"+WS0+"\\)","$1($2)")+source_modifier+" Removed BYTE/CHAR";
                return true;                
            }
                
        }catch( NumberFormatException e){
            //just ignore any numberformat exceptions here
        }
        return changed;    
    }
    
    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static boolean rewrite_Table_columns(String[] fs, String ulow, String line,int cl)
    {
        boolean changed=false;
//        if (line.contains("IDX_IND")) //for debugging purposes
//            stop=true;
        
        if (ulow.indexOf("default")>-1 && pattern_Default_in_parenth.matcher(ulow).matches())
        {                  
            line=line.replaceFirst(WS1+Default_tok+WS1+"\\(("+AnyChar_tok+")\\)"+WS1," DEFAULT $1 ")+source_modifier+" removed () around DEFAULT ";
            fs[cl]= line;
            ulow=line.toLowerCase();
            changed = true;
        }
        //no elseif here! keep block above separated.
        if  ( ulow.indexOf("raw")>-1 && pattern_Long_raw.matcher(ulow).matches()) //(matches(AnyChar_tok+WS1+"long"+WS1+"raw[ ,]*"+AnyChar_tok,ulow))  //replaces "long raw" first 
        {//removing LONG RAW  from column definition
            line = line.replaceFirst(WS1+Long_tok+WS1+Raw_tok+"([ ,]*)"," BLOB$1 ")+source_modifier+" removing LONG RAW  from column definition";
            fs[cl]= line;
            ulow=line.toLowerCase();
            changed=true;
        }
        else
        if ( ulow.indexOf("long")>-1 && pattern_Long_ColType.matcher(ulow).matches())  //(matches(WS1+Long_tok+"([ ,]*)",ulow)) //and if LONg is still there, replaces LONG with CLOB
        {
            line = line.replaceFirst(WS1+Long_tok+"([ ,]*)", " CLOB$1 ")+source_modifier+" removing LONG from column definition";
            fs[cl]= line;
            ulow=line.toLowerCase();
            changed=true;
        }
        
        if (ulow.trim().startsWith("lob (") )
        {
            line=change_string(line, line, source_modifier+ line, "commenting out lob clause",false);
            fs[cl]= line;
            ulow=line.toLowerCase();
            changed = true;
        }
        else if (ulow.indexOf("tablespace")>-1 && pattern_Tablespace.matcher(ulow).matches())
        {
            //now try to remove other following options until ; or / 
            int z=0;
            for ( ; false== fs[cl+z].trim().equals("/") &&  fs[cl+z].indexOf(";")==-1 ; z++)  //fs[cl+z].indexOf(")") ==-1
                fs[cl+z]=source_modifier+fs[cl+z]; 
            if ((fs[cl+z].indexOf(";")>0)&&(false==fs[cl+z].startsWith(source_modifier)))
                    fs[cl+z]=source_modifier+fs[cl+z]+crlf+";";
                
            fs[cl]=line.replaceFirst(WS0+"("+Tablespace_tok+WS1+Any_identifier_tok+")([\t ;]*)","$2 "+crlf+source_modifier+" $1");
            changed=true;
        }
        else if ( ulow.indexOf(" storage ")>-1 || ulow.indexOf(" storage(")>-1)
        {
            if ( ulow.indexOf(")") ==-1 )
            { //multi line storage clause
                fs[cl]=change_string(line, line, source_modifier + line, "removed STORAGE clause",false);
                int z=1;
                do {
                    fs[cl+z]=source_modifier+fs[cl+z];
                    z++;
                } while ( fs[cl+z].indexOf(")") ==-1 && ! fs[cl+z].trim().equals("/") );    
                if (fs[cl+z].indexOf(")")>0)
                    fs[cl+z]=source_modifier+fs[cl+z];
            }
            else{ //storage clause on same line
                int pcomm= ulow.indexOf(")");
                if (pcomm>-1)
                fs[cl]=line.substring(pcomm+1)+crlf+ source_modifier + line.substring(0,pcomm+1)+ " removed STORAGE clause" ;                
            }
            changed = true;
        }
        else if ( ulow.indexOf("null")>-1 && ( ! ulow.contains("not ") ) && 
                 ! pattern_Default_NULL.matcher(ulow).matches()  && // (ulow.contains("default null")==false) &&
                   pattern_NULL_Clause.matcher(ulow).matches() )    //replaces "null" on that line
            {
                line = line.replaceFirst(WS1+"("+null_tok+")"," ")+source_modifier+" removing NULL from column definition";
                fs[cl]=line;
                ulow=line.toLowerCase();
                changed=true;
            }            
        else if (ulow.indexOf("primary")>-1 && pattern_PK_CONSTRAINT.matcher(ulow).matches())
        { //now remove ENABLE keyword on primary key constraints: ALTER TABLE "CORE"."SCC_CONTACT_GEOADDRESS_T" ADD CONSTRAINT "CONTACT_GEOADDR_PK" PRIMARY KEY ("GEOADDR_ID") [rely] ENABLE [validate];
            fs[cl]=line.replaceFirst("\\)"+WS0+"("+Rely_optional_tok+WS0+Enable_tok+WS0+Novalidate_optional_tok+WS0+")","\\)")+crlf+source_modifier+" removing ENABLE and other keyword from primary key constraint";
            changed=true;            
        }
        else if (ulow.indexOf("organization")>-1 && ulow.indexOf("index")>-1 && pattern_Organization_Index.matcher(ulow).matches()) //(matches(AnyChar_tok+WS0+organization_tok+WS1+index_tok+AnyChar_tok,ulow)) 
        {   //replaces "organization index"
            fs[cl] = line.replaceFirst(WS1+"("+organization_tok+WS1+index_tok+WS0+"[^;,]*"+")([ ;]*)", "$2"+crlf+source_modifier+"$1")+source_modifier+" removing organization index with or without nocompress";                    
            changed=true;
        }
        else if (ulow.indexOf("enable")>-1 && pattern_NOT_NULL_ENABLE.matcher(ulow).matches()) //finally dong the columns
        {
            fs[cl] = line.replaceFirst(WS1+null_tok+WS1+Enable_tok+"([ ,]*)"," NULL $1 ")+source_modifier+" removing ENABLE keyword from column definition";
            changed=true;
        }
        else if (ulow.indexOf("partition")>-1 && pattern_Partition_by.matcher(ulow).matches()) //PARTITION BY RANGE ("DTE_DATE") 
        {
            fs[cl] = line.replaceFirst(WS0+"("+Partition_tok+WS1+By_tok+WS1+Any_identifier_tok+")"+WS0,source_modifier+" $1 ")+source_modifier+" removing PARTITION BY clauses...";
            changed=true; 
        }
        else if (ulow.indexOf("partition")>-1 && pattern_Partition_values.matcher(ulow).matches()) // [( ]partition t2 values less than (1675) tablespace user_data nologging [,)]
        { 
            fs[cl] = line.replaceFirst(WS0+"([\\(]*"+WS0+Partition_tok+WS1+Any_identifier_tok+WS1+Values_tok+WS1+Any_identifier_tok+WS0+"[^;,]*"+")"+"([;]*)","$2 "+source_modifier+" $1 "); //+source_modifier+" removing PARTITION BY clauses...";
            if (fs[cl].charAt(0)==';')
                fs[cl]=fs[cl].substring(1)+crlf+";";
            changed=true; 
        }    
        
        return changed;
    }
    
     /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/

    private static boolean simple_Rewrite_of_PLSQL(String[] fs, String[] tok2s, int cl,  int nblines) //String line
    {
        String line=fs[cl];
        try
        {
            boolean changed = false;
            if (line != null)
            {
                String[] toks = line.trim().split(WS1);
                String ulow = line.toLowerCase();
                //debugging purpose
//                if (line.indexOf("B2Y_AttributeStructure")>-1) //cl>= 187)
//                   stop=true;

                if (ulow.indexOf("varchar")>-1 || ulow.indexOf("float")>-1 || ulow.indexOf("number")>-1)
                {
                    changed=Process_datatype_limitations(fs, toks, cl,  line,  ulow);
                    if (changed) {
                        line=fs[cl];
                        toks= line.trim().split(WS1);
                        ulow = line.toLowerCase();
                    }
                }
                boolean create_frst_tok=toks[0].equalsIgnoreCase("create");
                boolean alter_frst_tok=toks[0].equalsIgnoreCase("alter");
                boolean table_2nd_tok=(( toks.length>2 && toks[1].equalsIgnoreCase("table")) ||
                                       ( toks.length>3 && toks[1].equalsIgnoreCase("global") && toks[2].equalsIgnoreCase("temporary")));
                
                if ((create_frst_tok || alter_frst_tok) && table_2nd_tok )
                {
                    in_create_table = (create_frst_tok && table_2nd_tok);
                }
                else if (create_frst_tok &&  ! table_2nd_tok )
                    in_create_table = false;
                else if  (ulow.indexOf("exec")>-1 && pattern_Exec.matcher(ulow).matches()) 
                    in_create_table = false;
                  
                if (in_create_table == true)
                {    
                     changed= changed || rewrite_Table_columns(fs, ulow,line,cl);
                }
                else if  (ulow.indexOf("restrict_references")>-1 && pattern_Pragma_Restrict_references.matcher(ulow).matches()) 
                { // change PRAGMA  restrict_reference into "+source_modifier+"PRAGMA...
                    fs[cl]=line.replaceFirst( WS0+"("+Pragma_tok+WS1+Restrict_reference_tok+")"+WS0, source_modifier+" removing $1");
                    changed=true; 
                } 
               else if (ulow.indexOf("using")>-1 && ulow.indexOf("index")>-1 && pattern_using_index.matcher(ulow).matches()) 
                {
                    changed=changed || BasicSimplerewriteByReplaceString( fs, " using index",";" + crlf, cl, line, ulow,false,false);
                }
               else if (ulow.indexOf("constraint")>-1 && pattern_ADD_CONSTRAINT1.matcher(ulow).matches())                                     
                   {  //now remove the ( ) around  " ADD ( CONSTRAINT ( ... )) ENABLE; " 
                   fs[cl]=line.replaceFirst(Add_tok+WS0+"\\("+WS0+Constraint_tok+WS1+"("+AnyChar_tok+")\\)"+WS0+Anykeyword_tok+";" , " ADD CONSTRAINT $1;")+crlf+source_modifier+" removing () and other keywords around CONSTRAINTS";
                   changed=true;            
                   }
               else if (ulow.indexOf("constraint")>-1 && pattern_ADD_CONSTRAINT2.matcher(ulow).matches())                    
               {  //now remove the ( ) around  " ADD ( CONSTRAINT ( ... )) ; " 
                   fs[cl]=line.replaceFirst(Add_tok+WS0+"\\("+WS0+Constraint_tok+WS1+"("+AnyChar_tok+")\\)"+WS0+";" , " ADD CONSTRAINT $1;")+source_modifier+" removing () around CONSTRAINTS";
                  changed=true;            
               }
               else if (ulow.indexOf("primary")>-1 && pattern_PK_CONSTRAINT.matcher(ulow).matches())
               { //now remove ENABLE keyword on primary key constraints: ALTER TABLE "CORE"."SCC_CONTACT_GEOADDRESS_T" ADD CONSTRAINT "CONTACT_GEOADDR_PK" PRIMARY KEY ("GEOADDR_ID") ENABLE;
                   fs[cl]=line.replaceFirst("\\)"+WS0+Enable_tok+WS0,"\\) ")+crlf+source_modifier+" removing ENABLE keyword from primary key constraint";
                   changed=true;            
               }
               else if (ulow.indexOf("references")>-1 && pattern_FK_CONSTRAINT2.matcher(ulow).matches())
               { //now remove ENABLE keyword on foreign  key constraints: ALTER TABLE "FRML_GRP_PATH" ADD CONSTRAINT "FK_FRGP_FRGD_2" FOREIGN KEY ("IDX_GROUP_CHILD") REFERENCES "FRML_GRP_DESC" ("IDX_IND") ENABLE
                   fs[cl]=line.replaceFirst("\\)"+WS0+Enable_tok+WS0+";","\\);")+crlf+source_modifier+" removing ENABLE keyword from foreign key constraint";
                   changed=true;            
               }
               else if (ulow.indexOf("check")>-1 && pattern_CHECK_CONSTRAINT1.matcher(ulow).matches())
                   {//ADD CONSTRAINT "CK_IERU_5" CHECK ( Str_State In ( 'on', 'off' ) ) ENABLE;
                   fs[cl]=line.replaceFirst("\\)"+WS0+Enable_tok+WS0+";","\\);")+crlf+source_modifier+" removing ENABLE keyword from check constraint";
                   changed=true;
                }   
               else if (ulow.indexOf("unique")>-1 && pattern_UNIQUE_CONSTRAINT.matcher(ulow).matches())
                   {//ALTER TABLE "TMP_NODEP" ADD CONSTRAINT "UN_TNOD_1_2" UNIQUE ("STR_TYPE", "NP") ENABLE
                   fs[cl]=line.replaceFirst("\\)"+WS0+Enable_tok+WS0+";","\\);")+crlf+source_modifier+" removing ENABLE keyword from unique constraint";
                   changed=true;
                   }
                else if (ulow.indexOf("table")>-1 && pattern_Create_Or_Replace_Type_Table.matcher(ulow).matches())  
                {  //CREATE OR REPLACE type WFCMASTER_MAINT_1 as table of 
                    fs[cl]=line.replaceFirst(WS1+As_Is_tok+WS1+Table_tok+WS1+Of_tok+WS1," as VARRAY(10) of ")+crlf+source_modifier+" rewriting is/as table of into VARRAY";
                    changed=true;            
                }
                else if (ulow.indexOf("table")>-1 && pattern_Create_Or_Replace_Type_Table_line2.matcher(ulow).matches()
                                                  && pattern_Create_Or_Replace_Type_Table_line1.matcher(fs[cl-1]).matches())  
                {  //CREATE OR REPLACE type WFCMASTER_MAINT_1 as table of when on 2 lines...                     
                    fs[cl]=fs[cl-1]+fs[cl]; //merge the 2 lines for processing
                    fs[cl-1]=source_modifier+fs[cl-1]; //comment out the next line so that it is not processed
                    fs[cl]=fs[cl].replaceFirst(WS1+As_Is_tok+WS1+Table_tok+WS1+Of_tok+WS1," as VARRAY(10) of ")+crlf+source_modifier+" rewriting is/as table of into VARRAY";
                    changed=true;            
                }
                else if (ulow.indexOf("object")>-1 && pattern_Create_Or_Replace_Type_Object_single_line.matcher(ulow).matches())                     
                    {  //now look for "create or replace type ..as object on one single line...
                        String IsAs_object_tok=ExtractTokenFromPattern(line,1, AnyChar_tok+or_tok+WS1+Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+"+\\s+("+As_Is_tok+"\\s+"+object_tok+")\\s*"+AnyChar_tok);
                        if (false==IsAs_object_tok.equals(""))
                            changed=changed || Process_Object_type( fs, cl,cl, IsAs_object_tok, nblines);                    
                    }
                else if (ulow.indexOf("object")>-1 && pattern_Create_Or_Replace_Type_Object_two_lines.matcher(ulow).matches() 
                         && (pattern_Create_Or_Replace_Type.matcher(fs[cl-1]).matches() || pattern_Create_Type.matcher(fs[cl-1]).matches()))           // && matches(AnyChar_tok+or_tok+WS1+Replace_tok+WS1+Type_tok+"\\s*"+AnyChar_tok,fs[cl-1])  )
                        {//now look for "create or replace type ..as object on 2 lines..
                            String IsAs_object_tok=ExtractTokenFromPattern(line,1, AnyChar_tok+"("+As_Is_tok+WS1+object_tok+")"+WS0+AnyChar_tok);
                            if (IsAs_object_tok.equals("")==false)
                                changed=changed || Process_Object_type( fs, cl,cl-1, IsAs_object_tok, nblines);
                        }
                else if (ulow.indexOf("object")>-1 && pattern_Create_Or_Replace_Type_Object_two_lines2.matcher(ulow).matches()  
                        && (pattern_Create_Or_Replace_Type.matcher(fs[cl-1]).matches() || pattern_Create_Type.matcher(fs[cl-1]).matches()))
                       {//now look for "create or replace type ..as object on 2 lines, 2nd line starting with  object only, AS on previous line..
                          String IsAs_tok=ExtractTokenFromPattern(fs[cl-1],1, AnyChar_tok+WS0+Create_or_Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1+"("+As_Is_tok+WS0+")"+WS0+AnyChar_tok);
                          String obj_tok=ExtractTokenFromPattern(line,1, AnyChar_tok+WS1+"("+object_tok+WS0+")\\("+WS0+AnyChar_tok);
                          if ((IsAs_tok.equals("")==false) && (obj_tok.equals("")==false))
                               changed=changed || Process_Object_type( fs, cl,cl-1, IsAs_tok+" "+obj_tok, nblines);
                       }
                else
                if (ulow.indexOf("type")>-1 &&   pattern_Create_Or_Replace_Type_as_scalar.matcher(ulow).matches() 
                                            && !(pattern_Create_Or_Replace_Type_Table_line2.matcher(fs[cl+1]).matches()) //make sure that table of is not part of the second line
                                            && !(pattern_Create_Or_Replace_Type_Object_two_lines2.matcher(fs[cl+1]).matches()))//make sure that objcet   is not starting the second line
                {
                    changed=BasicSimplerewriteByReplaceString( fs, "or replace type"," DISTINCT TYPE ", cl, line, ulow,false,true);
                }

                else if (ulow.indexOf("nowait")>-1 && ulow.indexOf("update")>-1 && pattern_For_Update_no_Wait.matcher(ulow).matches())                   
                {  //now remove " for update nowait;  " first....
                    fs[cl]=line.replaceFirst(WS0+For_tok+WS1+Update_tok+WS1+Nowait_tok+WS0+"[;]*" , ";")+source_modifier+" removing for update nowait";
                   changed=true;            
                }
                else if (ulow.indexOf("update")>-1 && pattern_For_Update.matcher(ulow).matches())                    
                {  //now remove " for update;  "  in second place if not solve before.
                    fs[cl]=line.replaceFirst(WS1+For_tok+WS1+Update_tok+WS0+";" , ";")+source_modifier+" removing for update ";
                   changed=true;            
                }
                else if (ulow.indexOf("nowait")>-1 && pattern_No_Wait.matcher(ulow).matches())                    
                {  //now remove "  nowait;  " last....
                    fs[cl]=line.replaceFirst(WS0+Nowait_tok+WS0+";" , ";")+source_modifier+" removing nowait";
                   changed=true;            
                }
                else
                if (ulow.indexOf("current_user")>-1 && pattern_Authid_Current_User.matcher(ulow).matches()) 
                { // remove AUTHID CURRENT_USER
                    fs[cl] = line.replaceFirst(WS0+AuthID_tok+WS1+Current_User_tok+WS0, " ")+source_modifier+" AUTHID CURRENT_USER";
                    changed = true;   
                }
                else
                if (ulow.indexOf("rollback")>-1 && pattern_Rollback_to_Savepoint.matcher(ulow).matches()) 
                    {// change "rollback S;" into "Rollback to Savepoint S;"
                        fs[cl]= line.replaceFirst(Rollback_tok+WS0+"[WwOoRrKk]*"+WS1+To_tok+WS1+"("+Any_identifier_tok+")"+WS0+";","ROLLBACK TO SAVEPOINT $1;")+source_modifier+" changed into Db2 rollback syntax";
                        changed=true;
                    }
                else
                if (ulow.indexOf("savepoint")>-1 && pattern_Savepoint.matcher(ulow).matches() &&      
                     ! pattern_Rollback_DB2_Savepoint.matcher(ulow).matches() && ! pattern_Rollback_to_Savepoints.matcher(ulow).matches())                       
                {// change savepoint into savepoint with retain cursors on rollback
                    fs[cl]= line.replaceFirst(Savepoint_tok+WS1+"("+Any_identifier_tok+")"+WS0+";","SAVEPOINT $1 ON ROLLBACK RETAIN CURSORS;")+source_modifier+" changed into Db2 savepoint syntax";
                    changed=true;
                }
                else if (ulow.indexOf("nocopy")>-1) // NOCOPY parms:  
                { // change PRAGMA restrict_references(xx, dd) into "+source_modifier+"PRAGMA restrict_references(xx,dd)"
                    int p = ulow.indexOf("nocopy");
                    String stleng = line.substring(p, p + 6);
                    String newline = line.replace(stleng, "");
                    changed = true;
                    fs[cl]=change_string(line, line, newline, "NOCOPY",false);
                }
                else                                         
                if (ulow.indexOf("mod")>-1 && pattern_Mod_Complex.matcher(ulow).matches())   
                {   //replacing   ...  ( expr) mod y = z ...   into mod( (expr),y)=z  like  (v_index + 3) mod 4 = 0 then
                    fs[cl]= line.replaceFirst(WS1+"(\\("+WS0+Any_basic_expression+WS0+"\\))"+WS0+Mod_tok+WS1+"("+Any_identifier_tok+")"+WS0+"="+WS0+"("+Any_integer_tok+")",
                                               " mod( $1,$2)= $3 ")+source_modifier+" mod as a DB2 MOD()";
                     changed=true;
                }
                else               
                if (ulow.indexOf("mod")>-1 && pattern_Mod_Complex2.matcher(ulow).matches())   
                 {   //replacing   ...  ( expr mod y) = z ...   into mod(expr,y)=z  like:  if ((L_MsgRec(i).MSGNR -1 MOD 10) = 0) then 
                    fs[cl]= line.replaceFirst(WS0+"\\("+WS0+"("+Any_basic_expression+")"+WS1+Mod_tok+WS1+"("+Any_identifier_tok+")"+WS0+"\\)"+WS0+"="+WS0+"("+Any_integer_tok+")",
                                              " mod( $1,$2)= $3 ")+source_modifier+" mod as a DB2 MOD()";
                    changed=true;
                 }
                else                       
                if (ulow.indexOf("mod")>-1 && pattern_Mod_simple.matcher(ulow).matches()) 
                 { //replacing    ...   x mod y = z ...  into mod(x,y)=z
                   fs[cl]= line.replaceFirst(WS1+"("+Any_identifier_tok+")"+WS1+Mod_tok+WS1+"("+Any_identifier_tok+")"+WS0+"="+WS0+"("+Any_integer_tok+")",
                                              " mod( $1,$2)= $3 ")+source_modifier+" mod as a DB2 MOD()";
                   changed=true;
                }
                else
                    if (ulow.indexOf("only")>-1 && pattern_With_Read_only.matcher(ulow).matches()) 
                    { //replacing   WITH READ ONLY clause for views
                        fs[cl]= line.replaceFirst(With_tok+WS1+Read_tok+WS1+Only_tok+WS0+"([;]*)", "$1")+crlf+source_modifier+" removed With read only clauses";
                        changed=true;
                    }                
                else 
                    if (ulow.indexOf("type") < ulow.indexOf("table") && 
                             (pattern_Is_Table_Of.matcher(ulow).matches() || pattern_Create_Or_Replace_Type_Table_pct_type.matcher(ulow).matches() )) 
                    { // add index by integer to "type xx IS TABLE OF   //WS0+Type_tok+WS1+Any_identifier_tok+WS1+IS_tok+WS1+Table_tok+WS1
                        fs[cl]=line.replaceFirst(Of_tok+"("+WS1+Any_SQLtype_tok+WS0+"[%"+Type_tok+"]*"+WS0+");", " of $1 index by integer; ")+source_modifier+" added index by integer clause";
                        changed = true;
                    }
                else if (ulow.indexOf("view")>-1 && pattern_Create_or_Replace_Force_View.matcher(ulow).matches())
                {
                    fs[cl]= line.replaceFirst(WS1+Force_tok+WS1+View_tok+WS1," VIEW ")+crlf+source_modifier+" removed FORCE keyword"; //CREATE OR REPLACE FORCE VIEW "PV_ADMIN"."ELMT_DESC_VW"
                    changed = true;
                }
                else if (ulow.indexOf("trim")>-1 && pattern_Trim_From.matcher(ulow).matches())
                {
                    fs[cl]= line.replaceFirst(WS0+Trim_tok+WS0+"\\("+WS0+"' '"+WS1+From_tok+WS1+"("+Any_identifier_tok+")"," trim($1")+source_modifier+" removed trim( ' ' from ..";
                    changed = true; 
                }
                else if (ulow.indexOf("enable")>-1 && ulow.indexOf("trigger")>-1 && pattern_ALTER_TRIGGER_ENABLE.matcher(ulow).matches()) //removing alter trigger statements
                {  //ALTER TRIGGER "TR_CAGL_AIR" ENABLE;
                    fs[cl] = line.replaceFirst( "("+Alter_tok+WS1+Trigger_tok+WS1+Any_identifier_tok+WS1+Enable_tok+")", source_modifier+" $1 ")+source_modifier+" removing ALTER TRIGGER ENABLE  statement";
                    changed=true;
                } 
                else if (ulow.indexOf("grant")>-1 && ulow.indexOf("execute")>-1 && pattern_Grant_Execute.matcher(ulow).matches()) 
                {  //changing Grant Execute On xxxx   into Grant Execute On Module xxxxx
                    fs[cl] = line.replaceFirst( "("+Grant_tok+WS1+Execute_tok+WS1+On_tok+WS1+")", " $1 MODULE ") +source_modifier+" Added MODULE keyword";
                    changed=true;
                } 
                else if (ulow.indexOf("dbms_lob.")>-1)
                {
                    changed=changed || Process_dbms_lobs(fs, cl,ulow );                   
                }
// Note to Patrick: to be continued...
//                else if (ulow.indexOf("dbms_sql.bind_variable")>-1)
//                { 
//                    changed=rewrite_dbms_sql(fs, cl, ulow, DBMS_SQL_Bind_Variable);                  
//                } 
//                else if (ulow.indexOf("dbms_sql.column_value")>-1)
//                { 
//                    changed=rewrite_dbms_sql(fs, cl, ulow, DBMS_SQL_COLUMN_VALUE);                  
//                } 
                else if ( ! Keep_wrapped_code && ulow.indexOf("wrapped")>-1 && pattern_Wrapped.matcher(ulow).matches()) //is it a wrapped packaged ?
                {
                    changed =changed ||  rewrite_Wrapped_code(fs, cl, nblines, changed, ulow,Remove_wrapped_code);
                }
                else if (ulow.indexOf("subtype")>-1 && (pattern_SubType.matcher(ulow).matches())) //matches(WS0+SubType_tok+WS1+Any_identifier_tok+WS1+IS_tok+WS1+Any_SQLtype_tok+WS0+";"+AnyChar_tok,line))
                {
                    changed=changed ||  rewrite_Sub_Type(fs, cl, nblines);
                }
                else if (ulow.indexOf("returning")>-1)
                    {
                    changed =changed ||  rewrite_Returning_Clause(fs, cl,ulow, nblines);
                    }
                else if ((ulow.indexOf("dbms_sql.is_open")>-1)&&( pattern_Is_open.matcher(ulow).matches()))
                {
                    changed =changed || rewrite_dbms_is_open(fs, cl, line);                
                }
                if (fp==0) //only go there in DB2 GA
                {
                    changed = changed || rewrite_fp0_of_PLSQL_features(fs, cl, line, toks, ulow,nblines);
                }
                // else
                // System.out.println("echo line ignored for file "+filetoprocess+":"+line);
                if (changed == true)
                {
                    nb_changed++;
                    String FinalModifiedLine=fs[cl];
                    Log(cl, FinalModifiedLine);
                } 
            }// endwhile
        }
     catch (Exception e)
        {
            System.out.print("Faulty line " + cl + "" + line);
            e.printStackTrace();
        }
        // System.out.print("-- "+l+" lines checked, "+c+" lines changed");
        return true;
    }

    /****************************************************************************************************
     * @param fs
     * @param cl
     * @param line
     ******************************************************************************************************/
    private static boolean rewrite_dbms_is_open(String[] fs, int cl, String line)
    {
        boolean found=false; 
        int kk=cl-1;
        //TODO
        //v_CursorID_isopened INTEGER; --XX for isopen below
        String cursor_variable=line.replaceFirst(
                IF_tok+WS0+"\\(*"+WS0+"("+DBMS_IS_OPEN_tok+")"+WS0+"\\(*"+WS0+"("+Any_identifier_tok+")"+WS0+"\\)*"+WS0+"[=]*"+WS0+"["+True_tok+"]*"+WS0+"\\)*"+WS0+Then_tok, 
                "$2").trim();
        Pattern Cursor_variable_declaration_pattern =Pattern.compile(AnyChar_tok+WS0+"(?i)"+cursor_variable+WS1+"[IiNn][NnUU][TtMm][EeBb][GgEe][EeRr][Rr]*"+WS0+";"+AnyChar_tok);
 
        String lcurvar=cursor_variable.toLowerCase();
        //trying to find the cursorvariable declarations above this line... 
        for (;  kk>=0 && found==false; kk--)
        {
           String tmp=fs[kk].toLowerCase();
           if ((tmp.indexOf(lcurvar)>-1) && ( Cursor_variable_declaration_pattern.matcher(tmp).matches()))
            {
               kk++;
               found=true;
            }
        }
            
        if (found)
        {
          if (fs[kk].indexOf(cursor_variable+" INTEGER;" )==-1) //not already declared
              fs[kk] = fs[kk].replaceFirst( "("+cursor_variable+")"+WS1+"[IiNn][NnUU][TtMm][EeBb][GgEe][EeRr][Rr]*"+WS0+";","$1 INTEGER; $1_isopened INTEGER;")+source_modifier+" needed for dbms_sql.is_open() below";
          
          fs[cl] = line.replaceFirst(
                IF_tok+WS0+"\\(*"+WS0+"("+DBMS_IS_OPEN_tok+")"+WS0+"\\(*"+WS0+"("+Any_identifier_tok+")"+WS0+"\\)*"+WS0+"[=]*"+WS0+"["+True_tok+"]*"+WS0+"\\)*"+WS0+Then_tok, 
                " $2_isopened:=$1($2);" +source_modifier+crlf+"   IF ($2_isopened =1) THEN ") +source_modifier+" is_open output in db2 is integer";
          return true;
        }
        else {
            Log(cl,"Couldn't find cursor Variable definition "+cursor_variable+" " +
            		"to modify dbms.is_open() call into an integer one. giving up on that one!");  
            return false;
        }
    }

    /****************************************************************************************************
     * this method removes or comment out  wrapped code that looks like binary.
     * @param fs
     * @param cl
     * @param nblines
     * @param changed
     * @param ulow
     * @return
     ******************************************************************************************************/
    private static boolean rewrite_Wrapped_code(String[] fs, int cl, int nblines, boolean changed, String ulow,boolean Remove_wrapped_code)
    {
        int pwrapped=ulow.indexOf(" wrapped");
        if (pwrapped==-1) pwrapped=ulow.indexOf("\twrapped");
        //verify that is it not part of a comment as we should ignore it then
        int poscomment=ulow.indexOf("--");
        if (poscomment == -1) 
            poscomment=ulow.indexOf("/*");
        char char_after_wrapped=(pwrapped+8 < ulow.length()) ? ulow.charAt(pwrapped+8) : 0x0a;
        if ( pwrapped > 0 && ( poscomment==-1 || poscomment > pwrapped+8 )  //verify that is it not part of a comment 
                && ( char_after_wrapped ==' ' || char_after_wrapped == '\t' || char_after_wrapped == 0x0A))
        { //comment out the following lines and remove chars  lines until end of stmt / 
            fs[cl+1]=source_modifier+" Removed/commented out wrapped code: "+fs[cl+1];
            int nbs=0;
            for (int jj=cl+2; ! (fs[jj].trim().equals("/") && jj< nblines); jj++) 
            {         
                //if (nbs==0)
                //     fs[jj]=source_modifier+" Removed/commented out wrapped code: "+fs[jj];
                //else 
                fs[jj]= ( Remove_wrapped_code) ? "" : source_modifier+fs[jj];                
                nbs++;
            }
            if (!silent)
                Log( (nbs)+" lines of wrapped code removed or commented out");
            changed=true;
        }
        return changed;
    }

    /***************************************************************************************************************************
     * @param fs
     * @param cl
     * @param line
     * @param toks
     * @param ulow
     * @param changed
     * @return
     *      * 
     **************************************************************************************************************************/
    private static boolean rewrite_fp0_of_PLSQL_features(String[] fs, int cl, String line, String[] toks, String ulow,int nblines)
    {
        boolean changed=false;
        if (ulow.indexOf("fetch")>-1 && ulow.indexOf("bulk")>-1)
            rewrite_FETCH_Bulk_Collect_Stmt(fs, cl, nblines); 
        else
        if (ulow.indexOf("forall")>-1)
            rewrite_FOR_ALL_Stmt(fs, cl, nblines); 
        else
        if (ulow.indexOf("type")>-1 && pattern_Create_Or_Replace_Type_Row.matcher(ulow).matches()) //matches(AnyChar_tok+or_tok+WS1+Replace_tok+WS1+Type_tok+WS1+Any_identifier_tok+WS1,ulow ) ) // (ulow.contains(" as row") && ulow.contains("or replace type")) //only for Db2 v9.7GA  (ulow.contains(" as row") && ulow.contains("or replace type")
        {
            changed=BasicSimplerewriteByReplaceString( fs, "or replace type"," TYPE ", cl, line, ulow,false,false);
        }
        else
        if (ulow.contains("=true") || ulow.contains("= true") || ulow.contains("true=") || ulow.contains("true =")
                || ulow.contains("<") || ulow.contains(">") || ulow.contains(" in ") || ulow.contains("!=")
                || ulow.contains("=") || ulow.contains(" like "))
        {
            // do nothing most likely already correct.
        }
        else{
            if (toks[0].equals("if(") || toks[0].equals("elsif(") || toks[0].equals("while("))
            { // remove the  ( from that token and push it to next token )
                toks[0] = toks[0].substring(0, toks[0].length() - 1);
                toks[1] = "(" + toks[1];
            }
            String firstToken = toks[0].toLowerCase();
            if (toks.length >= 2 && (firstToken.equals("if") || (firstToken.equals("elsif")) || (firstToken.equals("while")))
                    && (ulow.contains(" null") == false) && (ulow.contains("%isopen") == false) && (ulow.contains("%notfound") == false)
                    && (ulow.contains("%found") == false))
            {
                changed= Rewrite_IF_and_While(fs, toks, cl, line);
            }
        }
        return changed;
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String  Process_Defines( String[] tok2s, int cl, String inputLine, HashMap<String, String> V)
    {

        String[] toks = tok2s[1].trim().split("=", 2); // find out what is in the first part of the statement

        // if (toks[0].equalsIgnoreCase("DEFINE") ) // is it composed of only a DEFINE keyword ?
        // {
        String Value = TrimSpecificChar(toks[1].trim(), "\"");
        String VarName = toks[0].trim();
        String Valuereplaced = Value;

        if (Value.indexOf("&") > 0) // is there a reference to another variable in there ?
        {
            Valuereplaced = Replace_Var_In_String(V, Value, cl);
            if (Valuereplaced.equals(Value)) // nothing was changed, therfeore ask for value of variable
                Valuereplaced = InputFromUser("Please input value for variable " + VarName + ":");
        }
        InsertNewVariable(V, VarName, Valuereplaced);
        return  source_modifier+ inputLine; 
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String Process_UnDefine( String[] tok2s, int cl, String inputLine, HashMap<String, String> V)
    {
        String[] toks = tok2s[1].trim().split("=", 2); // find out what is in the first part of the statement
        String VarName = toks[0].trim();
        V.remove(VarName.trim());
        return source_modifier+ inputLine;
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static String AbsoluteFileName(String fn, String path)
    {
        return (path == null) ? fn.trim() : path + dirSepChar + fn.trim();
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static void Process_Include(String outputFile, String[] tok2s, int cl, String inputLine, String includePath,String[] fsparent, HashMap<String, String> variableparent)
            throws Exception
    {
        boolean fexists = false;
        String[] toks = tok2s[0].trim().split(" ", 2); // find out what is in the first/second part of the statement
        String includeFileName;
        if (toks[0].startsWith("@") && !(toks[0].equals("@@"))) // only @inlcudefilename kind of style
            includeFileName = toks[0].substring(1);
        else
            // START and @@ includefilenames kind of style
            includeFileName = tok2s[1];

        includeFileName = TrimSpecificChar(includeFileName.trim(), ";");
        includeFileName = TrimSpecificChar(includeFileName.trim(), "\"");

        String CompleteFileName = AbsoluteFileName(includeFileName, includePath);

        if ((new File(CompleteFileName)).exists())
            fexists = true;
        else
        {
            CompleteFileName = AbsoluteFileName(includeFileName, includePath) + ".sql";
            if ((new File(CompleteFileName)).exists())
                fexists = true;
            else
            {
                if (!silent)
                    System.out.println("couldn't find include file:"+CompleteFileName+" with includePath:"+includePath);
                CompleteFileName = AbsoluteFileName(includeFileName.toUpperCase(), includePath) + ".sql";
                if ((new File(CompleteFileName)).exists())
                    fexists = true;
                else
                {
                    if (!silent)
                        System.out.println("couldn't find include file:"+CompleteFileName+" with includePath:"+includePath);
                    CompleteFileName = AbsoluteFileName(includeFileName.toUpperCase(), includePath);
                    if ((new File(CompleteFileName)).exists())
                        fexists = true;
                    else
                    {
                        if (!silent)
                        System.out.println("couldn't find include file:"+CompleteFileName+" with includePath:"+includePath);
                        nbincludesnotfound++;
                        if (1 == 0) //debug code only
                        {
                            File dir = new File(includePath);
                            // It is also possible to filter the list of returned files.
                            // This example does not return any files that start with `.'.
                            FilenameFilter filter = new FilenameFilter() {
                                public boolean accept(File dir, String name)
                                {
                                    return !name.startsWith(".");
                                }
                            };
                            String[] children = dir.list(filter);
                            for (int i = 0; i < children.length; i++)
                                System.out.println(children[i]);
                        }
                    }
                }
            }
        }

        if (fexists == true)
        {
            nb_includes++;
            System.out.println("Line " + cl + " Processing include " + CompleteFileName);
            ProcessSQLPlusFile(outputFile, CompleteFileName,true,fsparent,cl,variableparent); // no output for includes.
        }
        else
        {
            System.out.println("Line " + cl + " Cannot find include " + CompleteFileName);
        }
    }
    /***************************************************************************************************************************
     * Rewrite specifc While loops having UDFs with out parms into simpler while loops
     **************************************************************************************************************************/
 
    private static void Process_While_Loop_With_UDF(String[] fs, int nblines, String whileUDF)
    {
       String Loop_replacement_string=";"+source_modifier+crlf+"   WHILE (outp) LOOP --PDA rewritting While loop as 2 steps for fucntion call returning parameters";        
       int outp_added_here=-1;
       for (int ii = 0; (ii < nblines); ii++)
        {
            String fsu = fs[ii].toUpperCase();
            String st = fsu.trim();
            if (isLineWeShouldIgnore(st)) //(st.length() == 0) || (st.startsWith("--")) || (st.startsWith("/")) || st.toUpperCase().startsWith("REM "))
            {  // do nothing,ignore line              
            }
            else
                if (fsu.contains(whileUDF.toUpperCase()))
                {
                    String tmpline=fs[ii].replace("WHILE "," outp :=");
                    int jj=ii; 
                    //find END LOOP... 
                    for ( ; (!fs[jj].contains("END LOOP;") && jj<nblines); jj++)
                        if ( jj >= nblines)
                            return; //give up, not found end of loop 
                    int kk=ii; 
                    //find variable declarations above ... 
                    for ( ; !(fs[kk].contains("ptr_") && fs[kk].contains(" NUMBER;")) && kk>=0; kk--)
                        if ( kk <=0)
                            return; //give up, not found variable declaration 
                         
                    //commit everything:
                    if (tmpline.indexOf(" LOOP")>-1)
                    {
                            fs[ii]=tmpline.replace(" LOOP",Loop_replacement_string); //";--PDA "+crlf+"   WHILE (outp) LOOP --PDA rewritting While loop as 2 steps for fucntion call returning parameters");
                            Log(ii, fs[ii]);
                            fs[jj]=tmpline.replace(" LOOP","; --PDA"+crlf+fs[jj]+" --PDA"); 
                            if ( outp_added_here != kk)
                            {
                                fs[kk]=" outp boolean; --PDA used for While loop rewritting as 2 steps"+crlf+fs[kk];
                                outp_added_here=kk;
                            }
                     }
                    ii=jj;
                }
        }//end while        
    }
    

    /***************************************************************************************************************************
     * Rewrite specifc Functions into Procedures
     **************************************************************************************************************************/
    private static void Rewrite_Function_into_proc(String[] fs, int nblines, String id, String retValType)
    {
        int p = 0;
        String fs3;
        String idu = id.toUpperCase();
        String search_function_begin="FUNCTION " + idu;
        String search_function_end="END " + idu;
        Pattern pattern_function_return_value  =Pattern.compile(AnyChar_tok+WS0+":="+WS0+idu+WS0+"\\("+WS0+AnyChar_tok+WS0+"\\)"+WS0+";"+AnyChar_tok);
        char state = '0'; // 0=init state F=FUNCTION , R=return ...., U = RETURN ( ), E=end ...
        String ReturnClause = ") RETURN " + retValType;
        // String[][] to_replace = new String[][] { { ") RETURN &TABLE%ROWTYPE", "&TABLE%ROWTYPE" },
        // { ") RETURN &VIEW%ROWTYPE", "&VIEW%ROWTYPE" } };

        // search lock_by_id__ string.
        int ii = 0;
        while (ii < nblines)
        {
            String fsu = fs[ii].toUpperCase();
            String fsutrim = fsu.trim();
            //if ((ii >= 653)&&(ii<656))
            //    stop = true;
            if (isLineWeShouldIgnore(fsutrim)) //(st.length() == 0) || (st.startsWith("--")) || (st.startsWith("/")) || st.toUpperCase().startsWith("REM "))
            {
                // do nothing
            }
            else
                switch (state) {
                    case '0':
                        if (fsu.contains(search_function_begin))
                        {
                            fs3 = fs[ii] = "PROCEDURE " + id + "( "+source_modifier+"function";
                            Log(ii, fs[ii]);
                            state = 'F';
                        }
                        break;
                    case 'F':
                        // for (int j = 0; j < to_replace.length; j++) {
                        if ((p = fsu.indexOf(ReturnClause)) >= 0) // to_replace[j][0])) >= 0)
                        {
                            String del;
                            if (fs[ii].length() > p + ReturnClause.length())
                                del = (fs[ii].charAt(p + ReturnClause.length()) == ';') ? ";" : "";// prototype found
                            else
                                del = "";
                            fs[ii] = fs[ii].substring(1, p) + ", outp " + retValType + ") " + del + source_modifier+ ReturnClause; // to_replace[j][1]
                            // +
                            // ")"+source_modifier+"
                            // "
                            // +to_replace[j][0];
                            Log(ii, fs[ii]);
                            state = (del.equals(";") ? 'U' : 'R'); // if prototype, dont go to R but go to U state
                        }
                        // }
                        break;
                    case 'R':
                        if (fsutrim.startsWith("RETURN ") && (fsu.indexOf(";") > 8))
                        {
                            p = fsu.indexOf("RETURN");
                            fs3 = fs[ii].substring(0, p - 1) + " outp:= " + fs[ii].substring(p + "RETURN ".length())+ source_modifier+"RETURN ";
                            fs[ii] = fs3;
                            Log(ii, fs[ii]);
                        }
                        else if (fsutrim.startsWith("RETURN(") && fsu.indexOf(";") > 9)
                        {
                            p = fsu.indexOf("RETURN(");
                            int pp1 = fsu.indexOf("(");
                            int pp = fsu.indexOf(")");
                            fs3 = fs[ii].substring(0, p - 1) + " outp:= " + fs[ii].substring(pp1 + 1, pp) + ";"+source_modifier+"RETURN ";
                            fs[ii] = fs3;
                            Log(ii, fs[ii]);
                        }
                        else if (fsu.contains(search_function_end) && fsu.indexOf(";") > 4) //"END"+ idu
                            state = 'U'; // switch to usage of this function now.
                        break;
                    case 'U':
                        if ( fsu.indexOf(idu)>-1 &&  pattern_function_return_value.matcher(fsu).matches() )   //for instance: oldrec_ := Get_Object_By_Keys___ (profile_id_, profile_section_, profile_entry_);
                        {
                            int p2 = fsu.indexOf(idu); //find position of function name
                            String function_name_in_line;
                            if (p2>-1)
                                function_name_in_line=fs[ii].substring(p2, p2+id.length());
                            else function_name_in_line=Any_identifier_tok; //assume it will work with identifier....
                            String fstemp=fs[ii].replaceFirst("("+Any_identifier_tok+")"+WS0+":="+WS0+"("+function_name_in_line+WS0+"\\("+WS0+AnyChar_tok+WS0+")\\)"+WS0+";","$2,$1\\);")+source_modifier+" rewritten "+function_name_in_line+"function call into a procedure call ";
                            fs[ii] = fstemp;
                            Log(ii, fs[ii]);
                        }
                        break;
                }// endswitch
            ii++;
        }// end while
    }

    /***************************************************************************************************************************
     * rewrite specific functions into procedures to workaround some cobra GA limitations
     **************************************************************************************************************************/
    private static void ProcessIFS_specific(String[] fs, int nblines) throws Exception
    {
        if (fp==0) //only process this  in DB2 V9.7 GA 
        {
            Rewrite_Function_into_proc(fs, nblines, "LOCK_BY_ID___", "&TABLE%ROWTYPE");
            Rewrite_Function_into_proc(fs, nblines, "LOCK_BY_KEYS___", "&TABLE%ROWTYPE");
            Rewrite_Function_into_proc(fs, nblines, "Get_Object_By_Id___", "&TABLE%ROWTYPE");
            Rewrite_Function_into_proc(fs, nblines, "Get_Object_By_Keys___", "&TABLE%ROWTYPE");
            Rewrite_Function_into_proc(fs, nblines, "GET_RECORD___", "&VIEW%ROWTYPE");
            Rewrite_Function_into_proc(fs, nblines, "Read_Document", "BOOLEAN");
        }
        Process_While_Loop_With_UDF(fs, nblines, "WHILE (Client_SYS.Get_Next_From_Attr");
    }



    /***********************************************************************************************************
     * find out if it is an empty line or a line we shouldn't do anything with
     ***********************************************************************************************************/
    private static boolean isLineWeShouldIgnore(String st)
    {
        //return ((st.length() == 0) || (st.equals("/")) || (st.startsWith("--")) );//|| st.startsWith("REM "));
        if (st.length()==0) return true;
        if (st.equals("/")) return true;
        if (st.startsWith("--")) return true;
        return false;
    }
    /***************************************************************************************************************************
     * rewrite FETCH cursor BULK COLLECT INTO coll LIMIT x statement .... 
     * FETCH CDriver BULK COLLECT INTO   TCommandesAPurger LIMIT MaxSize;
     * into 
     * FOR myloop in 1..MaxSize LOOP --PDA
         FETCH CDriver --PDA BULK COLLECT 
             INTO   TCommandesAPurger(myloop) ; --PDA LIMIT MaxSize;
        END LOOP; --PDA
     **************************************************************************************************************************/
    private static void rewrite_FETCH_Bulk_Collect_Stmt(String[] fs, int curLine, int nblines)
    {
        if (fp>0) //at least FP1 //don't rewrite bulk collect in fp1 or above
            return;
        String orgLine = fs[curLine];
        String indent=get_indent(orgLine);
        String fsutrim = orgLine.trim().toUpperCase();
        String[] toks = fsutrim.split(WS1);
        try
        {
            if ((toks.length<=5)&& ( fsutrim.indexOf(";")==-1))  
            { // try to merge the 2 lines if no ; found on the current line.. just in case the bulk collect is on 2 lines.
                fs[curLine+1] =fs[curLine]+" "+fs[curLine+1]; 
                fs[curLine] = source_modifier+ fs[curLine];
                //now re parse the line and prepare it for the rest
                curLine++;
                orgLine = fs[curLine];
                indent=get_indent(orgLine);
                fsutrim = orgLine.trim().toUpperCase();
                toks = fsutrim.split(WS1);
            }
            if ( fsutrim.indexOf(";") >-1)
            if (toks.length<7 && toks.length>4 && (toks[0].equals("FETCH")) && toks[2].equals("BULK") && toks[3].equals("COLLECT")&& toks[4].equals("INTO") )
            { //no limit clause  in there, create one.
                int posdelim=toks[5].indexOf(";"); //is last token having a semi-column ? 
                if (posdelim>-1)
                {                 
                    toks[5]=toks[5].substring(0,posdelim);                    
                }
                fs[curLine]=indent+"FOR myloop in 1..1000000 LOOP "+source_modifier+crlf+
                indent+"  "+toks[0]+' '+toks[1]+' '+toks[4]+' '+toks[5]+"(myloop);"+source_modifier+orgLine.trim()+crlf+
                indent+"  "+" EXIT WHEN "+toks[1]+"%notfound;"+crlf+ 
                indent+"END LOOP;"+source_modifier;
                Log(curLine, orgLine + " into " + fs[curLine] + " LOOP");
            }
            if ((toks.length>7) &&( fsutrim.indexOf(";")>-1))
             if ( ( ! isLineWeShouldIgnore(fsutrim)) && 
                    (toks[0].equals("FETCH") && toks[2].equals("BULK") && toks[3].equals("COLLECT")&& toks[4].equals("INTO") && toks[6].equals("LIMIT")))
            {   // found line to rewrite with FETCH  and BULK COLLECT and LIMIT keywords in the right place
                //fs[curLine] = fs[curLine].replaceFirst(toks[0], "FOR myloop in 1.."+toks[7]+" LOOP "+source_modifier+crlf+indent+toks[0]);
                int posdelim=toks[7].indexOf(";");
                if (posdelim>-1)
                {                   
                    toks[7]=toks[7].substring(0,posdelim);                    
                }
                String stmp="";
                boolean limit_1=toks[7].equals("1");
                if (limit_1==false) //don't generate loop if only one  in the limit
                    stmp= indent+"FOR myloop in 1.."+toks[7]+" LOOP "+source_modifier+crlf;
                stmp=stmp+indent+"  "+toks[0]+' '+toks[1]+' '+toks[4]+' '+toks[5]+((limit_1==false)?"(myloop);":"(1);")+source_modifier+orgLine.trim()+crlf;
                if (limit_1==false) //don't generate loop if only one  in the limit
                {
                  stmp=stmp+indent+"  "+" EXIT WHEN "+toks[1]+"%notfound;"+crlf;
                  stmp=stmp+indent+"END LOOP;"+source_modifier;
                }
                fs[curLine]=stmp;
                Log(curLine, orgLine + " into " + fs[curLine] + " LOOP");
            }// end if  
        } catch (Exception e)
        {
            Log(curLine, "OOPPS! ...Exception in rewriting FETCH BULK COLLECT LIMIT  statement..giving up on this line !!!" + fs[curLine]);
            e.printStackTrace();
        }
    }
     
    /***************************************************************************************************************************
     * rewrite FORALL statement .... FORALL i IN v_language_nm_list.first .. v_language_nm_list.last INSERT INTO COLL_CD_LNG.... ;
     * into FOR i in v_language_nm_list.first .. v_language_nm_list.last INSERT INTO ...
     **************************************************************************************************************************/
    private static void rewrite_FOR_ALL_Stmt(String[] fs, int curLine, int nblines)
    {
        if (fp>0) //at least FP1 //don't rewrite bulk collect in fp1 or above
            return;
        String orgLine = fs[curLine];
        String indent=get_indent(orgLine);
        String fsutrim = orgLine.trim().toUpperCase();
        String[] toks = fsutrim.split(WS1);
        try
        {
            if (toks.length<3)
            { // is IN on the next line? bring it back to the same line then and comment out the other one
                fs[curLine+1] =fs[curLine]+" "+fs[curLine+1]; 
                fs[curLine] = source_modifier+ fs[curLine];
                //now re parse the line and prepare it for the rest
                curLine++;
                orgLine = fs[curLine];
                indent=get_indent(orgLine);
                fsutrim = orgLine.trim().toUpperCase();
                toks = fsutrim.split(WS1);
            }
            if ((! isLineWeShouldIgnore(fsutrim)) && (toks[0].equals("FORALL") && toks[2].equals("IN") && orgLine.contains("..")))
            {// found line torewrite with FORALL and IN keyword?
                // is next stmt an insert/update/delete ?
                int nextstm=curLine+1;
                String nextline=fs[nextstm].trim();
                // remove empty lines and comment lines
                while ( (nextline.equals("")==true)|| (nextline.startsWith("--")==true))
                {
                   nextstm++;
                   nextline=fs[nextstm].trim();
                }
                String[] toksstmt=nextline.split(WS1);
                String Firstkw = toksstmt[0].toUpperCase();
                if (Firstkw.equals("INSERT") || Firstkw.equals("UPDATE") || Firstkw.equals("DELETE")||Firstkw.equals("EXECUTE"))
                {
                    // now look for end of this stmt (next ; )
                    int jj = nextstm;
                    int endofstmt = -1;
                    boolean found = false;
                    // now look below this line for INSERT/DELETE/UPDATE/EXECUTE end of statement
                    while (jj < nblines && found == false)
                    {
                        String fsutmp = fs[jj].trim().toUpperCase();
                        if (fsutmp.contains(";"))
                        {
                            endofstmt = jj;
                            found = true;
                        }
                        else if (fsutmp.startsWith("END ")) // hummm en dof stmt not found leave without touching anything
                        {
                            Log(curLine, "hummmmm...couldn't find end of FORALL... don't do antyhing with thisyet.. give up ");
                            return;
                        }
                        jj++;
                    }// end while
                    if ((found) && (nextstm > curLine))
                    {
                        String[] toksstmt2 = orgLine.trim().split(WS1);
                        //find comment position if any.
                        String stmp = fs[curLine].replaceFirst(toksstmt2[0], "FOR"); //change the FORALL into  a FOR keeping the same case
                        int poscomment=orgLine.indexOf("--");
                        if (poscomment>-1)
                             stmp=stmp.substring(0,poscomment-3)+" LOOP "+source_modifier+"changed into FOR loop instead"+stmp.substring(poscomment-3);
                        else
                            stmp=stmp+" LOOP "+source_modifier+"changed into FOR loop instead"; //no comment append direcrtly
                       fs[curLine] =stmp;
                        // String[] toksstmt2 = fs[nextstm].trim().split(WS1);
                        // if (toksstmt[0].equals("INSERT")) fs[nextstm]=fs[nextstm].replaceFirst(toksstmt2[0]," LOOP "+crlf+"INSERT")+source_modifier;
                        // else if (toksstmt[0].equals("DELETE")) fs[nextstm]=fs[nextstm].replaceFirst(toksstmt2[0]," LOOP"+crlf+" DELETE")+source_modifier;
                        // else if (toksstmt[0].equals("UPDATE")) fs[nextstm]=fs[nextstm].replaceFirst(toksstmt2[0]," LOOP UPDATE")+source_modifier;
                        stmp=fs[endofstmt];
                        fs[endofstmt] = stmp + crlf + indent+"END LOOP; "+source_modifier+"and of FORALL transformation ";
                        stmp=fs[endofstmt];
                        Log(curLine, orgLine + " into " + fs[curLine]);
                    }// end if
                }// end if stmt
                else if (toksstmt[0].equals("SELECT"))
                {
                    Log(curLine, "hummmmm...Found a FORALL with SELECT... don't do antyhing with thisyet.. give up ");
                    return;
                }
            }// end if isLine
        } catch (Exception e)
        {
            Log(curLine, "OOPPS! ...Exception in rewriting FORALL statement..giving up on this line !!!" + fs[curLine]);
            e.printStackTrace();
        }
    }
    /***************************************************************************************************************************
     * rewrite RETURNING ROWID clause into a SELECT rowID FROM table ; DELETE FROM .......
     **************************************************************************************************************************/
    private static String rewrite_DELETE_Returning_Clause(String[] fs, int curLine, int nblines,String indent, int deleteLinepos)
    {  //  generate a SELECT on the same line of the DELETE stmt
       Log(curLine, "skipping fixing ROWID issue for UPDATE/DELETE statement. Need to manually change it.");
//            String s0=""; int pinto=0;  int line_semi=0;   int line_into=0;
//            String[] tok1s = fs[curLine].trim().split(WS1);
//            String fsutmp=fs[curLine].toUpperCase();
//            String[] tokdel_upd = fsutmp.split(WS1);
//            String transition_table=(tokdel_upd[0].equalsIgnoreCase("DELETE") ? " OLD " : " NEW ");
//            if (line_semi==line_into)
//               s0 = fs[curLine].replaceFirst(tok1s[0],"SELECT").replace(";","")+" FROM "+transition_table+" TABLE( "+source_modifier+crlf+indent+fs[deleteLinepos];
//            else
//            {
//                for (int z=curLine; z<line_semi; z++) //concatenate all columns into a single RETURN INTO clause
//                    s0=s0+fs[z].trim()+" ";                        
//                s0 = s0.replaceFirst(tok1s[0],"SELECT").replace(";", "")+" FROM "+transition_table+" TABLE( "+source_modifier+crlf+indent+fs[deleteLinepos];
//            }
//            fs[deleteLinepos]=indent+s0;
//            //if (fs[curLine].contains(";"))
//            s0=fs[curLine].replaceFirst(tok1s[0], "); "+source_modifier+"RETURNING");
//            //else   s0=fs[curLine].replaceFirst(tok1s[0], "); "+source_modifier+"RETURNING");
//            fs[curLine]=s0;
//            for (int z=curLine+1; z<= line_semi; z++) //comment out all the  remaining up to line_semi
//            {
//                s0=indent+source_modifier+" "+fs[z].trim();
//                fs[z]=s0;
//            }
//            Log(curLine,fs[curLine]);   
        return fs[curLine];    
    }
    /***************************************************************************************************************************
     * rewrite RETURNING ROWID clause into a INSERT ..SELECT rowID FROM table ....
     **************************************************************************************************************************/
    private static boolean rewrite_INSERT_Returning_Clause(String[] fs, int curLine, int nblines,String indent, int insertLinepos, int valuesLinepos, boolean rowid_found_in_returning_clause)
    { // ok now we found an INSERT RETURNING INTO statement with ROWID
        boolean changed=false;
        int posinto = insertLinepos; // default think that INTO clause is on same line as INSERT tablename INTO...
        String tablename = "";
        String[] insertintotoks = fs[insertLinepos].trim().split(WS1);
        int posbrak=fs[insertLinepos].indexOf("(");
        if (insertintotoks.length > 2) // may be like an INSERT INTO tablename check it out...
        {
            tablename = insertintotoks[2];// if yes get tablename
            // remove everything after ( in that token.
            if (tablename.indexOf("(")>0)
                tablename=tablename.substring(0,tablename.indexOf("("));
            if (insertintotoks.length==4 && insertintotoks[3].equals("(")==false )
                posinto = insertLinepos+1;
            else
            if (posbrak > -1 && insertintotoks.length >4) //found a opening bracket on that line with probably a list of columns as well.
               posinto=insertLinepos;
            else
                posinto = insertLinepos+1; //most likely the list of columns is on next lines
        }
        else
        {
            // is the next line just an INTO table name clause?
            insertintotoks = fs[insertLinepos + 1].trim().split(" ", 6);
            if (insertintotoks[0].toUpperCase().equals("INTO") && insertintotoks.length > 1)
            {
                tablename = insertintotoks[1];// if yes get tablename
                posinto = insertLinepos + 1;
            }
        }
        if (rowid_found_in_returning_clause==true)
        {
            // String[][] columns = new String[2000][2]; //max amount of columns i one table now find columns for Where clause
            String WhereClause = ""; String s0 = "";  String s1 = "";
            boolean firstAND=true;
            for (int cc = 0; cc <= valuesLinepos - posinto - 1; cc++)
            {
                String[] s0toks = fs[cc + posinto ].trim().split(",");
                String[] s1toks = fs[cc + valuesLinepos].trim().split(",");
                if (s0toks.length != s1toks.length)
                    return false; //fs[curLine];//give up  if too many columns don't match on same line!!
                
                for (int tk=0; tk< s0toks.length; tk++) 
                {
                    s0=s0toks[tk].trim();s1=s1toks[tk].trim(); //assume all tokens fo rcolumns in the same way
                    if (s1.toUpperCase().contains(".NEXTVAL"))
                        return false; //fs[curLine]; // give up if a sequence is found there...need eyeballing.
                    
                    if ((s0.startsWith("--") && s1.startsWith("--"))||s1.toLowerCase().startsWith("returning"))
                    {
                        tk=s0toks.length+1;
                        continue;
                    }
                    if (s1.toLowerCase().startsWith("values"))
                        s1=s1.substring(6).trim();           
                    if ((s0.toLowerCase().startsWith("insert") || ( s0.toLowerCase().indexOf("into")>-1))&& (s0.indexOf("(")>-1))
                        s0=s0.substring(s0.indexOf("(")).trim();       
                    if (s0.endsWith(")") && s1.endsWith(")") )
                    {
                        s0 = s0.substring(0, s0.lastIndexOf(")")).trim(); s1 = s1.substring(0, s1.lastIndexOf(")")).trim();
                    }
                    else // both token either ends with ) or starts with  (  
                    if (s0.startsWith("(") && s1.startsWith("(") && cc==0 ) //remove openning brackets at beginning of lists if any
                    {
                        s0 = s0.substring(1).trim();  s1 = s1.substring(1).trim();
                    }
                    //if (s0.endsWith("))") && cc == valuesLinepos - posinto - 2) // replace last closing ) on the last column row
                    //    s0 = s0.substring(0, s0.lastIndexOf(")")).trim();
                    if (s0.endsWith(",")) // replace last comma
                        s0 = s0.substring(0, s0.lastIndexOf(",")).trim();
                    //if (s1.endsWith(")") && cc == valuesLinepos - posinto - 2) // replace last closing ) on the last column row
                    //    s1 = s1.substring(0, s1.lastIndexOf(")")).trim();
                    if (s1.endsWith(",")) // replace last comma
                        s1 = s1.substring(0, s1.lastIndexOf(",")).trim();
                    if (s0.length()>0 && s1.length()>0)
                    {
                        WhereClause = WhereClause + ((firstAND==false) ? " AND " : "") + s0.trim() + "=" + s1.trim();
                        firstAND=false;
                    }
                }//end of for tk
            }
            if ((tablename.length() > 0)&&(WhereClause.trim().length()>1)) // found tablename and where clause correctly populated
            {
                String stmp= fs[curLine];
                int preturning=stmp.toUpperCase().indexOf("RETURNING");
                if (preturning > -1) //must have RETURNIN clause here + knwo its position
                {
                    stmp=stmp.replace(";", "");
                    int lg="RETURNING".length();
                    String returningtoken=stmp.substring(preturning,preturning+lg);
                    stmp=stmp.replaceFirst(returningtoken,"; SELECT");
                    stmp=stmp+" FROM "+tablename+" WHERE "+WhereClause+";"+source_modifier+"rewrote RETURNING CLAUSE into a select";
                    fs[curLine]=stmp;
                    changed=true;
                }
            }
        }
//      else
//      { // generate a SELECT on the same line of the INSERT stmt
//          String s0 = fsu; 
//          for (int z=curLine+1; z<= line_semi; z++)
//              s0=s0+" "+fs[z].trim();
//          fs[insertLinepos]=s0.replace(toks[0],"SELECT").replace(";", "")+" FROM NEW TABLE( "+source_modifier+crlf+indent+fs[insertLinepos].trim();
//          s0=fs[insertLinepos];
//          //if (fs[curLine].contains(";"))
//              fs[curLine]=fs[curLine].replaceFirst(toks[0], ");"+source_modifier+"RETURNING");
//          //else 
//          //    fs[curLine]=fs[curLine].replaceFirst(toks[0], ");"+source_modifier+"RETURNING");                        
//          s0=fs[curLine]; //just for Debug 
//          for (int z=curLine+1; z<= line_semi; z++){
//              s0=get_indent(fs[z])+source_modifier+fs[z].trim();  
//              fs[z]=s0;
//          }
//      }// end else no rowid
        if (changed)
            Log(curLine, fs[curLine]);
        return changed; // fs[curLine];
    }//end of proc
    
    /***************************************************************************************************************************
     * rewrite RETURNING ROWID clause into a SELECT rowID FROM table ....
     **************************************************************************************************************************/
    private static boolean rewrite_Returning_Clause(String[] fs, int curLine, String fslow, int nblines)
    {
        String fsutrim = fslow.trim();
        String[] toks = fsutrim.split(WS1);
        boolean found_returning_into=false;
        boolean found_semi =false;   
        boolean rowid_found_in_returning_clause =false;
        try
        {
            int pinto=0;  //int line_semi=0;   int line_into=0;
            //int cl=curLine; 
            for (int cl=curLine; (cl < nblines && (found_returning_into == false || found_semi == false)); cl++)
            {
                String s=fs[cl];
                int pos_comment=s.indexOf("--");
                int pos_semi=s.indexOf(";");
                if (s.toLowerCase().contains("rowid"))
                    rowid_found_in_returning_clause=true;
                if (found_semi==false)
                    if ((pos_semi>=0 && pos_comment == -1 )|| ( pos_semi>=0 && pos_comment > pos_semi))
                    {
                        found_semi = true;// line_semi = cl;
                    }
                if (found_returning_into==false)
                {
                    String[] tok3s= s.trim().split(WS1);
                    for (pinto = 0; pinto < tok3s.length && found_returning_into == false; pinto++)
                    {
                        if (tok3s[pinto].equalsIgnoreCase("into"))
                        {
                            //line_into = cl;
                            found_returning_into = true;
                        }
                    }
                }
                //                /cl++;
            }// end while

            if ((! isLineWeShouldIgnore(fslow) ) && (toks[0].equals("returning") && found_returning_into  && found_semi  &&
                    rowid_found_in_returning_clause ))
            { // found line torewrite with returning keyword?
                pinto--; // decrement as it was incremented by one extra when found.
                int jj = curLine - 1;  int insertLinepos = -1; int valuesLinepos = -1;  int deleteLinepos=-1;
                boolean found = false;  String fsutmp=""; String indent="";
                // is there a ROWID in the RETURNING CLAUSE ? then flag it as we will have to generate an extra SELECT stmt instead of a SELECT FROM NEW TABLE()
                ///////boolean rowid_found = (fsu.contains("ROWID"));
                // now look above this line for INSERT keyword, stop if found BEGIN or IS on a single line or up to beginning of file
                while (jj > 0 && found == false)
                {
                    fsutmp = fs[jj].trim().toLowerCase();
                    String tmptok[]=fsutmp.split(WS1);
                    if (tmptok[0].equals("insert")  || tmptok[0].startsWith("insert(") ) //|| (fsutmp.startsWith("INSERT INTO ") == true)
                    {
                        found = true;
                        insertLinepos = jj;
                        indent=get_indent(fs[jj]);
                    }
                    else
                        if (tmptok[0].equals("delete")  ||  tmptok[0].equals("update") )
                        {
                            found = true;
                            deleteLinepos = jj;
                            indent=get_indent(fs[jj]);
                        }
                        else 
                            if ((tmptok[0].equals("values") ) || tmptok[0].startsWith("values(")||tmptok[0].startsWith("values ("))
                            {
                                if ( tmptok[0].endsWith("(") && tmptok.length<2)
                                    valuesLinepos=jj+1; //list of columns must be on next line, not this one
                                else
                                if (tmptok.length>=2 && (tmptok[1].equals("(") ||tmptok[0].endsWith("(")) )
                                    valuesLinepos = jj;
                                else     
                                    valuesLinepos=jj+1; //2 tokens with last one being a ( means values are on next line
                            }
                            else 
                                if ((tmptok[0].equals("is") ) || (tmptok[0].equals("begin")) || (tmptok[0].equals("function")) ||(tmptok[0].equals("procedure")))
                                {//do nothing
                                }
                    jj--;
                }
                if ((found) && (deleteLinepos > -1) )
                {//  generate a SELECT on the same line of the DELETE stmt
                    Log(curLine, "skipping fixing ROWID issue for UPDATE/DELETE statement. Need to manually change it.");
                    //return rewriteDELETEReturningClause(fs, curLine, nblines, indent, deleteLinepos);
                }
                else
                    if ((found) && (valuesLinepos > -1) && (insertLinepos > -1)&&(rowid_found_in_returning_clause))
                    { // ok now we found an INSERT RETURNING INTO statement with ROWID
                        return rewrite_INSERT_Returning_Clause( fs, curLine, nblines,indent, insertLinepos, valuesLinepos, rowid_found_in_returning_clause);
                    }
                    else
                        Log("hummmmm...can't find beginning of RETURNING clause statement");
            }// endif
            return false; //fs[curLine];
        } catch (Exception e)
        {
            Log(curLine, "OOPPS! ...Exception in rewriting Returning Clause statement..giving up on this line !!!" + fs[curLine]);
            e.printStackTrace();
            return false;
        }
    } // end rewriteReturningClause
    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static boolean rewrite_Sub_Type(String[] fs, int curLine, int nblines)
    {
        String line=fs[curLine];
        //subtype T_B2cCDetTarAbrCollResIndx is varchar2(100);

            String[] toks = line.trim().split(WS1);            
            String subtype_name=toks[1].trim();
            String subtype_Type=toks[3].replace(';', ' ').trim();
            Log(curLine,"found subtype "+toks[1]+" representing "+toks[3]);
            //finally comment out the subtype line
            fs[curLine]=source_modifier+" subtype replaced further in the file: "+fs[curLine];
            Log(curLine, fs[curLine]);
            //NOw find all subtypes usage in the same object ( up to "/" or end of file.)
            boolean done=false;
            String subtype_nameL=subtype_name.toLowerCase();
            int lg_subtype_name=subtype_nameL.length();
            boolean same_type_redeclaration=false;
            String st;
            String[] toks2;
            String stlo;
            for (int ii=curLine+1; (ii < nblines) && (done==false); ii++)
            {
                st = fs[ii]; 
                if (isLineWeShouldIgnore(st.trim()))  
                {// continue to next line...
                    continue;
                 }   
                stlo=st.toLowerCase();
                //is this a subtype re-declaration of the same type ?
                if (stlo.indexOf("subtype")>-1 && (pattern_SubType.matcher(st).matches()))
                {
                    toks2 = stlo.trim().split(WS1);            
                    String subtype_name2=toks2[1].trim();
                    //String subtype_Type2=toks2[3].replace(';', ' ').trim();
                    if (subtype_name2.equals(subtype_nameL))
                    {
                        same_type_redeclaration=true;
                        done=true;
                    }
                }
                
                if (! same_type_redeclaration) //try to replace the subtype now
                { 
                    //st=st.toUpperCase();
                    int lg_st=st.length();
                    //String curFragment=st.substring(0);
                    int pos_curFragment=stlo.indexOf(subtype_nameL); //substring(0)
                    for(int jj=0; jj<lg_st && pos_curFragment >-1; /*jj++*/)
                    {
                       int pstart=pos_curFragment +jj; //curFragment.indexOf(subtype_nameU)+jj;
                       if (pstart>-1)
                       {
                         int pend=pstart+lg_subtype_name;
                         char charstart=stlo.charAt(pstart-1);
                         char chend=' ';
                         if (pend < lg_st) 
                             chend=stlo.charAt(pend);
                         else fs[ii]=fs[ii]+' ';
                         if (((charstart==' ')||(charstart=='\t')) && //(charstart=='.')
                              (( chend==' ' || chend==';' || chend==':'|| chend==','|| chend==')' || chend=='\t'))) //chend=='.' ||
                         {
                             String orgStr=charstart+fs[ii].substring(pstart,pend)+chend;
                             fs[ii]=fs[ii].replace(orgStr, charstart+subtype_Type+chend)+source_modifier +"replaced "+orgStr+" with following subtype:"+subtype_Type;
                             Log(ii, "new subtype line replaced:"+fs[ii]);
                          }
                         jj+=pend;
                       }
                       jj++;
                       if (jj<lg_st) //curFragment=st.substring(jj);
                          pos_curFragment=stlo.substring(jj).indexOf(subtype_nameL);
                       else pos_curFragment=-1;
                    }//endoffor
                }
            }//endofwhile
        return true; // fs[curLine];
    }

    /***************************************************************************************************************************
     * replace multiple whitespaces between words with single blank
     **************************************************************************************************************************/
    public static String itrim(String source)
    {
        return source.replaceAll("\\s{2,}", " ");
    }

    /***************************************************************************************************************************
     * trim specific characters at end of stin
     **************************************************************************************************************************/
    private static String TrimSpecificChar(String stin, String char_to_trim)
    {
        while (stin.endsWith(char_to_trim))
        {
            int p = stin.lastIndexOf(char_to_trim);
            stin = stin.substring(0, p);
        }
        while (stin.startsWith(char_to_trim))
        {
            stin = stin.substring(char_to_trim.length());
        }
        return stin;
    }

    /***********************************************************************************************************
     * 
     * 
     ***********************************************************************************************************/
    private static void comment_out_C_style_comments(String[] fs, int curLine, int nblines)
    {
        fs[curLine].replaceFirst("/*", source_modifier);
        while (!fs[curLine].contains("*/") && curLine < nblines - 1)
        {
            // comment out the following line as SQL style comment....
            String tmp = fs[curLine];
            // remove any / in between
            fs[curLine] = source_modifier+ tmp.replaceAll("/", " ");
            tmp = fs[curLine];
            curLine++;
        }// edn while
        // until end of file or end of comment found.
        if (curLine < nblines - 1 && fs[curLine].contains("*/"))
        { // found end of C style comment...
            fs[curLine] = source_modifier + fs[curLine];
            String tmp = fs[curLine];
            // is something correct after the */
            int p = fs[curLine].indexOf("*/");
            if (p > -1)
            {
                tmp = fs[curLine].substring(0, p);
                fs[curLine] = tmp.replaceAll("/", " ") + crlf + fs[curLine].substring(p + 2);
                tmp = fs[curLine];
            }
        }

    }

    /***********************************************************************************************************
     * Read a File in memory
     * it can also merge lines that have been split on multi lines  by a sqlplus extractions ( usually truncated @80 chars) 
     ***********************************************************************************************************/
    private static int ReadFileIntoMemory(String filein,String[] fs)
    {
        int nblines=0;
        int prevlgline=0;
        boolean lastlinemerged=false;
        try {
            BufferedReader bread = new BufferedReader(new FileReader(filein));
            while ((fs[nblines] = bread.readLine()) != null)
            {
                if ( merge_lines>0)
                {
                    String InputLine=fs[nblines];
                    if (InputLine.length()>0 && prevlgline==merge_lines)
                    {
                        Character c=InputLine.charAt(0); //first character a letter or number ? 
                        if (nblines>=2  && ((Character.isLetterOrDigit(c) || c=='_'|| c=='"'|| c=='.'|| c=='\'')))
                        { // is this a split line?
                            int lg=fs[nblines-1].length();
                            if (lg>0)
                            {
                                char c2=fs[nblines-1].charAt(lg-1);
                                if (((lastlinemerged=true && prevlgline==merge_lines) || lg==merge_lines) &&
                                        (Character.isLetterOrDigit(c2)|| c2=='_'|| c2=='"'|| c2=='.'|| c2=='\''))
                                { //merge lines
                                    prevlgline=fs[nblines].length();
                                    fs[nblines-1]=fs[nblines-1]+fs[nblines];
                                    fs[nblines]="";
                                    nblines--;
                                    lastlinemerged=true;
                                }else lastlinemerged=false;
                            }else lastlinemerged=false;                        
                        } else lastlinemerged=false;
                    } else lastlinemerged=false;
                    if ( lastlinemerged==false)  
                        prevlgline=fs[nblines].length();
                }
                nblines++;
            }//end while
            bread.close();
            return nblines;
        }
        catch (java.io.FileNotFoundException f)
        {
            System.out.println("Cannot find file [" + filein + "] while loading line "+nblines);
            return 0;
        }
        catch (Exception f)
        {
            System.out.println("Exception while reading file [" + filein + "]");
            f.printStackTrace();
            return 0;
        }        
    }

    /***********************************************************************************************************
     * Write whole FS memory into File
     ***********************************************************************************************************/
    private static int WriteFileFromMemory(String fileoutput, String fileToProcess, String[] fs, int nblines)
    {
      BufferedWriter  fwrite=null;
      int jj=-1;
      try{
          //PrintStream fwrite = new PrintStream(new FileOutputStream(fileoutput));
          fwrite = new BufferedWriter(new FileWriter(fileoutput));
          for ( jj=0; jj< nblines; jj++)
          {
              if (fs[jj] != null)
                  fwrite.write(fs[jj]); //fwrite.println(fs[jj]);   write all lines
              fwrite.newLine();
          }         
      } catch (java.io.FileNotFoundException f)
      {
          if (!silent)
              System.out.println("Cannot find file [" + fileToProcess + "]");
          return -1;
      } catch (Exception e)
      {
          e.printStackTrace();
          return -1;
      }
      finally {
          //Close the BufferedWriter
          try {
              if (fwrite != null) {
                  fwrite.flush();
                  fwrite.close();
              }
              return jj;
          } catch (IOException e) {
              e.printStackTrace();
              return -1;
          }
      }      
    }
    
    /***********************************************************************************************************
     * count the  number of lines in a File..
     ***********************************************************************************************************/
   public static int  Count_lines_in_file(String file_name) throws IOException
    { 
       int nblines=0;
       long Q1=System.currentTimeMillis();
       BufferedReader bread = new BufferedReader(new FileReader(file_name));
       while (bread.readLine() != null)
       {
           nblines++;
       }
       bread.close();
       long Q2=System.currentTimeMillis();
       if (!silent)
           System.out.println(" Counted "+nblines+" lines of DDL and PL/SQL in "+((Q2-Q1)/1000.0)+"sec"); 
       return nblines;
    }
    
    /***********************************************************************************************************
     * Process a SQLPLUS File..can be called recursively by includes...
     ***********************************************************************************************************/
    public static String TurboFixOraSQL(String oraSQL,   
               boolean lsilent, boolean loutputallinone, boolean lremovecomments, String lsource_modifier, 
               boolean ldont_modify_comments)
    {
    	String outputOraSQL = "";
    	int nblines = 0;
    	String[] fs;
    	String line;
        silent=lsilent;
        outputallinone=loutputallinone;
        removecomments=lremovecomments;
        source_modifier=lsource_modifier;
        dont_modify_comments=ldont_modify_comments;
        BufferedReader reader = new BufferedReader(new java.io.StringReader(oraSQL));
    	try
		{
			while (reader.readLine() != null)
			{
				nblines++;
			}
	    	reader.close();
			fs = new String[nblines];
	    	reader = new BufferedReader(new java.io.StringReader(oraSQL));
	    	int i = 0;
	    	while ((line = reader.readLine()) != null)
	    	{
	    		fs[i++] = line;
	    	}
	    	reader.close();
	        ProcessSQLPlusBuffer(fs, nblines, null, null,null);
	        StringBuffer bufferOutput = new StringBuffer();
	        for (i = 0; i < nblines; ++i)
	        {
	        	if (fs[i] != null)
	        	   bufferOutput.append(fs[i]+crlf);
	        }
	        outputOraSQL = bufferOutput.toString();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return outputOraSQL;
    }
    
    private static void ProcessSQLPlusBuffer(String[] fs, int nblines, String includePath, String fileoutput,HashMap<String, String> VariableParent)
    {
        long Qstart=System.currentTimeMillis();
        HashMap<String, String> Variable=new HashMap<String, String>();
        if (VariableParent!=null) // if we have a parent hashmap for the variables, copy it locally as we need to keep a separate version of these
            Variable.putAll(VariableParent);            
        try
        {
            boolean in_package = false;
            String InputLine;
                        
            if(IFS==true )
              ProcessIFS_specific(fs, nblines);

            int curLine = -1;
            while (curLine < nblines - 1)
            {
                curLine++;
                alllines++;

                InputLine = fs[curLine]; // itrim(tmp);
                String st = InputLine.trim();
               // if (curLine >=1250 )
                //    stop = true;
 
                if (st.startsWith("/") && !(st.startsWith("/*"))) //if it is an SQLPLus end of stmt, reset objects state variables
                {
                    in_create_table = false;
                    in_package = false;
                }
                if (isLineWeShouldIgnore(st))
                { // empty line just print it and continue
                    if ((st.startsWith("--")) && (removecomments))
                        ;// remove the comment from the file by doing nothing...
                    else
                       // WritePlSQL(fw, InputLine);
                    continue;
                }

               // String[] tok2s = st.split(WS1, 2);//initial quick parsing
                if (st.startsWith("/*") && !st.contains("*/"))
                {// not a /* */ comment on the same line
                    if ( dont_modify_comments==false) 
                        comment_out_C_style_comments(fs, curLine, nblines);
                }
                else if (st.startsWith("/*") &&  st.endsWith("*/") )
                {// It is  a /* */ comment on the same line
                    if ( dont_modify_comments==false)
                        fs[curLine]=source_modifier+fs[curLine];
                        //comment_out_C_style_comments(fs, curLine, nblines);
                }     
                else //if (tok2s.length >= 1) // is line worth looking at?
                {
                    String output= (InputLine.indexOf('&') > -1) ? 
                                         Replace_Var_In_String(Variable, InputLine, curLine) : InputLine;
                    fs[curLine] = output;
                    String LowSt = output.toLowerCase();
                    String[] tok2s = output.trim().split(WS1, 2);
                    String firstkw = tok2s[0].toUpperCase();
                    if (LowSt.contains("create ") && LowSt.contains(" package "))
                    {
                        in_package = true;
                        in_create_table = false;
                    }
                    else
                    if (( ! sqlplus_mode ) &&( ! in_package))
                    {
                        in_package=Process_SQLPLUS_commands(Variable, InputLine, fs, curLine, st, output, tok2s, firstkw,in_package);
                    }
                    
                    if (firstkw.equals("START") && !(tok2s[1].toLowerCase().startsWith("with")) || firstkw.equals("@@")
                            || firstkw.startsWith("@"))
                    {
                        in_create_table = false;
                        in_package = false;
                        fs[curLine]=source_modifier+ st;
                        Process_Include((outputallinone) ? fileoutput : null, tok2s, curLine, InputLine, includePath,fs,Variable);
                    }
                    else if (firstkw.equals("COMMENT"))
                    { // found somehting like: COMMENT ON COLUMN FND_TAB_COMMENTS.table_name IS  'FLAGS=KM--L^DATATYPE=STRING(30)^PROMPT=Table Name^';
                        in_create_table = false;
                        in_package = false;
                        Process_Comment_On_Column(tok2s, curLine, output); //, Variable);
                    }
                    else
                    {
                         simple_Rewrite_of_PLSQL(fs, tok2s, curLine, nblines); //output
                    }
                }

            }// end while
        } catch (Exception f)
        {
        	f.printStackTrace();
        } finally
        {
            long Qstop=System.currentTimeMillis();
            if (silent == false)
                Display_final_stats(Variable,Qstart,Qstop);
        }
    }
    
    /***********************************************************************************************************
     * Process a SQLPLUS File..can be called recursively by includes...
     * @throws IOException 
     ***********************************************************************************************************/
    private static void ProcessSQLPlusFile(String fileoutput, String filein, boolean isinclude,String[] fsParent,int clparent,HashMap<String, String> VariableParent) throws IOException
    {        
        int  nb_lines=Count_lines_in_file( filein);
        if (nb_lines==0)
            return;
        String[] fs = new String[nb_lines+10]; //max_lines];
        File f1 = new File(filein);
        String includePath = f1.getParent(); // /default, thinks we have all includes in same directory as filein
        if (!silent)
            System.out.println("includePath ["+includePath+"] from Plsql file="+filein);
        nb_lines=ReadFileIntoMemory(filein,fs);  //for some reasons we got different nb lines second time by removing wrapper source
        
        ProcessSQLPlusBuffer(fs, nb_lines, includePath, fileoutput,VariableParent);
        
        if ( isinclude)
            fsParent[clparent]=fsParent[clparent]+crlf+MergeAllIncludeLinesIntoOneSingleline(fs); //all local fs lines into the parent line.
        else
        if (fileoutput!=null) {//only null  when we are processing an include file..so no need to rewrite it.
        	WriteFileFromMemory(fileoutput, filein, fs, nb_lines);
        }
    }
    /***********************************************************************************************************
     * Merges a SQLPLUS INCLUDE File..in its parent array of string by concatenating all into one single StringBuffer
     ***********************************************************************************************************/
    private static String MergeAllIncludeLinesIntoOneSingleline( String[] fs)
    {
       StringBuffer mergedInclude=new StringBuffer();
       for (int ii=0; ii< fs.length; ii++)
           if (fs[ii] != null)
                   mergedInclude.append(fs[ii]+crlf);
       return mergedInclude.toString();
    }

    /**************************************************************************************************
     * Only replace and rewrite  these SQLPLUS commands when SQLPLUS_mode is set to false.
     * @param Variable
     * @param InputLine
     * @param fs
     * @param curLine
     * @param st
     * @param output
     * @param tok2s
     * @param firstkw
     *************************************************************************************************/
    private static boolean Process_SQLPLUS_commands(HashMap<String, String> Variable, String InputLine, String[] fs, int curLine, String st, String output,
            String[] tok2s, String firstkw,boolean in_package)
    {
        //boolean in_package=false;
        
        if (firstkw.equals("SHOW") || firstkw.equals("REM") || firstkw.equals("VARIABLE") || firstkw.equals("ECHO"))
        {
            fs[curLine]=source_modifier+ st;
        }  
        else if (firstkw.equals("SET") && curLine>=1 && !(fs[curLine-1].toUpperCase().contains("UPDATE ")) && tok2s.length>1)
        {
            if (in_SET_Command(tok2s[1]) == true)
                fs[curLine]=source_modifier+ st;
            else
                if (tok2s[1].toUpperCase().equals("DEFINE OFF"))
                    {
                    process_SQLPLUS_VARS=false;
                    fs[curLine]=source_modifier+ st;
                    }
             else
                    if (tok2s[1].toUpperCase().equals("DEFINE ON"))
                    {
                        process_SQLPLUS_VARS=true;
                        fs[curLine]=source_modifier+ st;
                    }
            else
            if (tok2s[1].toUpperCase().startsWith("TRANSACTION "))
                fs[curLine]=source_modifier+ st;
            else 
                if (tok2s[1].toUpperCase().startsWith("SERVEROUT "))
                    fs[curLine]=fs[curLine].replaceFirst(tok2s[1].substring(0,10), "SERVEROUTPUT ")+";";
        }
        else if (firstkw.equals("DEFINE"))
        {
            in_create_table = false;
            in_package = false;
            fs[curLine]=Process_Defines( tok2s, curLine, InputLine, Variable);
        }
        else if (firstkw.equals("UNDEFINE"))
        {
            in_create_table = false;
            in_package = false;
            fs[curLine]=Process_UnDefine(tok2s, curLine, InputLine, Variable);
        }
        else if (firstkw.equals("PROMPT"))
        {
            in_create_table = false;
            in_package = false;
            output = Replace_Var_In_String(Variable, InputLine, curLine);
            fs[curLine]=output.replaceFirst(tok2s[0], "ECHO ")+crlf+"/"; 
        }
        else if (firstkw.equals("EXEC"))
        {
            in_create_table = false;
            in_package = false;
            // output = ReplaceVarInString(fw, Variable, InputLine, curLine);
            fs[curLine]=output.replaceFirst(tok2s[0], "CALL ");
        }
        return in_package;
    }


    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    private static void Display_final_stats(HashMap<String, String> V,long Qst, long Qstp)
    {
        Log("=================================================");
        if (Qstp == Qst)
           Log("" + alllines + " of lines processed in "+(Qstp-Qst)/1000.0+"sec :"+nb_changed+ " changes of code done.");
        else
           Log("" + alllines + " of lines processed in "+(Qstp-Qst)/1000.0+"sec :("+ ((1000*alllines)/(Qstp-Qst))+" lines/sec) "+nb_changed+ " changes of code done.");
        Log("" + nb_includes + " include files processed. " + nbincludesnotfound + " includes not found");
        DumpVariableTable(V);
        Log("Done");
    }

    /***************************************************************************************************************************
     * Process main options
     **************************************************************************************************************************/
    private static void Process_extra_args(String[] args)
    {
        for (int i = 2; i < args.length; i++)
        {
            String ag = args[i].toUpperCase();
            if (ag.startsWith("-IFS"))
                IFS = true;
            else
            if (ag.startsWith("-SILENT"))
                silent = true;
            else if (ag.startsWith("-MERGE_ALL"))
                outputallinone = true;
            else if (ag.startsWith("-?") || ag.startsWith("-h"))
                usage();
            else if (ag.startsWith("-NO_COMMENT"))
                removecomments = true;
            else if (ag.startsWith("-MAX_LINES="))
                max_lines = Integer.parseInt(ag.substring(11));
            else if (ag.startsWith("-MODIFIER="))
                source_modifier = " --"+ag.substring(10)+' ';
            else if (ag.startsWith("-DONT_MODIFY_COMM") ||ag.startsWith("-DONT_MOD_COMM") )
                dont_modify_comments = true;
            else if (ag.startsWith("-MERGE_LINES="))
                merge_lines = Integer.parseInt(ag.substring(13));    
            else if (ag.startsWith("-KEEP_WRAPPED"))
                Keep_wrapped_code = true;
            else if (ag.startsWith("-REMOVE_WRAPPED"))
                Remove_wrapped_code = true;           
                        
            else if (ag.startsWith("-FP="))
            {
                fp = Integer.parseInt(ag.substring(4));
            }
            else
            if (ag.startsWith("-SQLPLUS_MODE"))
                sqlplus_mode = true;
        }
        if ( !silent) 
            System.out.println("setting for fixpack="+fp);
    }

    /***************************************************************************************************************************
     * 
     **************************************************************************************************************************/
    public static void main(String[] args)
    {
        String fileToProcess = "C:\\MTK\\projects\\IFS\\10tb83i.sql";  
        String fileoutput = "output.sql";

        if (args.length >= 2)
        {
            fileToProcess = args[0];
            fileoutput = args[1];
            Process_extra_args(args);
        }
        else
            usage();

         try{
             ProcessSQLPlusFile(fileoutput, fileToProcess,false,null,-1,null);
         }
         catch(Exception e)
         {
             e.printStackTrace();
         }
    }

}
