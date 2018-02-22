/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Albupload
{
   private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");
   private static String server = "", dbName = "";
   private static int port, fetchSize = 100;
   private static Properties login = new Properties();
   private static String INPUT_FILE_NAME = "", driverName = "com.ibm.db2.jcc.DB2Driver";
   private Connection bladeConn = null;
   private static boolean autoCommit = true;

   private static String getURL()
   {
       String url = "";
       url = "jdbc:db2://" + server + ":" + port + "/" + dbName;                
       return url;
   }
   
   private int create_temp_table() throws SQLException
   {
      int retCode = -1;
      String sqlStatement = "CREATE TABLE SCH1.BOOK LIKE INV.BOOK";
      try
      {
         PreparedStatement countStatement = bladeConn.prepareStatement(sqlStatement);
         retCode = countStatement.executeUpdate();
      } catch (SQLException ex)
      {
         if (ex.getErrorCode() == -601)
         {
            log("Table SCH1.BOOK already exists.");
         } else
            throw ex;
      }
      return retCode;
   }
   
   private int load_data() throws SQLException
   {
      long rows_read = 0, rows_skipped = 0, rows_loaded = 0, rows_rejected = 0, rows_deleted= 0, rows_committed = 0; 
      ResultSet Reader = null;      
      int retCode = -1;
      String sqlStatement = "CALL SYSPROC.ADMIN_CMD('LOAD  FROM " + INPUT_FILE_NAME + 
      " OF DEL " + 
      " MODIFIED BY   CODEPAGE=1208  COLDEL~ ANYORDER USEDEFAULTS CHARDEL\"\" DELPRIORITYCHAR NOROWWARNINGS" + 
      " METHOD P (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66)" + 
      " REPLACE INTO SCH1.BOOK" + 
      " (" + 
      " INV_ID," + 
      " WORK_ID," + 
      " WORK_CODE," + 
      " TIGER_ALL," + 
      " ORIGINAL_UPLOAD_DT," + 
      " UPLOAD_DT," + 
      " UPLOAD_LOG_ID," + 
      " LAST_MINOR_UPDATE_DT," + 
      " LAST_UPDATE_DT," + 
      " SELLER_ID," + 
      " SELLER_REC," + 
      " SELLER_LOCATION," + 
      " ACTUAL_COPIES," + 
      " REPLICATED_COPIES," + 
      " SELLER_COST," + 
      " SELLER_NOTES," + 
      " MEDIA_FORMAT," + 
      " RECORD_LANGUAGE," + 
      " BOOK_CONDITION," + 
      " JACKET_CONDITION," + 
      " EAN," + 
      " ISBN," + 
      " AUTHOR," + 
      " ILLUS," + 
      " TITLE," + 
      " PLACE_PUB," + 
      " PUBLISHER," + 
      " DATE_PUB," + 
      " LANGUAGE," + 
      " EDITION," + 
      " VOLUMES," + 
      " BINDING," + 
      " CONDITION_NOTES," + 
      " NOTES," + 
      " KEYWORDS," + 
      " UPLOAD_PRICE," + 
      " UPLOAD_CURRENCY," + 
      " DISCOUNT_PERCENT," + 
      " USD_DEALER_PAYABLE," + 
      " USD_DEALER_LIST_PRICE," + 
      " CONSUMER_PRICE," + 
      " LIBRARY_PRICE," + 
      " WHOLESALE_PRICE," + 
      " C2C_PRICE," + 
      " FLAGS," + 
      " FLAG_SIG," + 
      " FLAG_1ST," + 
      " FLAG_MPR," + 
      " FLAG_EXL," + 
      " FLAG_DJK," + 
      " FLAG_ISB," + 
      " FLAG_UND," + 
      " FLAG_BCE," + 
      " FLAG_ARC," + 
      " FLAG_NODJKI," + 
      " FLAG_CLR," + 
      " FLAG_MEF," + 
      " ON_HOLD," + 
      " HOLD_TYPE," + 
      " NORMED_TITLE," + 
      " NORMED_AUTHOR," + 
      " NORMED_ILLUS," + 
      " NORMED_PUBLISHER," + 
      " NORMED_DATE_PUB" + 
      " )" + 
      " NONRECOVERABLE " + 
      " INDEXING MODE AUTOSELECT')";
      PreparedStatement prepStatement = bladeConn.prepareStatement(sqlStatement);
      retCode = prepStatement.executeUpdate();
      Reader = prepStatement.executeQuery();
      while (Reader.next()) 
      {
         rows_read = Reader.getLong(1);
         rows_skipped = Reader.getLong(2);
         rows_loaded = Reader.getLong(3);
         rows_rejected = Reader.getLong(4);
         rows_deleted = Reader.getLong(5);
         rows_committed = Reader.getLong(6);
      }
      log ("rows_read = " + rows_rejected);
      log ("rows_skipped " + rows_skipped);
      log ("rows_loaded " + rows_loaded);
      log ("rows_rejected " + rows_rejected);
      log ("rows_deleted " + rows_deleted);
      log ("rows_committed " + rows_committed);
      if (Reader != null)
          Reader.close();      
      return retCode;
   }
   
   private int do_merge() throws SQLException
   {
      int retCode = -1;
      String sqlStatement = "MERGE INTO INV.BOOK AR USING " + 
      " (SELECT INV_ID, WORK_ID, WORK_CODE, TIGER_ALL, TIGER_PQ, " + 
      " ORIGINAL_UPLOAD_DT, UPLOAD_DT, UPLOAD_LOG_ID, LAST_MINOR_UPDATE_DT, LAST_UPDATE_DT, " + 
      " SELLER_ID, SELLER_REC, SELLER_LOCATION, ACTUAL_COPIES, REPLICATED_COPIES, SELLER_COST, " + 
      " SELLER_NOTES, MEDIA_FORMAT, RECORD_LANGUAGE,  BOOK_CONDITION, JACKET_CONDITION, EAN, " + 
      " ISBN, AUTHOR, ILLUS, TITLE, PLACE_PUB, PUBLISHER, DATE_PUB, \"LANGUAGE\", EDITION, " + 
      " \"VOLUMES\", BINDING, CONDITION_NOTES, NOTES, KEYWORDS, UPLOAD_PRICE, UPLOAD_CURRENCY, " + 
      " DISCOUNT_PERCENT, USD_DEALER_PAYABLE, USD_DEALER_LIST_PRICE, CONSUMER_PRICE, LIBRARY_PRICE, " + 
      " WHOLESALE_PRICE, C2C_PRICE, FLAGS, FLAG_SIG, FLAG_1ST, FLAG_MPR, FLAG_EXL, FLAG_DJK, " + 
      " FLAG_ISB, FLAG_UND, FLAG_BCE, FLAG_ARC, FLAG_NODJKI, FLAG_CLR, FLAG_MEF, ON_HOLD, " + 
      " HOLD_TYPE, NORMED_TITLE, NORMED_AUTHOR, NORMED_ILLUS, NORMED_PUBLISHER, NORMED_DATE_PUB" + 
      " FROM SCH1.BOOK) AC" + 
" ON (AR.INV_ID = AC.INV_ID)" + 
" WHEN MATCHED THEN UPDATE SET " + 
      " (AR.WORK_ID,AR.WORK_CODE,AR.TIGER_ALL,AR.TIGER_PQ,AR.ORIGINAL_UPLOAD_DT,AR.UPLOAD_DT,AR.UPLOAD_LOG_ID," + 
      " AR.LAST_MINOR_UPDATE_DT,AR.LAST_UPDATE_DT,AR.SELLER_ID,AR.SELLER_REC,AR.SELLER_LOCATION,AR.ACTUAL_COPIES," + 
      " AR.REPLICATED_COPIES,AR.SELLER_COST,AR.SELLER_NOTES,AR.MEDIA_FORMAT,AR.RECORD_LANGUAGE,AR.BOOK_CONDITION," + 
      " AR.JACKET_CONDITION,AR.EAN,AR.ISBN,AR.AUTHOR,AR.ILLUS,AR.TITLE,AR.PLACE_PUB,AR.PUBLISHER,AR.DATE_PUB," + 
      " AR.\"LANGUAGE\",AR.EDITION,AR.\"VOLUMES\",AR.BINDING,AR.CONDITION_NOTES,AR.NOTES,AR.KEYWORDS,AR.UPLOAD_PRICE," + 
      " AR.UPLOAD_CURRENCY,AR.DISCOUNT_PERCENT,AR.USD_DEALER_PAYABLE,AR.USD_DEALER_LIST_PRICE,AR.CONSUMER_PRICE," + 
      " AR.LIBRARY_PRICE,AR.WHOLESALE_PRICE,AR.C2C_PRICE,AR.FLAGS,AR.FLAG_SIG,AR.FLAG_1ST,AR.FLAG_MPR,AR.FLAG_EXL," + 
      " AR.FLAG_DJK,AR.FLAG_ISB,AR.FLAG_UND,AR.FLAG_BCE,AR.FLAG_ARC,AR.FLAG_NODJKI,AR.FLAG_CLR,AR.FLAG_MEF," + 
      " AR.ON_HOLD,AR.HOLD_TYPE,AR.NORMED_TITLE,AR.NORMED_AUTHOR,AR.NORMED_ILLUS,AR.NORMED_PUBLISHER," + 
      " AR.NORMED_DATE_PUB) " + 
      " = " + 
      " (AC.WORK_ID,AC.WORK_CODE,AC.TIGER_ALL,AC.TIGER_PQ,AC.ORIGINAL_UPLOAD_DT,AC.UPLOAD_DT,AC.UPLOAD_LOG_ID," + 
      " AC.LAST_MINOR_UPDATE_DT,AC.LAST_UPDATE_DT,AC.SELLER_ID,AC.SELLER_REC,AC.SELLER_LOCATION,AC.ACTUAL_COPIES," + 
      " AC.REPLICATED_COPIES,AC.SELLER_COST,AC.SELLER_NOTES,AC.MEDIA_FORMAT,AC.RECORD_LANGUAGE,AC.BOOK_CONDITION," + 
      " AC.JACKET_CONDITION,AC.EAN,AC.ISBN,AC.AUTHOR,AC.ILLUS,AC.TITLE,AC.PLACE_PUB,AC.PUBLISHER,AC.DATE_PUB," + 
      " AC.\"LANGUAGE\",AC.EDITION,AC.\"VOLUMES\",AC.BINDING,AC.CONDITION_NOTES,AC.NOTES,AC.KEYWORDS,AC.UPLOAD_PRICE," + 
      " AC.UPLOAD_CURRENCY,AC.DISCOUNT_PERCENT,AC.USD_DEALER_PAYABLE,AC.USD_DEALER_LIST_PRICE,AC.CONSUMER_PRICE," + 
      " AC.LIBRARY_PRICE,AC.WHOLESALE_PRICE,AC.C2C_PRICE,AC.FLAGS,AC.FLAG_SIG,AC.FLAG_1ST,AC.FLAG_MPR,AC.FLAG_EXL," + 
      " AC.FLAG_DJK,AC.FLAG_ISB,AC.FLAG_UND,AC.FLAG_BCE,AC.FLAG_ARC,AC.FLAG_NODJKI,AC.FLAG_CLR,AC.FLAG_MEF," + 
      " AC.ON_HOLD,AC.HOLD_TYPE,AC.NORMED_TITLE,AC.NORMED_AUTHOR,AC.NORMED_ILLUS,AC.NORMED_PUBLISHER," + 
      " AC.NORMED_DATE_PUB) " + 
" WHEN NOT MATCHED THEN INSERT" + 
      " (AR.INV_ID,AR.WORK_ID,AR.WORK_CODE,AR.TIGER_ALL,AR.TIGER_PQ,AR.ORIGINAL_UPLOAD_DT,AR.UPLOAD_DT,AR.UPLOAD_LOG_ID," + 
      " AR.LAST_MINOR_UPDATE_DT,AR.LAST_UPDATE_DT,AR.SELLER_ID,AR.SELLER_REC,AR.SELLER_LOCATION,AR.ACTUAL_COPIES," + 
      " AR.REPLICATED_COPIES,AR.SELLER_COST,AR.SELLER_NOTES,AR.MEDIA_FORMAT,AR.RECORD_LANGUAGE,AR.BOOK_CONDITION," + 
      " AR.JACKET_CONDITION,AR.EAN,AR.ISBN,AR.AUTHOR,AR.ILLUS,AR.TITLE,AR.PLACE_PUB,AR.PUBLISHER,AR.DATE_PUB," + 
      " AR.\"LANGUAGE\",AR.EDITION,AR.\"VOLUMES\",AR.BINDING,AR.CONDITION_NOTES,AR.NOTES,AR.KEYWORDS,AR.UPLOAD_PRICE," + 
      " AR.UPLOAD_CURRENCY,AR.DISCOUNT_PERCENT,AR.USD_DEALER_PAYABLE,AR.USD_DEALER_LIST_PRICE,AR.CONSUMER_PRICE," + 
      " AR.LIBRARY_PRICE,AR.WHOLESALE_PRICE,AR.C2C_PRICE,AR.FLAGS,AR.FLAG_SIG,AR.FLAG_1ST,AR.FLAG_MPR,AR.FLAG_EXL," + 
      " AR.FLAG_DJK,AR.FLAG_ISB,AR.FLAG_UND,AR.FLAG_BCE,AR.FLAG_ARC,AR.FLAG_NODJKI,AR.FLAG_CLR,AR.FLAG_MEF," + 
      " AR.ON_HOLD,AR.HOLD_TYPE,AR.NORMED_TITLE,AR.NORMED_AUTHOR,AR.NORMED_ILLUS,AR.NORMED_PUBLISHER," + 
      " AR.NORMED_DATE_PUB) " + 
      " VALUES " + 
      " (AC.INV_ID,AC.WORK_ID,AC.WORK_CODE,AC.TIGER_ALL,AC.TIGER_PQ,AC.ORIGINAL_UPLOAD_DT,AC.UPLOAD_DT,AC.UPLOAD_LOG_ID," + 
      " AC.LAST_MINOR_UPDATE_DT,AC.LAST_UPDATE_DT,AC.SELLER_ID,AC.SELLER_REC,AC.SELLER_LOCATION,AC.ACTUAL_COPIES," + 
      " AC.REPLICATED_COPIES,AC.SELLER_COST,AC.SELLER_NOTES,AC.MEDIA_FORMAT,AC.RECORD_LANGUAGE,AC.BOOK_CONDITION," + 
      " AC.JACKET_CONDITION,AC.EAN,AC.ISBN,AC.AUTHOR,AC.ILLUS,AC.TITLE,AC.PLACE_PUB,AC.PUBLISHER,AC.DATE_PUB," + 
      " AC.\"LANGUAGE\",AC.EDITION,AC.\"VOLUMES\",AC.BINDING,AC.CONDITION_NOTES,AC.NOTES,AC.KEYWORDS,AC.UPLOAD_PRICE," + 
      " AC.UPLOAD_CURRENCY,AC.DISCOUNT_PERCENT,AC.USD_DEALER_PAYABLE,AC.USD_DEALER_LIST_PRICE,AC.CONSUMER_PRICE," + 
      " AC.LIBRARY_PRICE,AC.WHOLESALE_PRICE,AC.C2C_PRICE,AC.FLAGS,AC.FLAG_SIG,AC.FLAG_1ST,AC.FLAG_MPR,AC.FLAG_EXL," + 
      " AC.FLAG_DJK,AC.FLAG_ISB,AC.FLAG_UND,AC.FLAG_BCE,AC.FLAG_ARC,AC.FLAG_NODJKI,AC.FLAG_CLR,AC.FLAG_MEF," + 
      " AC.ON_HOLD,AC.HOLD_TYPE,AC.NORMED_TITLE,AC.NORMED_AUTHOR,AC.NORMED_ILLUS,AC.NORMED_PUBLISHER," + 
      " AC.NORMED_DATE_PUB) "; 
      PreparedStatement prepStatement = bladeConn.prepareStatement(sqlStatement);
      retCode = prepStatement.executeUpdate();
      return retCode;
   }
   
   public void run () throws Exception
   {
      int retCode;
      create_temp_table();
      retCode = load_data();
      log ("return Code = " + retCode);
      retCode = do_merge();
      log ("return Code = " + retCode);
      if (bladeConn != null)
        bladeConn.close();
   }
   
   public Albupload()
   {
      try
      {
          if (bladeConn != null)
          {
              bladeConn.close();
          }
          Class.forName(driverName).newInstance();
          log("Driver " + driverName + " loaded");
          bladeConn = DriverManager.getConnection(getURL(), login);
          bladeConn.setAutoCommit(autoCommit);
          log("JDBC driver " + bladeConn.getMetaData().getDriverName() + " Version = " + bladeConn.getMetaData().getDriverVersion());
      } catch (Exception ex)
      {
          log("sql exception connecting " + getURL() + " " + ex.getMessage());
          System.exit(-1);
      }      
   }
   
   private static void log(String msg)
   {
      System.out.println("[" + timestampFormat.format(new Date()) + "] " + msg);          
   }

   public static void main(String[] args) throws Exception
   {
       String uid, pwd;
       if (args.length < 6)
       {
           System.out.println("usage: java -Xmx600m -DOUTPUT_DIR=./output ibm.Albupload input_filename, server dbName " +
                              "port uid pwd");
           System.exit(-1);
       }
       INPUT_FILE_NAME = args[0];       
       server = args[1];
       dbName = args[2];
       port = Integer.parseInt(args[3]);
       uid = args[4];
       pwd = args[5]; 
       log("server:" + server);
       log("dbName:" + dbName);
       log("port:" + port);
       log("uid:" + uid);
       login.setProperty("user", uid);
       login.setProperty("password", pwd);
       Albupload pg = new Albupload();
       pg.run();
   }

}
