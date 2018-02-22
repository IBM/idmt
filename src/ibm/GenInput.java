/*
 * IBM Corporation
 * Author: vikram.khatri@us.ibm.com
 * 
 */
package ibm;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GenInput
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss.SSS");

    private static void log(String msg)
    {
        System.out.println("[" + timestampFormat.format(new Date()) + "] " + msg);
    }

    public static void main(String[] args)
    {
        String dbSourceName, db2SchemaName, srcSchemaName, server, dbName, uid, pwd, compatibility, selectSchemaList, caseSensitiveNess;
        int port;
        
        if (args.length < 11)
        {
            System.out.println("usage: java ibm.GenInput dbSourceName srcSchemaName db2SchemaName selectedSchemaList server dbname portnum uid pwd compatibility casesensitiveness");
            System.out.println("srcSchemaName: ':' delimited list of source schema. If 'ALL', build list from the source database");
            System.out.println("db2SchemaName: ':' delimited list of target schema. If 'ALL', copy this list as it is from if srcSchemaName.");
            System.out.println("selectedSchemaList: ':' delimited subset srcSchemaName to extract. If 'ALL', copy this list as it is from if srcSchemaName.");
            System.out.println("For MS Access, srcSchemaName is ignored since access does not have concept of schema but db2SchemaName is required for a schema name in DB2");
            System.out.println("For Sybase using db2 compatibility, specify compatibility=true");
            System.out.println("casesensitiveness can be true or false. If true, retain case name for schema, table, columns as it is from source database");
            System.exit(-1);
        }
        dbSourceName = args[0];
        srcSchemaName = args[1];
        db2SchemaName = args[2];
        selectSchemaList = args[3];
        server = args[4];
        dbName = args[5];
        port = Integer.parseInt(args[6]);
        uid = args[7];
        pwd = args[8];
        compatibility = args[9];
        caseSensitiveNess = args[10];
        pwd = IBMExtractUtilities.Decrypt(pwd);        
        if (srcSchemaName.equalsIgnoreCase("ALL"))
           db2SchemaName = "ALL";
        if (dbSourceName.equalsIgnoreCase("domino"))
           dbName = server;
        if (caseSensitiveNess.equalsIgnoreCase("false")) 
        	db2SchemaName = db2SchemaName.toUpperCase();
        log("dbSourceName:" + dbSourceName);
        log("db2SchemaName:" + db2SchemaName);
        log("srcSchemaName:" + srcSchemaName);
        log("selectSchemaList:" + selectSchemaList);
        log("server:" + server);
        log("port:" + port);
        log("dbName:" + dbName);
        log("uid:" + uid);
        log("compatibility:" + compatibility);
        log("caseSensitiveNess:" + caseSensitiveNess);
        
        IBMExtractUtilities.CreateTableScript(dbSourceName, db2SchemaName, srcSchemaName, selectSchemaList, server, port, dbName, uid, pwd, Boolean.valueOf(compatibility), caseSensitiveNess);
    }
}
