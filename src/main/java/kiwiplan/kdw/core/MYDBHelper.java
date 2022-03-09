package kiwiplan.kdw.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.logging.Logger;

public final class MYDBHelper extends DBHelper {
    static Logger rootLogger = LogHelper.getRootLogger();

    public boolean connectDB() {
        String url = "";
        dbPort   = DB_PORT;
        hostName = DB_HOSTNAME;
        userName = DB_USERNAME;
        passWord = DB_PASSWORD;
        dbDriver = MYSQL_DRIVER;
        url      = "jdbc:mysql://" + hostName + ":" + dbPort + "?allowMultiQueries=true";
        return connectDB(hostName, userName, passWord, dbDriver, dbPort, url);
    }

    public void executeUpdateSQL(String sql) throws SQLException {
        rootLogger.info(sql);

        if ((conn == null) || (!conn.isValid(2))) {
            connectDB();
        }

        PreparedStatement stmt;

        stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        stmt.execute(sql);

        // closeDBConnection();
    }

    public void main(String[] args) {
        String hostName  = ConfigHelper.getInstance().getConfig("Database.MSDBServer");    // $NON-NLS-1$
        String userName  = ConfigHelper.getInstance().getConfig("Database.MSDBUser");      // $NON-NLS-1$
        String passWord  = ConfigHelper.getInstance().getConfig("Database.MSDBPasswd");    // $NON-NLS-1$
        String dbName    = "kdw_awlive_comptibility_821feb16_esp";                         // $NON-NLS-1$
        String tableName = "agtAgent";
        String dbPort    = DB_PORT;
        String dbDriver  = MSSQL_DRIVER;

        // SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA
        String sqlcmd        = "USE " + dbName + "; SELECT * FROM INFORMATION_SCHEMA.TABLES";
        String sqlForeignKey = "";

        sqlForeignKey =
            "USE " + dbName + "; "
            + "SELECT KCU1.COLUMN_NAME AS COLUMN_NAME, KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME, KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME"
            + " FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC"
            + " INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG"
            + " AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA" + " AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
            + " INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2"
            + " ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG"
            + " AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA"
            + " AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME"
            + " AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION" + " WHERE KCU1.TABLE_NAME='" + tableName + "'";

        String url = "";

        url = "jdbc:sqlserver://" + hostName + ":" + dbPort + ";";
        connectDB(hostName, userName, passWord, dbDriver, dbPort, url);

        try {
            ResultSet rs = executeSQL(sqlForeignKey);

            printResultSet(rs, 10);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println();
    }

    public ResultSet getColumnRSList(String dbName, String tableName) {
        ResultSet columnRS = null;
        String    sql      = "";

        sql = "SELECT COLUMN_NAME, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName + "' AND "
              + " TABLE_NAME='" + tableName + "'";

        try {
            columnRS = executeSQL(sql);
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + sql);
            e.printStackTrace();

            return null;
        }

        return columnRS;
    }

    public String getColumnStrList(String dbName, String tableName) {
        String columnList = "";
        String sql        = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName
                            + "' AND " + " TABLE_NAME='" + tableName + "'";

        try {
            ResultSet columnRS1 = executeSQL(sql);

            while (columnRS1.next()) {

                // go through each column
                String columnName = columnRS1.getString("COLUMN_NAME");

                if (columnList.isEmpty()) {
                    columnList = columnName;
                } else {
                    columnList += "," + columnName;
                }
            }
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + sql);
            e.printStackTrace();

            return null;
        }

        return columnList;
    }

    public Connection getDBConnection(String dbName) {
        String url = "";
        dbPort   = DB_PORT;
        hostName = DB_HOSTNAME;
        userName = DB_USERNAME;
        passWord = DB_PASSWORD;
        dbDriver = MYSQL_DRIVER;
        url      = "jdbc:mysql://" + hostName + ":" + dbPort + "/" + dbName + "?allowMultiQueries=true";
        connectDB(hostName, userName, passWord, dbDriver, dbPort, url);
        return conn;
    }

    public ResultSet getForgenKeyRSList(String dbName, String tableName) {
        ResultSet columnRS = null;
        String    sql      = "";

        sql = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME";
        sql = sql + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='" + dbName + "' AND TABLE_NAME='"
              + tableName + "' AND REFERENCED_TABLE_NAME IS NOT NULL";

        try {
            columnRS = executeSQL(sql);
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + sql);
            e.printStackTrace();

            return null;
        }

        return columnRS;
    }

    public String getPrimaryKey(String dbName, String tableName) {
        String    primaryKey = "";    // $NON-NLS-1$
        ResultSet columnRS;

        try {
            columnRS = getColumnRSList(dbName, tableName);

            while (columnRS.next()) {
                String columnKey = columnRS.getString("COLUMN_KEY");    // $NON-NLS-1$

                if (columnKey.equals("PRI")) {                          // $NON-NLS-1$
                    primaryKey = columnRS.getString("COLUMN_NAME");     // $NON-NLS-1$

                    return primaryKey;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return primaryKey;
    }

    public long getRecordNumber(String dbName, String tableName) {
        long recordNumber = 0;

        // get column list
        String sql = "SELECT count(*) as recordnumber FROM " + dbName + "." + tableName;

        try {
            ResultSet columnRS = executeSQL(sql);

            if (columnRS.next()) {
                recordNumber = columnRS.getLong("recordnumber");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return recordNumber;
    }

    public boolean isTableEmpty(String dbName, String tableName) {
        String sql = "";

        sql = "SELECT * FROM " + dbName + "." + tableName + " limit 2";

        ResultSet rs;

        try {
            rs = executeSQL(sql);

            if (rs.next()) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public ResultSet getTableRSList(String dbName) {
        ResultSet tableRS = null;
        String    sql     = "";

        sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + dbName + "'";

        try {
            tableRS = executeSQL(sql);
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + sql);
            e.printStackTrace();

            return null;
        }

        return tableRS;
    }
}
