package kiwiplan.kdw.core;

import java.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public abstract class DBHelper {
    public final static String DB_PORT = ConfigHelper.getInstance().getConfig("Database.DBPort");
    public final static String DB_HOSTNAME = ConfigHelper.getInstance().getConfig("Database.DBServer");
    public final static String DB_USERNAME = ConfigHelper.getInstance().getConfig("Database.DBUser");
    public final static String DB_PASSWORD = ConfigHelper.getInstance().getConfig("Database.DBPasswd");
    public final static String databaseType = ConfigHelper.getInstance().getConfig("Database.DBTYPE");
    public final static String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";    // "com.mysql.jdbc.Driver";
    public final static String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public final static String DATABASE_MSSQL = "MSSQL";
    public final static String DATABASE_MYSQL = "MYSQL";
    static Logger rootLogger = LogHelper.getRootLogger();
    private static DBHelper dbHelper = null;
    protected Connection conn;
    String hostName;
    String userName;
    String passWord;

    // static String dbName;
    String dbDriver;
    String dbPort;

    protected DBHelper() {
    }

    public void closeDBConnection() {
        try {
            if (conn != null && !conn.isClosed())
                conn.close();
            conn = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract boolean connectDB();

    public boolean connectDB(String hostName, String userName, String passWord, String dbDriver, String dbPort,
                             String url) {
        boolean connected = false;

        conn = null;

        try {
            //Class.forName(dbDriver);
            conn = DriverManager.getConnection(url, userName, passWord);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connected;
    }

    public ResultSet executeSQL(String sql) throws Exception {

//      rootLogger.info(sql);
        ResultSet rs = null;

        if ((conn == null) || (!conn.isValid(2))) {
            connectDB();
        }

        PreparedStatement stmt;

        stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery();

        // closeDBConnection();
        return rs;
    }

    public abstract void executeUpdateSQL(String sql) throws SQLException;

    public String printResultSet(ResultSet rs, int rowCountLimit) throws SQLException {
        return printResultSet(rs, rowCountLimit, 10);
    }

    public String printResultSet(ResultSet rs, int rowCountLimit, int colCountLimit) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        int rowNumber = 0;
        StringBuilder buffer = new StringBuilder();
        String headLine = "\n";

        for (int i = 1; (i <= columnsNumber) && (i <= colCountLimit); i++) {
            int displaySize = rsmd.getColumnDisplaySize(i);

            if (displaySize > 50) {
                displaySize = 50;
            }

            String tmpV = rsmd.getColumnName(i).substring(0, Math.min(50, rsmd.getColumnName(i).length()));

            headLine += String.format("%" + displaySize + "s", tmpV) + "| ";
        }

        buffer.append(headLine);

        if (rsmd.getColumnCount() > colCountLimit) {
            buffer.append(" ...");
        }

        while (rs.next()) {
            rowNumber++;

            if (rowNumber > rowCountLimit) {
                continue;
            }

            String rsLine = "";

            for (int i = 1; (i <= columnsNumber) && (i <= colCountLimit); i++) {
                String columnValue = rs.getString(i);

                if (columnValue == null) {
                    columnValue = "null";
                }

                int displaySize = rsmd.getColumnDisplaySize(i);
                String colTitle = rsmd.getColumnName(i);

                if (colTitle.length() > displaySize) {
                    displaySize = colTitle.length();
                }

                if (displaySize > 50) {
                    displaySize = 50;
                }

                rsLine += String.format("%" + displaySize + "s",
                        columnValue.substring(0, Math.min(50, columnValue.length()))) + "| ";
            }

            buffer.append("\n" + rsLine);
        }
        if (rowNumber > rowCountLimit) {
            buffer.append("\nOnly the first 10 records displayed in " + rowNumber);
        }

//      rootLogger.fine(buffer.toString());
        return buffer.toString();
    }

    public abstract ResultSet getColumnRSList(String dbName, String tableName);

    public abstract String getColumnStrList(String dbName, String tableName);

    public abstract Connection getDBConnection(String dbName);

    public List<String> getDBVersions(String dbName) {
        List<String> dbVersions = new ArrayList<>();
        String SQL = String.format("select version from %s.kp_database_version order by version", dbName);

        try {
            ResultSet resultSet = DBHelper.getInstance().executeSQL(SQL);

            while (resultSet.next()) {
                dbVersions.add(resultSet.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dbVersions;
    }

    public abstract ResultSet getForgenKeyRSList(String dbName, String tableName);


    public static synchronized DBHelper getInstance() {
        if (dbHelper == null) {
            switch (databaseType) {
                case DATABASE_MSSQL:
                    dbHelper = new MSDBHelper();

                    break;

                case DATABASE_MYSQL:
                    dbHelper = new MYDBHelper();

                    break;
            }
        }

        return dbHelper;
    }

    public abstract String getPrimaryKey(String dbName, String tableName);

    public abstract long getRecordNumber(String dbName, String tableName);

    public int getRowNumber(ResultSet columnRecordsRS) throws SQLException {
        columnRecordsRS.last();

        int rowNumber = columnRecordsRS.getRow();

        columnRecordsRS.beforeFirst();

        return rowNumber;
    }

    public abstract boolean isTableEmpty(String dbName, String tableName);

    public abstract ResultSet getTableRSList(String dbName);
}
