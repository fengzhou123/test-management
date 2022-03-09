package kiwiplan.kdw.tasks;

import java.sql.ResultSet;

import java.util.logging.Logger;

import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.LogHelper;

public class CheckSchema {
    static Logger logger = LogHelper.getRootLogger();

    public static boolean checkFieldNameContains(String dbName, String strFind) {
        boolean   bHas = false;
        ResultSet tableRS;

        try {
            tableRS = DBHelper.getInstance().getTableRSList(dbName);

            while (tableRS.next()) {
                String    tableName = tableRS.getString("TABLE_NAME");
                ResultSet columnRS  = DBHelper.getInstance().getColumnRSList(dbName, tableName);

                while (columnRS.next()) {
                    String columnName = columnRS.getString("COLUMN_NAME");

                    if (columnName.contains(strFind)) {
                        bHas = true;
                        logger.info("Table : " + tableName + " Column : " + columnName);
                    }
                }
            }
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return bHas;
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
//      LogHelper.initializeLog("CheckSchema");
//      LogHelper.truncate();
//        LogHelper.setLogLevel(LogHelper.LOGLEVEL_INFO);
        CheckSchema.checkFieldNameContains("java23_masterDW", "company");
    }
}
