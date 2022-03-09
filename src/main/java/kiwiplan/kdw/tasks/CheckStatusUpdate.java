package kiwiplan.kdw.tasks;

import java.sql.Date;
import java.sql.ResultSet;

import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.LogHelper;

//updateRecords should be used with checkUpdatedRecords in pair using one CheckStatusUpdate object
public class CheckStatusUpdate {
    static Logger rootLogger = LogHelper.getRootLogger();

    /*
     *  it should be refactor later as below. One row may be updated multiple columns
     * since some records will be updated only several columns changed together
     * tablename {
     *          row1, {columnname, originalvalue, chagnedvalue}, {columnname, originalvalue, chagnedvalue},...}
     *          row2, {columnname, originalvalue, chagnedvalue}, {columnname, originalvalue, chagnedvalue},...}
     *          row3, {columnname, originalvalue, chagnedvalue}, {columnname, originalvalue, chagnedvalue},...}
     *          ...
     * } // end of tablename
     */
    static HashMap<String, HashMap<String, String>> changedFieldsMap;

    // So far, just use a MAP to store the table and original value. All the original values will be converted to String

    /*
     *  each row will be updated with one column. And the sequence in this changes Map should be same as in DB
     * {
     * tablename, {{columnname, originalvalue}, {columnname, originalvalue}, ...}
     * tablename, {{columnname, originalvalue}, {columnname, originalvalue}, ...}
     * }
     *
     *
     */
    static {
        changedFieldsMap = new HashMap<>();
    }

    String excludeTableList =
        "kp_database_version,dwsiterunlog,dwsiteconfig,dwcustomeraddress,dwsupplieraddress,dwproductdesign";

    ///////////////// Compare //////////////////////
    public boolean checkUpdatedRecords(String revision, String dbName) {
        rootLogger.info("===============================================================");
        rootLogger.info("========================= checkUpdateRecords ==================");
        rootLogger.info("===============================================================");

        boolean bTestResult = true;

        DBHelper.getInstance().getDBConnection(dbName);

        // get table list
        String sql = "SELECT * FROM inf" + "ormation_schema.tables where table_schema='" + dbName + "'";

        try {
            ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (inExcludeTableList(tableName)) {
                    continue;
                }

                rootLogger.info("Table name :::::::::::::::" + tableName);

                HashMap<String, String> tableChangesMap = changedFieldsMap.get(tableName);

                bTestResult = checkUpdatedTableRecords(revision, dbName, tableName, tableChangesMap);
            }

            tableRS.close();
        } catch (Exception e) {
            e.printStackTrace();

            return false;
        }

        rootLogger.info("===============================================================");
        rootLogger.info("====================== updateRecords DONE =====================");
        rootLogger.info("===============================================================");

        return bTestResult;
    }

    public boolean checkUpdatedTableRecords(String revision, String dbName, String tableName,
                                            HashMap<String, String> tableChangesMap)
            throws Exception {

        // TODO check the record_status field changed to 'U'
        boolean bTestResult = true;

        // get column list
        ResultSet columnRS            = DBHelper.getInstance().getColumnRSList(dbName, tableName);
        int       recordNumberUpdated = 0;
        String    sPrimaryKey         = DBHelper.getInstance().getPrimaryKey(dbName, tableName);
        String    sql                 = "select * from " + dbName + "." + tableName + " order by " + sPrimaryKey;
        ResultSet columnRecordsRS     = DBHelper.getInstance().executeSQL(sql);
        int       rowNumber           = DBHelper.getInstance().getRowNumber(columnRecordsRS);

        if (rowNumber == 0) {
            rootLogger.info("\n======================" + tableName + " is empty" + " ======================\n");

            return bTestResult;
        }

        // get foreign key constrains
        ResultSet foreignKeysRS = DBHelper.getInstance().getForgenKeyRSList(dbName, tableName);

        while (columnRS.next()) {

            // go through each column
            String columnKey = columnRS.getString("COLUMN_KEY");

            if (columnKey.equals("PRI")) {

                // primary key. Bypass
                continue;
            }

            String columnName = columnRS.getString("COLUMN_NAME");

            if (inColumnFilterList(columnName)) {
                continue;
            }

            if (KDWHelper.isNaturalKey(revision, tableName, columnName)) {

                // bypass natural keys
                continue;
            }

            ///////////////////////////////////////////////////////////////////////////////////////
            // do not update the columns with foreign key constrains since it will failed without disable the constrains.
            // and also will involve compare logic to update it even we disable the constrains manually
            boolean isForeignKey = false;

            foreignKeysRS.first();

            while (foreignKeysRS.next()) {

                // go through each column
                String foreignKeyName = foreignKeysRS.getString("COLUMN_NAME");

                if (columnName.equalsIgnoreCase(foreignKeyName)) {
                    isForeignKey = true;

                    break;
                }
            }

            if (isForeignKey) {
                continue;
            }

            /////////////////////////////////////////////////////////////////////////////////////
            String columnType = columnRS.getString("DATA_TYPE");

            rootLogger.info("Table : " + tableName + " columnName : " + columnName);

            String originalValue = tableChangesMap.get(columnName);
            String currentValue  = null;

            /*
             * In 8.0_250 KDW build, all data types are as below:
             *      char
             *       text
             *       varchar
             *
             *       bigint
             *       int
             *       tinyint
             *       smallint
             *       double
             *       decimal
             *
             *       date
             *       datetime
             *       timestamp
             */
            if (columnRecordsRS.next()) {
                if (columnType.contains("char") || columnType.contains("text")) {
                    currentValue = columnRecordsRS.getNString(columnName);
                } else if (columnType.contains("int")
                           || columnType.contains("double")
                           || columnType.contains("decimal")) {
                    double originalNumber = columnRecordsRS.getDouble(columnName);

                    currentValue = String.valueOf(originalNumber);
                } else if (columnType.contains("time") || columnType.contains("date")) {
                    Date originalDate = columnRecordsRS.getDate(columnName);

                    if (originalDate != null) {
                        currentValue = originalDate.toString();
                    } else {
                        currentValue = "";
                    }
                } else {
                    rootLogger.info("============ Unknown Types: " + columnType + " ============");
                }

                if (!originalValue.equals(currentValue)) {
                    rootLogger.info("Table: " + tableName + " Column: " + columnName
                                    + " Updated but failed to be reverted : Current value is : " + currentValue
                                    + " Original value is : " + originalValue);
                    bTestResult = false;
                }

                recordNumberUpdated++;
            } else {

                // it means there is no enough records which can be changed
                // for each column
                rootLogger.info("\n============ Table : " + tableName
                                + " don't have enough records to update columns. Nothing to compare ============\n");

                break;
            }
        }

        columnRecordsRS.close();
        columnRS.close();
        foreignKeysRS.close();
        rootLogger.info("\n============|| " + recordNumberUpdated + " || records updated and compared for table: "
                        + tableName + " ============\n");

        return bTestResult;
    }

    private static boolean inColumnFilterList(String columnName) {
        boolean inList = false;

        // bypass the record last changed column
        if (columnName.contains("record_last_changed")) {
            inList = true;
        }

        if (columnName.contains("record_status")) {
            inList = true;
        }

        if (columnName.contains("record_origin")) {
            inList = true;
        }

        return inList;
    }

    private boolean inExcludeTableList(String tableName) {
        StringTokenizer st = new StringTokenizer(excludeTableList, ",");

        while (st.hasMoreTokens()) {
            if (st.nextToken().equalsIgnoreCase(tableName)) {
                return true;
            }
        }

        return false;
    }

    public static void main(String[] args) {
        String            dbName = "java12_masterDW";
        CheckStatusUpdate test   = new CheckStatusUpdate();

        test.updateRecords("8.0", dbName);
    }

    public boolean updateRecords(String revision, String dbName) {
        rootLogger.info("===============================================================");
        rootLogger.info("========================= updateRecords =======================");
        rootLogger.info("===============================================================");
        DBHelper.getInstance().getDBConnection(dbName);

        // get table list
        String sql = "SELECT * FROM inf" + "ormation_schema.tables where table_schema='" + dbName + "'";

        try {
            ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (inExcludeTableList(tableName)) {
                    continue;
                }

                rootLogger.info("Table name :::::::::::::::" + tableName);

                HashMap<String, String> tableChangesMap = new HashMap<>();

                updateTableRecords(revision, dbName, tableName, tableChangesMap);
                changedFieldsMap.put(tableName, tableChangesMap);
            }

            tableRS.close();
        } catch (Exception e) {
            e.printStackTrace();

            return false;
        }

        rootLogger.info("===============================================================");
        rootLogger.info("====================== updateRecords DONE =====================");
        rootLogger.info("===============================================================");

        return true;
    }

    public void updateTableRecords(String revision, String dbName, String tableName,
                                   HashMap<String, String> tableChangesMap)
            throws Exception {

        // TODO : if it's an ESP table, then, need to update the records in another way. change the mainttime.
        // get column list
        ResultSet columnRS            = DBHelper.getInstance().getColumnRSList(dbName, tableName);
        int       recordNumberUpdated = 0;
        String    sPrimaryKey         = DBHelper.getInstance().getPrimaryKey(dbName, tableName);
        String    sql                 = "select * from " + dbName + "." + tableName + " order by " + sPrimaryKey;
        ResultSet columnRecordsRS     = DBHelper.getInstance().executeSQL(sql);
        int       rowNumber           = DBHelper.getInstance().getRowNumber(columnRecordsRS);

        if (rowNumber == 0) {
            rootLogger.info("\n======================" + tableName + " is empty" + " ======================\n");

            return;
        }

        // get foreign key constrains
        ResultSet foreignKeysRS = DBHelper.getInstance().getForgenKeyRSList(dbName, tableName);

        while (columnRS.next()) {

            // go through each column
            String columnKey = columnRS.getString("COLUMN_KEY");

            if (columnKey.equals("PRI")) {

                // primary key. Bypass
                continue;
            }

            String columnName = columnRS.getString("COLUMN_NAME");

            if (inColumnFilterList(columnName)) {
                continue;
            }

            if (KDWHelper.isNaturalKey(revision, tableName, columnName)) {

                // bypass natural keys
                continue;
            }

            ///////////////////////////////////////////////////////////////////////////////////////
            // do not update the columns with foreign key constrains since it will failed without disable the constrains.
            // and also will involve compare logic to update it even we disable the constrains manually
            boolean isForeignKey = false;

            foreignKeysRS.first();

            while (foreignKeysRS.next()) {

                // go through each column
                String foreignKeyName = foreignKeysRS.getString("COLUMN_NAME");

                if (columnName.equalsIgnoreCase(foreignKeyName)) {
                    isForeignKey = true;

                    break;
                }
            }

            if (isForeignKey) {
                continue;
            }

            /////////////////////////////////////////////////////////////////////////////////////
            String columnType = columnRS.getString("DATA_TYPE");

            rootLogger.info("Table : " + tableName + " columnName : " + columnName);

            String originalValue = null;

            /*
             * In 8.0_250 KDW build, all data types are as below:
             *      char
             *       text
             *       varchar
             *
             *       bigint
             *       int
             *       tinyint
             *       smallint
             *       double
             *       decimal
             *
             *       date
             *       datetime
             *       timestamp
             */
            if (columnRecordsRS.next()) {
                if (columnType.contains("char") || columnType.contains("text")) {
                    originalValue = columnRecordsRS.getNString(columnName);

                    if (originalValue != null) {
                        if (originalValue.equals("T")) {
                            columnRecordsRS.updateString(columnName, "S");
                        } else {
                            columnRecordsRS.updateString(columnName, "T");
                        }
                    } else {
                        columnRecordsRS.updateString(columnName, "T");
                    }

                    columnRecordsRS.updateRow();
                } else if (columnType.contains("int")
                           || columnType.contains("double")
                           || columnType.contains("decimal")) {
                    double originalNumber = columnRecordsRS.getDouble(columnName);

                    originalValue = String.valueOf(originalNumber);

                    if (originalNumber == 2) {
                        columnRecordsRS.updateDouble(columnName, 1);
                    } else {
                        columnRecordsRS.updateDouble(columnName, (originalNumber / 2) + 1);
                    }

                    columnRecordsRS.updateRow();
                } else if (columnType.contains("time") || columnType.contains("date")) {
                    Date date;
                    Date originalDate = columnRecordsRS.getDate(columnName);

                    if (originalDate != null) {
                        date          = new Date(columnRecordsRS.getDate(columnName).getTime() + 86400000);
                        originalValue = originalDate.toString();
                    } else {
                        date = new Date(86400000);
                    }

                    columnRecordsRS.updateDate(columnName, date);
                    columnRecordsRS.updateRow();
                } else {
                    rootLogger.info("============ Unknown Types: " + columnType + " ============");
                }

                tableChangesMap.put(columnName, originalValue);
                recordNumberUpdated++;
            } else {

                // it means there is no enough records which can be changed
                // for each column
                rootLogger.info("\n============ Table : " + tableName
                                + " don't have enough records to update columns ============\n");

                break;
            }
        }

        columnRecordsRS.close();
        columnRS.close();
        foreignKeysRS.close();
        rootLogger.info("\n============|| " + recordNumberUpdated + " || records updated for table: " + tableName
                        + " ============\n");
    }
}
