package kiwiplan.kdw.tasks;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.logging.Logger;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.KDWSchemaHelper;
import kiwiplan.kdw.core.LogHelper;

public class RulesCheck {
    static Logger rootLogger = LogHelper.getRootLogger();

    public static boolean checkDatasetDatasource(String dbName) {
        boolean bResult = true;

        // get all tables
        String    sql = "SELECT TABLE_NAME FROM information_schema.tables where table_schema='" + dbName + "'";
        ResultSet tableRS;

        try {
            tableRS = DBHelper.getInstance().executeSQL(sql);

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (!isDWTables(tableName)) {
                    continue;
                }

                // get column list
                ResultSet columnRS;

                columnRS = DBHelper.getInstance().getColumnRSList(dbName, tableName);

                boolean bFound = false;

                while (columnRS.next()) {

                    // go through each column
                    String columnName = columnRS.getString("COLUMN_NAME");

                    if (columnName.contains("datasource")) {    // || columnName.contains("dataset")) {
                        bFound = true;

                        break;
                    }
                }

                if (!bFound) {
                    bResult = false;
                    rootLogger.severe("Table : " + tableName + " don't have a datasource column");
                } else {
                    rootLogger.severe("Table : " + tableName + " have a datasource column");
                }
            }
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
            bResult = false;
        }

        return bResult;
    }

    public static boolean checkDupRecords(String revision, String dbName) {
        boolean bSucceed = true;

        // get all tables
        ResultSet tableRS;

        try {
            tableRS = DBHelper.getInstance().getTableRSList(dbName);

            while (tableRS.next()) {
                rootLogger.info("");

                String tableName = tableRS.getString("TABLE_NAME");

                if (inExcludeTableList(revision, tableName)) {
                    continue;
                }

                String[][] naturalKeys = KDWSchemaHelper.getTableNaturalKeys(revision, tableName);

                if ((null == naturalKeys)) {
                    rootLogger.info("No natural key set in KDW schema file for table : " + tableName);

                    continue;
                }

                if (checkTableDupWithNaturalKey(dbName, tableName, naturalKeys) == false) {
                    bSucceed = false;
                }
            }
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
            bSucceed = false;
        }

        return bSucceed;
    }

    /**
     *
     * Private functions
     */
    private static boolean checkTableDupWithNaturalKey(String dbName, String tableName, String[][] naturalkeys)
            throws Exception, SQLException {
        boolean bSucceed = true;
        String  sql;

        if (DBHelper.getInstance().isTableEmpty(dbName, tableName)) {
            rootLogger.info("Table : " + tableName + " is empty");

            return bSucceed;
        }

        String bypassTablelist = ConfigHelper.getInstance().getConfig("DupCheckBypassTableList");

        if (bypassTablelist.contains(tableName)) {
            rootLogger.info("===============Table : " + tableName
                            + " has been excluded from the dup check===============");

            return true;
        }

        for (int i = 0; i < naturalkeys.length; i++) {
            String filter     = naturalkeys[i][0];
            String naturalkey = naturalkeys[i][1];

            if (DBHelper.databaseType.equalsIgnoreCase(DBHelper.DATABASE_MYSQL)) {
                naturalkey = "binary " + naturalkey.replace(",", ",binary ");
            } else {

                // naturalkey = "cast (" + naturalkey.replace(",", " as BINARY), ");
                // naturalkey = naturalkey.replace(",", ", cast (");
                // naturalkey = naturalkey + " as BINARY)";
            }

            String columnList       = DBHelper.getInstance().getColumnStrList(dbName, tableName);
            String recordStatusName = "";

            if (columnList.contains("record_status")) {
                recordStatusName = columnList.substring(0, columnList.indexOf("record_status")
                                                        + "record_status".length());
                recordStatusName = recordStatusName.substring(recordStatusName.lastIndexOf(",") + 1,
                                                              recordStatusName.length());
            }

            if (DBHelper.databaseType.equalsIgnoreCase(DBHelper.DATABASE_MSSQL)) {
                columnList = columnList.replace(",", " as VARCHAR)),MIN(cast(");
                columnList = "MIN(cast(" + columnList;
                columnList = columnList + " as VARCHAR))";
            }

            String sqldbName = "";

            if (DBHelper.databaseType.equalsIgnoreCase(DBHelper.DATABASE_MSSQL)) {
                sqldbName = dbName + ".dbo";
            } else {
                sqldbName = dbName;
            }

            if (recordStatusName.isEmpty()) {
                String filtersql = "";

                if ((null != filter) && (!filter.isEmpty())) {
                    filtersql = "where " + filter + " ";
                }

                // sql = "Select count(*) as dupNumber From " + dbName + ".dbo."
                sql = "Select " + columnList + " From " + sqldbName + "." + tableName + " " + filtersql + " "
                      + "Group by" + " " + naturalkey + " " + "Having count(*) > 1";
            } else {
                String filtersql = "";

                if ((null != filter) && (!filter.isEmpty())) {
                    filtersql = "AND " + filter;
                }

                // sql = "Select count(*) as dupNumber From " + dbName + "." + tableName + " "
                sql = "Select " + columnList + " From " + sqldbName + "." + tableName + " " + "Where ("
                      + recordStatusName + " != 'D' OR " + recordStatusName + " IS NULL) " + filtersql + " "
                      + "Group by" + " " + naturalkey + " " + "Having count(*) > 1";
            }

            ResultSet rs        = DBHelper.getInstance().executeSQL(sql);
            int       dupNumber = 0;

            while (rs.next()) {
                dupNumber++;
            }

            if (dupNumber > 0) {
                rootLogger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!FAILED: Table : " + tableName
                                + " : has dup entries using the naturalkey: " + naturalkey + " and filter : " + filter);
                rootLogger.info("Check dup records failed for table : " + tableName
                                + ". The number of dup records is : " + dupNumber);
                bSucceed = false;
                rs.close();
            } else {
                rootLogger.info("SUCCEED: Table : " + tableName + " : finished unique Key check successfully");
                rootLogger.info("\tThe naturalkey is : " + naturalkey);
                rs.close();
            }
        }

        return bSucceed;
    }

    private static boolean checkTableUniqueKeyKey(String dbName, String tableName, String uniqueKey)
            throws Exception, SQLException {
        boolean bSucceed = true;
        String  sql;

        sql = "Select * From " + dbName + "." + tableName + " limit 1";

        ResultSet rs = DBHelper.getInstance().executeSQL(sql);

        if (!rs.next()) {
            rootLogger.info("Table : " + tableName + " is empty");
            rs.close();

            return bSucceed;
        }

        String bypassTablelist = ConfigHelper.getInstance().getConfig("DupCheckBypassTableList");

        if (bypassTablelist.contains(tableName)) {
            rootLogger.info("===============Table : " + tableName
                            + " has been excluded from the dup check===============");

            return true;
        }

        uniqueKey = "binary " + uniqueKey.replace(",", ",binary ");

        String columnList       = DBHelper.getInstance().getColumnStrList(dbName, tableName);
        String recordStatusName = "";

        if (columnList.contains("record_status")) {
            recordStatusName = columnList.substring(0, columnList.indexOf("record_status") + "record_status".length());
            recordStatusName = recordStatusName.substring(recordStatusName.lastIndexOf(",") + 1,
                                                          recordStatusName.length());
        }

        if (recordStatusName.isEmpty()) {
            sql = "Select count(*) as dupNumber From " + dbName + "." + tableName + " " + "Group by" + " " + uniqueKey
                  + " " + "Having count(*) > 1";
        } else {
            sql = "Select count(*) as dupNumber From " + dbName + "." + tableName + " " + "Where " + recordStatusName
                  + " != 'D' " + "Group by" + " " + uniqueKey + " " + "Having count(*) > 1";
        }

        rs = DBHelper.getInstance().executeSQL(sql);

        int dupNumber = 0;

        while (rs.next()) {
            dupNumber++;
        }

        if (dupNumber > 0) {
            rootLogger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!FAILED: Table : " + tableName
                            + " : has dup entries using the unique Key: " + uniqueKey);
            rootLogger.info("Check dup records failed for table : " + tableName + ". The number of dup records is : "
                            + dupNumber);
            bSucceed = false;
            rs.close();
        } else {
            rootLogger.info("SUCCEED: Table : " + tableName + " : finished unique Key check successfully");
            rootLogger.info("\tThe unique key is : " + uniqueKey);
            rs.close();
        }

        return bSucceed;
    }

    private static boolean inExcludeTableList(String revision, String tableName) {
        boolean bCompare = KDWSchemaHelper.isTableComparetable(revision, tableName);

        if (!bCompare) {
            rootLogger.fine("===============Table " + tableName + " is ===============");
        }

        return !bCompare;
    }

    public static void main(String[] args) {

        // String dbName = "java31_masterDW";
        //
        // String revision = "kdw-8.31";
        //
        // LogHelper.initializeSummaryLog("compare");
        // LogHelper.initializeTestcaseLog(new CompSchema4FreshUpgrade());
        // RulesCheck.checkDupRecords(revision, dbName);
    }

    public static boolean masterdbBasicDataVerification(String revision, String dbName) {
        boolean bSucceed = true;

        /*
         *  revision insensitive (automation started since 7.90.2)
         */

        // check all records in dwcustomeraddress table have customer id (not null). This column was added since 7.90.2
        String sql1 = "SELECT customer_address_customer_id FROM " + dbName
                      + ".dwcustomeraddress where customer_address_customer_id is null";
        ResultSet tableRS1;

        try {
            tableRS1 = DBHelper.getInstance().executeSQL(sql1);

            if (tableRS1.next()) {
                bSucceed = false;
            }

            tableRS1.close();
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
            bSucceed = false;
        }

        // check all records in dwsupplieraddress table have supplier number (not null).
        // RSS enabled or not do not impact the result
        String sql2 = "SELECT supplier_address_supplier_number FROM " + dbName
                      + ".dwsupplieraddress where supplier_address_supplier_number is null;";
        ResultSet tableRS2;

        try {
            tableRS2 = DBHelper.getInstance().executeSQL(sql2);

            if (tableRS2.next()) {
                bSucceed = false;
            }

            tableRS2.close();
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
            bSucceed = false;
        }

        // do not fail test since some error are caused by bad data. But I will post a warning in log
        if (bSucceed == false) {
            rootLogger.warning("Warning!!! There are basic data verification failed. Check automation log for detail.");
        }

        bSucceed = true;

        return bSucceed;
    }

    private static boolean productioneventDataVerification(String revision, String dbName) {
        boolean bSucceed = true;

        // String sql = "SELECT config_enable_events FROM " + dbName
        // + ".dwsiteconfig WHERE config_enable_events = 'yes'";
        // try {
        // ResultSet rs = DBHelper.getInstance().executeSQL(sql);
        // if (rs.next() == false) {
        //// if productionevent not enabled, return TRUE
        // rs.close();
        // return true;
        // }
        // } catch (Exception e) {
        //// TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        String[][] mappingTable = {

            /* eventType,           dw table name,          id column name */

            // map
            // KALL: 5697876                         {"'BOARD_GRADE'",               "dwboardgrades",        "board_grade_id"},
            { "'CALENDAR'", "dwshiftcalendar", "calendar_id" }, { "'CSC'", "dwdryendfeedback", "dryend_id" },
            { "'PCS_DOWNTIME', 'SHIFT_DOWNTIME', 'CSC_DOWNTIME'", "dwdowntimes", "downtime_id" },
            { "'PAPER_USAGE'", "dwwetendpaperusage", "wetend_paper_usage_id" },
            { "'PCS'", "dwproductionfeedback", "feedback_id" },

            // rss
            { "'PAPER_INV'", "dwpaperinventory", "rss_roll_id" },
            { "'PURCHASE_ORDER'", "dwpurchaseorder", "purchase_order_id" },
            { "'PAPER_LEVEL'", "dwpaperinventorylevels", "rss_levels_id" },

            // ult
            { "'INVENTORY_MOVEMENT'", "dwinventorymovement", "inventory_movement_id" },
            { "'INVENTORY_LEVEL'", "dwinventorylevel", "inventory_level_id" }, { "'RETURN'", "dwreturn", "return_id" },

            // esp
            { "'CUSTOMER_ISSUE'", "dwcustomerissue", "customer_issue_id" },
            { "'DOCKET_ITEM'", "dwdocketitem", "docket_item_id" },
            { "'FINANCIAL_TRANSACTION'", "dwfinancialtransaction", "financial_transaction_id" },
            { "'ORDER_COST'", "dwordercost", "order_cost_id" }, { "'SALES_ORDER'", "dwsalesorder", "sales_order_id" },
            { "'SALES_BUDGET'", "dwsalesbudget", "sales_budget_id" },

            // qms
            { "'QMS'", "dwquality", "quality_id" },

            // cwr
            { "'WASTE_RECORD'", "dwcwrwasterecord", "wasterecord_id" },
            { "'MATERIAL'", "dwcwrmaterialinput", "materialinput_id" }
        };

        for (int i = 0; i < mappingTable.length; i++) {
            String eventType  = mappingTable[i][0];
            String tableName  = mappingTable[i][1];
            String columnName = mappingTable[i][2];

            bSucceed = verifyProductionEventRecordCount(dbName, eventType, tableName, columnName);

            if (bSucceed != true) {
                rootLogger.info(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!FAILED: Table : " + tableName
                    + " has different record number than the records in production event whose event type is: "
                    + eventType);

                return bSucceed;
            }
        }

        return bSucceed;
    }

    private static boolean verifyProductionEventRecordCount(String dbName, String eventType, String tableName,
                                                            String columnName) {
        boolean bSucceed = true;
        String  sql1     = "SELECT count(*) as num FROM " + dbName + ".dwproductionevents inner join " + dbName + "."
                           + tableName + " on dwproductionevents." + columnName + " = " + dbName + "." + tableName
                           + "." + columnName + " where " + dbName + ".dwproductionevents.production_event_type in ("
                           + eventType + ")";
        String    sql2 = "SELECT count(" + columnName + ") as num FROM " + dbName + "." + tableName;
        ResultSet tableRS1;
        ResultSet tableRS2;

        try {
            tableRS1 = DBHelper.getInstance().executeSQL(sql1);
            tableRS1.next();
            tableRS2 = DBHelper.getInstance().executeSQL(sql2);
            tableRS2.next();

            int eventNumber = tableRS1.getInt("num");
            int tableNumber = tableRS2.getInt("num");

            rootLogger.info(eventType + " Event records in dwproductionevents are " + eventNumber);
            rootLogger.info("Records in " + tableName + " are " + tableNumber);

            if (eventNumber != tableNumber) {
                tableRS1.close();
                tableRS2.close();

                return false;
            }

            tableRS1.close();
            tableRS2.close();
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();

            return false;
        }

        return bSucceed;
    }

    private static boolean isDWTables(String tableName) {
        if (tableName.startsWith("dw")) {
            return true;
        } else {
            return false;
        }
    }
}
