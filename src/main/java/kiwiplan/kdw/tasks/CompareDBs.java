package kiwiplan.kdw.tasks;

import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.KDWSchemaHelper;
import kiwiplan.kdw.core.LogHelper;

public abstract class CompareDBs {
    protected static String tableFilterList        = "";
    protected static String columnFilterList       = "";
    protected static String globalColumnFilterList = ConfigHelper.getInstance()
                                                                 .getConfig("KDWDB.globalColumnFilterList");

    // the compareDB class is designed to compare two dbs in same revision.
    // compareSameColMasterDBs can be used to compare two revisions, while, it need be optimized.
    static Logger             rootLogger = LogHelper.getRootLogger();
    private static CompareDBs compareDB;
    private Logger            logger;

    public abstract boolean compareMasterDBs(String revision, String dbName1, String dbName2);

    public abstract boolean compareNormalDBsTableWithSameSchemaWithRoundAndStarconvet(String dbName1, String dbName2,
                                                                                      String tableName,
                                                                                      List<String> naturalkeys,
                                                                                      Map<String, String> rounding,
                                                                                      Map<String, String> starlen);

    public abstract boolean compareNormalDBsWithSameSchema(String dbName1, String dbName2);

    /*
     * Only compare the columns with same column name
     */
    public abstract boolean compareSameColMasterDBs(String oldrevision, String newrevision, String dbName1,
                                                    String dbName2);

    // this function only compare columns with same column name in two dbs.
    public abstract boolean compareSameColTableWithNKey(String oldrevision, String newrevision, String dbName1,
                                                        String dbName2, String tableName,
                                                        List<String> idTrustTableList);

    public abstract boolean compareSameColTableWithoutNKey(String oldrevision, String newrevision, String dbName1,
                                                           String dbName2, String tableName, boolean excludeID);

    // this function only compare two dbs in one revision
    public abstract boolean compareTableWithNKey(String revision, String dbName1, String dbName2, String tableName,
                                                 List<String> idTrustTableList);

    // this function only compare two dbs in one revision
    public abstract boolean compareTableWithNKeyCRC(String revision, String dbName1, String dbName2, String tableName,
                                                    List<String> idTrustTableList);

    public abstract boolean compareTableWithoutNKey(String dbName1, String dbName2, String tableName,
                                                    boolean excludeID);

    protected abstract void fillInfo4KeyReplacement(List<String> tableNameList, List<String> srcColNameList,
                                                    List<String> foreignColNameList, List<String> naturalkeyList,
                                                    String revision, String dbName, String tableName,
                                                    String naturalkey, List<String> idTrustTableList);

    public abstract void fillnaturalkeyReplacementInfo(String revision, String dbName, String tableName,
                                                       List<String> idTrustTableList, List<String> tableNameList,
                                                       List<String> srcColNameList, List<String> foreignColNameList,
                                                       List<String> naturalkeyList);

    protected boolean inColumnFilterList(String columnName) {
        boolean inList = false;

        // bypass the record last changed column
        if (columnName.contains("record_last_changed")) {
            inList = true;
        }

        if (columnName.contains("record_status")) {
            inList = true;
        }

        if (columnName.contains("record_created")) {
            inList = true;
        }

        if (columnName.contains("rss_roll_site_run_id")) {
            inList = true;
        }

        if (getColumnFilterList().contains(columnName)) {
            inList = true;
        }

        if (inList) {
            rootLogger.fine("===============Column " + columnName + " is excluded from comparation===============");
        }

        return inList;
    }

    protected boolean inExcludeTableList(String revision, String tableName) {
        boolean inList   = false;
        boolean bCompare = KDWSchemaHelper.isTableComparetable(revision, tableName);

        if (bCompare == false) {
            inList = true;
        }

        if (getTableFilterList().contains(tableName)) {
            inList = true;
        }

        if (inList) {
            rootLogger.fine("===============Table " + tableName + " is excluded from comparation===============");
        }

        return inList;
    }

    protected void printnaturalkeyReplacement(List<String> tableNameList, List<String> srcColNameList,
                                              List<String> foreignColNameList, List<String> naturalkeyList) {

//      rootLogger.info("==========Print Natural Key Replacement==========");
        for (int index = 0; index < tableNameList.size(); index++) {
            rootLogger.fine("Natural Key Item: " + tableNameList.get(index) + " " + srcColNameList.get(index) + " "
                            + foreignColNameList.get(index) + " " + naturalkeyList.get(index));
        }

//      rootLogger.info("==========Print Natural Key Replacement END==========");
    }

    protected boolean isColInList(List<String> columnList, String columnName) {
        Iterator<String> colsIter = columnList.iterator();

        while (colsIter.hasNext()) {
            String tempName = colsIter.next();

            if (tempName.equalsIgnoreCase(columnName)) {
                return true;
            }
        }

        return false;
    }

    public static String getColumnFilterList() {
        return columnFilterList + " " + globalColumnFilterList;
    }

    public static void setColumnFilterList(String columnFilterList) {
        rootLogger.info("columnFilterList set as : " + columnFilterList);
        CompareDBs.columnFilterList = columnFilterList;
    }

    protected abstract boolean getDifferentColCompareNormalDBsWithSameSchema(String dbName1, String dbName2,
                                                                             List<String> differentColumnList,
                                                                             Map<String, String> starlenMap);

    public static synchronized CompareDBs getInstance() {
        if (compareDB == null) {
            switch (DBHelper.databaseType) {
            case DBHelper.DATABASE_MSSQL :
                compareDB = new MSCompareDBs();

                break;

            case DBHelper.DATABASE_MYSQL :
                compareDB = new MysqlCompareDBs();

                break;
            }
        }

        return compareDB;
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public static String getTableFilterList() {
        return tableFilterList;
    }

    public static void setTableFilterList(String tableFilterList) {
        rootLogger.info("TableFilterList set as : " + tableFilterList);
        CompareDBs.tableFilterList = tableFilterList;
    }
}
