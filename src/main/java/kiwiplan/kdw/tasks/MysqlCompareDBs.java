package kiwiplan.kdw.tasks;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.util.*;
import java.util.logging.Level;

import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.KDWSchemaHelper;
import kiwiplan.kdw.core.LogHelper;
import kiwiplan.kdw.utils.OSUtil;

public class MysqlCompareDBs extends CompareDBs {
    public boolean compareMasterDBs(String revision, String dbName1, String dbName2) {
        getLogger().info("=========================================================================");
        getLogger().info("Comparing DBs with revision: " + revision);
        getLogger().info("    " + dbName1 + "\t" + DBHelper.getInstance().getDBVersions(dbName1));
        getLogger().info("    " + dbName2 + "\t" + DBHelper.getInstance().getDBVersions(dbName2));
        getLogger().info("=========================================================================");

        // get table list
        String       sql              = "SELECT * FROM inf" + "ormation_schema.tables where table_schema='" + dbName1
                                        + "'";
        List<String> idTrustTableList = new ArrayList<String>();
        Set<String>  dbTableList      = new HashSet<>();

        try {
            ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (inExcludeTableList(revision, tableName)) {
                    continue;
                }

                dbTableList.add(tableName);
            }

            tableRS.close();

            // first, try sum of crc with id
            getLogger().info(
                "\n\n++++++++++++++ First Pass: do not exclude ID and do not replace natural key ++++++++++++++\n\n");

            for (String tableName : dbTableList) {

                // compare each tables result without id excluded and without
                // natural key replaced
                // the idTrustTableList will gives a list of table which we can
                // trust the ids which means do not to replace the id with
                // naturalkey
                boolean bResult = compareTableWithoutNKey(dbName1, dbName2, tableName, false);

                if (bResult) {
                    idTrustTableList.add(tableName);
                    getLogger().info("Passed Table: " + tableName);
                } else {
                    getLogger().info("Failed Table: " + tableName);
                }
            }

            // second, try sum of crc without id, only for failed table list
            getLogger().info(
                "\n\n+++++++++++++++++++ Second Pass: exclude ID but do not replace natural key +++++++++++++++++++\n\n");

//          int          failedTableNum  = idfailedTableList.size();
//          List<String> failedTableList = new Vector<String>();
            for (String tableName : dbTableList) {
                if (idTrustTableList.contains(tableName)) {
                    continue;
                }

                // compare each tables ETL result with id excluded but without
                // natural key replaced
                boolean bResult = compareTableWithoutNKey(dbName1, dbName2, tableName, true);

                // the comparison of some tables may pass if exclude the
                // auto incremental id
                if (bResult) {
                    getLogger().info("Passed Table: " + tableName);
                    idTrustTableList.add(tableName);
                } else {
                    getLogger().info("Failed Table: " + tableName);
                }
            }

            // third, replace foreign key, try sum of crc
            getLogger().info(
                "\n\n+++++++++++++++++++++ Third Pass: exclude ID and replace natural key +++++++++++++++++++++\n\n");

            for (String tableName : dbTableList) {
                if (idTrustTableList.contains(tableName)) {
                    continue;
                }

                // compare each tables result with id excluded and with natural
                // key replaced
                // bResult = compareTableWithNKeyCRC(revision, dbName1, dbName2,
                // tableName, idTrustTableList);
                boolean bResult = compareTableWithNKey(revision, dbName1, dbName2, tableName, idTrustTableList);

                if (bResult) {
                    getLogger().info("Passed Table: " + tableName);
                    idTrustTableList.add(tableName);
                } else {
                    getLogger().info("Failed Table: " + tableName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (String tableName : dbTableList) {
            if (!idTrustTableList.contains(tableName)) {
                getLogger().severe("Failed Table: " + tableName);
            }
        }

        return dbTableList.size() == idTrustTableList.size();
    }

    public boolean compareNormalDBsTableWithSameSchemaWithRoundAndStarconvet(String dbName1, String dbName2,
                                                                             String tableName,
                                                                             List<String> naturalkeys,
                                                                             Map<String, String> rounding,
                                                                             Map<String, String> starlen) {

        // the final failed column list
        List<String> failedColumnList = new Vector<String>();

        // use old way ( without unique key ) to get which columns are different
        List<String>     differentColumnList = new Vector<String>();
        boolean          bResult             = getDifferentColCompareNormalDBsWithSameSchema(dbName1,
                                                                                             dbName2,
                                                                                             differentColumnList,
                                                                                             starlen);
        String           naturalkeysJoinStr  = "";
        Iterator<String> naturalkeyIter      = naturalkeys.iterator();

        while (naturalkeyIter.hasNext()) {
            String tempColName = naturalkeyIter.next();

            naturalkeysJoinStr += "t1." + tempColName + " = t2." + tempColName + " AND ";
        }

        // remove the last AND
        naturalkeysJoinStr = naturalkeysJoinStr.substring(0, naturalkeysJoinStr.length() - 4);

        if (bResult) {
            return true;
        } else {
            Iterator<String> colsIter = differentColumnList.iterator();

            while (colsIter.hasNext()) {
                String columnName = colsIter.next();

                /**
                 * SELECT * from db1.tablename
                 * join on db2.tablename
                 * where db1tolerantcol - db2tolerantcol > tolerance
                 */
                String finalSQL  = "";
                String selectSQL = "SELECT ";
                String fromSQL   = " FROM " + dbName1 + "." + tableName + " AS t1 ";
                String joinSQL   = " JOIN ";
                String whereSQL  = " WHERE ";

                if (isColInList(naturalkeys, columnName)) {
                    getLogger().severe("Column " + columnName + " is different and is part of unique. ");

                    return false;
                }

                String roundFact = rounding.get(columnName);

                if (roundFact != null) {
                    if (roundFact.equalsIgnoreCase("0")) {

                        // if rounding is no, the fail is fail
                        failedColumnList.add(columnName);
                    } else {

                        // if rounding allowed, there is still a chance
                        // TODO: delete with starlen here
                        selectSQL += "t1." + columnName + " - " + "t2." + columnName + " AS " + columnName + "diff";
                        joinSQL   += dbName2 + "." + tableName + " AS t2 ON " + naturalkeysJoinStr;
                        whereSQL  += "ABS(t1." + columnName + " - " + "t2." + columnName + ") > " + roundFact;
                        finalSQL  = selectSQL + " " + fromSQL + " " + joinSQL + " " + whereSQL;

                        ResultSet rs;

                        try {
                            rs = DBHelper.getInstance().executeSQL(finalSQL);

                            if (rs.next()) {
                                failedColumnList.add(columnName);
                                getLogger().info(DBHelper.getInstance().printResultSet(rs, 10));
                            }
                        } catch (Exception e) {

                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
            }

            Iterator<String> failedColsIter    = failedColumnList.iterator();
            String           failedColsListStr = "";

            while (failedColsIter.hasNext()) {
                failedColsListStr += failedColsIter.next() + " ";
            }

            if (!failedColsListStr.isEmpty()) {
                getLogger().severe("Table " + tableName + " : difference columns (after tolerance for rounding) are : "
                                   + failedColsListStr);

                return false;
            }

            return true;
        }
    }

    public boolean compareNormalDBsWithSameSchema(String dbName1, String dbName2) {
        boolean bFinalResult = true;

        List<String> db1VersionList = DBHelper.getInstance().getDBVersions(dbName1);
        List<String> db2VersionList = DBHelper.getInstance().getDBVersions(dbName2);

        getLogger().info("========================================================================");
        getLogger().info("Compare Normal DBs with Same Schema: ");
        getLogger().info("    " + dbName1 + ", " + ((db1VersionList.size() == 0) ? db1VersionList.toString(): ""));
        getLogger().info("    " + dbName2 + ", " + ((db2VersionList.size() == 0) ? db2VersionList.toString(): ""));
        getLogger().info("========================================================================");

        // get table list
        String sql = "SELECT * FROM information_schema.tables where table_schema='" + dbName1 + "'";

        try {
            ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);

            // first, try sum of crc with id
            getLogger().info("\n\n++++++++++++++ Compare table from two databases using sum of crc ++++++++++++++\n\n");

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (getTableFilterList().contains(tableName)) {
                    continue;
                }

                // compare each tables result without id excluded and without
                // natural key replaced
                // the idTrustTableList will gives a list of table which we can
                // trust the ids which means do not to replace the id with
                // naturalkey
                if (!compareTableWithoutNKey(dbName1, dbName2, tableName, false)) {
                    bFinalResult = false;
                    rootLogger.info("============================================ Difference=======");

                    long db1RecordNumber = KDWHelper.getRecordNumber(dbName1, tableName);
                    long db2RecordNumber = KDWHelper.getRecordNumber(dbName2, tableName);

                    getLogger().severe("DB table " + dbName1 + "." + tableName + " total records number: "
                                       + db1RecordNumber);
                    getLogger().severe("DB table " + dbName2 + "." + tableName + " total records number: "
                                       + db2RecordNumber);

                    if (db1RecordNumber != db2RecordNumber) {
                        rootLogger.info("Record Number of Table : " + tableName + " is different. " + dbName1 + ":"
                                        + db1RecordNumber + " " + dbName2 + ":" + db2RecordNumber);
                    }

                    rootLogger.info("total difference: ");

                    // use sum(crc32(machine_number)) to get the sum value for each column and compare the values to find columns with difference
                    // use the columns in group by clause
                    // TODO: Get all columns
                    String compareSqlDiffColSelect1 = "Select ";
                    String compareSqlDiffColSelect2 = "Select ";

                    ///////////////////////////
                    // get column list
                    String sqlCol = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1
                                    + "' AND " + " TABLE_NAME='" + tableName + "'";
                    ResultSet columnRS1 = DBHelper.getInstance().executeSQL(sqlCol);

                    while (columnRS1.next()) {
                        String columnName = columnRS1.getString("COLUMN_NAME");

                        compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(crc32(" + columnName + ")),";
                        compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(crc32(" + columnName + ")),";
                    }

                    // remove the last ","
                    compareSqlDiffColSelect1 =
                        compareSqlDiffColSelect1.substring(0, compareSqlDiffColSelect1.lastIndexOf(","));
                    compareSqlDiffColSelect2 =
                        compareSqlDiffColSelect2.substring(0, compareSqlDiffColSelect2.lastIndexOf(","));

                    String            sumSql1        = compareSqlDiffColSelect1 + " FROM " + dbName1 + "." + tableName
                                                       + " ";
                    String            sumSql2        = compareSqlDiffColSelect2 + " FROM " + dbName2 + "." + tableName
                                                       + " ";
                    ResultSet         sumColumnsRS1  = DBHelper.getInstance().executeSQL(sumSql1);
                    ResultSet         sumColumnsRS2  = DBHelper.getInstance().executeSQL(sumSql2);
                    String            columnDiffList = "";
                    ResultSetMetaData rsmd           = sumColumnsRS1.getMetaData();
                    int               columnTotal    = rsmd.getColumnCount();

                    sumColumnsRS1.next();
                    sumColumnsRS2.next();

                    for (int columnIndex = 1; columnIndex <= columnTotal; columnIndex++) {
                        double tempColValue1 = sumColumnsRS1.getDouble(columnIndex);
                        double tempColValue2 = sumColumnsRS2.getDouble(columnIndex);

                        if (tempColValue1 != tempColValue2) {
                            String colName = rsmd.getColumnName(columnIndex);

                            colName        = colName.substring("sum(crc32(".length(), colName.length() - 2);
                            columnDiffList = columnDiffList + " " + colName;
                        }
                    }

                    getLogger().severe("Table : " + tableName + " different column list are : ");
                    getLogger().severe("    " + columnDiffList);
                    getLogger().severe("============================================ Difference End=======");
                }
            }

            tableRS.close();
        } catch (Exception e) {
            e.printStackTrace();
            bFinalResult = false;
        }

        return bFinalResult;
    }

    /*
     * Only compare the column values with same column name
     */
    public boolean compareSameColMasterDBs(String oldrevision, String newrevision, String dbName1, String dbName2) {
        boolean bResult = true;

        getLogger().info("====================================================================");
        getLogger().info("Comparing DB table values with the same column name: ");
        getLogger().info("    " + dbName1 + ", " + DBHelper.getInstance().getDBVersions(dbName1));
        getLogger().info("    " + dbName2 + ", " + DBHelper.getInstance().getDBVersions(dbName2));
        getLogger().info("====================================================================");

        // get table list
        String       sql               = "SELECT * FROM information_schema.tables where table_schema='" + dbName1 + "'";
        List<String> idTrustTableList  = new Vector<String>();
        List<String> idfailedTableList = new Vector<String>();

        try {
            ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);

            // first, try sum of crc with id
            getLogger().info(
                "\n\n++++++++++++++ First Pass: do not exclude ID and do not replace natural key ++++++++++++++\n\n");

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (inExcludeTableList(oldrevision, tableName)) {
                    continue;
                }

                // compare each tables result without id excluded and without
                // natural key replaced
                // the idTrustTableList will gives a list of table which we can
                // trust the ids which means do not to replace the id with
                // naturalkey
                bResult = compareSameColTableWithoutNKey(oldrevision, newrevision, dbName1, dbName2, tableName, false);

                if (bResult) {
                    idTrustTableList.add(tableName);
                } else {
                    idfailedTableList.add(tableName);
                }
            }

            tableRS.close();

            // second, try sum of crc without id, only for failed table list
            getLogger().info(
                "\n\n+++++++++++++++++++ Second Pass: exclude ID but do not replace natural key +++++++++++++++++++\n\n");

            int          failedTableNum  = idfailedTableList.size();
            List<String> failedTableList = new Vector<String>();

            for (int tableIndex = 0; tableIndex < failedTableNum; tableIndex++) {
                String tableName = idfailedTableList.get(tableIndex);

                // compare each tables ETL result with id excluded but without
                // natural key replaced
                bResult = compareSameColTableWithoutNKey(oldrevision, newrevision, dbName1, dbName2, tableName, true);

                if (!bResult) {

                    // the comparison of some tables may pass if exclude the
                    // auto incremental id
                    failedTableList.add(tableName);
                }
            }

            // third, replace foreign key, try sum of crc
            getLogger().info(
                "\n\n+++++++++++++++++++++ Third Pass: exclude ID and replace natural key +++++++++++++++++++++\n\n");

            for (int tableIndex = 0; tableIndex < failedTableList.size(); tableIndex++) {
                String tableName = failedTableList.get(tableIndex);

                // compare each tables result with id excluded and with natural
                // key replaced
                // bResult = compareTableWithNKeyCRC(revision, dbName1, dbName2,
                // tableName, idTrustTableList);
                bResult = compareSameColTableWithNKey(oldrevision,
                                                      newrevision,
                                                      dbName1,
                                                      dbName2,
                                                      tableName,
                                                      idTrustTableList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            bResult = false;
        }

        return bResult;
    }

    // this function only compare columns with same column name in two dbs.
    public boolean compareSameColTableWithNKey(String oldrevision, String newrevision, String dbName1, String dbName2,
                                               String tableName, List<String> idTrustTableList) {
        List<String> tableNameList      = new ArrayList<>();
        List<String> baseColNameList    = new ArrayList<>();
        List<String> foreignColNameList = new ArrayList<>();
        List<String> naturalkeyList     = new ArrayList<>();

        fillnaturalkeyReplacementInfo(oldrevision,
                                      dbName1,
                                      tableName,
                                      idTrustTableList,
                                      tableNameList,
                                      baseColNameList,
                                      foreignColNameList,
                                      naturalkeyList);

        // replacing natural key. get foreign key constrain below
        // Use java11Test0
        // SELECT *
        // FROM
        // information_schema.KEY_COLUMN_USAGE
        // WHERE TABLE_SCHEMA='java11Test0' AND TABLE_NAME='dwwetendpaperusage'
        // AND referenced_table_name IS NOT NULL;
        // if idTrustTableList is not null, it means will replace naturalkey for
        // ids. And the id of the tables in idTrustTableList do not need be
        // replaced
        boolean bCompareResult = true;

        if (DBHelper.getInstance().isTableEmpty(dbName1, tableName)
                && DBHelper.getInstance().isTableEmpty(dbName2, tableName)) {
            getLogger().info("==========Empty: Table " + tableName + " is empty");

            return bCompareResult;
        }

        String compareSqlWhole = "";

        // this is table alias, to add column prefix like: s0.xxx. and add
        // prefix during join.
        // base table is always: s
        String baseTableAlias    = "s";
        String db1baseTableAlias = "db1s";
        String db2baseTableAlias = "db2s";

        // get column list
        String sql1 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";
        String sql2 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName2 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";

        try {
            ResultSet columnRS1 = DBHelper.getInstance().executeSQL(sql1);
            ResultSet columnRS2 = DBHelper.getInstance().executeSQL(sql2);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select s.COL1, s.COL2, s.COL3 ...,
             * s1.COL1, s2.COL1 FROM dbName1.Table s left join dbName1.Table1 s1
             * on s.xxx = s1.xxx_xxx .... union all select s.COL1, s.COL2,
             * s.COL3 ..., s1.COL1, s2.COL1 FROM dbName2.Table s left join
             * dbName2.Table1 s1 on s.xxx = s1.xxx_xxx .... ) tmp group by col1,
             * col2, ...
             */
            String compareSqlUnionSelect = "SELECT *";
            String compareSql1           = "";
            String compareSql2           = "";
            String joinColSqls1          = "";
            String joinTableSqls1        = "";
            String joinColSqls2          = "";
            String joinTableSqls2        = "";
            String compareSqlTail        = "";
            String compareSqlGroupBy     = "";
            String compareSqlCondition   = "HAVING COUNT(*) = 1";
            String compareSqlOrderBy     = "";
            int    priKeyIndex           = 0;

            // get column list for RS1
            String rs1ColumnList = "";

            while (columnRS1.next()) {
                String columnName = columnRS1.getString("COLUMN_NAME");

                rs1ColumnList = columnName + "," + rs1ColumnList;
            }

            while (columnRS2.next()) {
                StringTokenizer rs1StrTocken = new StringTokenizer(rs1ColumnList, ",");

                // go through each column
                String columnName = columnRS2.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS2.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    continue;
                }

                // if RS2 has same column name as the one in RS1
                boolean bFound = false;

                while (rs1StrTocken.hasMoreTokens()) {
                    String rs1ColTocken = rs1StrTocken.nextToken();

                    if (rs1ColTocken.equalsIgnoreCase(columnName)) {
                        bFound = true;

                        break;
                    }
                }

                if (bFound == false) {
                    continue;
                }

                if (compareSql1.isEmpty()) {
                    compareSql1 = "SELECT '" + dbName1 + "' as dbname, " + db1baseTableAlias + "." + columnName;
                    compareSql2 = "SELECT '" + dbName2 + "' as dbname, " + db2baseTableAlias + "." + columnName;
                } else {
                    compareSql1 = compareSql1 + ", " + db1baseTableAlias + "." + columnName;
                    compareSql2 = compareSql2 + ", " + db2baseTableAlias + "." + columnName;
                }

                boolean inReplacedColList = false;

                for (int baseCol = 0; baseCol < baseColNameList.size(); baseCol++) {
                    if (columnName.equalsIgnoreCase(baseColNameList.get(baseCol))) {
                        inReplacedColList = true;

                        break;
                    }
                }

                if (inReplacedColList != true) {
                    if (compareSqlGroupBy.isEmpty()) {
                        compareSqlGroupBy = "GROUP BY " + columnName;
                        compareSqlOrderBy = "ORDER BY " + columnName;
                    } else {
                        compareSqlGroupBy = compareSqlGroupBy + ", " + columnName;
                        compareSqlOrderBy = compareSqlOrderBy + ", " + columnName;
                    }
                }
            }

            priKeyIndex = 0;

            /*
             * while (columnRS2.next()) { // go through each column String
             * columnName2 = columnRS2.getString("COLUMN_NAME");
             *
             * // bypass the record last changed column if
             * (columnName2.contains("record_last_changed")) { continue; }
             *
             * // primary key String columnKey =
             * columnRS2.getString("COLUMN_KEY"); if (columnKey.equals("PRI") &&
             * (priKeyIndex == 0)) { priKeyIndex++; continue; }
             *
             * compareSql2 = compareSql2 + ", " + baseTableAlias + "." +
             * columnName2; }
             */
            int    foreignTableAliasIndex = 0;
            String foreignTableAlias1     = "";
            String foreignTableAlias2     = "";
            String foreignColAliasPre     = "";

            // join tables
            for (int index = 0; index < tableNameList.size(); index++) {
                foreignTableAliasIndex++;

                // the table alias to be join are like: s1, s2, etc
                foreignTableAlias1 = db1baseTableAlias + foreignTableAliasIndex;
                foreignTableAlias2 = db2baseTableAlias + foreignTableAliasIndex;
                foreignColAliasPre = baseTableAlias + foreignTableAliasIndex;

                String joinTableName  = tableNameList.get(index);
                String joinBaseCol    = baseColNameList.get(index);
                String joinForeignCol = foreignColNameList.get(index);
                String naturalkeys    = naturalkeyList.get(index);

                joinTableSqls1 = " " + joinTableSqls1 + " LEFT JOIN " + dbName1 + "." + joinTableName + " "
                                 + foreignTableAlias1 + " ON " + foreignTableAlias1 + "." + joinForeignCol + " = "
                                 + db1baseTableAlias + "." + joinBaseCol;
                joinTableSqls2 = " " + joinTableSqls2 + " LEFT JOIN " + dbName2 + "." + joinTableName + " "
                                 + foreignTableAlias2 + " ON " + foreignTableAlias2 + "." + joinForeignCol + " = "
                                 + db2baseTableAlias + "." + joinBaseCol;

                StringTokenizer stnaturalkeys = new StringTokenizer(naturalkeys, " ,\t\n");

                // get same column name for joinTable
                String sameColumnList = "";

                // get column list
                String joinsql1 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1 + "' AND "
                                  + " TABLE_NAME='" + joinTableName + "'";
                String joinsql2 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName2 + "' AND "
                                  + " TABLE_NAME='" + joinTableName + "'";
                ResultSet joincolumnRS1 = DBHelper.getInstance().executeSQL(joinsql1);
                ResultSet joincolumnRS2 = DBHelper.getInstance().executeSQL(joinsql2);

                // get column list for RS1
                String rs1joinColumnList = "";

                while (joincolumnRS1.next()) {
                    String columnName = joincolumnRS1.getString("COLUMN_NAME");

                    rs1joinColumnList = columnName + "," + rs1joinColumnList;
                }

                while (joincolumnRS2.next()) {
                    StringTokenizer rs1joinStrTocken = new StringTokenizer(rs1joinColumnList, ",");

                    // go through each column
                    String joincolumnName = joincolumnRS2.getString("COLUMN_NAME");

                    // if RS2 has same column name as the one in RS1
                    while (rs1joinStrTocken.hasMoreTokens()) {
                        String rs1joinColTocken = rs1joinStrTocken.nextToken();

                        if (rs1joinColTocken.equalsIgnoreCase(joincolumnName)) {
                            if (sameColumnList.isEmpty()) {
                                sameColumnList = rs1joinColTocken;
                            } else {
                                sameColumnList = sameColumnList + "," + rs1joinColTocken;
                            }

                            break;
                        }
                    }
                }

                while (stnaturalkeys.hasMoreTokens()) {

                    // here, two db tables using same alias. need to check if
                    // it's ok or not
                    String naturalkeyCol = stnaturalkeys.nextToken().trim();

                    if (inColumnFilterList(naturalkeyCol)) {
                        continue;
                    }

                    boolean bFound = false;

                    getLogger().info("sameColumnList is : " + sameColumnList);
                    getLogger().info("unique Key column Name is : " + naturalkeyCol);

                    // check if the natural key in the current column list with same name
                    StringTokenizer sameColumnToken = new StringTokenizer(sameColumnList, ",");

                    while (sameColumnToken.hasMoreTokens()) {
                        String tempCol = sameColumnToken.nextToken().trim();

                        if (tempCol.equalsIgnoreCase(naturalkeyCol)) {
                            bFound = true;

                            break;
                        }
                    }

                    if (bFound == false) {
                        continue;
                    }

                    joinColSqls1 = joinColSqls1 + ", " + foreignTableAlias1 + "." + naturalkeyCol + " AS "
                                   + foreignColAliasPre + naturalkeyCol;
                    joinColSqls2 = joinColSqls2 + ", " + foreignTableAlias2 + "." + naturalkeyCol + " AS "
                                   + foreignColAliasPre + naturalkeyCol;

                    if (compareSqlGroupBy.isEmpty()) {
                        compareSqlGroupBy = "GROUP BY " + foreignColAliasPre + naturalkeyCol;
                        compareSqlOrderBy = "ORDER BY " + foreignColAliasPre + naturalkeyCol;
                    } else {
                        compareSqlGroupBy = compareSqlGroupBy + "," + foreignColAliasPre + naturalkeyCol;
                        compareSqlOrderBy = compareSqlOrderBy + "," + foreignColAliasPre + naturalkeyCol;
                    }
                }
            }

            compareSql1     = compareSql1 + joinColSqls1 + compareSqlTail;
            compareSql2     = compareSql2 + joinColSqls2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + "." + tableName
                              + " " + db1baseTableAlias + " " + joinTableSqls1 + " UNION ALL " + compareSql2 + " FROM "
                              + dbName2 + "." + tableName + " " + db2baseTableAlias + " " + joinTableSqls2 + " ) tmp "
                              + compareSqlGroupBy + " " + compareSqlCondition + " " + compareSqlOrderBy;
            getLogger().fine("Final SQL script for compare: " + compareSqlWhole);
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                getLogger().severe("==========FAILED: Comparing Table " + tableName + " Failed. ");

//              getLogger().severe("============================================ Difference    =======");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
//              getLogger().severe("============================================ Difference End=======");
                // print rows
            } else {
                getLogger().info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            getLogger().info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

    public boolean compareSameColTableWithoutNKey(String oldrevision, String newrevision, String dbName1,
                                                  String dbName2, String tableName, boolean excludeID) {
        boolean bCompareResult  = true;
        String  compareSqlWhole = "";

        if (DBHelper.getInstance().isTableEmpty(dbName1, tableName)
                && DBHelper.getInstance().isTableEmpty(dbName2, tableName)) {
            getLogger().info("==========Empty: Table " + tableName + " is empty");

            return bCompareResult;
        }

        // get column list
        String sql1 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";
        String sql2 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName2 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";

        try {
            ResultSet columnRS1 = DBHelper.getInstance().executeSQL(sql1);
            ResultSet columnRS2 = DBHelper.getInstance().executeSQL(sql2);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select sum(crc32(concat_ws('', COL1, COL2,
             * COL3 ...))) as crc FROM dbName1.Table union all select
             * sum(crc32(concat_ws('', COL1, COL2, COL3 ...))) as crc FROM
             * dbName2.Table ) tmp group by crc
             */
            String compareSqlUnionSelect = "SELECT *";

            // String compareSql1 = "SELECT SUM(CRC32(CONCAT_WS('' ";
            // String compareSql2 = "SELECT SUM(CRC32(CONCAT_WS('' ";
            // String compareSqlTail = "))) as crc";
            // String compareSqlGroupBy = "GROUP BY crc";
            String compareSql1         = "SELECT '" + dbName1 + "' as dbname ";
            String compareSql2         = "SELECT '" + dbName2 + "' as dbname ";
            String compareSqlTail      = "";
            String compareSqlGroupBy   = "GROUP BY ";
            String compareSqlCondition = "HAVING COUNT(*) = 1";
            int    priKeyIndex         = 0;

            // get column list for RS2
            String rs2ColumnList = "";

            while (columnRS2.next()) {
                String columnName = columnRS2.getString("COLUMN_NAME");

                columnName    = columnName.trim();
                rs2ColumnList = columnName + "," + rs2ColumnList;
            }

            int colIndex = 0;

            while (columnRS1.next()) {
                StringTokenizer rs2StrTocken = new StringTokenizer(rs2ColumnList, ",");

                // go through each column
                String columnName = columnRS1.getString("COLUMN_NAME");

                columnName = columnName.trim();

                if (inColumnFilterList(columnName)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS1.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    if (excludeID) {
                        continue;
                    }
                }

                // if RS2 has same column name as the one in RS1
                boolean bFound = false;

                while (rs2StrTocken.hasMoreTokens()) {
                    String rs2ColTocken = rs2StrTocken.nextToken();

                    if (rs2ColTocken.equalsIgnoreCase(columnName)) {
                        bFound = true;

                        break;
                    }
                }

                if (bFound == false) {
                    continue;
                }

                // compareSql1 = compareSql1 + ", IFNULL(" + columnName
                // + ", 'NULL')";
                // compareSql1 = compareSql1 + ", " + "'" + separator + "'";
                //
                // compareSql2 = compareSql2 + ", IFNULL(" + columnName
                // + ", 'NULL')";
                // compareSql2 = compareSql2 + ", " + "'" + separator + "'";
                if (colIndex == 0) {

                    // compareSql2 = compareSql2 + ", ";
                    compareSqlGroupBy = compareSqlGroupBy + columnName;
                } else {

                    // compareSql2 = compareSql2 + ", ";
                    compareSqlGroupBy = compareSqlGroupBy + ", " + columnName;
                }

                compareSql1 = compareSql1 + ", " + columnName;

                // compareSql1 = compareSql1 + ", ";
                compareSql2 = compareSql2 + ", " + columnName;
                colIndex++;
            }

            // priKeyIndex = 0;
            compareSql1     = compareSql1 + compareSqlTail;
            compareSql2     = compareSql2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + "." + tableName
                              + " UNION ALL " + compareSql2 + " FROM " + dbName2 + "." + tableName + ") tmp "
                              + compareSqlGroupBy + " " + compareSqlCondition;
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                getLogger().severe("==========FAILED: Comparing Table " + tableName + " Failed. Excluding ID flag as: "
                                   + excludeID);
                getLogger().severe("============================================ Difference   =======");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
                getLogger().severe("============================================ Difference End=======");

                // print rows
            } else {
                getLogger().info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            getLogger().severe("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

    // this function only compare two dbs in one revision
    public boolean compareTableWithNKey(String revision, String dbName1, String dbName2, String tableName,
                                        List<String> idTrustTableList) {
        List<String> tableNameList      = new ArrayList<>();
        List<String> baseColNameList    = new ArrayList<>();
        List<String> foreignColNameList = new ArrayList<>();
        List<String> naturalkeyList     = new ArrayList<>();

        fillnaturalkeyReplacementInfo(revision,
                                      dbName1,
                                      tableName,
                                      idTrustTableList,
                                      tableNameList,
                                      baseColNameList,
                                      foreignColNameList,
                                      naturalkeyList);

        // replacing natural key. get foreign key constrain below
        // Use java11Test0
        // SELECT *
        // FROM
        // information_schema.KEY_COLUMN_USAGE
        // WHERE TABLE_SCHEMA='java11Test0' AND TABLE_NAME='dwwetendpaperusage'
        // AND referenced_table_name IS NOT NULL;
        // if idTrustTableList is not null, it means will replace naturalkey for
        // ids. And the id of the tables in idTrustTableList do not need be
        // replaced
        boolean bCompareResult  = true;
        String  compareSqlWhole = "";

        // this is table alias, to add column prefix like: s0.xxx. and add
        // prefix during join.
        // base table is always: s
        String baseTableAlias    = "s";
        String db1baseTableAlias = "db1s";
        String db2baseTableAlias = "db2s";

        if (DBHelper.getInstance().isTableEmpty(dbName1, tableName)
                && DBHelper.getInstance().isTableEmpty(dbName2, tableName)) {
            getLogger().info("==========Empty: Table " + tableName + " is empty");

            return bCompareResult;
        }

        // get column list
        String table1ColListSql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1 + "' AND "
                                  + " TABLE_NAME='" + tableName + "'";
        String table2ColListSql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName2 + "' AND "
                                  + " TABLE_NAME='" + tableName + "'";

        try {
            ResultSet table1ColumnRS1 = DBHelper.getInstance().executeSQL(table1ColListSql);
            ResultSet table2ColumnRS2 = DBHelper.getInstance().executeSQL(table2ColListSql);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select s.COL1, s.COL2, s.COL3 ...,
             * s1.COL1, s2.COL1 FROM dbName1.Table s left join dbName1.Table1 s1
             * on s.xxx = s1.xxx_xxx .... union all select s.COL1, s.COL2,
             * s.COL3 ..., s1.COL1, s2.COL1 FROM dbName2.Table s left join
             * dbName2.Table1 s1 on s.xxx = s1.xxx_xxx .... ) tmp group by col1,
             * col2, ...
             */
            String compareSqlUnionSelect = "SELECT *";
            String compareSql1           = "";
            String compareSql2           = "";
            String joinColSqls1          = "";
            String joinTableSqls1        = "";
            String joinColSqls2          = "";
            String joinTableSqls2        = "";
            String compareSqlTail        = "";
            String compareSqlGroupBy     = "";
            String compareSqlOrderBy     = "";
            String compareSqlCondition   = "HAVING COUNT(*) = 1";
            int    priKeyIndex           = 0;

            // here, we assume columnName of table in db1 is same as in db2
            while (table1ColumnRS1.next()) {

                // go through each column
                // primary key
                String columnKey = table1ColumnRS1.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    continue;
                }

                String columnName = table1ColumnRS1.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName)) {
                    continue;
                }

                if (compareSql1.isEmpty()) {
                    compareSql1 = "SELECT 'old' as tableT, " + db1baseTableAlias + "." + columnName;
                    compareSql2 = "SELECT 'new' as tableT, " + db2baseTableAlias + "." + columnName;
                } else {
                    compareSql1 = compareSql1 + ", " + db1baseTableAlias + "." + columnName;
                    compareSql2 = compareSql2 + ", " + db2baseTableAlias + "." + columnName;
                }

                boolean inReplacedColList = false;

                for (int baseCol = 0; baseCol < baseColNameList.size(); baseCol++) {
                    if (columnName.equalsIgnoreCase(baseColNameList.get(baseCol))) {
                        inReplacedColList = true;

                        break;
                    }
                }

                if (inReplacedColList != true) {
                    if (compareSqlGroupBy.isEmpty()) {
                        compareSqlGroupBy = "GROUP BY " + columnName;
                        compareSqlOrderBy = "ORDER BY " + columnName;
                    } else {
                        compareSqlGroupBy = compareSqlGroupBy + ", " + columnName;
                        compareSqlOrderBy = compareSqlOrderBy + ", " + columnName;
                    }
                }
            }

            priKeyIndex = 0;

            /*
             * while (table2ColumnRS2.next()) { // go through each column String
             * columnName2 = table2ColumnRS2.getString("COLUMN_NAME");
             *
             * // bypass the record last changed column if
             * (columnName2.contains("record_last_changed")) { continue; }
             *
             * // primary key String columnKey =
             * table2ColumnRS2.getString("COLUMN_KEY"); if (columnKey.equals("PRI") &&
             * (priKeyIndex == 0)) { priKeyIndex++; continue; }
             *
             * compareSql2 = compareSql2 + ", " + baseTableAlias + "." +
             * columnName2; }
             */
            int    foreignTableAliasIndex = 0;
            String foreignTableAlias1     = "";
            String foreignTableAlias2     = "";
            String foreignColAliasPre     = "";

            // join tables
            for (int index = 0; index < tableNameList.size(); index++) {
                foreignTableAliasIndex++;

                // the table alias to be join are like: s1, s2, etc
                foreignTableAlias1 = db1baseTableAlias + foreignTableAliasIndex;
                foreignTableAlias2 = db2baseTableAlias + foreignTableAliasIndex;
                foreignColAliasPre = baseTableAlias + foreignTableAliasIndex;

                String joinTableName  = tableNameList.get(index);
                String joinBaseCol    = baseColNameList.get(index);
                String joinForeignCol = foreignColNameList.get(index);
                String naturalkeys    = naturalkeyList.get(index);

                joinTableSqls1 = " " + joinTableSqls1 + " LEFT JOIN " + dbName1 + "." + joinTableName + " "
                                 + foreignTableAlias1 + " ON " + foreignTableAlias1 + "." + joinForeignCol + " = "
                                 + db1baseTableAlias + "." + joinBaseCol;
                joinTableSqls2 = " " + joinTableSqls2 + " LEFT JOIN " + dbName2 + "." + joinTableName + " "
                                 + foreignTableAlias2 + " ON " + foreignTableAlias2 + "." + joinForeignCol + " = "
                                 + db2baseTableAlias + "." + joinBaseCol;

                StringTokenizer stnaturalkeys = new StringTokenizer(naturalkeys, " ,\t\n");

                while (stnaturalkeys.hasMoreTokens()) {

                    // here, two db tables using same alias. need to check if
                    // it's ok or not
                    String naturalkeyCol = stnaturalkeys.nextToken().trim();

                    if (inColumnFilterList(naturalkeyCol)) {
                        continue;
                    }

                    joinColSqls1 = joinColSqls1 + ", " + foreignTableAlias1 + "." + naturalkeyCol + " AS "
                                   + foreignColAliasPre + naturalkeyCol;
                    joinColSqls2 = joinColSqls2 + ", " + foreignTableAlias2 + "." + naturalkeyCol + " AS "
                                   + foreignColAliasPre + naturalkeyCol;

                    if (compareSqlGroupBy.isEmpty()) {
                        compareSqlGroupBy = "GROUP BY " + foreignColAliasPre + naturalkeyCol;
                        compareSqlOrderBy = "ORDER BY " + foreignColAliasPre + naturalkeyCol;
                    } else {
                        compareSqlGroupBy = compareSqlGroupBy + "," + foreignColAliasPre + naturalkeyCol;
                        compareSqlOrderBy = compareSqlOrderBy + "," + foreignColAliasPre + naturalkeyCol;
                    }
                }
            }

            compareSql1     = compareSql1 + joinColSqls1 + compareSqlTail;
            compareSql2     = compareSql2 + joinColSqls2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + "." + tableName
                              + " " + db1baseTableAlias + " " + joinTableSqls1 + " UNION ALL " + compareSql2 + " FROM "
                              + dbName2 + "." + tableName + " " + db2baseTableAlias + " " + joinTableSqls2 + " ) tmp "
                              + compareSqlGroupBy + " " + compareSqlCondition + " " + compareSqlOrderBy;
            getLogger().fine("Final SQL script for compare: " + compareSqlWhole);
            table1ColumnRS1.close();
            table2ColumnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;

//              getLogger().severe("==========FAILED: Comparing Table " + tableName + " Failed. ");
                getLogger().info("============================================ Difference    =======");
                compareRS.first();

                long db1RecordNumber = KDWHelper.getRecordNumber(dbName1, tableName);
                long db2RecordNumber = KDWHelper.getRecordNumber(dbName2, tableName);

                getLogger().severe("DB table " + dbName1 + "." + tableName + " total records number: "
                                   + db1RecordNumber);
                getLogger().severe("DB table " + dbName2 + "." + tableName + " total records number: "
                                   + db2RecordNumber);

                if (db1RecordNumber != db2RecordNumber) {
                    getLogger().info("Record Number of Table : " + tableName + " is different. " + dbName1 + ":"
                                     + db1RecordNumber + " " + dbName2 + ":" + db2RecordNumber);
                }

//              getLogger().info("Database 2 tables difference: ");
//              getLogger().info(DBHelper.getInstance().printResultSet(compareRS, 10));
                // use sum(crc32(machine_number)) to get the sum value for each column and compare the values to find columns with difference
                // use the columns in group by clause
                String          columns                  = compareSqlGroupBy.substring("GROUP BY ".length());
                StringTokenizer stColumns                = new StringTokenizer(columns, " ,");
                String          compareSqlDiffColSelect1 = "Select ";
                String          compareSqlDiffColSelect2 = "Select ";

                while (stColumns.hasMoreTokens()) {
                    String column    = stColumns.nextToken().trim();
                    int    colIndex1 = compareSql1.indexOf("AS " + column) - 1;
                    int    colIndex2 = compareSql2.indexOf("AS " + column) - 1;

                    if (colIndex1 > 0) {
                        String column1 = compareSql1.substring(0, colIndex1);

                        column1                  = column1.substring(column1.lastIndexOf(",") + 1);
                        compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(crc32(" + column1 + ")),";

                        String column2 = compareSql2.substring(0, colIndex2);

                        column2                  = column2.substring(column2.lastIndexOf(",") + 1);
                        compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(crc32(" + column2 + ")),";
                    } else {
                        compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(crc32(" + column + ")),";
                        compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(crc32(" + column + ")),";
                    }
                }

                // remove the last ","
                compareSqlDiffColSelect1 =
                    compareSqlDiffColSelect1.substring(0, compareSqlDiffColSelect1.lastIndexOf(","));
                compareSqlDiffColSelect2 =
                    compareSqlDiffColSelect2.substring(0, compareSqlDiffColSelect2.lastIndexOf(","));

                String sumSql1 = compareSqlDiffColSelect1 + " FROM " + dbName1 + "." + tableName + " "
                                 + db1baseTableAlias + " " + joinTableSqls1;
                String sumSql2 = compareSqlDiffColSelect2 + " FROM " + dbName2 + "." + tableName + " "
                                 + db2baseTableAlias + " " + joinTableSqls2;
                ResultSet         sumColumnsRS1  = DBHelper.getInstance().executeSQL(sumSql1);
                ResultSet         sumColumnsRS2  = DBHelper.getInstance().executeSQL(sumSql2);
                List<String>      columnDiffList = new ArrayList<>();
                ResultSetMetaData rsmd           = sumColumnsRS1.getMetaData();
                int               columnTotal    = rsmd.getColumnCount();

                sumColumnsRS1.next();
                sumColumnsRS2.next();

                for (int columnIndex = 1; columnIndex <= columnTotal; columnIndex++) {
                    double tempColValue1 = sumColumnsRS1.getDouble(columnIndex);
                    double tempColValue2 = sumColumnsRS2.getDouble(columnIndex);

                    if (tempColValue1 != tempColValue2) {
                        String colName = rsmd.getColumnName(columnIndex);

                        colName = colName.substring("sum(crc32(".length(), colName.length() - 2);

                        int dotIndex = colName.indexOf(".");

                        if (dotIndex > 0) {
                            String tempName = colName.substring(dotIndex - 4, dotIndex);

                            colName = tempName.substring(tempName.lastIndexOf("s")) + colName.substring(dotIndex + 1);
                        }

                        columnDiffList.add(colName);
                    }
                }

//              getLogger().info("Different column list is : " + columnDiffList);
                getLogger().info("Table : " + tableName + " different column values are : " + columnDiffList);

//              String sql = createSQL4DiffTables(dbName1, dbName2, tableName, columnDiffList);
                String sql = this.simplifySQL(dbName1, tableName, compareSqlWhole, columnDiffList);

                getLogger().info("Simplified SQL : " + sql.replaceAll("\\s+", " "));

                try {
                    ResultSet diffResult = DBHelper.getInstance().executeSQL(sql);
                    String    rsText     = DBHelper.getInstance().printResultSet(diffResult, 10);

                    getLogger().info(rsText);
                } catch (Exception e) {
                    e.printStackTrace();
                    getLogger().severe("error to exec above SQL: " + e.getMessage());
                }

                getLogger().info("============================================ Difference End=======");

                // print rows
            } else {
                getLogger().info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            getLogger().log(Level.SEVERE, "Exception after executed SQL script: " + compareSqlWhole + "\nMessage: " + e.getMessage(), e);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

//  public String createSQL4DiffTables(String db1Name, String db2Name, String tableName, List<String> colNames){
//
//      List<String> dbVersions = DBHelper.getInstance().getDBVersions(db1Name);
//      String rev = dbVersions.get(dbVersions.size() - 1);
//      int dashIndex = rev.indexOf('-');
//      rev = rev.substring(0, dashIndex);
//      String uniqueKey = KDWSchemaHelper.getUniqueKey(rev, tableName);
//      String[] keyArray = uniqueKey.split(",");
//      for(String key: keyArray){
//          if(key.trim().length() > 0){
//              colNames.add(key.trim());
//          }
//      }
//
//
//      colNames.removeIf(item-> item == null || item.trim().length()==0);
//      List<String> db1sColNames = new ArrayList<>();
//      List<String> db2sColNames = new ArrayList<>();
//      for(String name: colNames){
//          db1sColNames.add("db1s." + name);
//          db2sColNames.add("db2s." + name);
//      }
//
//      String db1sColNameList = String.join(",", db1sColNames);
//      String db2sColNameList = String.join(",", db2sColNames);
//      String colNameList = String.join(",", colNames);
//
//      String SQL = "SELECT *\n" +
//              String.format("FROM (SELECT 'old_%s'   AS tableName,\n", tableName) +
//              db1sColNameList +
//              String.format("      FROM %s.%s db1s\n", db1Name, tableName) +
//              "      UNION ALL\n" +
//              String.format("      SELECT 'new_%s'   AS tableName,\n", tableName) +
//                  db2sColNameList +
//
//              String.format("      FROM %s.%s db2s\n", db2Name, tableName)+
//              "     )tmpTable\n" +
//              String.format("GROUP BY %s\n", colNameList) +
//              "         \n" +
//              "HAVING COUNT(*) = 1\n" +
//              String.format("ORDER BY %s\n", uniqueKey);
//
//      return SQL;
//  }
    // this function only compare two dbs in one revision
    public boolean compareTableWithNKeyCRC(String revision, String dbName1, String dbName2, String tableName,
                                           List<String> idTrustTableList) {
        List<String> tableNameList      = new ArrayList<>();
        List<String> baseColNameList    = new ArrayList<>();
        List<String> foreignColNameList = new ArrayList<>();
        List<String> naturalkeyList     = new ArrayList<>();

        fillnaturalkeyReplacementInfo(revision,
                                      dbName1,
                                      tableName,
                                      idTrustTableList,
                                      tableNameList,
                                      baseColNameList,
                                      foreignColNameList,
                                      naturalkeyList);

        // replacing natural key. get foreign key constrain below
        // Use java11Test0
        // SELECT *
        // FROM
        // information_schema.KEY_COLUMN_USAGE
        // WHERE TABLE_SCHEMA='java11Test0' AND TABLE_NAME='dwwetendpaperusage'
        // AND referenced_table_name IS NOT NULL;
        // if idTrustTableList is not null, it means will replace naturalkey for
        // ids. And the id of the tables in idTrustTableList do not need be
        // replaced
        boolean bCompareResult  = true;
        String  compareSqlWhole = "";

        // this is table alias, to add column prefix like: s0.xxx. and add
        // prefix during join.
        // base table is always: s
        String baseTableAlias = "s";

        // as we use concat function to concat all fields of one row together
        // and then make a digest. This separator is used to separate
        // each field to avoid the situation: a=1, b=23 and a=12,b=3
        String separator = "~";

        if (DBHelper.getInstance().isTableEmpty(dbName1, tableName)
                && DBHelper.getInstance().isTableEmpty(dbName2, tableName)) {
            getLogger().info("==========Empty: Table " + tableName + " is empty");

            return bCompareResult;
        }

        // get column list
        String sql1 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";
        String sql2 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName2 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";

        try {
            ResultSet columnRS1 = DBHelper.getInstance().executeSQL(sql1);
            ResultSet columnRS2 = DBHelper.getInstance().executeSQL(sql2);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select sum(crc32(concat_ws('', s.COL1,
             * s.COL2, s.COL3 ..., s1.COL1, s2.COL1))) as crc FROM dbName1.Table
             * s left join dbName1.Table1 s1 on s.xxx = s1.xxx_xxx .... union
             * all select sum(crc32(concat_ws('', s.COL1, s.COL2, s.COL3 ...,
             * s1.COL1, s2.COL1))) as crc FROM dbName2.Table s left join
             * dbName2.Table1 s1 on s.xxx = s1.xxx_xxx .... ) tmp group by crc
             */
            String compareSqlUnionSelect = "SELECT *";
            String compareSql1           = "SELECT SUM(CRC32(CONCAT_WS('' ";
            String compareSql2           = "SELECT SUM(CRC32(CONCAT_WS('' ";
            String joinColSqls1          = "";
            String joinTableSqls1        = "";
            String joinColSqls2          = "";
            String joinTableSqls2        = "";
            String compareSqlTail        = "))) as crc";
            String compareSqlGroupBy     = "GROUP BY crc";
            String compareSqlCondition   = "HAVING COUNT(*) = 1";
            int    priKeyIndex           = 0;

            while (columnRS1.next()) {

                // go through each column
                String columnName1 = columnRS1.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName1)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS1.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    continue;
                }

                compareSql1 = compareSql1 + ", IFNULL(" + baseTableAlias + "." + columnName1 + ", 'NULL')";
                compareSql1 = compareSql1 + ", " + "'" + separator + "'";
            }

            priKeyIndex = 0;

            while (columnRS2.next()) {

                // go through each column
                String columnName2 = columnRS2.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName2)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS2.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    continue;
                }

                compareSql2 = compareSql2 + ", IFNULL(" + baseTableAlias + "." + columnName2 + ", 'NULL')";
                compareSql2 = compareSql2 + ", " + "'" + separator + "'";
            }

            int    foreignTableAliasIndex = 0;
            String foreignTableAlias      = "";

            // join tables
            for (int index = 0; index < tableNameList.size(); index++) {
                foreignTableAliasIndex++;

                // the table alias to be join are like: s1, s2, etc
                foreignTableAlias = baseTableAlias + foreignTableAliasIndex;

                String joinTableName  = tableNameList.get(index);
                String joinBaseCol    = baseColNameList.get(index);
                String joinForeignCol = foreignColNameList.get(index);
                String naturalkeys    = naturalkeyList.get(index);

                joinTableSqls1 = " " + joinTableSqls1 + " LEFT JOIN " + dbName1 + "." + joinTableName + " "
                                 + foreignTableAlias + " ON " + foreignTableAlias + "." + joinForeignCol + " = "
                                 + baseTableAlias + "." + joinBaseCol;
                joinTableSqls2 = " " + joinTableSqls2 + " LEFT JOIN " + dbName2 + "." + joinTableName + " "
                                 + foreignTableAlias + " ON " + foreignTableAlias + "." + joinForeignCol + " = "
                                 + baseTableAlias + "." + joinBaseCol;

                StringTokenizer stnaturalkeys = new StringTokenizer(naturalkeys, " ,\t\n");

                while (stnaturalkeys.hasMoreTokens()) {

                    // here, two db tables using same alias. need to check if
                    // it's ok or not
                    String naturalkeyCol = stnaturalkeys.nextToken().trim();

                    joinColSqls1 = joinColSqls1 + ", IFNULL(" + foreignTableAlias + "." + naturalkeyCol + ", 'NULL')";
                    joinColSqls1 = joinColSqls1 + ", " + "'" + separator + "'";
                    joinColSqls2 = joinColSqls2 + ", IFNULL(" + foreignTableAlias + "." + naturalkeyCol + ", 'NULL')";
                    joinColSqls2 = joinColSqls2 + ", " + "'" + separator + "'";
                }
            }

            compareSql1     = compareSql1 + joinColSqls1 + compareSqlTail;
            compareSql2     = compareSql2 + joinColSqls2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + "." + tableName
                              + " " + baseTableAlias + " " + joinTableSqls1 + " UNION ALL " + compareSql2 + " FROM "
                              + dbName2 + "." + tableName + " " + baseTableAlias + " " + joinTableSqls2 + " ) tmp "
                              + compareSqlGroupBy + " " + compareSqlCondition;
            getLogger().info("Final SQL script for compare: " + compareSqlWhole);
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                getLogger().severe("==========FAILED: Comparing Table " + tableName + " Failed. ");
                getLogger().severe("============================================ Difference    =======");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
                getLogger().severe("============================================ Difference End=======");

                // print rows
            } else {
                getLogger().info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            getLogger().info("Exception after executed SQL script: " + e.getMessage());
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

    public boolean compareTableWithoutNKey(String dbName1, String dbName2, String tableName, boolean excludeID) {
        boolean bCompareResult  = true;
        String  compareSqlWhole = "";

        // as we use concat function to concat all fields of one row together
        // and then make a digest. This separator is used to separate
        // each field to avoid the situation: a=1, b=23 and a=12,b=3
        String separator = "~";

        if (DBHelper.getInstance().isTableEmpty(dbName1, tableName)
                && DBHelper.getInstance().isTableEmpty(dbName2, tableName)) {
            getLogger().info("\t\tTable " + tableName + " is empty");

            return bCompareResult;
        }

        // get column list
        String sql1 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";
        String sql2 = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName2 + "' AND "
                      + " TABLE_NAME='" + tableName + "'";

        try {
            ResultSet columnRS1 = DBHelper.getInstance().executeSQL(sql1);
            ResultSet columnRS2 = DBHelper.getInstance().executeSQL(sql2);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select sum(crc32(concat_ws('', COL1, COL2,
             * COL3 ...))) as crc FROM dbName1.Table union all select
             * sum(crc32(concat_ws('', COL1, COL2, COL3 ...))) as crc FROM
             * dbName2.Table ) tmp group by crc
             */
            String compareSqlUnionSelect = "SELECT *";
            String compareSql1           = "SELECT SUM(CRC32(CONCAT_WS('' ";
            String compareSql2           = "SELECT SUM(CRC32(CONCAT_WS('' ";
            String compareSqlTail        = "))) as crc";
            String compareSqlGroupBy     = "GROUP BY crc";
            String compareSqlCondition   = "HAVING COUNT(*) = 1";
            int    priKeyIndex           = 0;

            while (columnRS1.next()) {

                // go through each column
                String columnName1 = columnRS1.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName1)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS1.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    if (excludeID) {
                        continue;
                    }
                }

                compareSql1 = compareSql1 + ", IFNULL(" + columnName1 + ", 'NULL')";
                compareSql1 = compareSql1 + ", " + "'" + separator + "'";
            }

            priKeyIndex = 0;

            while (columnRS2.next()) {

                // go through each column
                String columnName2 = columnRS2.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName2)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS2.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    if (excludeID) {
                        continue;
                    }
                }

                compareSql2 = compareSql2 + ", IFNULL(" + columnName2 + ", 'NULL')";
                compareSql2 = compareSql2 + ", " + "'" + separator + "'";
            }

            columnRS1.close();
            columnRS2.close();
            compareSql1     = compareSql1 + compareSqlTail;
            compareSql2     = compareSql2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + "." + tableName
                              + " UNION ALL " + compareSql2 + " FROM " + dbName2 + "." + tableName + " ) tmp "
                              + compareSqlGroupBy + " " + compareSqlCondition;

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                getLogger().warning(
                    String.format("\tCompare tables [%s] in 2 databases with Exclude ID flag [%s] not successful!",
                                  tableName,
                                  excludeID));
            } else {
                getLogger().info(
                    String.format("\tCompare tables [%s] in 2 databases with Exclude ID flag [%s] successful!",
                                  tableName,
                                  excludeID));
            }

            compareRS.close();
        } catch (Exception e) {
            getLogger().info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

    protected void fillInfo4KeyReplacement(List<String> tableNameList, List<String> srcColNameList,
                                           List<String> foreignColNameList, List<String> naturalkeyList,
                                           String revision, String dbName, String tableName, String naturalkey,
                                           List<String> idTrustTableList) {

        // get foreign key constrains
        String sqlForeignKey = "";

        sqlForeignKey = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME";
        sqlForeignKey = sqlForeignKey + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='" + dbName
                        + "' AND TABLE_NAME='" + tableName + "' AND REFERENCED_TABLE_NAME IS NOT NULL";

        try {
            getLogger().fine(sqlForeignKey);

            ResultSet rsForeignKeys = DBHelper.getInstance().executeSQL(sqlForeignKey);

            while (rsForeignKeys.next()) {
                boolean bTrustTable = false;

                // go through each column
                String foreignTableName = rsForeignKeys.getString("REFERENCED_TABLE_NAME");

                // search in trusttablelist
                for (int tableIndex = 0; tableIndex < idTrustTableList.size(); tableIndex++) {
                    if (idTrustTableList.get(tableIndex).equalsIgnoreCase(foreignTableName)) {
                        bTrustTable = true;
                    }
                }

                if (bTrustTable) {
                    continue;
                }

                // for each id in naturalkey, need to replace it with
                // corresponding naturalkey if the corresponding table is not in
                // trusttablelist
                boolean         bInKeyList    = false;
                StringTokenizer stnaturalkeys = new StringTokenizer(naturalkey, " ,");

                while (stnaturalkeys.hasMoreTokens()) {
                    String key = stnaturalkeys.nextToken();

                    if (key.equalsIgnoreCase(rsForeignKeys.getString("REFERENCED_COLUMN_NAME"))) {
                        bInKeyList = true;
                    }
                }

                // if not in unique key list, no need for replacement
                if (!bInKeyList) {
                    continue;
                }

                boolean bTableReplaced = false;

                for (int tableIndex = 0; tableIndex < tableNameList.size(); tableIndex++) {
                    if (tableNameList.get(tableIndex).equalsIgnoreCase(foreignTableName)) {
                        bTableReplaced = true;
                    }
                }

                if (bTableReplaced) {
                    continue;
                }

                // if in unique key list, need replace the natural key
                // recursively
                String recforeignnaturalkey = KDWSchemaHelper.getUniqueKey(revision, foreignTableName);

                // here, we need to get unique keys for table, and replace the
                // id with unique key recursively.
                // table, unique key, column name for join
                tableNameList.add(foreignTableName);
                srcColNameList.add(rsForeignKeys.getString("COLUMN_NAME"));
                foreignColNameList.add(rsForeignKeys.getString("REFERENCED_COLUMN_NAME"));
                naturalkeyList.add(recforeignnaturalkey);
                fillInfo4KeyReplacement(tableNameList,
                                        srcColNameList,
                                        foreignColNameList,
                                        naturalkeyList,
                                        revision,
                                        dbName,
                                        foreignTableName,
                                        recforeignnaturalkey,
                                        idTrustTableList);
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    public void fillnaturalkeyReplacementInfo(String revision, String dbName, String tableName,
                                              List<String> idTrustTableList, List<String> tableNameList,
                                              List<String> srcColNameList, List<String> foreignColNameList,
                                              List<String> naturalkeyList) {

        // get foreign key constrains
        String sqlForeignKey = "";

        sqlForeignKey = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME";
        sqlForeignKey = sqlForeignKey + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='" + dbName
                        + "' AND TABLE_NAME='" + tableName + "' AND REFERENCED_TABLE_NAME IS NOT NULL";

        try {
            rootLogger.fine(sqlForeignKey);

            ResultSet rsForeignKeys = DBHelper.getInstance().executeSQL(sqlForeignKey);

            while (rsForeignKeys.next()) {
                boolean bTrustTable = false;

                // go through each column
                String foreignTableName = rsForeignKeys.getString("REFERENCED_TABLE_NAME");

                // search in trusttablelist
                for (int tableIndex = 0; tableIndex < idTrustTableList.size(); tableIndex++) {
                    if (idTrustTableList.get(tableIndex).equalsIgnoreCase(foreignTableName)) {
                        bTrustTable = true;
                    }
                }

                if (bTrustTable) {
                    continue;
                }

                String foreignnaturalkey = KDWSchemaHelper.getUniqueKey(revision, foreignTableName);

                // here, we need to get unique keys for table, and replace the
                // id with unique key recursively.
                // table, unique key, column name for join
                tableNameList.add(foreignTableName);
                srcColNameList.add(rsForeignKeys.getString("COLUMN_NAME"));
                foreignColNameList.add(rsForeignKeys.getString("REFERENCED_COLUMN_NAME"));
                naturalkeyList.add(foreignnaturalkey);
                fillInfo4KeyReplacement(tableNameList,
                                        srcColNameList,
                                        foreignColNameList,
                                        naturalkeyList,
                                        revision,
                                        dbName,
                                        foreignTableName,
                                        foreignnaturalkey,
                                        idTrustTableList);
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        printnaturalkeyReplacement(tableNameList, srcColNameList, foreignColNameList, naturalkeyList);
    }

    public static void main(String[] args) {
        CompareDBs.getInstance().setLogger(LogHelper.getRootLogger());
        CompareDBs.getInstance()
                  .compareMasterDBs("9.10.1",
                                    "kdw_9101_storareleaseregression_0_old_masterDW",
                                    "kdw_9101_storareleaseregression_0_new_masterDW");
    }

    public String simplifySQL(String db1Name, String tableName, String longSQL, List<String> diffColNameList) {
        List<String> dbVersions = DBHelper.getInstance().getDBVersions(db1Name);
        String       rev        = dbVersions.get(dbVersions.size() - 1);
        int          dashIndex  = rev.indexOf('-');

        rev = rev.substring(0, dashIndex);

        String   uniqueKey = KDWSchemaHelper.getUniqueKey(rev, tableName);
        String[] keyArray  = uniqueKey.split(",");

        for (String key : keyArray) {
            if (key.trim().length() > 0) {
                diffColNameList.add(key.trim());
            }
        }

        String shortSQL     = longSQL;
        int    orderByIndex = longSQL.indexOf("ORDER BY");

        if (orderByIndex > 0) {
            String   columnNameStr   = longSQL.substring(orderByIndex + "order by".length());
            String[] columnNameArray = columnNameStr.split("\\s+");

            for (int i = 0; i < columnNameArray.length - 1; i++) {
                String colName                 = columnNameArray[i].trim();
                String trimColNameWithoutComma = colName;

                if (colName.endsWith(",")) {
                    trimColNameWithoutComma = colName.substring(0, colName.length() - 1);
                }

                if ((colName.length() <= 0) || diffColNameList.contains(trimColNameWithoutComma)) {
                    continue;
                }

                shortSQL = shortSQL.replace(" db1s." + colName, " ");
                shortSQL = shortSQL.replace(" db2s." + colName, " ");
                shortSQL = shortSQL.replace(" " + colName, " ");
            }
        }

        return shortSQL;
    }

    protected boolean getDifferentColCompareNormalDBsWithSameSchema(String dbName1, String dbName2,
                                                                    List<String> differentColumnList,
                                                                    Map<String, String> starlenMap) {
        boolean bFinalResult = true;

        getLogger().info("============================================================");
        getLogger().info("Compare Normal DBs With Same Schema: ");
        getLogger().info("    " + dbName1 + ", " + DBHelper.getInstance().getDBVersions(dbName1));
        getLogger().info("    " + dbName2 + ", " + DBHelper.getInstance().getDBVersions(dbName2));
        getLogger().info("============================================================");

        // get table list
        String sql = "SELECT * FROM information_schema.tables where table_schema='" + dbName1 + "'";

        try {
            ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);

            // first, try sum of crc with id
            getLogger().info("\n\n++++++++++++++ Compare table from two databases using sum of crc ++++++++++++++\n\n");

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (getTableFilterList().contains(tableName)) {
                    continue;
                }

                // compare each tables result without id excluded and without
                // natural key replaced
                // the idTrustTableList will gives a list of table which we can
                // trust the ids which means do not to replace the id with
                // naturalkey
                if (!compareTableWithoutNKey(dbName1, dbName2, tableName, false)) {
                    bFinalResult = false;
                    getLogger().severe("============================================ Difference=======");

                    long db1RecordNumber = KDWHelper.getRecordNumber(dbName1, tableName);
                    long db2RecordNumber = KDWHelper.getRecordNumber(dbName2, tableName);

                    getLogger().severe("DB table " + dbName1 + "." + tableName + " total records number: "
                                       + db1RecordNumber);
                    getLogger().severe("DB table " + dbName2 + "." + tableName + " total records number: "
                                       + db2RecordNumber);

                    if (db1RecordNumber != db2RecordNumber) {
                        getLogger().info("Record Number of Table : " + tableName + " is different. " + dbName1 + ":"
                                         + db1RecordNumber + " " + dbName2 + ":" + db2RecordNumber);
                    }

                    getLogger().severe("total difference: ");

                    // use sum(crc32(machine_number)) to get the sum value for each column and compare the values to find columns with difference
                    // use the columns in group by clause
                    // TODO: Get all columns
                    String compareSqlDiffColSelect1 = "Select ";
                    String compareSqlDiffColSelect2 = "Select ";

                    ///////////////////////////
                    // get column list
                    String sqlCol = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" + dbName1
                                    + "' AND " + " TABLE_NAME='" + tableName + "'";
                    ResultSet columnRS1 = DBHelper.getInstance().executeSQL(sqlCol);

                    while (columnRS1.next()) {
                        String columnName = columnRS1.getString("COLUMN_NAME");
                        String starlen    = starlenMap.get("COLUMN_NAME");

                        if ((starlen != null) &&!starlen.isEmpty()) {
                            compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(crc32(IF( length(" + columnName
                                                       + ")>" + Integer.parseInt(starlen) + ", '****', " + columnName
                                                       + "))),";
                            compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(crc32(IF( length(" + columnName
                                                       + ")>" + Integer.parseInt(starlen) + ", '****', " + columnName
                                                       + "))),";
                        } else {
                            compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(crc32(" + columnName + ")),";
                            compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(crc32(" + columnName + ")),";
                        }
                    }

                    // remove the last ","
                    compareSqlDiffColSelect1 =
                        compareSqlDiffColSelect1.substring(0, compareSqlDiffColSelect1.lastIndexOf(","));
                    compareSqlDiffColSelect2 =
                        compareSqlDiffColSelect2.substring(0, compareSqlDiffColSelect2.lastIndexOf(","));

                    String            sumSql1        = compareSqlDiffColSelect1 + " FROM " + dbName1 + "." + tableName
                                                       + " ";
                    String            sumSql2        = compareSqlDiffColSelect2 + " FROM " + dbName2 + "." + tableName
                                                       + " ";
                    ResultSet         sumColumnsRS1  = DBHelper.getInstance().executeSQL(sumSql1);
                    ResultSet         sumColumnsRS2  = DBHelper.getInstance().executeSQL(sumSql2);
                    List<String>      columnDiffList = new ArrayList<>();
                    ResultSetMetaData rsmd           = sumColumnsRS1.getMetaData();
                    int               columnTotal    = rsmd.getColumnCount();

                    sumColumnsRS1.next();
                    sumColumnsRS2.next();

                    for (int columnIndex = 1; columnIndex <= columnTotal; columnIndex++) {
                        double tempColValue1 = sumColumnsRS1.getDouble(columnIndex);
                        double tempColValue2 = sumColumnsRS2.getDouble(columnIndex);

                        if (tempColValue1 != tempColValue2) {
                            String colName = rsmd.getColumnName(columnIndex);

                            colName = colName.substring("sum(crc32(".length(), colName.length() - 2);
                            columnDiffList.add(colName);
                            differentColumnList.add(colName);
                        }
                    }

                    getLogger().severe("Table : " + tableName + " different column list are : " + columnDiffList);
                    getLogger().severe("============================================ Difference End=======");
                }
            }

            tableRS.close();
        } catch (Exception e) {
            e.printStackTrace();
            bFinalResult = false;
        }

        return bFinalResult;
    }
}
