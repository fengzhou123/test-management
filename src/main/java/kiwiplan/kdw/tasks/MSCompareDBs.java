package kiwiplan.kdw.tasks;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.util.*;
import java.util.logging.Logger;

import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.KDWSchemaHelper;
import kiwiplan.kdw.core.LogHelper;

public class MSCompareDBs extends CompareDBs {

    // this is to enable MS SQL support migrating from MYSQL
    protected static String sql_crccalc = "CONVERT(BIGINT,CHECKSUM(CONVERT(VARCHAR(255),";
    protected static String sql_ifnull  = "ISNULL(CONVERT(VARCHAR(255),";
    protected static String sql_concat  = "CONCAT";
    static Logger           rootLogger  = LogHelper.getRootLogger();

    public boolean compareMasterDBs(String revision, String dbName1, String dbName2) {
        boolean bResult = true;

        rootLogger.info("================================================================================");
        rootLogger.info("Comparing DBs: " + dbName1 + " and " + dbName2);
        rootLogger.info("================================================================================");

        // get table list
        List<String> idTrustTableList  = new Vector<String>();
        List<String> idfailedTableList = new Vector<String>();

        try {
            ResultSet tableRS = DBHelper.getInstance().getTableRSList(dbName1);

            // first, try sum of crc with id
            rootLogger.info("\n++++++++++++++ First Pass: do not exclude ID and do not replace natural key ++++\n");

            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                if (inExcludeTableList(revision, tableName)) {
                    continue;
                }

                // compare each tables result without id excluded and without
                // natural key replaced
                // the idTrustTableList will gives a list of table which we can
                // trust the ids which means do not to replace the id with
                // naturalkey
                bResult = compareTableWithoutNKey(dbName1, dbName2, tableName, false);

                if (bResult) {
                    idTrustTableList.add(tableName);
                } else {
                    idfailedTableList.add(tableName);
                }
            }

            tableRS.close();

            // second, try sum of crc without id, only for failed table list
            rootLogger.info("\n+++++++++++++++++++ Second Pass: exclude ID but do not replace natural key +++++\n");

            int          failedTableNum  = idfailedTableList.size();
            List<String> failedTableList = new Vector<String>();

            for (int tableIndex = 0; tableIndex < failedTableNum; tableIndex++) {
                String tableName = idfailedTableList.get(tableIndex);

                // compare each tables ETL result with id excluded but without
                // natural key replaced
                bResult = compareTableWithoutNKey(dbName1, dbName2, tableName, true);

                if (!bResult) {

                    // the comparison of some tables may pass if exclude the
                    // auto incremental id
                    failedTableList.add(tableName);
                }
            }

            // third, replace foreign key, try sum of crc
            rootLogger.info("\n+++++++++++++++++++++ Third Pass: exclude ID and replace natural key +++++++++++\n");

            for (int tableIndex = 0; tableIndex < failedTableList.size(); tableIndex++) {
                String tableName = failedTableList.get(tableIndex);

                // compare each tables result with id excluded and with natural
                // key replaced
                // bResult = compareTableWithNKeyCRC(revision, dbName1, dbName2,
                // tableName, idTrustTableList);
                bResult = compareTableWithNKey(revision, dbName1, dbName2, tableName, idTrustTableList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            bResult = false;
        }

        return bResult;
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
                 * SELECT * from db1.tablename join on db2.tablename where
                 * db1tolerantcol - db2tolerantcol > tolerance
                 */
                String finalSQL  = "";
                String selectSQL = "SELECT ";
                String fromSQL   = " FROM " + dbName1 + "." + tableName + " AS t1 ";
                String joinSQL   = " JOIN ";
                String whereSQL  = " WHERE ";

                if (isColInList(naturalkeys, columnName)) {
                    rootLogger.info("Column " + columnName + " is different and is part of unique. ");

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
                                DBHelper.getInstance().printResultSet(rs, 10);
                            }
                        } catch (Exception e) {
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
                rootLogger.info("Table " + tableName + " : difference columns (after tolerance for rounding) are : "
                                + failedColsListStr);

                return false;
            }

            return true;
        }
    }

    public boolean compareNormalDBsWithSameSchema(String dbName1, String dbName2) {
        boolean bFinalResult = true;

        rootLogger.info("=================================================================");
        rootLogger.info("compareNormalDBsWithSameSchema: " + dbName1 + " and " + dbName2);
        rootLogger.info("=================================================================");

        // get table list
        try {
            ResultSet tableRS = DBHelper.getInstance().getTableRSList(dbName1);

            // first, try sum of crc with id
            rootLogger.info("\n++++++++++++++ Compare table from two databases using sum of crc +++++++++++++++\n");

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
                    rootLogger.info("============================================ Difference     ====================");

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

                    // use sum(" + sql_crccalc + "(machine_number)) to get the sum value for
                    // each column and compare the values to find columns with
                    // difference
                    // use the columns in group by clause
                    String compareSqlDiffColSelect1 = "Select ";
                    String compareSqlDiffColSelect2 = "Select ";

                    ///////////////////////////
                    // get column list
                    ResultSet columnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, tableName);

                    while (columnRS1.next()) {
                        String columnName = columnRS1.getString("COLUMN_NAME");

                        compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(" + sql_crccalc + columnName
                                                   + ")))),";
                        compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(" + sql_crccalc + columnName
                                                   + ")))),";
                    }

                    // remove the last ","
                    compareSqlDiffColSelect1 =
                        compareSqlDiffColSelect1.substring(0, compareSqlDiffColSelect1.lastIndexOf(","));
                    compareSqlDiffColSelect2 =
                        compareSqlDiffColSelect2.substring(0, compareSqlDiffColSelect2.lastIndexOf(","));

                    String sumSql1 = "",
                           sumSql2 = "";

                    sumSql1 = compareSqlDiffColSelect1 + " FROM " + dbName1 + ".dbo." + tableName + " ";
                    sumSql2 = compareSqlDiffColSelect2 + " FROM " + dbName2 + ".dbo." + tableName + " ";

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
                            columnDiffList = columnDiffList + " " + columnIndex;
                        }
                    }

                    rootLogger.info("Different column list is : " + columnDiffList);
                    rootLogger.info("Table : " + tableName + " different column list is : " + columnDiffList);
                    rootLogger.info("============================================ Difference End=====================");
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
     * Only compare the columns with same column name
     */
    public boolean compareSameColMasterDBs(String oldrevision, String newrevision, String dbName1, String dbName2) {
        boolean bResult = true;

        rootLogger.info("================================================================================");
        rootLogger.info("Comparing DBs: " + dbName1 + " and " + dbName2);
        rootLogger.info("================================================================================");

        // get table list
        List<String> idTrustTableList  = new Vector<String>();
        List<String> idfailedTableList = new Vector<String>();

        try {
            ResultSet tableRS = DBHelper.getInstance().getTableRSList(dbName1);

            // first, try sum of crc with id
            rootLogger.info("\n++++++++++ First Pass: do not exclude ID and do not replace natural key ++++++++\n");

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
            rootLogger.info("\n+++++++++++++++++++ Second Pass: exclude ID but do not replace natural key +++++\n");

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
            rootLogger.info("\n+++++++++++++++++ Third Pass: exclude ID and replace natural key +++++++++++++++\n");

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
            rootLogger.info("==========Empty: Table " + tableName + " is empty");

            // return bCompareResult;
        }

        try {

            // get column list
            ResultSet columnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, tableName);
            ResultSet columnRS2 = DBHelper.getInstance().getColumnRSList(dbName2, tableName);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select s.COL1, s.COL2, s.COL3 ...,
             * s1.COL1, s2.COL1 FROM dbName1.Table s left join dbName1.Table1 s1
             * on s.xxx = s1.xxx_xxx .... union all select s.COL1, s.COL2,
             * s.COL3 ..., s1.COL1, s2.COL1 FROM dbName2.Table s left join
             * dbName2.Table1 s1 on s.xxx = s1.xxx_xxx .... ) tmp group by col1,
             * col2, ...
             */
            String compareSqlUnionSelect = "SELECT MIN(dbName),";
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

                boolean inReplacedColList = false;

                for (int baseCol = 0; baseCol < baseColNameList.size(); baseCol++) {
                    if (columnName.equalsIgnoreCase(baseColNameList.get(baseCol))) {
                        inReplacedColList = true;

                        break;
                    }
                }

                if (inReplacedColList != true) {
                    if (compareSql1.isEmpty()) {
                        compareSql1 = "SELECT '" + dbName1 + "' as dbname, " + db1baseTableAlias + "." + columnName;
                        compareSql2 = "SELECT '" + dbName2 + "' as dbname, " + db2baseTableAlias + "." + columnName;
                    } else {
                        compareSql1 = compareSql1 + ", " + db1baseTableAlias + "." + columnName;
                        compareSql2 = compareSql2 + ", " + db2baseTableAlias + "." + columnName;
                    }

                    compareSqlUnionSelect = compareSqlUnionSelect + columnName + ",";
                }

                if (inReplacedColList != true) {
                    if (compareSqlGroupBy.isEmpty()) {
                        compareSqlGroupBy = "GROUP BY " + columnName;
                        compareSqlOrderBy = "Order BY " + columnName;
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

                joinTableSqls1 = " " + joinTableSqls1 + " LEFT JOIN " + dbName1 + ".dbo." + joinTableName + " "
                                 + foreignTableAlias1 + " ON " + foreignTableAlias1 + "." + joinForeignCol + " = "
                                 + db1baseTableAlias + "." + joinBaseCol;
                joinTableSqls2 = " " + joinTableSqls2 + " LEFT JOIN " + dbName2 + ".dbo." + joinTableName + " "
                                 + foreignTableAlias2 + " ON " + foreignTableAlias2 + "." + joinForeignCol + " = "
                                 + db2baseTableAlias + "." + joinBaseCol;

                StringTokenizer stnaturalkeys = new StringTokenizer(naturalkeys, " ,\t\n");

                // get same column name for joinTable
                String sameColumnList = "";

                // get column list
                ResultSet joincolumnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, joinTableName);
                ResultSet joincolumnRS2 = DBHelper.getInstance().getColumnRSList(dbName2, joinTableName);

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

                    rootLogger.info("sameColumnList is : " + sameColumnList);
                    rootLogger.info("unique Key column Name is : " + naturalkeyCol);

                    // check if the natural key in the current column list with
                    // same name
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
                        compareSqlOrderBy = "Order BY " + foreignColAliasPre + naturalkeyCol;
                    } else {
                        compareSqlGroupBy = compareSqlGroupBy + "," + foreignColAliasPre + naturalkeyCol;
                        compareSqlOrderBy = compareSqlOrderBy + "," + foreignColAliasPre + naturalkeyCol;
                    }
                }
            }

            if (compareSql1.isEmpty()) {
                joinColSqls1 = joinColSqls1.substring(joinColSqls1.indexOf(",") + 1);
                compareSql1  = "SELECT " + joinColSqls1 + compareSqlTail;
                joinColSqls2 = joinColSqls2.substring(joinColSqls2.indexOf(",") + 1);
                compareSql2  = "SELECT " + joinColSqls2 + compareSqlTail;
            } else {
                compareSql1 = compareSql1 + joinColSqls1 + compareSqlTail;
                compareSql2 = compareSql2 + joinColSqls2 + compareSqlTail;
            }

            compareSqlUnionSelect = compareSqlUnionSelect.substring(0, compareSqlUnionSelect.length() - 1);
            compareSqlWhole       = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + ".dbo."
                                    + tableName + " " + db1baseTableAlias + " " + joinTableSqls1 + " UNION ALL "
                                    + compareSql2 + " FROM " + dbName2 + ".dbo." + tableName + " " + db2baseTableAlias
                                    + " " + joinTableSqls2 + " ) tmp " + compareSqlGroupBy + " " + compareSqlCondition
                                    + " " + compareSqlOrderBy;
            rootLogger.info("Final SQL script for compare: " + compareSqlWhole);
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                rootLogger.info("==========FAILED: Comparing Table " + tableName + " Failed. ");
                rootLogger.info("============================================ Difference    =====================");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
                rootLogger.info("============================================ Difference End=====================");

                // print rows
            } else {
                rootLogger.info("==========SUCCEED: Comparing Table " + tableName + " Succeed.====================");
            }

            compareRS.close();
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + compareSqlWhole);
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
            rootLogger.info("==========Empty: Table " + tableName + " is empty");

            // return bCompareResult;
        }

        // get column list
        try {
            ResultSet columnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, tableName);
            ResultSet columnRS2 = DBHelper.getInstance().getColumnRSList(dbName2, tableName);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select sum(" + sql_crccalc + "(" + sql_concat + "('', COL1, COL2,
             * COL3 ...))) as crc FROM dbName1.Table union all select
             * sum(" + sql_crccalc + "(" + sql_concat + "('', COL1, COL2, COL3 ...))) as crc FROM
             * dbName2.Table ) tmp group by crc
             */
            String compareSqlUnionSelect = "SELECT *";

            // String compareSql1 = "SELECT SUM(" + sql_crccalc + "(" + sql_concat + "('' ";
            // String compareSql2 = "SELECT SUM(" + sql_crccalc + "(" + sql_concat + "('' ";
            // String compareSqlTail = "))) as crc";
            // String compareSqlGroupBy = "GROUP BY crc";
            String compareSql1         = "";
            String compareSql2         = "";
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

                // compareSql1 = compareSql1 + ", " + sql_ifnull + "(" + columnName
                // + ", 'NULL')";
                // compareSql1 = compareSql1 + ", " + "'" + separator + "'";
                //
                // compareSql2 = compareSql2 + ", " + sql_ifnull + "(" + columnName
                // + ", 'NULL')";
                // compareSql2 = compareSql2 + ", " + "'" + separator + "'";
                if (colIndex == 0) {

                    // compareSql2 = compareSql2 + ", ";
                    compareSqlGroupBy = compareSqlGroupBy + columnName;
                } else {

                    // compareSql2 = compareSql2 + ", ";
                    compareSqlGroupBy = compareSqlGroupBy + ", " + columnName;
                }

                if (compareSql1.isEmpty()) {
                    compareSql1 = "SELECT " + columnName;
                    compareSql2 = "SELECT " + columnName;
                } else {
                    compareSql1 = compareSql1 + ", " + columnName;
                    compareSql2 = compareSql2 + ", " + columnName;
                }

                colIndex++;
            }

            // priKeyIndex = 0;
            compareSql1     = compareSql1 + compareSqlTail;
            compareSql2     = compareSql2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + ".dbo."
                              + tableName + " UNION ALL " + compareSql2 + " FROM " + dbName2 + ".dbo." + tableName
                              + ") tmp " + compareSqlGroupBy + " " + compareSqlCondition;
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                rootLogger.info("==========FAILED: Comparing Table " + tableName + " Failed. Excluding ID flag as: "
                                + excludeID);
                rootLogger.info("============================================ Difference   =========");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
                rootLogger.info("============================================ Difference End========");

                // print rows
            } else {
                rootLogger.info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + compareSqlWhole);
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
            rootLogger.info("==========Empty: Table " + tableName + " is empty");

            // return bCompareResult;
        }

        try {

            // get column list
            ResultSet columnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, tableName);
            ResultSet columnRS2 = DBHelper.getInstance().getColumnRSList(dbName2, tableName);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select s.COL1, s.COL2, s.COL3 ...,
             * s1.COL1, s2.COL1 FROM dbName1.Table s left join dbName1.Table1 s1
             * on s.xxx = s1.xxx_xxx .... union all select s.COL1, s.COL2,
             * s.COL3 ..., s1.COL1, s2.COL1 FROM dbName2.Table s left join
             * dbName2.Table1 s1 on s.xxx = s1.xxx_xxx .... ) tmp group by col1,
             * col2, ...
             */
            String compareSqlUnionSelect = "SELECT MIN(dbName),";
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
            while (columnRS1.next()) {

                // go through each column
                // primary key
                String columnKey = columnRS1.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    continue;
                }

                String columnName = columnRS1.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName)) {
                    continue;
                }

                boolean inReplacedColList = false;

                for (int baseCol = 0; baseCol < baseColNameList.size(); baseCol++) {
                    if (columnName.equalsIgnoreCase(baseColNameList.get(baseCol))) {
                        inReplacedColList = true;

                        break;
                    }
                }

                if (inReplacedColList != true) {
                    if (compareSql1.isEmpty()) {
                        compareSql1 = "SELECT '" + dbName1 + "' as dbname, " + db1baseTableAlias + "." + columnName;
                        compareSql2 = "SELECT '" + dbName2 + "' as dbname, " + db2baseTableAlias + "." + columnName;
                    } else {
                        compareSql1 = compareSql1 + ", " + db1baseTableAlias + "." + columnName;
                        compareSql2 = compareSql2 + ", " + db2baseTableAlias + "." + columnName;
                    }

                    compareSqlUnionSelect = compareSqlUnionSelect + columnName + ",";
                }

                if (inReplacedColList != true) {
                    if (compareSqlGroupBy.isEmpty()) {
                        compareSqlGroupBy = "GROUP BY " + columnName;
                        compareSqlOrderBy = "Order BY " + columnName;
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

                joinTableSqls1 = " " + joinTableSqls1 + " LEFT JOIN " + dbName1 + ".dbo." + joinTableName + " "
                                 + foreignTableAlias1 + " ON " + foreignTableAlias1 + "." + joinForeignCol + " = "
                                 + db1baseTableAlias + "." + joinBaseCol;
                joinTableSqls2 = " " + joinTableSqls2 + " LEFT JOIN " + dbName2 + ".dbo." + joinTableName + " "
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
                        compareSqlOrderBy = "Order BY " + foreignColAliasPre + naturalkeyCol;
                    } else {
                        compareSqlGroupBy = compareSqlGroupBy + "," + foreignColAliasPre + naturalkeyCol;
                        compareSqlOrderBy = compareSqlOrderBy + "," + foreignColAliasPre + naturalkeyCol;
                    }
                }
            }

            if (compareSql1.isEmpty()) {
                joinColSqls1 = joinColSqls1.substring(joinColSqls1.indexOf(",") + 1);
                compareSql1  = "SELECT " + joinColSqls1 + compareSqlTail;
                joinColSqls2 = joinColSqls2.substring(joinColSqls2.indexOf(",") + 1);
                compareSql2  = "SELECT " + joinColSqls2 + compareSqlTail;
            } else {
                compareSql1 = compareSql1 + joinColSqls1 + compareSqlTail;
                compareSql2 = compareSql2 + joinColSqls2 + compareSqlTail;
            }

            compareSqlUnionSelect = compareSqlUnionSelect.substring(0, compareSqlUnionSelect.length() - 1);
            compareSqlWhole       = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + ".dbo."
                                    + tableName + " " + db1baseTableAlias + " " + joinTableSqls1 + " UNION ALL "
                                    + compareSql2 + " FROM " + dbName2 + ".dbo." + tableName + " " + db2baseTableAlias
                                    + " " + joinTableSqls2 + " ) tmp " + compareSqlGroupBy + " " + compareSqlCondition
                                    + " " + compareSqlOrderBy;
            rootLogger.info("Final SQL script for compare: " + compareSqlWhole);
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                rootLogger.info("==========FAILED: Comparing Table " + tableName + " Failed. ");
                rootLogger.info("============================================ Difference =======");
                compareRS.first();

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
                DBHelper.getInstance().printResultSet(compareRS, 10);

                // use sum(" + sql_crccalc + "(machine_number)) to get the sum value for each
                // column and compare the values to find columns with difference
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
                        compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(" + sql_crccalc + column1 + ")))),";

                        String column2 = compareSql2.substring(0, colIndex2);

                        column2                  = column2.substring(column2.lastIndexOf(",") + 1);
                        compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(" + sql_crccalc + column2 + ")))),";
                    } else {
                        compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(" + sql_crccalc + column + ")))),";
                        compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(" + sql_crccalc + column + ")))),";
                    }
                }

                // remove the last ","
                compareSqlDiffColSelect1 =
                    compareSqlDiffColSelect1.substring(0, compareSqlDiffColSelect1.lastIndexOf(","));
                compareSqlDiffColSelect2 =
                    compareSqlDiffColSelect2.substring(0, compareSqlDiffColSelect2.lastIndexOf(","));

                String sumSql1 = compareSqlDiffColSelect1 + " FROM " + dbName1 + ".dbo." + tableName + " "
                                 + db1baseTableAlias + " " + joinTableSqls1;
                String sumSql2 = compareSqlDiffColSelect2 + " FROM " + dbName2 + ".dbo." + tableName + " "
                                 + db2baseTableAlias + " " + joinTableSqls2;
                ResultSet         sumColumnsRS1  = DBHelper.getInstance().executeSQL(sumSql1);
                ResultSet         sumColumnsRS2  = DBHelper.getInstance().executeSQL(sumSql2);
                String            columnDiffList = "";
                ResultSetMetaData rsmd           = sumColumnsRS1.getMetaData();
                int               columnTotal    = rsmd.getColumnCount();

                sumColumnsRS1.next();
                sumColumnsRS2.next();

                String          tempSql = sumSql1.replace(")))),", "@");
                StringTokenizer st      = new StringTokenizer(tempSql, "@");

                for (int columnIndex = 1; columnIndex <= columnTotal; columnIndex++) {
                    String colName = st.nextToken();

                    colName = colName.substring(colName.lastIndexOf(","));

                    double tempColValue1 = sumColumnsRS1.getDouble(columnIndex);
                    double tempColValue2 = sumColumnsRS2.getDouble(columnIndex);

                    if (tempColValue1 != tempColValue2) {

//                      String colName = "columnIndex" + columnIndex;
                        columnDiffList = columnDiffList + " " + colName;
                    }
                }

                rootLogger.info("Different column list is : " + columnDiffList);
                rootLogger.info("Tabl : " + tableName + " different column list is : " + columnDiffList);
                rootLogger.info("============================================ Difference End=====");

                // print rows
            } else {
                rootLogger.info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

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
            rootLogger.info("==========Empty: Table " + tableName + " is empty");

            // return bCompareResult;
        }

        try {

            // get column list
            ResultSet columnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, tableName);
            ResultSet columnRS2 = DBHelper.getInstance().getColumnRSList(dbName2, tableName);

            /**
             * k Compose the compare sql script The final format will be like
             * below: select * from ( select sum(" + sql_crccalc + "(" + sql_concat + "('', s.COL1,
             * s.COL2, s.COL3 ..., s1.COL1, s2.COL1))) as crc FROM dbName1.Table
             * s left join dbName1.Table1 s1 on s.xxx = s1.xxx_xxx .... union
             * all select sum(" + sql_crccalc + "(" + sql_concat + "('', s.COL1, s.COL2, s.COL3 ...,
             * s1.COL1, s2.COL1))) as crc FROM dbName2.Table s left join
             * dbName2.Table1 s1 on s.xxx = s1.xxx_xxx .... ) tmp group by crc
             */
            String compareSqlUnionSelect = "SELECT *";
            String compareSql1           = "SELECT SUM(" + sql_crccalc + "(" + sql_concat + "('' ";
            String compareSql2           = "SELECT SUM(" + sql_crccalc + "(" + sql_concat + "('' ";
            String joinColSqls1          = "";
            String joinTableSqls1        = "";
            String joinColSqls2          = "";
            String joinTableSqls2        = "";
            String compareSqlTail        = "))))) as crc";
            String compareSqlGroupBy     = "GROUP BY crc";
            String compareSqlCondition   = "HAVING COUNT(*) = 1";
            int    priKeyIndex           = 0;

            while (columnRS1.next()) {

                // go through each column
                String columnName1 = columnRS1.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName1)) {
                    continue;
                }

                // skip the first primary key
                String columnKey = columnRS1.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    continue;
                }

                compareSql1 = compareSql1 + ", " + sql_ifnull + baseTableAlias + "." + columnName1 + "), 'NULL')";
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

                compareSql2 = compareSql2 + ", " + sql_ifnull + baseTableAlias + "." + columnName2 + "), 'NULL')";
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

                    joinColSqls1 = joinColSqls1 + ", " + sql_ifnull + foreignTableAlias + "." + naturalkeyCol
                                   + "), 'NULL')";
                    joinColSqls1 = joinColSqls1 + ", " + "'" + separator + "'";
                    joinColSqls2 = joinColSqls2 + ", " + sql_ifnull + foreignTableAlias + "." + naturalkeyCol
                                   + ", 'NULL')";
                    joinColSqls2 = joinColSqls2 + ", " + "'" + separator + "'";
                }
            }

            compareSql1     = compareSql1 + joinColSqls1 + compareSqlTail;
            compareSql2     = compareSql2 + joinColSqls2 + compareSqlTail;
            compareSqlWhole = compareSqlUnionSelect + " FROM ( " + compareSql1 + " FROM " + dbName1 + "." + tableName
                              + " " + baseTableAlias + " " + joinTableSqls1 + " UNION ALL " + compareSql2 + " FROM "
                              + dbName2 + "." + tableName + " " + baseTableAlias + " " + joinTableSqls2 + " ) tmp "
                              + compareSqlGroupBy + " " + compareSqlCondition;
            rootLogger.info("Final SQL script for compare: " + compareSqlWhole);
            columnRS1.close();
            columnRS2.close();

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                rootLogger.info("==========FAILED: Comparing Table " + tableName + " Failed. ");
                rootLogger.info("============================================ Difference    =====");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
                rootLogger.info("============================================ Difference End======");

                // print rows
            } else {
                rootLogger.info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

    public boolean compareTableWithoutNKey(String dbName1, String dbName2, String tableName, boolean excludeID) {
        boolean bCompareResult  = true;
        String  compareSqlWhole = "";

        if (DBHelper.getInstance().isTableEmpty(dbName1, tableName)
                && DBHelper.getInstance().isTableEmpty(dbName2, tableName)) {
            rootLogger.info("==========Empty: Table " + tableName + " is empty");

            return bCompareResult;
        }

        try {

            // get column list
            ResultSet columnRS = DBHelper.getInstance().getColumnRSList(dbName1, tableName);

//          ResultSet columnRS2 = DBHelper.getInstance().getColumnRSList(dbName2, tableName);
            String columnList = "";

//          String columnList2 = "";
            int priKeyIndex = 0;

            while (columnRS.next()) {

                // go through each column
                String columnName = columnRS.getString("COLUMN_NAME");

                if (inColumnFilterList(columnName)) {
                    continue;
                }

                // primary key
                String columnKey = columnRS.getString("COLUMN_KEY");

                if (columnKey.equals("PRI") && (priKeyIndex == 0)) {
                    priKeyIndex++;

                    if (excludeID) {
                        continue;
                    }
                }

                if (columnList.isEmpty()) {
                    columnList = columnName;

//                  columnList2 = columnName1;
                } else {
                    columnList = columnList + ", " + columnName;
                }
            }

            // SELECT CHECKSUM_AGG(CHECKSUM(*)) from kdw8501_cur_m.dbo.dwcorrugatorproductionorders
            compareSqlWhole = "SELECT * FROM (";
            compareSqlWhole = compareSqlWhole + "SELECT CHECKSUM_AGG(CHECKSUM(" + columnList + ")) as crc from "
                              + dbName1 + ".dbo." + tableName;
            compareSqlWhole = compareSqlWhole + " UNION ALL ";
            compareSqlWhole = compareSqlWhole + "SELECT CHECKSUM_AGG(CHECKSUM(" + columnList + ")) as crc  from "
                              + dbName2 + ".dbo." + tableName;
            compareSqlWhole = compareSqlWhole + ") tmp GROUP BY crc HAVING count(*) = 1";

            ResultSet compareRS = DBHelper.getInstance().executeSQL(compareSqlWhole);

            if (compareRS.next()) {
                bCompareResult = false;
                rootLogger.info("==========FAILED: Comparing Table " + tableName + " Failed. Excluding ID flag as: "
                                + excludeID);
                rootLogger.info(
                    "============================================ Difference ============================================");
                compareRS.first();

                // DBHelper.getInstance().printResultSet(compareRS);
                rootLogger.info(
                    "============================================ Difference End=========================================");

                // print rows
            } else {
                rootLogger.info("==========SUCCEED: Comparing Table " + tableName + " Succeed.");
            }

            compareRS.close();
        } catch (Exception e) {
            rootLogger.info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();
            bCompareResult = false;
        }

        return bCompareResult;
    }

    protected void fillInfo4KeyReplacement(List<String> tableNameList, List<String> srcColNameList,
                                           List<String> foreignColNameList, List<String> naturalkeyList,
                                           String revision, String dbName, String tableName, String naturalkey,
                                           List<String> idTrustTableList) {
        try {

            // get foreign key constrains
            ResultSet rsForeignKeys = DBHelper.getInstance().getForgenKeyRSList(dbName, tableName);

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
        try {

            // get foreign key constrains
            ResultSet rsForeignKeys = DBHelper.getInstance().getForgenKeyRSList(dbName, tableName);

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

//      LogHelper.initializeSummaryLog("compare");
//      LogHelper.initializeTestcaseLog(new CompSchema4FreshUpgrade());
        // String sql = "select a.trim as atrim, b.trim as btrim from "
        // + "kdw_kdw821_kdwmapreport_0.csc_hc_production_report_map_1 a"
        // + " join kdw_kdw821_mapreport_0.csc_hc_production_report_map_1 b "
        // + "on a.prog_num = b.prog_num"
        // + " where a.total_area - b.total_area = 0"
        // + " and a.trim - b.trim > 1";
        // DBHelper.getInstance().connectDB();
        //
        // ResultSet rs = null;
        // try {
        // rs = DBHelper.getInstance().executeSQL(sql);
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        //
        // try {
        // DBHelper.getInstance().printResultSet(rs, false);
        // } catch (SQLException e) {
        // e.printStackTrace();
        // }
        // CompareDBs.getInstance().compareSameColMasterDBs("kdw-8.10", "kdw-8.31",
        // "kdw_kdw831_ipuat_1_old_masterDW",
        // "kdw_kdw831_ipuat_1_new_masterDW");
//            CompareDBs.getInstance().compareNormalDBsWithSameSchema(
//                            "java03_masterDW",
//                            "kdw8501_cur_m");
//            CompareDBs.getInstance().compareNormalDBsWithSameSchema(
//                            "java03_masterDW",
//                            "java02_masterDW");
        CompareDBs.getInstance().compareMasterDBs("kdw-8.50.1", "java01_masterDW", "java02_masterDW");

//      CompareDBs.getInstance().compareSameColMasterDBs("kdw-8.50.1", "kdw-8.50.1",
//      "kdw8501_cur_m",
//      "kdw8501_cur_m");
    }

    public static String getColumnFilterList() {
        return columnFilterList + " " + globalColumnFilterList;
    }

    public static void setColumnFilterList(String columnFilterList) {
        rootLogger.info("columnFilterList set as : " + columnFilterList);
        CompareDBs.columnFilterList = columnFilterList;
    }

    protected boolean getDifferentColCompareNormalDBsWithSameSchema(String dbName1, String dbName2,
                                                                    List<String> differentColumnList,
                                                                    Map<String, String> starlenMap) {
        boolean bFinalResult = true;

        rootLogger.info("====================================================================");
        rootLogger.info("compareNormalDBsWithSameSchema: " + dbName1 + " and " + dbName2);
        rootLogger.info("=====================================================================");

        // get table list
        try {
            ResultSet tableRS = DBHelper.getInstance().getTableRSList(dbName1);

            // first, try sum of crc with id
            rootLogger.info("\n\n++++++++++++++ Compare table from two databases using sum of crc ++++++++++++++\n\n");

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
                    rootLogger.info("=========================================== Difference   ======");

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

                    // use sum(" + sql_crccalc + "(machine_number)) to get the sum value for
                    // each column and compare the values to find columns with
                    // difference
                    // use the columns in group by clause
                    String compareSqlDiffColSelect1 = "Select ";
                    String compareSqlDiffColSelect2 = "Select ";

                    // get column list
                    ResultSet columnRS1 = DBHelper.getInstance().getColumnRSList(dbName1, tableName);

                    while (columnRS1.next()) {
                        String columnName = columnRS1.getString("COLUMN_NAME");
                        String starlen    = starlenMap.get("COLUMN_NAME");

                        if ((starlen != null) &&!starlen.isEmpty()) {
                            compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(" + sql_crccalc + "(IF( length("
                                                       + columnName + ")>" + Integer.parseInt(starlen) + ", '****', "
                                                       + columnName + ")))),";
                            compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(" + sql_crccalc + "(IF( length("
                                                       + columnName + ")>" + Integer.parseInt(starlen) + ", '****', "
                                                       + columnName + ")))),";
                        } else {
                            compareSqlDiffColSelect1 = compareSqlDiffColSelect1 + "sum(" + sql_crccalc + columnName
                                                       + ")))),";
                            compareSqlDiffColSelect2 = compareSqlDiffColSelect2 + "sum(" + sql_crccalc + columnName
                                                       + ")))),";
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

                            colName        = colName.substring(("sum(" + sql_crccalc).length(), colName.length() - 2);
                            columnDiffList = columnDiffList + " " + colName;
                            differentColumnList.add(colName);
                        }
                    }

                    rootLogger.info("Different column list is : " + columnDiffList);
                    rootLogger.info("Tabl : " + tableName + " different column list is : " + columnDiffList);
                    rootLogger.info(
                        "============================================ Difference End=========================================");
                }
            }

            tableRS.close();
        } catch (Exception e) {
            e.printStackTrace();
            bFinalResult = false;
        }

        return bFinalResult;
    }

    public static String getTableFilterList() {
        return tableFilterList;
    }

    public static void setTableFilterList(String tableFilterList) {
        rootLogger.info("TableFilterList set as : " + tableFilterList);
        CompareDBs.tableFilterList = tableFilterList;
    }
}
