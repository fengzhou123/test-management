package kiwiplan.kdw.batches;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.utils.OSUtil;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.transform.Result;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.sql.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import com.jcraft.jsch.*;
import org.w3c.dom.ls.LSOutput;

public class EspDatabaseRestoration {

    public static void main(String[] args) {

        String mssqlServerDBType = ConfigHelper.getInstance().getConfig("MSSQL_DBTYPE");
        String mssqlServerHostname = ConfigHelper.getInstance().getConfig("MSSQL_HOSTNAME");    // $NON-NLS-1$
        String mssqlServerPort = ConfigHelper.getInstance().getConfig("MSSQL_PORT");
        String mssqlServerUsername = ConfigHelper.getInstance().getConfig("MSSQL_USERNAME");      // $NON-NLS-1$
        String mssqlServerPassword = ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD");    // $NON-NLS-1$
        String espDBName = "esp_auto_restoration";
        String bakFileLocation = "N:\\9.60.1_WIP\\teststora.bak";

        String mysqlServerDBType = ConfigHelper.getInstance().getConfig("MYSQL_DBTYPE");
        String mysqlServerHostname = ConfigHelper.getInstance().getConfig("MYSQL_HOSTNAME");    // $NON-NLS-1$
        String mysqlServerPort = ConfigHelper.getInstance().getConfig("MYSQL_PORT");
        String mysqlServerUsername = ConfigHelper.getInstance().getConfig("MYSQL_USERNAME");      // $NON-NLS-1$
        String mysqlServerPassword = ConfigHelper.getInstance().getConfig("MYSQL_PASSWORD");

        String kdwHostname = "nzvader";
        String kdwUsername = "java23";
        String kdwPassword = "mapadm99";
        String baseDirectory = "/javaservice/java23";
        String installerDirectory = "/javaservice/java23/installers";
        String siteIdentifier = "kdw_auto_upgrade";
        String dbType = "MSSQL";
        String dbHostname = "nzkdwsql19";
        String dbPort = "1433";
        String dbUsername = "sa";
        String dbPassword = "k1w1116!";
        String dbPasswordEncrypted = "mPNslaNpwOQExJfWE1chTg==";
        String jenkinsHostname = "nzjenkins3";
        String jenkinsUsername = "installers";
        String jenkinsPassword = "installers";
        String javaHome = "/javaservice/java1.11.0";
        String baseKdwVersion = "9.54.1";
        String baseKdwBuild = "57";
        String upgradeKdwVersion = "9.60.1";
        String upgradeKdwBuild = "23";
        String kdwAdminConsoleHostname = "nzvader";
        String kdwAdminConsolePort = "8443";

        String kdwInstallPath = "/javaservice/java23/services/etl/java23_9601025";

        try {

//            restoreEspDatabase(sqlServerDbType, sqlServerHostname, sqlServerDbPort, sqlServerUsername,
//                               sqlServerPassword, espDBName, bakFileLocation);

//            upgradeKDW(kdwHostname, kdwUsername, kdwPassword, baseDirectory, installerDirectory, siteIdentifier,
//                       dbType, dbHostname, dbPort, dbUsername, dbPassword, dbPasswordEncrypted, jenkinsHostname,
//                       jenkinsUsername, jenkinsPassword, javaHome, baseKdwVersion, baseKdwBuild, upgradeKdwVersion,
//                       upgradeKdwBuild);

//            InvokeTasksViaSchedulerAndExecutor(kdwHostname, kdwUsername, kdwPassword, kdwInstallPath,
//                    kdwAdminConsoleHostname, kdwAdminConsolePort);

            String mysqlDBName1 = "java23_9721023_master_datawarehouse";
            String mysqlDBName2 = "java23_9721027_master_datawarehouse";

            String mssqlDBName1 = "java27_9711038_master_datawarehouse";
            String mssqlDBName2= "java27_9711038a_master_datawarehouse";

            DBHelper.getInstance().connectDB();

//            compareDatabaseSchema(mysqlServerDBType, mysqlServerHostname, mysqlServerPort, mysqlServerUsername, mysqlServerPassword, mysqlDBName1, mysqlDBName2);

//            compareTableSchemasInDatabase(mssqlServerDBType, mssqlServerHostname, mssqlServerPort, mssqlServerUsername, mssqlServerPassword, mssqlDBName1, mssqlDBName2);

            String mysqlDbName1 = "java23_9721042_master_datawarehouse";
            String mysqlDbName2 = "java23_9721042a_master_datawarehouse";
            String mysqlTableName = "sch_task_group";

            String mssqlDbName1 = "java27_9711038_master_datawarehouse";
            String mssqlDbName2 = "java27_9711038a_master_datawarehouse";
            String mssqlTableName = "sch_job";

            String masterDBName = "java25_9721upgms" + "_master_datawarehouse";

            Connection conn = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);
            String tasksSql = "select count(*) as numOfRecords from [" + masterDBName + "].[dbo].[sch_task_queue]";
            ResultSet taskRS = EspDatabaseRestoration.executeSQL(conn, tasksSql);

            taskRS.next();
            System.out.println("tasksSql "+ taskRS.getString("numOfRecords"));

            /*
            Connection conn = EspDatabaseRestoration.getConnection(mysqlServerDBType, mysqlServerHostname, mysqlServerPort, mysqlServerUsername, mysqlServerPassword);

            compareTableData(conn, mysqlServerDBType, mysqlDbName1, mysqlDbName2, mysqlTableName);

             */

//            compareDatabaseData(mysqlServerDBType, mysqlServerHostname, mysqlServerPort, mysqlServerUsername, mysqlServerPassword, mysqlDbName1, mysqlDbName2);

            /*
            Connection conn = EspDatabaseRestoration.getConnection(mysqlServerDBType, mysqlServerHostname, mysqlServerPort, mysqlServerUsername, mysqlServerPassword);

            System.out.println(compareTableData(conn, mysqlServerDBType, mysqlDbName1, mysqlDbName2, mysqlTableName).toString());

             */

//            compareDatabaseData(mysqlServerDBType, mysqlServerHostname, mysqlServerPort, mysqlServerUsername, mysqlServerPassword, mysqlDbName1, mysqlDbName2);

            /*
            Connection conn = EspDatabaseRestoration.getConnection(mssqlServerDBType, mssqlServerHostname, mssqlServerPort, mssqlServerUsername, mssqlServerPassword);

            compareTableData(conn, mssqlServerDBType, mssqlDbName1, mssqlDbName2, mssqlTableName);

             */



        } catch (Exception e) {

            e.printStackTrace();

        }

    }

    public static void startSchedulerAndExecutor(String kdwHostname, String kdwUsername, String kdwPassword) {

        String schedulerStartCmd = "$PENTAHODIR/kiwiplan/bin/startScheduler.sh";
        String executorStartCmd = "$PENTAHODIR/kiwiplan/bin/startExecutor.sh";
        String checkSchedulerStatusCmd = "grep \"Started Application\" -i " + "$PENTAHODIR/kiwiplan/logs/scheduler.log;echo $?";

        OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, schedulerStartCmd);

        OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, executorStartCmd);

    }



    /*
    public static void InvokeTasksViaSchedulerAndExecutor(String kdwHostname, String kdwUsername, String kdwPassword,
                                                          String kdwInstallPath, String kdwAdminConsoleHostname,
                                                          String kdwAdminConsolePort) throws Exception{

        String adminConsoleAccessCmd = "curl -k https://nzvader:8443/";
        String executorStartCmd = kdwInstallPath + "/current/kiwiplan/bin/startExecutor.sh";
        int sshPort = 22;
        int timeOut = 5;
        int secsToSleep = 1;
        int totalSleepTime = 0;
//        String remoteCmdRunSuccessCode = 0;
        String remoteCmdRunOutput;

//        OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, schedulerStartCmd);

        String startingSchedulerCmd = kdwInstallPath + "/current/kiwiplan/bin/startScheduler.sh";
        String schedulerProcessStatusCmd = "PROCESS_STR=$(" + kdwInstallPath + "/current/kiwiplan/bin/listRunningKDWServices.sh | grep java23);" +
                                           "PROCESS_ID=$(echo ${PROCESS_STR} | cut -d' ' -f1);" +
                                           "PROCESS_LISTEN_STATUS=$(netstat -atnp | grep -i \"LISTEN      ${PROCESS_ID}/java\");echo $?";

        checkStatusPeriodicallyAfterIssuingCommand (kdwHostname, kdwUsername, kdwPassword, kdwInstallPath, kdwAdminConsoleHostname,
                                                    kdwAdminConsolePort, startingSchedulerCmd, schedulerProcessStatusCmd);

        runCommandRemotely(kdwHostname, sshPort, kdwUsername, kdwPassword, executorStartCmd);

    }

    public static void checkStatusPeriodicallyAfterIssuingCommand (String kdwHostname, String kdwUsername, String kdwPassword,
                                                                   String kdwInstallPath, String kdwAdminConsoleHostname,
                                                                   String kdwAdminConsolePort, String cmd, String statusCheckCmd) throws Exception {

        int sshPort = 22;
        int timeOut = 5;
        int secsToSleep = 1;
        int totalSleepTime = 0;

        runCommandRemotely(kdwHostname, sshPort, kdwUsername, kdwPassword, cmd);

        while (true) {

            if (totalSleepTime > timeOut) {

                throw new Exception("Scheduler doesn't start properly before timeout");

            } else {

                String schedulerProcessStatusCmdOutput = runCommandRemotely(kdwHostname, sshPort, kdwUsername, kdwPassword, statusCheckCmd);
                System.out.println("schedulerProcessStatusCmdOutput is " + schedulerProcessStatusCmdOutput);

                if (schedulerProcessStatusCmdOutput.contains("0")) {

                    break;

                } else {

                    TimeUnit.SECONDS.sleep(secsToSleep);
                    totalSleepTime += secsToSleep;

                }

            }

        }

    }
     */

    public static boolean upgradeKDW(String kdwHostname, String kdwUsername, String kdwPassword, String baseDirectory,
                                     String installerDirectory, String siteIdentifier, String dbType, String dbHostname,
                                     String dbPort, String dbUsername, String dbPasswordEncrypted,
                                     String jenkinsHostname, String jenkinsUsername, String jenkinsPassword, String javaHome,
                                     String upgradeKdwRevision, String upgradeKdwBuild) {


        String kdwUpgradeCmd = "export BASE_DIRECTORY=" + baseDirectory + ";export INSTALLER_DIRECTORY=" + installerDirectory + ";" +
                                    "export SITE_IDENTIFIER=" + siteIdentifier + ";export DATABASE_BRAND=" + dbType + ";" +
                                    "export DATABASE_HOST=" + dbHostname + ";export DATABASE_PORT=" + dbPort + ";" +
                                    "export DATABASE_USERNAME=" + dbUsername + ";export DATABASE_PASSWORD=" + dbPasswordEncrypted + ";" +
                                    "export JENKINS_SERVER=" + jenkinsHostname + ";export JENKINS_USERNAME=" + jenkinsUsername + ";" +
                                    "export JENKINS_PASSWORD=" + jenkinsPassword + ";export JAVA_HOME=" + javaHome + ";" +
                                    "cd /javaservice/java23/kdw_auto_upgrade_scripts/kdw-auto-install-upgrade;" +
                                    "./upgrade -p kdw -r " + upgradeKdwRevision + " -b " + upgradeKdwBuild;

        return OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, kdwUpgradeCmd);

    }

    public static void restoreEspDatabase(String dbType, String dbHostname, String dbPort, String dbUsername,
                                          String dbPassword, String espDBName, String bakFileLocation) throws Exception {

        Connection connection = getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);

        System.out.println("Drop the database " + espDBName + ", if it exists.");

        removeDatabase(connection, espDBName);

        System.out.println("Restore the database " + espDBName);

        String dbRestoreSql = "restore database " + espDBName + " from disk = '" + bakFileLocation + "'" +
                                " with move 'espbox2_Data' to 'E:\\Data\\" + espDBName + "'" +
                                ", move 'espbox2_Log' to 'F:\\Logs\\" + espDBName + "'";

        executeUpdate(connection, dbRestoreSql);

        System.out.println("Close DB connection.");

        closeDBConnection(connection);

    }

    public static Connection getConnection(String dbType, String dbHostname, String dbPort, String dbUsername, String dbPassword) throws Exception {

        String url = null;
        java.sql.Connection conn = null;

        if (dbType.equalsIgnoreCase("MYSQL")) {

            url = "jdbc:mysql://" + dbHostname + ":" + dbPort + "/" + "?allowMultiQueries=true";

        } else if (dbType.equalsIgnoreCase("MSSQL")) {

            url = "jdbc:sqlserver://" + dbHostname + ":" + dbPort;

        }

        System.out.println("URL is " + url);
        //Class.forName(dbDriver);
        conn = DriverManager.getConnection(url, dbUsername, dbPassword);

        return conn;

    }

    public static void closeDBConnection(Connection conn) throws Exception {

        if (conn != null && !conn.isClosed()) {

            conn.close();

        }

    }

    public static void removeDatabase(Connection conn, String dbName) throws Exception {

        System.out.println("Drop the database " + dbName + ", if it exists.");

        String dbDropSql = "drop database if exists " + dbName;

        executeUpdate(conn, dbDropSql);

    }

    public static ResultSet executeSQL(Connection conn, String sql) throws Exception {

        ResultSet rs = null;
        PreparedStatement stmt;

        System.out.println("SQL string: " + sql);

        stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery();

        return rs;
    }


    public static void executeUpdate(Connection conn, String sql) throws SQLException {

        Statement currentStatement = null;

        currentStatement = conn.createStatement();
        currentStatement.executeUpdate(sql);

    }


    public static List<Integer> compareTableData(Connection conn, String dbType, String dbName1, String dbName2, String tableName) {

        String idInitSql = "select @a:=0, @b:=0, @c:=0, @d:=0";
        String columnNamesSql;
        String dataComparisonSql;
        List<Integer> differentDataRowInOrderedTable = new ArrayList<>();

        try {

            columnNamesSql = (dbType.equalsIgnoreCase("MYSQL")) ?
                    "select column_name from information_schema.columns where table_schema= '" + dbName1 + "' and table_name = '" + tableName +
                            "' and EXTRA <> 'auto_increment'" :
                    "select * from " + dbName1 + ".information_schema.columns where table_name = '" + tableName + "' and ordinal_position <> '1'";


            ResultSet columnNamesRS = EspDatabaseRestoration.executeSQL(conn, columnNamesSql);
            String columnNameConcatStr = "";

            while (columnNamesRS.next()) {

                columnNameConcatStr += columnNamesRS.getString("column_name") + ", ";

            }

            columnNameConcatStr = columnNameConcatStr.substring(0, columnNameConcatStr.length() - 2);

            if (dbType.equalsIgnoreCase("MYSQL")) {

                dataComparisonSql = "select case when tbl_ordered_concat_union.concat1 = tbl_ordered_concat_union.concat2 then 'same' else 'different' " +
                        "end as comparison_result from " +
                        "(select * from " +
                        "(select @a:=@a+1 as id_a, CONCAT_WS('~', " + columnNameConcatStr + ") as concat1 from " + dbName1 + "." + tableName + " order by " +
                        "CONCAT_WS('~', " + columnNameConcatStr + ")) as tbl_ordered_concat1 left join " +
                        "(select @b:=@b+1 as id_b, CONCAT_WS('~', " + columnNameConcatStr + ") as concat2 from " + dbName2 + "." + tableName + " order by " +
                        "CONCAT_WS('~', " + columnNameConcatStr + ")) as tbl_ordered_concat2 on id_a = id_b " +
                        "union " +
                        "select * from " +
                        "(select @c:=@c+1 as id_c, CONCAT_WS('~', " + columnNameConcatStr + ") as concat3 from " + dbName1 + "." + tableName + " order by " +
                        "CONCAT_WS('~', " + columnNameConcatStr + ")) as tbl_ordered_concat3 right join " +
                        "(select @d:=@d+1 as id_d, CONCAT_WS('~', " + columnNameConcatStr + ") as concat4 from " + dbName2 + "." + tableName + " order by " +
                        "CONCAT_WS('~', " + columnNameConcatStr + ")) as tbl_ordered_concat4 on id_c = id_d " +
                        "where tbl_ordered_concat3.concat3 is null) as tbl_ordered_concat_union";

                EspDatabaseRestoration.executeSQL(conn, idInitSql);

            } else {

                dataComparisonSql = "select case when concat1 = concat2 then 'same' else 'different' end as comparison_result from " +
                        "(select row_number() over(order by CONCAT_WS('~', " + columnNameConcatStr + ")) as id_a, CONCAT_WS('~', " + columnNameConcatStr +
                        ") as concat1 from [" + dbName1 + "].[dbo].[" + tableName + "]) as tbl_ordered_concat1 " +
                        "full outer join " +
                        "(select row_number() over(order by CONCAT_WS('~', " + columnNameConcatStr + ")) as id_b, CONCAT_WS('~', " + columnNameConcatStr +
                        ") as concat2 from [" + dbName2 + "].[dbo].[" + tableName + "]) as tbl_ordered_concat2 on id_a = id_b";

            }

            ResultSet dataComparisonRS = EspDatabaseRestoration.executeSQL(conn, dataComparisonSql);

            int rowIndex = 1;

            while (dataComparisonRS.next()) {

                if (dataComparisonRS.getString("comparison_result").equalsIgnoreCase("different")) {

//                    System.out.println("Difference rowIndex: " + rowIndex);
                    differentDataRowInOrderedTable.add(rowIndex);

                }

                rowIndex++;

//                System.out.println(dataComparisonRS.getString("concat1") + " " + dataComparisonRS.getString("concat2"));

            }

        } catch (Exception e) {

//            getLogger().info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();

        }

        return differentDataRowInOrderedTable;

    }


    public static void compareDatabaseData(String dbType, String dbHostname, String dbPort,
                                           String dbUsername, String dbPassword,
                                           String dbName1, String dbName2) {


        String tableNamesDb1Sql;
        String tableNamesDb2Sql;
        List<String> sameDataTables = new ArrayList<String>();
        List<String> differentDataTables = new ArrayList<String>();
        Set<String> tableList = new HashSet<>();
        Connection conn = null;

        if (dbType.equalsIgnoreCase("MYSQL")) {

            tableNamesDb1Sql = "select table_name from information_schema.tables where table_schema='" + dbName1 + "' order by table_name";
            tableNamesDb2Sql = "select table_name from information_schema.tables where table_schema='" + dbName2 + "' order by table_name";

        } else {

            tableNamesDb1Sql = "select table_name from " + dbName1 + ".information_schema.tables order by table_name;";
            tableNamesDb2Sql = "select table_name from " + dbName2 + ".information_schema.tables order by table_name;";

        }

        try {

            conn = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);

            ResultSet tableNamesDb1RS = EspDatabaseRestoration.executeSQL(conn, tableNamesDb1Sql);
            ResultSet tableNamesDb2RS = EspDatabaseRestoration.executeSQL(conn, tableNamesDb2Sql);
            List<String> tableNamesDb1List = new ArrayList<String>();
            List<String> tableNamesDb2List = new ArrayList<String>();

            while (tableNamesDb1RS.next()) {

                tableNamesDb1List.add(tableNamesDb1RS.getString("table_name"));

            }

            while (tableNamesDb2RS.next()) {

                tableNamesDb2List.add(tableNamesDb2RS.getString("table_name"));

            }

            System.out.println("tableNamesDb1RS: " + tableNamesDb1List.toString());
            System.out.println("tableNamesDb2RS: " + tableNamesDb2List.toString());

            if (!tableNamesDb1List.toString().equals(tableNamesDb2List.toString())) {

                System.out.println("Two databases have different tables.");
                return;

            } else {

                System.out.println("Two databases have identical tables. Now compare data between each pair of tables.");

            }

            for (String tableName: tableNamesDb1List) {

                String tableRowCountDb1Sql = "select count(*) as numOfRows from " + dbName1 + "." + tableName;
                String tableRowCountDb2Sql = "select count(*) as numOfRows from " + dbName2 + "." + tableName;

                ResultSet tableRowCountDb1RS = EspDatabaseRestoration.executeSQL(conn, tableRowCountDb1Sql);
                ResultSet tableRowCountDb2RS = EspDatabaseRestoration.executeSQL(conn, tableRowCountDb2Sql);
                tableRowCountDb1RS.next();
                tableRowCountDb2RS.next();

                if (tableRowCountDb1RS.getString("numOfRows").equalsIgnoreCase("0") ||
                        tableRowCountDb2RS.getString("numOfRows").equalsIgnoreCase("0")) {

                    System.out.println("Please note in the table pair " + tableName + " at least one of the tables has no data");

                } else {

                    List<Integer> differentDataRowInOrderedTableList = compareTableData(conn, dbType, dbName1, dbName2, tableName);

                    if (differentDataRowInOrderedTableList.size() == 0) {

                        System.out.println("Two tables " + tableName + " have identical data");

                    } else {

                        System.out.println("Two tables " + tableName + " have some differences in data.");
                        System.out.println("The rows having difference in the ordered table are: " + differentDataRowInOrderedTableList.toString());

                    }

                }

                System.out.println("-------------------------------------------------\n");

            }

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            try {

                if(conn != null) conn.close();

            } catch (SQLException se) {

                se.printStackTrace();

            }

        }

    }

    public static boolean compareTableSchema(Connection conn, String dbType, String dbName1, String dbName2, String tableName) {

        boolean tblSchemaCompareResult = false;
        String inBothTblsColsSql;
        String inTbl1NotInTbl2ColsSql;
        String inTbl2NotInTbl1ColsSql;

        if (dbType.equalsIgnoreCase("MYSQL")) {

            inBothTblsColsSql = "select concat_ws(' ', column_name, column_type) as col_name_type from information_schema.columns where table_schema = '" +
                    dbName1 + "' and " + "table_name = '" + tableName + "' and concat_ws(' ', column_name, column_type) in (" +
                    "select concat_ws(' ', column_name, column_type) from information_schema.columns where table_schema= '" +
                    dbName2 + "' and " + "table_name = '" + tableName + "')";

            inTbl1NotInTbl2ColsSql = "select concat_ws(' ', column_name, column_type) as col_name_type from information_schema.columns where table_schema='" +
                    dbName1 + "' and " + "table_name = '" + tableName + "' and concat_ws(' ', column_name, column_type) not in (" +
                    "select concat_ws(' ', column_name, column_type) from information_schema.columns where table_schema= '" +
                    dbName2 + "' and " + "table_name = '" + tableName + "')";

            inTbl2NotInTbl1ColsSql = "select concat_ws(' ', column_name, column_type) as col_name_type from information_schema.columns where table_schema='" +
                    dbName2 + "' and " + "table_name = '" + tableName + "' and concat_ws(' ', column_name, column_type) not in (" +
                    "select concat_ws(' ', column_name, column_type) from information_schema.columns where table_schema= '" +
                    dbName1 + "' and " + "table_name = '" + tableName + "')";

        } else {

            inBothTblsColsSql = "select concat_ws(' ', column_name, data_type) as col_name_type from " + dbName1 + ".information_schema.columns where table_name = '" +
                    tableName + "' and concat_ws(' ', column_name, data_type) in (" +
                    "select concat_ws(' ', column_name, data_type) from " + dbName2 + ".information_schema.columns where table_name= '" + tableName + "')";

            inTbl1NotInTbl2ColsSql = "select concat_ws(' ', column_name, data_type) as col_name_type from " + dbName1 + ".information_schema.columns where table_name = '" +
                    tableName + "' and concat_ws(' ', column_name, data_type) not in (" +
                    "select concat_ws(' ', column_name, data_type) from " + dbName2 + ".information_schema.columns where table_name= '" + tableName + "')";

            inTbl2NotInTbl1ColsSql = "select concat_ws(' ', column_name, data_type) as col_name_type from " + dbName2 + ".information_schema.columns where table_name = '" +
                    tableName + "' and concat_ws(' ', column_name, data_type) not in (" +
                    "select concat_ws(' ', column_name, data_type) from " + dbName1 + ".information_schema.columns where table_name= '" + tableName + "')";

        }

        try {

            ResultSet inBothTblsColsResultSet = EspDatabaseRestoration.executeSQL(conn, inBothTblsColsSql);
            ResultSet inTbl1NotInTbl2ColsResultSet = EspDatabaseRestoration.executeSQL(conn, inTbl1NotInTbl2ColsSql);
            ResultSet inTbl2NotInTbl1ColsResultSet = EspDatabaseRestoration.executeSQL(conn, inTbl2NotInTbl1ColsSql);

            List<String> inBothTblsColsList = new ArrayList<String>();

            while (inBothTblsColsResultSet.next()) {

                inBothTblsColsList.add(inBothTblsColsResultSet.getString("col_name_type"));

            }

            System.out.println("The columns in both tables (" + tableName + "):");
            System.out.println(inBothTblsColsList.toString());

            List<String> inTbl1NotInTbl2ColsList = new ArrayList<String>();

            while (inTbl1NotInTbl2ColsResultSet.next()) {

                inTbl1NotInTbl2ColsList.add(inTbl1NotInTbl2ColsResultSet.getString("col_name_type"));

            }

            System.out.println("The columns in table1 but not in table2 (" + tableName + "):");
            System.out.println(inTbl1NotInTbl2ColsList.toString());

            List<String> inTbl2NotInTbl1ColsList = new ArrayList<String>();

            while (inTbl2NotInTbl1ColsResultSet.next()) {

                inTbl2NotInTbl1ColsList.add(inTbl2NotInTbl1ColsResultSet.getString("col_name_type"));

            }

            System.out.println("The column in table2 but not in table1 ("  + tableName + "):");
            System.out.println(inTbl2NotInTbl1ColsList.toString());

            if (!inBothTblsColsList.isEmpty() && inTbl1NotInTbl2ColsList.isEmpty() && inTbl2NotInTbl1ColsList.isEmpty()) {

                tblSchemaCompareResult = true;

            }


        } catch (Exception e) {

//            getLogger().info("Exception after executed SQL script: " + compareSqlWhole);
            e.printStackTrace();

        }

        return tblSchemaCompareResult;

    }

    public static void compareDatabaseSchema(String dbType, String dbHostname, String dbPort,
                                                String dbUsername, String dbPassword,
                                                String dbName1, String dbName2) {

        // get table list
        String selectingTblsSql;
        List<String> sameSchemaTables = new ArrayList<String>();
        List<String> differentSchemaTables = new ArrayList<>();
        Set<String> tableList = new HashSet<>();
        Connection conn = null;

        if (dbType.equalsIgnoreCase("MYSQL")) {

            selectingTblsSql = "select table_name from information_schema.tables where table_schema='" + dbName1 + "'";

        } else {

            selectingTblsSql = "select table_name from " + dbName1 + ".information_schema.tables;";

        }

        try {

            conn = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);

            ResultSet tableRS = EspDatabaseRestoration.executeSQL(conn, selectingTblsSql);

            while (tableRS.next()) {

                String tableName = tableRS.getString("table_name");

                boolean bResult = compareTableSchema(conn, dbType, dbName1, dbName2, tableName);

                if (bResult) {

                    sameSchemaTables.add(tableName);
//                    getLogger().info("Passed Table: " + tableName);

                } else {
//
//                    getLogger().info("Failed Table: " + tableName);
                    differentSchemaTables.add(tableName);

                }

            }

            System.out.println("The tables having identical schemas  in two databases: " + sameSchemaTables.toString());

            System.out.println("The tables having different schemas in two databases: " + differentSchemaTables.toString());

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            try {

                if(conn != null) conn.close();

            } catch (SQLException se) {

                se.printStackTrace();

            }

        }

    }




}