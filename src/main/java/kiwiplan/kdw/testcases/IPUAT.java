package kiwiplan.kdw.testcases;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;

import kiwiplan.kdw.core.*;
import kiwiplan.kdw.tasks.CompareDBs;
import kiwiplan.kdw.testcases.base.BaseTestcase;
import kiwiplan.kdw.utils.OSUtil;

public class IPUAT extends BaseTestcase {
    public IPUAT() {
        super("IP UAT");

        String text =
            "IP UAT Test. Query master DBs of ETL result of different revision to generate various tables then comppare them.";

        setTestcaseDescription(text);
    }

    private void createTablesForUATDatabase(String scriptsPath, String sourceDatabase, String targetDatabase,
                                            int testIndex) {

        /*
         * SET @EnterReportDate = '2014-03-01';
         * SET @PlantDataset = 'INTERNATIONAL PAPER - BOWLING GREEN, KY';
         * create table CorrugatorDailyPerformanceReportCSCHC (
         */
        File path = new File(scriptsPath);

        if (!path.exists()) {

            // path did not exist
            logger.severe("Path " + scriptsPath + " does not exist!");

            return;
        }

        // get script file list
        for (final File fileEntry : path.listFiles()) {

            // set parameters. Will move to config file later
            String sql = "use " + sourceDatabase + ";";

            // below variables come from IP's UAT scripts. Some scripts use same name variable but different meanning.
            // The scripts were updated accordingly. And original scripts are save in the "original" folder
            String enterReportDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                        testIndex,
                                                                                        "EnterReportDate");
            String enterReportPeriod = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                          testIndex,
                                                                                          "EnterReportPeriod");
            String enterNoShiftCode = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                         testIndex,
                                                                                         "EnterNoShiftCode");
            String plantDataset = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                     testIndex,
                                                                                     "PlantDataset");
            String period1StartDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                         testIndex,
                                                                                         "Period1StartDate");
            String period1EndDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                       testIndex,
                                                                                       "Period1EndDate");
            String reportStartDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                        testIndex,
                                                                                        "ReportStartDate");
            String reportEndDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                      testIndex,
                                                                                      "ReportEndDate");
            String enterStartDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                       testIndex,
                                                                                       "EnterStartDate");
            String enterEndDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                     testIndex,
                                                                                     "EnterEndDate");

            sql = sql + "SET @EnterReportPeriod = '" + enterReportPeriod + "';";    // Some script set the value as date, like '2014-03-01'
            sql = sql + "SET @EnterNoShiftCode = '" + enterNoShiftCode + "';";
            sql = sql + "SET @PlantDataset = '" + plantDataset + "';";

            if (fileEntry.getName().equalsIgnoreCase("RollInventory")) {
                java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd");
                Calendar                   cal    = Calendar.getInstance();

                enterStartDate = reportStartDate = period1StartDate = enterReportDate = format.format(cal.getTime());
                cal.add(Calendar.DAY_OF_MONTH, 3);
                enterEndDate = reportEndDate = period1EndDate = format.format(cal.getTime());
            }

            sql = sql + "SET @EnterReportDate = '" + enterReportDate + "';";
            sql = sql + "SET @Period1StartDate = '" + period1StartDate + "';";
            sql = sql + "SET @Period1EndDate = '" + period1EndDate + "';";
            sql = sql + "SET @ReportStartDate = '" + reportStartDate + "';";
            sql = sql + "SET @ReportEndDate = '" + reportEndDate + "';";
            sql = sql + "SET @EnterStartDate = '" + enterStartDate + "';";
            sql = sql + "SET @EnterEndDate = '" + enterEndDate + "';";
            sql = sql + "create database if not exists " + targetDatabase + ";";
            sql = sql + "ALTER DATABASE " + targetDatabase + " CHARACTER SET utf8;";
            sql = sql + "drop table if exists " + targetDatabase + "." + fileEntry.getName() + ";";
            sql = sql + "create table if not exists ";
            sql = sql + targetDatabase + "." + fileEntry.getName() + " ";

            // read content from file
            try {
                String scriptContent = new String(Files.readAllBytes(Paths.get(scriptsPath + "/"
                                                                               + fileEntry.getName())));

                sql = sql + scriptContent;
            } catch (IOException e) {

                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            sql = sql + ";";

            try {
                DBHelper.getInstance().executeUpdateSQL(sql);
            } catch (Exception e) {

                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String sourceDatabase = "java10_masterDW";
        String targetDatabase = "kdw_auto_test1";
        String sql            = "use " + sourceDatabase + ";";

        sql = sql + "SET @EnterReportDate = '2014-03-01';";
        sql = sql + "SET @EnterReportPeriod = '2014-03-01';";
        sql = sql + "SET @EnterNoShiftCode = '805'";
        sql = sql + "SET @PlantDataset = 'INTERNATIONAL PAPER - BOWLING GREEN, KY';";
        sql = sql + "SET @Period1StartDate = '2014-03-01';";
        sql = sql + "SET @Period1EndDate = '2014-03-30';";
        sql = sql + "SET @ReportStartDate = '2014-03-01';";
        sql = sql + "SET @ReportEndDate = '2014-03-30';";
        sql = sql + "SET @EnterStartDate = '2014-03-01';";
        sql = sql + "SET @EnterEndDate = '2014-03-30';";
        sql = sql + "create database if not exists " + targetDatabase + ";";
        sql = sql + "create table if not exists ";
        sql = sql + targetDatabase + "." + "CorrugatorOrderAnalysisComparisonReport" + " (";

        // read content from file
        try {
            String scriptContent =
                new String(
                    Files.readAllBytes(
                        Paths.get(
                            "D:\\data\\kdw\\automation\\kdw-8.14.1\\testdata\\testcases\\ipuat\\kdw-7.90\\CorrugatorOrderAnalysisComparisonReport")));

            sql = sql + scriptContent;
        } catch (IOException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        sql = sql + ");";
        sql = sql.trim();

        try {
            System.out.println(sql);
            DBHelper.getInstance().executeUpdateSQL(sql);
            sql = "select * from " + targetDatabase + "." + "CorrugatorOrderAnalysisComparisonReport";
            DBHelper.getInstance().executeSQL(sql);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        logger.info("Start the sub testcase : " + testname);

        String runStartDate   = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "runstartdate");
        String runEndDate     = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "runenddate");
        String period         = runStartDate + " " + runEndDate;
        String siteconfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "siteconfigname");
        String kpsitefile     = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteconfigname + ".xml";
        String kpsitefilebase = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteconfigname + "_base.xml";
        String etlconfigname  = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "etlconfigname");
        String kpetlfile           = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + ".xml";
        String kpetlfilebase       = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + "_base.xml";
        String baseInstallerFolder = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                        testIndex,
                                                                                        "baseinstallerpath");
        String baseInstallerName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                      testIndex,
                                                                                      "baseinstallername");
        String baseInstallerPath = baseInstallerFolder + "/" + baseInstallerName;
        String baseRevision      = KDWHelper.getRevFromBuildName(baseInstallerName);
        String newInstallerName  = getLatestBuildName();
        String newRevision       = KDWHelper.getRevFromBuildName(newInstallerName);
        String newInstallerPath  = ConfigHelper.getInstance()
                                               .getConfig("BuildRootFolder") + "/" + newRevision + "/"
                                                                             + getLatestBuildName();

        // fresh install base build
        logger.info("Step1: Fresh install revision: " + baseInstallerName);

        if (KDWHelper.kdwFreshInstall(baseRevision, hostname, baseInstallerPath, username, password, true) == false) {
            logger.info("Failed to Install KDW revision : " + baseInstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        // run ETL to get base data
        logger.info("Step2: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(baseRevision, hostname, username, password, kpsitefilebase, kpetlfilebase, period) == false) {
            logger.info("Failed to run ETL " + period);    // $NON-NLS-1$
            logger.info("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // copy the ETL result for debugging purpose
        String dbnamebackold = "kdw_" + newRevision.replace(".",
                                                            "").replace("-",
                                                                        "") + "_"
                                                                            + getClass().getSimpleName().toLowerCase()
                                                                            + "_" + testIndex + "_old";

        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackold);
        backupLogs(dbnamebackold);

        Date             today       = Calendar.getInstance().getTime();
        SimpleDateFormat sdf         = new SimpleDateFormat("yyyy/MM/dd");
        String           todayPeriod = sdf.format(today) + " " + sdf.format(today);

        // check result
        logger.info("Step3: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.info("KDW ETL running failed. Period: " + period + " Installer: " + baseInstallerPath);
            logger.info("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // generate UAT DB and tables for base revision
        String uatReportDBBase = "kdw_" + newRevision.replace(".",
                                                              "").replace("-",
                                                                          "") + "_uat_reportdb_" + testIndex + "_old";
        String baseScriptsPath = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + baseRevision;

        createTablesForUATDatabase(baseScriptsPath, username + "_masterDW", uatReportDBBase, testIndex);

        // fresh install new revision
        logger.info("Step4: Fresh install new revision: " + newInstallerName);

        if (KDWHelper.kdwFreshInstall(newRevision, hostname, newInstallerPath, username, password, false) == false) {
            logger.info("Failed to install KDW revision : " + newInstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        // run ETL
        logger.info("Step5: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(newRevision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            logger.info("Failed to run ETL " + period);    // $NON-NLS-1$
            logger.info("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // copy the ETL result for debugging purpose
        String dbnamebacknew = "kdw_" + newRevision.replace(".",
                                                            "").replace("-",
                                                                        "") + "_"
                                                                            + getClass().getSimpleName().toLowerCase()
                                                                            + "_" + testIndex + "_new";

        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);
        backupLogs(dbnamebacknew);

        // check result
        logger.info("Step6: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.info("KDW ETL running failed. Period: " + period + " Installer: " + newInstallerPath);
            logger.info("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // generate UAT DB and tables for latest revision
        String uatReportDBLatest = "kdw_" + newRevision.replace(".",
                                                                "").replace("-",
                                                                            "") + "_uat_reportdb_" + testIndex + "_new";
        String latestScriptsPath = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + newRevision;

        createTablesForUATDatabase(latestScriptsPath, username + "_masterDW", uatReportDBLatest, testIndex);

        // compare data
        logger.info("Step7: Compare the UAT reports");

        // set the filter list
        // get table/column filter list
        String tableFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "tablefilterlist");
        String columnFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                     testIndex,
                                                                                     "columnfilterlist");

        CompareDBs.setColumnFilterList(columnFilterList);
        CompareDBs.setTableFilterList(tableFilterList);

        if (CompareDBs.getInstance().compareNormalDBsWithSameSchema(uatReportDBBase, uatReportDBLatest) == false) {
            logger.severe("The two ETL results are different!");

            return ERROR_CODE.ERROR;
        }

        // unset the filter list. Need to unset them since they are static
        CompareDBs.setColumnFilterList("");
        CompareDBs.setTableFilterList("");

        return ERROR_CODE.OK;
    }
}
