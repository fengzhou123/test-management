package kiwiplan.kdw.testcases.base;

import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.tasks.RulesCheck;
import kiwiplan.kdw.utils.OSUtil;

public class FreshInstallTwiceETLDupCheck extends BaseTestcase {
    public FreshInstallTwiceETLDupCheck(String name) {
        super(name);

        String text = "This testcase tests the scnerio of run same simple ETL twice "
                      + "based on a fresh installed latest KDW build and then check for duplicate records.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {
        Date             today       = Calendar.getInstance().getTime();
        SimpleDateFormat sdf         = new SimpleDateFormat("yyyy/MM/dd");
        String           todayPeriod = sdf.format(today);

        System.out.println(todayPeriod);
    }

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        logger.severe("Start the sub testcase : " + testname);

        String installerName = getLatestBuildName();
        String revision      = KDWHelper.getRevFromBuildName(installerName);
        String installerPath = ConfigHelper.getInstance()
                                           .getConfig("BuildRootFolder") + "/" + revision + "/" + getLatestBuildName();
        String className      = getClass().getSimpleName().toLowerCase();
        String dbnameback     = "kdw_" + revision.replace(".", "").replace("-", "") + "_" + className + "_" + testIndex;
        String runStartDate   = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "runstartdate");
        String runEndDate     = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "runenddate");
        String period         = runStartDate + " " + runEndDate;
        String siteconfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "siteconfigname");
        String kpsitefile    = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteconfigname + ".xml";
        String etlconfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                  testIndex,
                                                                                  "etlconfigname");
        String kpetlfile = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + ".xml";

        // fresh install
        logger.severe("Step1: Fresh install revision: " + installerName);

        if (KDWHelper.kdwFreshInstall(revision, hostname, installerPath, username, password, false) == false) {
            logger.severe("Failed to Install KDW revision : " + installerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        // run ETL twice
        logger.severe("Step2: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(revision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            logger.severe("Failed to run first time of ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }

        logger.severe("Step3: Run ETL again: " + period);

        if (KDWHelper.kdwRunETL(revision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            logger.severe("Failed to run second time of ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }

        // check result
        logger.severe("Step4: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + installerPath);
            logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }

        // check dup
        String masterDB = username + "_masterDW";

        logger.severe("Step5: Check dup records of master database: " + masterDB);

        if (RulesCheck.checkDupRecords(revision, masterDB) == false) {
            logger.severe("Check dup records failed " + masterDB);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }

        // check basic data
        logger.severe("Step6: Basic data verification of master database: " + masterDB);

        if (RulesCheck.masterdbBasicDataVerification(revision, masterDB) == false) {
            logger.severe("Basic data verification failed " + masterDB);    // $NON-NLS-1$
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        return ERROR_CODE.OK;
    }
}
