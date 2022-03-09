package kiwiplan.kdw.testcases.base;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.tasks.RulesCheck;
import kiwiplan.kdw.utils.OSUtil;

public class FreshInstallSimpleETL extends BaseTestcase {
    public FreshInstallSimpleETL(String name) {
        super(name);

        String text = "This testcase tests the scenario of run simple ETL "
                      + "based on a fresh installed latest KDW build and then check for duplicate records.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {}

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {

        // get sub test configurations
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        this.logger.severe("Start the sub testcase : " + testname);

        // get revision from build number
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

        // Start the test
        // fresh install
        this.logger.severe("Step1: Fresh install revision: " + installerName);

        if (KDWHelper.kdwFreshInstall(revision, hostname, installerPath, username, password, false) == false) {
            this.logger.severe("Failed to Install KDW revision : " + installerName + " for user: " + username);
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // run ETL
        this.logger.severe("Step2: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(revision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            this.logger.severe("Failed to run first time of ETL " + period);    // $NON-NLS-1$
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // check result
        this.logger.severe("Step3: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            this.logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + installerPath);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        backupLogs(dbnameback);

        // check dup
        String masterDB = username + "_masterDW";

        this.logger.severe("Step4: Check dup records of master database: " + masterDB);

        if (RulesCheck.checkDupRecords(revision, masterDB) == false) {
            this.logger.severe("Check dup records failed " + masterDB);    // $NON-NLS-1$
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // check basic data
        this.logger.severe("Step5: Basic data verification of master database: " + masterDB);

        if (RulesCheck.masterdbBasicDataVerification(revision, masterDB) == false) {
            this.logger.severe("Basic data verification failed " + masterDB);    // $NON-NLS-1$
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        return ERROR_CODE.OK;
    }
}
