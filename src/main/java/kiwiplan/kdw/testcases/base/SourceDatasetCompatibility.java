package kiwiplan.kdw.testcases.base;

import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.tasks.CompareDBs;
import kiwiplan.kdw.tasks.RulesCheck;
import kiwiplan.kdw.utils.OSUtil;

public abstract class SourceDatasetCompatibility extends BaseTestcase {
    public SourceDatasetCompatibility(String name) {
        super(name);

        String text =
            "This testcase tests the scnerio of install latest KDW revision "
            + "and then run ETL against previous revision of source datasets and latest revision. Then, compare the results.";

        setTestcaseDescription(text);
    }

    abstract protected boolean prepareBaseDatasets(int testIndex);

    abstract protected boolean prepareLatestDatasets(int testIndex);

    abstract protected List<String> getCheckEtlConvertedCountsProducts(int testIndex);

    @Override
    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        this.logger.severe("Start the sub testcase : " + testname);

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
        String kpetlfile        = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + ".xml";
        String newInstallerName = getLatestBuildName();
        String revision         = KDWHelper.getRevFromBuildName(newInstallerName);
        String newInstallerPath = ConfigHelper.getInstance()
                                              .getConfig("BuildRootFolder") + "/" + revision + "/"
                                                                            + getLatestBuildName();

        // get table/column filter list
        String tableFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "tablefilterlist");
        String columnFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                     testIndex,
                                                                                     "columnfilterlist");
        String dbnamebackbase = "kdw_" + revision.replace(".",
                                                          "").replace("-",
                                                                      "") + "_"
                                                                          + getClass().getSimpleName().toLowerCase()
                                                                          + "_base_" + testIndex;
        String dbnamebacknew = "kdw_" + revision.replace(".",
                                                         "").replace("-",
                                                                     "") + "_"
                                                                         + getClass().getSimpleName().toLowerCase()
                                                                         + "_new_" + testIndex;

        // Fresh install
        this.logger.severe("Step1: Fresh install revision: " + newInstallerName);

        if (KDWHelper.kdwFreshInstall(revision, hostname, newInstallerPath, username, password, false) == false) {
            this.logger.severe("Failed to Install KDW revision : " + newInstallerName + " for user: " + username);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackbase);

            return ERROR_CODE.ERROR;
        }
/*
        this.logger.severe("Step2: Prepare the base datasets.");

        if (prepareBaseDatasets(testIndex) == false) {
            this.logger.severe("Failed to Prepare the base datasets.");

            return ERROR_CODE.ERROR;
        }
*/
        // run ETL
        this.logger.severe("Step3: Run ETL against old revision of source datasets: " + period);

        if (KDWHelper.kdwRunETL(revision, hostname, username, password, kpsitefilebase, kpetlfile, period) == false) {
            this.logger.severe("Failed to run first time of ETL " + period);    // $NON-NLS-1$
            this.logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackbase);

            return ERROR_CODE.ERROR;
        }

        // check result
        this.logger.severe("Step4: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            this.logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + newInstallerPath);
            this.logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackbase);

            return ERROR_CODE.ERROR;
        }

        this.logger.severe("Step4-1: Check if each product has data converted");

        if (!KDWHelper.kdwCheckETLConvertedCounts(username, getCheckEtlConvertedCountsProducts(testIndex))) {
            this.logger.severe("KDW ETL converted counts check failed. Period: " + period + " Installer: " + newInstallerPath);
            this.logger.severe("Test Step Failed ");
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackbase);

            return ERROR_CODE.ERROR;
        }
/*
        // backup database for compare
        backupLogs(dbnamebackbase);
        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackbase);

        this.logger.severe("Step5: Prepare the latest datasets.");

        if (prepareLatestDatasets(testIndex) == false) {
            this.logger.severe("Failed to Prepare the latest datasets.");

            return ERROR_CODE.ERROR;
        }
*/
        this.logger.severe("Step6: Fresh install revision: " + newInstallerName);

        if (KDWHelper.kdwFreshInstall(revision, hostname, newInstallerPath, username, password, false) == false) {
            this.logger.severe("Failed to Install KDW revision : " + newInstallerName + " for user: " + username);
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

            return ERROR_CODE.ERROR;
        }

        // run ETL against latest source datasets
        this.logger.severe("Step7: Run ETL against new revision of source datasets: " + period);

        if (KDWHelper.kdwRunETL(revision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            this.logger.severe("Failed to run ETL " + period);    // $NON-NLS-1$
            this.logger.severe("Test Step Failed ");
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

            return ERROR_CODE.ERROR;
        }

        /*
        this.logger.severe("Step7-1: Run ETL: " + todayPeriod + " again");

        if (KDWHelper.kdwRunETL(revision, hostname, username, password, kpsitefile, kpetlfile, todayPeriod) == false) {
            this.logger.severe("Failed to run second time of ETL " + todayPeriod);    // $NON-NLS-1$
            this.logger.severe("Test Step Failed ");
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

            return ERROR_CODE.ERROR;
        }
         */

        // check result
        this.logger.severe("Step8: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            this.logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + newInstallerPath);
            this.logger.severe("Test Step Failed ");
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

            return ERROR_CODE.ERROR;
        }

        this.logger.severe("Step8-1: Check if each product has data converted");

        if (!KDWHelper.kdwCheckETLConvertedCounts(username, getCheckEtlConvertedCountsProducts(testIndex))) {
            this.logger.severe("KDW ETL converted counts check failed. Period: " + period + " Installer: " + newInstallerPath);
            this.logger.severe("Test Step Failed");
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);
            return ERROR_CODE.ERROR;
        }

        /*
        // check dup
        String masterDB = username + "_masterDW";

        this.logger.severe("Step9: Check dup records of master database: " + masterDB);

        if (RulesCheck.checkDupRecords(revision, masterDB) == false) {
            this.logger.severe("Check dup records failed " + masterDB);    // $NON-NLS-1$
            this.logger.severe("Test Step Failed ");
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

            return ERROR_CODE.ERROR;
        }

        // check basic data
        this.logger.severe("Step10: Basic data verification of master database: " + masterDB);

        if (RulesCheck.masterdbBasicDataVerification(revision, masterDB) == false) {
            this.logger.severe("Basic data verification failed " + masterDB);    // $NON-NLS-1$
            backupLogs(dbnamebacknew);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // backup database for compare
        backupLogs(dbnamebacknew);
        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

        // compare data
        this.logger.severe("Step11: Compare the two results");

        // set the filter list
        CompareDBs.setColumnFilterList(columnFilterList);
        CompareDBs.setTableFilterList(tableFilterList);

        if (CompareDBs.getInstance()
                      .compareMasterDBs(revision, dbnamebackbase + "_masterDW", dbnamebacknew + "_masterDW") == false) {
            this.logger.severe("The two ETL results have difference!");
            this.logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // unset the filter list. Need to unset them since they are static
        CompareDBs.setColumnFilterList("");
        CompareDBs.setTableFilterList("");
         */

        return ERROR_CODE.OK;
    }

    protected String getTestConfigValue(int testIndex, String configKey) {
        return TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, configKey);
    }
}
