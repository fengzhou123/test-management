package kiwiplan.kdw.testcases.base;

import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.tasks.CompareDBs;
import kiwiplan.kdw.utils.OSUtil;

public class ReleaseRegression extends BaseTestcase {
    public ReleaseRegression(String name) {
        super(name);

        String text =
            "Fresh install latest released revision and latest build of current revision. Run ETL of them and compare the results.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {}

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        logger.severe("Start the sub testcase : " + testname);

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

        // get table/column filter list
        String tableFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "tablefilterlist");
        String columnFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                     testIndex,
                                                                                     "columnfilterlist");

        // fresh install base build
        logger.severe("Step1: Fresh install revision: " + baseInstallerName);

        if (KDWHelper.kdwFreshInstall(baseRevision, hostname, baseInstallerPath, username, password, true) == false) {
            logger.severe("Failed to Install KDW revision : " + baseInstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        // run ETL to get base data
        logger.severe("Step2: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(baseRevision, hostname, username, password, kpsitefilebase, kpetlfilebase, period) == false) {
            logger.severe("Failed to run ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        Date             today       = Calendar.getInstance().getTime();
        SimpleDateFormat sdf         = new SimpleDateFormat("yyyy/MM/dd");
        String           todayPeriod = sdf.format(today) + " " + sdf.format(today);

        // check result
        logger.severe("Step3: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + baseInstallerPath);
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        String dbnamebackold = "kdw_" + newRevision.replace(".",
                                                            "").replace("-",
                                                                        "") + "_"
                                                                            + getClass().getSimpleName().toLowerCase()
                                                                            + "_" + testIndex + "_old";

        backupLogs(dbnamebackold);
        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackold);

        // fresh install new revision
        logger.severe("Step4: Fresh install new revision: " + newInstallerName);

        if (KDWHelper.kdwFreshInstall(newRevision, hostname, newInstallerPath, username, password, false) == false) {
            logger.severe("Failed to install KDW revision : " + newInstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        // run ETL
        logger.severe("Step5: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(newRevision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            logger.severe("Failed to run ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // check result
        logger.severe("Step6: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + newInstallerPath);
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        String dbnamebacknew = "kdw_" + newRevision.replace(".",
                                                            "").replace("-",
                                                                        "") + "_"
                                                                            + getClass().getSimpleName().toLowerCase()
                                                                            + "_" + testIndex + "_new";

        backupLogs(dbnamebacknew);
        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);

        // compare data
        logger.severe("Step7: Compare the two results");

        // set the filter list
        CompareDBs.setColumnFilterList(columnFilterList);
        CompareDBs.setTableFilterList(tableFilterList);

        if (CompareDBs.getInstance()
                      .compareMasterDBs(baseRevision,
                                        dbnamebackold + "_masterDW",
                                        dbnamebacknew + "_masterDW") == false) {
            logger.severe("The two ETL results have difference!");
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        // unset the filter list. Need to unset them since they are static
        CompareDBs.setColumnFilterList("");
        CompareDBs.setTableFilterList("");

        return ERROR_CODE.OK;
    }
}
