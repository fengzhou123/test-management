package kiwiplan.kdw.testcases.base;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.tasks.CompareDBs;
import kiwiplan.kdw.utils.OSUtil;

public class DailyRegression extends BaseTestcase {
    public DailyRegression(String name) {
        super(name);

        String text = "Fresh install previous build and latest build. Run ETL of them and compare the results.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {}

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
        String kpetlfile        = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + ".xml";
        String kpetlfilebase    = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + "_base.xml";
        String preInstallerName = getPreBuildName();
        String preRevision      = KDWHelper.getRevFromBuildName(preInstallerName);
        String preInstallerPath = ConfigHelper.getInstance()
                                              .getConfig("BuildRootFolder") + "/" + preRevision + "/"
                                                                            + getPreBuildName();
        String newInstallerName = getLatestBuildName();
        String newRevision      = KDWHelper.getRevFromBuildName(newInstallerName);
        String newInstallerPath = ConfigHelper.getInstance()
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
        logger.severe("Step1: Fresh install revision: " + preInstallerName);

        if (KDWHelper.kdwFreshInstall(preRevision, hostname, preInstallerPath, username, password, false) == false) {
            logger.severe("Failed to Install KDW revision : " + preInstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        // run ETL to get base data
        logger.severe("Step2: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(preRevision, hostname, username, password, kpsitefilebase, kpetlfilebase, period) == false) {
            logger.severe("Failed to run ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

//      Date today = Calendar.getInstance().getTime();
//      SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
//      String todayPeriod = sdf.format(today) + " " + sdf.format(today);
//      logger.severe("Step2-1: Run ETL: " + todayPeriod + " to get data in level tables");
//      if (KDWHelper.kdwRunETL(hostname, username, password, kpsitefile, kpetlfilebase, todayPeriod) == false) {
//              logger.severe("Failed to run first time of ETL " + todayPeriod); //$NON-NLS-1$
//              bResult = false;
//              logger.severe("Test Step Failed ");
//              return bResult;
//      }
        // check result
        logger.severe("Step3: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + preInstallerPath);
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }

        String dbnamebackold = "kdw_" + newRevision.replace(".",
                                                            "").replace("-",
                                                                        "") + "_"
                                                                            + getClass().getSimpleName().toLowerCase()
                                                                            + "_" + testIndex + "_old";

        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebackold);
        backupLogs(dbnamebackold);

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

        KDWHelper.copyKDWDBs(hostname, username, password, dbnamebacknew);
        backupLogs(dbnamebacknew);

        // set the filter list
        CompareDBs.setColumnFilterList(columnFilterList);
        CompareDBs.setTableFilterList(tableFilterList);

        // compare data
        logger.severe("Step7: Compare the two results");

        if (CompareDBs.getInstance()
                      .compareMasterDBs(preRevision,
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
