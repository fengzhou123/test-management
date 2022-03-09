package kiwiplan.kdw.testcases.base;

import kiwiplan.kdw.batches.RunBatch;
import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.utils.OSUtil;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class UpgradeWithHistDataOrig extends BaseTestcase {

    public UpgradeWithHistDataOrig() {

        super("KDW upgrade with historical data");

        String text = "This testcase tests the scnerio of run ETL "
                      + "based on a previous releaesd KDW revision and then upgrade to latest revision"
                      + " after that, run same ETL and check duplicate records.";

        setTestcaseDescription(text);
    }

//    public static void main(String[] args) {}

    void populateCostcenters(String hostname, String username, String password) {
        String sqlFileName = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + "populate_rt_costcenters.sql";
        String cmd         = "mysql -h " + hostname + " -u" + username + " -p" + password + " " + username
                             + "_masterDW < " + sqlFileName;

        OSUtil.runRemoteCommand(hostname, username, password, cmd);
    }

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {

        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        logger.severe("Start the sub testcase : " + testname);

        String runStartDate   = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "runstartdate");
        String runEndDate     = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "runenddate");
        String period         = runStartDate + " " + runEndDate;
        String siteconfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "siteconfigname");
        String kpsitefile     = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteconfigname + ".xml";
//        String kpsitefilebase = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteconfigname + "_base.xml";
        String etlconfigname  = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "etlconfigname");
        String kpetlfile           = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + ".xml";
//        String kpetlfilebase       = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + "_base.xml";

//        String baseInstallerFolder = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "baseinstallerpath");

//        String baseInstallerName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "baseinstallername");
//        String baseInstallerPath = baseInstallerFolder + "/" + baseInstallerName;
//        String baseRevision      = KDWHelper.getRevFromBuildName(baseInstallerName);
//        String newInstallerName  = getLatestBuildName();
//        String newRevision       = KDWHelper.getRevFromBuildName(newInstallerName);

        String sourceDBType = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "sourcedbtype");
        String mapSourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "mapsourcerevision");
        String mapsourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "mapsourcerestore");
        String mesSourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "messourcerevision");
        String mesSourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "messourcerestore");

        String baseAppJavaVersion = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "baseappjavaversion");
        String upgradeKdwJavaVersion = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "upgradekdwjavaversion");

        String espDBType = ConfigHelper.getInstance().getConfig("MSSQL_DB_TYPE");
        String espDBHostName = ConfigHelper.getInstance().getConfig("MSSQL_HOSTNAME");
        String espDBPort = ConfigHelper.getInstance().getConfig("MSSQL_PORT");
        String espDBUserName = ConfigHelper.getInstance().getConfig("MSSQL_USERNAME");
        String espDBPassword = ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD");
        String espDBName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espdbname");
        String espBackupFilePath = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espbackupfilepath");

        String kdwSiteIdentifier = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "kdwsiteidentifier");
        String kdwDBType = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "kdwdbtype");
        String baseKdwRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "basekdwrevision");
        String upgradeKdwRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "upgradekdwrevision");
        String baseKdwInstallerName = RunBatch.getLatestRevBuildName(baseKdwRevision);
        String baseKdwBuild = baseKdwInstallerName.substring(("kdw-"+baseKdwRevision).length(), baseKdwInstallerName.length()-3).replaceFirst("^0*", "");
        String baseKdwInstallerDirectory = ConfigHelper.getInstance().getConfig("BuildRootFolder") + "/" + baseKdwRevision;
        String baseKdwInstallerPath = baseKdwInstallerDirectory + "/" + baseKdwInstallerName;
        String upgradeKdwInstallerName = RunBatch.getLatestRevBuildName(upgradeKdwRevision);
        String upgradeKdwInstallerDirectory = ConfigHelper.getInstance().getConfig("BuildRootFolder") + "/" + upgradeKdwRevision;
        String upgradeKdwInstallerPath  = upgradeKdwInstallerDirectory + "/" + upgradeKdwInstallerName;
        String upgradeKdwBuild = upgradeKdwInstallerName.substring(("kdw-"+upgradeKdwRevision).length(), upgradeKdwInstallerName.length()-3).replaceFirst("^0*", "");

        String kdwUsername = ConfigHelper.getInstance().getConfig("KDW_INSTALLATION_USERNAME");
        String kdwPassword = ConfigHelper.getInstance().getConfig("KDW_INSTALLATION_PASSWORD");
        String kdwBaseDirectory = ConfigHelper.getInstance().getConfig("KDW_INSTALLATION_BASE_DIRECTORY");

        String kdwDBHostname = (kdwDBType.equalsIgnoreCase("MYSQL")) ? ConfigHelper.getInstance().getConfig("MYSQL_HOSTNAME") :
                                                                                    ConfigHelper.getInstance().getConfig("MSSQL_HOSTNAME");
        String kdwDBPort = (kdwDBType.equalsIgnoreCase("MYSQL")) ? ConfigHelper.getInstance().getConfig("MYSQL_PORT") :
                                                                                ConfigHelper.getInstance().getConfig("MSSQL_PORT");
        String kdwDBUsername = (kdwDBType.equalsIgnoreCase("MYSQL")) ? ConfigHelper.getInstance().getConfig("MYSQL_USERNAME") :
                                                                                    ConfigHelper.getInstance().getConfig("MSSQL_USERNAME");
        String kdwDBPassword = (kdwDBType.equalsIgnoreCase("MYSQL")) ? ConfigHelper.getInstance().getConfig("MYSQL_PASSWORD") :
                                                                                    ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD");
        String kdwDBPasswordEncrypted = (kdwDBType.equalsIgnoreCase("MYSQL")) ? ConfigHelper.getInstance().getConfig("MYSQL_PASSWORD_ENCRYPTED") :
                                                                                            ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD_ENCRYPTED");

        String installerJenkinsHostname = ConfigHelper.getInstance().getConfig("INSTALLER_JENKINS_HOSTNAME");
        String installerJenkinsUsername = ConfigHelper.getInstance().getConfig("INSTALLER_JENKINS_USERNAME");
        String installerJenkinsPassword = ConfigHelper.getInstance().getConfig("INSTALLER_JENKINS_PASSWORD");

        String baseAppJavaHome = "/javaservice/java1." + baseAppJavaVersion + ".0";
        String upgradeKdwJavaHome = "/javaservice/java1." + upgradeKdwJavaVersion + ".0";

        String baseKdwDBBakPrefix = "kdw_" + baseKdwRevision.replace(".", "").replace("-", "") +
                                    "_" + getClass().getSimpleName().toLowerCase() + "_" + testIndex;

        Date today = Calendar.getInstance().getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        String todayPeriod = sdf.format(today) + " " + sdf.format(today);

        // Restore source databases
        logger.severe("Step1: Source MAP and MES database restoration: ");

        if (prepareSourceMapAndMesDatabases(hostname, username, password, sourceDBType, mapSourceRevision, mapsourceRestore, mesSourceRevision,
                baseAppJavaVersion, mesSourceRestore) == false) {
            logger.severe("Failed to Install KDW revision : " + baseKdwInstallerName + " for user: " + username);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // Restore source ESP database.
        logger.severe("Step2: Source ESP database restoration: ");

        if (KDWHelper.restoreEspDatabase(espDBType, espDBHostName, espDBPort, espDBUserName, espDBPassword, espDBName, espBackupFilePath) == false) {
            logger.severe("Failed to Install KDW revision : " + baseKdwInstallerName + " for user: " + username);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // fresh install base build
        logger.severe("Step3: Base KDW install revision: " + baseKdwRevision);

        String kdwInstallationCode = "installation";

        if (KDWHelper.installOrUpgradeKDW(hostname, kdwUsername, kdwPassword, kdwBaseDirectory, baseKdwInstallerDirectory, kdwSiteIdentifier,
                kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPasswordEncrypted, installerJenkinsHostname,
                installerJenkinsUsername, installerJenkinsPassword, baseAppJavaHome, baseKdwRevision, baseKdwBuild, kdwInstallationCode) == false) {
            logger.severe("Failed to Install KDW revision : " + baseKdwInstallerName + " for user: " + username);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        /*
        if (RTUpgradeWithHistData.class.isInstance(this)) {
            populateCostcenters(hostname, username, password);
            logger.info("Populate All Cost Centers For RT Dataset.");
        }
        */


        /*
        // run ETL to get historical data
        logger.severe("Step3: Run ETL: " + period);

        if (KDWHelper.kdwRunETL(hostname, kdwUsername, kdwPassword, kpsitefile, kpetlfile, period) == false) {
            logger.severe("Failed to run first time of ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }
        */

        /*
        logger.severe("Step2-1: Run ETL: " + todayPeriod + " to get data in level tables");

        if (KDWHelper.kdwRunETL(hostname, username, password, kpsitefile, kpetlfilebase, todayPeriod) == false) {
            logger.severe("Failed to run first time of ETL " + todayPeriod);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(dbnameback);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }
        */

        /*
        // check result
        logger.severe("Step4: Check if ETL succeeded or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + baseKdwInstallerPath);
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // upgrade
        logger.severe("Step5: Upgrade to revision: " + upgradeKdwInstallerName);

        String kdwUpgradeCode = "upgrade";

        if (KDWHelper.installOrUpgradeKDW(kdwHostname, kdwUsername, kdwPassword, kdwBaseDirectory, upgradeKdwInstallerDirectory,
                kdwSiteIdentifier, kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPasswordEncrypted,
                installerJenkinsHostname, installerJenkinsUsername, installerJenkinsPassword, upgradeKdwJavaHome, upgradeKdwRevision,
                upgradeKdwBuild, kdwUpgradeCode) == false) {
            logger.severe("Failed to Upgrade to KDW revision : " + upgradeKdwInstallerName + " for user: " + username);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // run ETL
        logger.severe("Step6: After upgrade, run ETL: " + period);

        if (KDWHelper.kdwRunETL(hostname, kdwUsername, kdwPassword, kpsitefile, kpetlfile, period) == false) {
            logger.severe("Failed to run ETL " + period + " after upgrade");    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }
         */

        /*
        logger.severe("Step5-1: Run ETL: " + todayPeriod + " again");

        if (KDWHelper.kdwRunETL(hostname, username, password, kpsitefile, kpetlfile, todayPeriod) == false) {
            logger.severe("Failed to run second time of ETL " + todayPeriod);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(dbnameback);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }
        */

        /*
        // check result
        logger.severe("Step7: Check if ETL succeeds or not");

        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + upgradeKdwInstallerPath);
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }
         */

        /*
        // check dup
        String masterDB = username + "_masterDW";

        logger.severe("Step7: Check dup records of master database: " + masterDB);

        if (RulesCheck.checkDupRecords(newRevision, masterDB) == false) {
            logger.severe("Check dup records failed " + masterDB);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(dbnameback);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);

            return ERROR_CODE.ERROR;
        }
        */

        /*
        // check basic data
        logger.severe("Step8: Basic data verification of master database: " + masterDB);

        if (RulesCheck.masterdbBasicDataVerification(newRevision, masterDB) == false) {
            logger.severe("Basic data verification failed " + masterDB);    // $NON-NLS-1$
            backupLogs(dbnameback);
            KDWHelper.copyKDWDBs(hostname, username, password, dbnameback);
            logger.severe("Test Step Failed ");

            return ERROR_CODE.ERROR;
        }
         */

        /*
        // make backup
        backupLogs(baseKdwDBBakPrefix);
        KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);
        */

        return ERROR_CODE.OK;
    }
}
