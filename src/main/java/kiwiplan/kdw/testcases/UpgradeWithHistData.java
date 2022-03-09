package kiwiplan.kdw.testcases;

import kiwiplan.kdw.batches.RunBatch;
import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.testcases.base.BaseTestcase;
import kiwiplan.kdw.utils.OSUtil;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class UpgradeWithHistData extends BaseTestcase {

    public String kdwInstallationCode = "Installation";
    public String kdwUpgradeCode = "Upgrade";

    public UpgradeWithHistData() {

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

        String runStartDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "runstartdate");
        String runEndDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "runenddate");
        String period = runStartDate + " " + runEndDate;
        String siteConfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "siteconfigname");
        String kpSitesFile = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteConfigname + ".xml";
        String mySqlConnectorFile = System.getProperty("user.dir") + "/kdw_miscellaneous_files/mysql-connector-java-5.1.48.jar";

        String sourceDBType = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "sourcedbtype");
        String mapSourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "mapsourcerevision");
        String mapsourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "mapsourcerestore");
        String mesSourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "messourcerevision");
        String mesSourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "messourcerestore");

        String rev1AppJavaVersion = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "rev1appjavaversion");
        String rev2AppJavaVersion = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "rev2appjavaversion");

        String espDBType = ConfigHelper.getInstance().getConfig("MSSQL_DB_TYPE");
        String espDBHostname = ConfigHelper.getInstance().getConfig("MSSQL_HOSTNAME");
        String espDBPort = ConfigHelper.getInstance().getConfig("MSSQL_PORT");
        String espDBUsername = ConfigHelper.getInstance().getConfig("MSSQL_USERNAME");
        String espDBPassword = ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD");
        String espDBName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espdbname");
        String espBackupFilePath = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espbackupfilepath");

        String sourceAppUsername = ConfigHelper.getInstance().getConfig("SOURCE_APP_INSTALLATION_USERNAME");
        String kdwBaseDirectory = "/javaservice/" + username;

        String kdwDBType = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "kdwdbtype");
        String kdwRevision1 = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "kdwrevision1");
        String kdwRevision2 = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "kdwrevision2");
        String kdwRev1SiteIdentifier = (kdwDBType.equalsIgnoreCase("MYSQL")) ? username + "_" + kdwRevision1.replace(".", "") + "my":
                username + "_" + kdwRevision1.replace(".", "") + "ms";
        String kdwUpgradeSiteIdentifier = (kdwDBType.equalsIgnoreCase("MYSQL")) ? username + "_" + kdwRevision2.replace(".", "") + "umy" :
                username + "_" + kdwRevision2.replace(".", "") + "ums";
        String kdwRev2SiteIdentifier = (kdwDBType.equalsIgnoreCase("MYSQL")) ? username + "_" + kdwRevision2.replace(".", "") + "my":
                username + "_" + kdwRevision2.replace(".", "") + "ms";

        System.out.println("kdwRev1SiteIdentifier: " + kdwRev1SiteIdentifier);

        String kdwRev1InstallerName = RunBatch.getLatestRevBuildName(kdwRevision1);

        System.out.println("///////////////////////////////// kdwRev1InstallerName " + kdwRev1InstallerName);

        String kdwRev1Build = kdwRev1InstallerName.substring(("kdw-"+kdwRevision1).length(), kdwRev1InstallerName.length()-3).replaceFirst("^0*", "");

        System.out.println("///////////////////////////////// kdwRev1Build" + kdwRev1Build);

        String kdwRev1InstallerDirectory = ConfigHelper.getInstance().getConfig("KdwBuildDirectoryForUpgradeScript") + "/" + kdwRevision1;
        String kdwRev1InstallerPath = kdwRev1InstallerDirectory + "/" + kdwRev1InstallerName;
        String kdwRev2InstallerName = RunBatch.getLatestRevBuildName(kdwRevision2);
        String kdwRev2InstallerDirectory = ConfigHelper.getInstance().getConfig("KdwBuildDirectoryForUpgradeScript") + "/" + kdwRevision2;
        String kdwRev2InstallerPath  = kdwRev2InstallerDirectory + "/" + kdwRev2InstallerName;
        String kdwRev2Build = kdwRev2InstallerName.substring(("kdw-"+kdwRevision2).length(), kdwRev2InstallerName.length()-3).replaceFirst("^0*", "");

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

        String rev1AppJavaHome = "/javaservice/java1." + rev1AppJavaVersion + ".0";
        String rev2AppJavaHome = "/javaservice/java1." + rev2AppJavaVersion + ".0";

        String baseKdwDBBakPrefix = "kdw_" + kdwRevision1.replace(".", "").replace("-", "") +
                                    "_" + getClass().getSimpleName().toLowerCase() + "_" + testIndex;

        Date today = Calendar.getInstance().getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        String todayPeriod = sdf.format(today) + " " + sdf.format(today);


        // Restore source databases
        logger.severe("Step1: Source MAP and MES database restoration: ");

        if (prepareSourceMapAndMesDatabases(hostname, sourceAppUsername, password, sourceDBType, mapSourceRevision, mapsourceRestore, mesSourceRevision,
                rev1AppJavaVersion, mesSourceRestore) == false) {
            logger.severe("Failed to Install KDW revision : " + kdwRev1InstallerName + " for user: " + sourceAppUsername);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // Restore source ESP database.
        logger.severe("Step2: Source ESP database restoration: ");

        if (KDWHelper.restoreEspDatabase(espDBType, espDBHostname, espDBPort, espDBUsername, espDBPassword, espDBName, espBackupFilePath) == false) {
            logger.severe("Failed to restore ESP database : " + espDBName + " from backup file " + espBackupFilePath);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }


        // Install and configure revision1 KDW using the kdwRev1SiteIdentifier, run ETL and validate results.
        logger.severe("Step3: Install and configure revision1 KDW using the kdwRev1SiteIdentifier, run ETL and validate results.");
        if (KDWHelper.installAndConfigureKdwRunEtlAndCheckResults(hostname, username, password, kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername,
                kdwDBPassword, kdwDBPasswordEncrypted, kdwBaseDirectory, kdwRev1InstallerDirectory,
                kdwRev1SiteIdentifier, installerJenkinsHostname, installerJenkinsUsername, installerJenkinsPassword,
                rev1AppJavaHome, kdwRevision1, kdwRev1Build, kpSitesFile, mySqlConnectorFile, period, sourceDBType) == false) {

            logger.severe("Failed to Install KDW revision : " + kdwRev1InstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;

        }

        // Fresh installation of base KDW for upgrade.
        // Clean up existing KDW databases and installation
        logger.severe("Step 4: Install and configure revision1 KDW using the kdwUpgradeSiteIdentifier, run ETL and validate results. The next step is KDW upgrade to revision2");

        if (KDWHelper.installAndConfigureKdwRunEtlAndCheckResults(hostname, username, password, kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername,
                kdwDBPassword, kdwDBPasswordEncrypted, kdwBaseDirectory, kdwRev1InstallerDirectory,
                kdwUpgradeSiteIdentifier, installerJenkinsHostname, installerJenkinsUsername, installerJenkinsPassword,
                rev1AppJavaHome, kdwRevision1, kdwRev1Build, kpSitesFile, mySqlConnectorFile, period, sourceDBType) == false) {

            logger.severe("Failed to Install KDW revision : " + kdwRev1InstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;

        }

        // Stop the scheduler and executor of the kdwUpgradeSiteIdentifier.
        logger.severe("Step5: Stop the scheduler and executor of the kdwUpgradeSiteIdentifier.");

        if (KDWHelper.killExistingSchedulerAndExecutorProcesses(hostname, username, password, kdwUpgradeSiteIdentifier) == false) {

            logger.severe("Failed to stop the scheduler and executor of the kdwUpgradeSiteIdentifier.");

            return ERROR_CODE.ERROR;

        }

        // upgrade to another revision.
        logger.severe("Step6: Upgrade to revision: " + kdwRev2InstallerName);

        if (KDWHelper.installOrUpgradeKDW(hostname, username, password, kdwBaseDirectory, kdwRev2InstallerDirectory,
                kdwUpgradeSiteIdentifier, kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPasswordEncrypted,
                installerJenkinsHostname, installerJenkinsUsername, installerJenkinsPassword, rev2AppJavaHome, kdwRevision2,
                kdwRev2Build, kdwUpgradeCode) == false) {
            logger.severe("Failed to Upgrade to KDW revision : " + kdwRev2InstallerName + " for user: " + username);
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // Copy MYSQL connector and kp_sites.xml to the remote machine.
        logger.severe("Step6-1: Copy MYSQL connector and kp_sites.xml files to the remote machine");

        if (KDWHelper.copyMysqlConnectorAndKdwConfigurationFiles(hostname, username, password, kdwUpgradeSiteIdentifier, kpSitesFile, mySqlConnectorFile) == false) {

            logger.severe("Failed to copy MYSQL connector and kp_sites_n1_n.xml files to the remote machine.");    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;

        }

        // run ETL
        logger.severe("Step6-2: After upgrade, run ETL: " + period);

        if (KDWHelper.runKdwEtl(kdwRevision2, hostname, username, password, kdwUpgradeSiteIdentifier, period,
                kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword) == false) {
            logger.severe("Failed to run ETL " + period);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        // check result
        logger.severe("Step6-3: Check if ETL succeeds or not");

       if (KDWHelper.checkKdwEtlResult(kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword, kdwUpgradeSiteIdentifier) == false) {
            logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + kdwRev2InstallerPath);
            logger.severe("Test Step Failed ");
            backupLogs(baseKdwDBBakPrefix);
            KDWHelper.copyKDWDBs(hostname, username, password, baseKdwDBBakPrefix);

            return ERROR_CODE.ERROR;
        }

        logger.severe("Step7: Install and configure revision2 KDW using the kdwRev2SiteIdentifier, run ETL and validate results.");
        if (KDWHelper.installAndConfigureKdwRunEtlAndCheckResults(hostname, username, password, kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername,
                kdwDBPassword, kdwDBPasswordEncrypted, kdwBaseDirectory, kdwRev2InstallerDirectory,
                kdwRev2SiteIdentifier, installerJenkinsHostname, installerJenkinsUsername, installerJenkinsPassword,
                rev2AppJavaHome, kdwRevision2, kdwRev2Build, kpSitesFile, mySqlConnectorFile, period, sourceDBType) == false) {

            logger.severe("Failed to Install KDW revision : " + kdwRev1InstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;

        }

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

    /*
    public boolean InstallAndConfigureKdwRunEtlAndCheckResults(String hostname, String username, String password, String kdwDBType,
                                                               String kdwDBHostname, String kdwDBPort, String kdwDBUsername, String kdwDBPassword,
                                                               String kdwDBPasswordEncrypted, String kdwBaseDirectory,
                                                               String kdwInstallerDirectory, String kdwSiteIdentifier, String installerJenkinsHostname,
                                                               String installerJenkinsUsername, String installerJenkinsPassword,
                                                               String kdwJavaHome, String kdwRevision, String kdwBuild, String kpSitesFile,
                                                               String mySqlConnectorFile, String etlPeriod, String sourceDBType) {

        //Clean up existing KDW databases and installation
        logger.severe("Clean up existing KDW databases and installation");

        if (KDWHelper.removeExistingKdwDBAndInstallation(hostname, username, password, kdwDBType, kdwDBHostname, kdwDBPort,
                kdwDBUsername, kdwDBPassword, kdwSiteIdentifier) == false) {
            logger.severe("Failed to remove existing KDW databases and installation.");

            return false;
        }

        // fresh install base build
        logger.severe("KDW install revision: " + kdwRevision + " build: " + kdwBuild);

        if (KDWHelper.installOrUpgradeKDW(hostname, username, password, kdwBaseDirectory, kdwInstallerDirectory, kdwSiteIdentifier,
                kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPasswordEncrypted, installerJenkinsHostname,
                installerJenkinsUsername, installerJenkinsPassword, kdwJavaHome, kdwRevision, kdwBuild, kdwInstallationCode) == false) {
            logger.severe("Failed to Install KDW revision : " + kdwRevision + " build: " + kdwBuild);

            return false;
        }

        // Copy MYSQL connector and kp_sites.xml to the remote machine.
        logger.severe("Copy MYSQL connector and kp_sites.xml to the remote machine.");

        if (KDWHelper.copyMysqlConnectorAndKdwConfigurationFiles(hostname, username, password, kdwSiteIdentifier, kpSitesFile, mySqlConnectorFile) == false) {

            logger.severe("Failed to copy MYSQL connector, kp_sites_n1_n.xml and kp_etl.xml files to the remote machine.");    // $NON-NLS-1$

            return false;

        }

        // Invoke load_schedule_configure.sh to load sites and datasets information to database.
        logger.severe("Invoke dwdate_initialization and load_schedule_configure scripts.");

        if (KDWHelper.invokeDwdateInitAndLoadScheduleConfigure(hostname, username, password, kdwSiteIdentifier, sourceDBType) == false) {
            logger.severe("Failed to invoke load_schedule_configure.sh to load sites and datasets information to database.");    // $NON-NLS-1$
            logger.severe("Test Step Failed ");

            return false;
        }

        // run ETL to get historical data
        logger.severe("Run ETL: " + etlPeriod);

        if (KDWHelper.runKdwEtl(kdwRevision, hostname, username, password, kdwSiteIdentifier, etlPeriod,
                kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword) == false) {
            logger.severe("Failed to run first time of ETL " + etlPeriod);    // $NON-NLS-1$
            logger.severe("Test Step Failed ");

            return false;
        }

        // check result
        logger.severe("Check if ETL succeeded or not");

        if (KDWHelper.checkKdwEtlResult(kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword, kdwSiteIdentifier) == false) {
            logger.severe("KDW ETL running failed. Period: " + etlPeriod);

            return false;
        }


        return true;


    }

     */

}
