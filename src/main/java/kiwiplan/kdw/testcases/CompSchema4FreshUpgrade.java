package kiwiplan.kdw.testcases;

import java.util.List;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.testcases.base.BaseTestcase;
import kiwiplan.kdw.utils.OSUtil;

public class CompSchema4FreshUpgrade extends BaseTestcase {
    public CompSchema4FreshUpgrade() {
        super("Schema Compare for Fresh Install and Upgrade Install");

        String text =
            "This testcase tests the scenario of install previous released KDW revision "
            + "and then upgrade to latest revision. Do another fresh install of latest revision. Compare the database schema.";

        setTestcaseDescription(text);
    }

    public ERROR_CODE dbSchemaIntegratyCheck(String hostname, String username, String password, String revision,
                                             String testcaseTempPath) {
        ERROR_CODE bResult           = ERROR_CODE.OK;
        String     revisionNumber    = revision;    // .substring(revision.indexOf("kdw-") + 4);
        String     preProcessWorking =
            "tail -n +5 $PENTAHODIR/kiwiplan/conf/working_db_schema.sql > $PENTAHODIR/kiwiplan/conf/temp_working.sql;";

        preProcessWorking += "sed -i '/^[[:space:]]*$/d' $PENTAHODIR/kiwiplan/conf/temp_working.sql;";
        preProcessWorking += "sed -i '/^[[:space:]]*$/d' $PENTAHODIR/../../../sites/" + username
                             + "/current/template/kp-etl-core/sqlsrc/kp-etl-core-" + revisionNumber + "-INSTALL.sql;";

        String cmdWorkingDiff = "diff $PENTAHODIR/kiwiplan/conf/temp_working.sql $PENTAHODIR/../../../sites/"
                                + username + "/current/template/kp-etl-core/sqlsrc/kp-etl-core-" + revisionNumber
                                + "-INSTALL.sql";
        String cmdWorking = preProcessWorking + cmdWorkingDiff;

        this.logger.severe("---Working Database Schema Integrity Check---");

        if (OSUtil.runRemoteCommand(hostname, username, password, cmdWorking) == false) {
            cmdWorking += " > " + testcaseTempPath + "/working_schemaintegraty.diff";
            OSUtil.runRemoteCommand(hostname, username, password, cmdWorking);

            List<String> diffLines = OSUtil.getRemoteSingleCommandOut(hostname, username, password, cmdWorkingDiff);
            String       error     = this.printDiffLines(diffLines);

            if (this.addError(error)) {
                bResult = ERROR_CODE.ERROR;
            } else {
                bResult = ERROR_CODE.WARN;
            }
        }

        String preProcessMaster =
            "tail -n +5 $PENTAHODIR/kiwiplan/conf/master_db_schema.sql > $PENTAHODIR/kiwiplan/conf/temp_master.sql;";

        preProcessMaster += "sed -i '/^[[:space:]]*$/d' $PENTAHODIR/kiwiplan/conf/temp_master.sql;";
        preProcessMaster += "sed -i '/^[[:space:]]*$/d' $PENTAHODIR/../../../sites/" + username
                            + "/current/template/kp-etl-datawarehouse/sqlsrc/kp-etl-datawarehouse-" + revisionNumber
                            + "-INSTALL.sql;";

        String cmdMasterDiff = "diff $PENTAHODIR/kiwiplan/conf/temp_master.sql $PENTAHODIR/../../../sites/" + username
                               + "/current/template/kp-etl-datawarehouse/sqlsrc/kp-etl-datawarehouse-" + revisionNumber
                               + "-INSTALL.sql";
        String cmdMaster = preProcessMaster + cmdMasterDiff;

        this.logger.severe("---Master Database Schema Integrity Check---");

        if (OSUtil.runRemoteCommand(hostname, username, password, cmdMaster) == false) {
            cmdMaster += " > " + testcaseTempPath + "/master_schemaintegraty.diff";
            OSUtil.runRemoteCommand(hostname, username, password, cmdMaster);

            List<String> diffLines = OSUtil.getRemoteSingleCommandOut(hostname, username, password, cmdMasterDiff);
            String       error     = this.printDiffLines(diffLines);

            if (this.addError(error) || (bResult == ERROR_CODE.ERROR)) {
                bResult = ERROR_CODE.ERROR;
            } else {
                bResult = ERROR_CODE.WARN;
            }
        }

        return bResult;
    }

    public static void main(String[] args) {
        CompSchema4FreshUpgrade compschema = new CompSchema4FreshUpgrade();

        compschema.runAllSubTests(-1);
    }

    @Override
    public void prepareAttachments() {
        String tempPath                    = getRemoteTestcaseTempPath();
        String sqlDumpFiles                = tempPath + "/" + "*.sql";
        String sqlDiffFiles                = tempPath + "/" + "*.diff";
        String sqlSchemaIntegratyDiffFiles = tempPath + "/" + "*.diff";
        String attachmentFiles             = sqlDumpFiles + " " + sqlDiffFiles + " " + sqlSchemaIntegratyDiffFiles;

        setAdditionalLogAttachmentFiles(attachmentFiles);
        super.prepareAttachments();
    }

    public ERROR_CODE runSubTest(String hostname, String userName, String password, int testIndex) {
        ERROR_CODE bResult = ERROR_CODE.OK;

        // get sub test configurations
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        this.logger.severe("Start the sub testcase : " + testname);

        // fresh install and copy the db to backup
        String baseInstallerFolder = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                        testIndex,
                                                                                        "baseinstallerpath");
        String baseInstallerName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                      testIndex,
                                                                                      "baseinstallername");
        String baseInstallerPath = baseInstallerFolder + "/" + baseInstallerName;
        String baserevision      = KDWHelper.getRevFromBuildName(baseInstallerName);
        String newInstallerName  = getLatestBuildName();
        String newRevision       = KDWHelper.getRevFromBuildName(newInstallerName);
        String newInstallerPath  = ConfigHelper.getInstance()
                                               .getConfig("BuildRootFolder") + "/" + newRevision + "/"
                                                                             + getLatestBuildName();
        String className   = getClass().getSimpleName().toLowerCase();
        String dbnamefresh = "kdw_" + newRevision.replace(".",
                                                          "").replace("-",
                                                                      "") + "_" + className + "_" + testIndex
                                                                          + "_dbfresh";
        String dbnameupgrade = "kdw_" + newRevision.replace(".",
                                                            "").replace("-",
                                                                        "") + "_" + className + "_" + testIndex
                                                                            + "_dbupgrade";

        this.logger.severe("Step1: Fresh install revision: " + newInstallerName);

        if (KDWHelper.kdwFreshInstall(newRevision, hostname, newInstallerPath, userName, password, false) == false) {
            this.logger.severe("Failed to Install KDW revision : " + newInstallerName + " for user: " + userName);

            return ERROR_CODE.ERROR;
        }

//      if (!KDWHelper.getUTMode()) {
        this.logger.severe("Step1-1: Schema Integrity check for fresh installed instance");

        if (this.dbSchemaIntegratyCheck(hostname,
                                        userName,
                                        password,
                                        newRevision,
                                        this.getRemoteTestcaseTempPath()) == ERROR_CODE.ERROR) {
            return ERROR_CODE.ERROR;
        }

//      }
        backupLogs(dbnamefresh);

        if (KDWHelper.copyKDWDBs(hostname, userName, password, dbnamefresh) == false) {
            this.logger.severe("Failed to copy kdw database for user : " + userName);

            return ERROR_CODE.ERROR;
        }

        // fresh install old version and upgrade it and copy the db to another backup
        this.logger.severe("Step2: Fresh install revision: " + baseInstallerName);

        if (KDWHelper.kdwFreshInstall(baserevision, hostname, baseInstallerPath, userName, password, true) == false) {
            this.logger.severe("Failed to Install KDW revision : " + baseInstallerName + " for user: " + userName);

            return ERROR_CODE.ERROR;
        }

        this.logger.severe("Step3: Upgrade to revision: " + newInstallerName);

        if (KDWHelper.kdwUpgradeInstall(newRevision, hostname, newInstallerPath, userName, password) == false) {
            this.logger.severe("Failed to Upgrade KDW from revision : " + baseInstallerName + " to revision : "
                               + newInstallerName + "\n" + " for user: " + userName);

            return ERROR_CODE.ERROR;
        }

        this.logger.severe("Step3-1: Schema Integrity check for upgraded instance");
        bResult = this.dbSchemaIntegratyCheck(hostname,
                                              userName,
                                              password,
                                              newRevision,
                                              this.getRemoteTestcaseTempPath());

        if (bResult == ERROR_CODE.ERROR) {
            return ERROR_CODE.ERROR;
        }

        backupLogs(dbnameupgrade);

        if (KDWHelper.copyKDWDBs(hostname, userName, password, dbnameupgrade) == false) {
            this.logger.severe("Failed to copy kdw database for user : " + userName);

            return ERROR_CODE.ERROR;
        }

        String compdb1 = dbnamefresh + "_masterDW";
        String compdb2 = dbnameupgrade + "_masterDW";

        // compare the schema of the two dbs
        this.logger.severe("Step4: Compare master database");

        List<String> diffLines = KDWHelper.kdwCompareSchema(hostname,
                                                            userName,
                                                            password,
                                                            compdb1,
                                                            compdb2,
                                                            this.getRemoteTestcaseTempPath());

        if (this.hasDiffLines(diffLines)) {
            this.logger.severe("KDW schema not match between : " + compdb1 + " and " + compdb2);

            StringBuilder diffOut = new StringBuilder();

            diffOut.append("\n----------------------------------------------------------------DIFF----\n");

            for (String line : diffLines) {
                diffOut.append(line + '\n');
            }

            diffOut.append("-------------------------------------------------------------------------\n");
            this.logger.severe(diffOut.toString());

            if (this.addError(diffOut.toString()) || (bResult == ERROR_CODE.ERROR)) {
                bResult = ERROR_CODE.ERROR;
            } else {
                bResult = ERROR_CODE.WARN;
            }
        }

        compdb1 = dbnamefresh + "_workingDW";
        compdb2 = dbnameupgrade + "_workingDW";

        // compare the schema of the two dbs
        this.logger.severe("Step5: Compare working database");
        diffLines = KDWHelper.kdwCompareSchema(hostname,
                                               userName,
                                               password,
                                               compdb1,
                                               compdb2,
                                               this.getRemoteTestcaseTempPath());

        if (this.hasDiffLines(diffLines)) {
            this.logger.severe("KDW schema not match between : " + compdb1 + " and " + compdb2);

            StringBuilder diffOut = new StringBuilder();

            diffOut.append("\n----------------------------------------------------------------DIFF----\n");

            for (String line : diffLines) {
                diffOut.append(line + '\n');
            }

            diffOut.append("-------------------------------------------------------------------------\n");
            this.logger.severe(diffOut.toString());

            if (this.addError(diffOut.toString()) || (bResult == ERROR_CODE.ERROR)) {
                bResult = ERROR_CODE.ERROR;
            } else {
                bResult = ERROR_CODE.WARN;
            }
        }

        return bResult;
    }
}
