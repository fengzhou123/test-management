package kiwiplan.kdw.testcases;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.MSDBHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.testcases.base.SourceDatasetCompatibility;
import kiwiplan.kdw.utils.OSUtil;

import java.util.Arrays;
import java.util.List;

public class SourceDatasetCompatibilityEsp extends SourceDatasetCompatibility {
    public SourceDatasetCompatibilityEsp() {
        super("ESP: Source datasets Compatibility Test");

        String text =
            "This testcase tests the scenario of install latest KDW revision "
            + "and then run ETL against previous revision of source datasets and latest revision. Then, compare the results.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
        String sql = "DROP DATABASE [kdw_awlive_comptibility_780oct13_esp];";

        sql = sql + "RESTORE DATABASE [kdw_awlive_comptibility_780oct13_esp] "
              + "FROM  DISK = N'N:\\7.80_PatchRelease_10Aug2017\testebxawlive.bak' "
              + "WITH  FILE = 1,  MOVE N'espbox2_Data' TO "
              + "N'E:\\Program Files\\Microsoft SQL Server\\MSSQL12.MSSQLSERVER\\MSSQL\\DATA\\kdw_awlive_comptibility_780oct13_esp_Data.mdf',  "
              + "MOVE N'espbox2_Log' TO N'F:\\Program Files\\Microsoft SQL Server\\MSSQL12.MSSQLSERVER\\MSSQL\\Data\\kdw_awlive_comptibility_780oct13_esp_Log.ldf',  "
              + "NOUNLOAD,  STATS = 5;";

        try {
            (new MSDBHelper()).executeUpdateSQL(sql);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected boolean prepareBaseDatasets(int testIndex) {

        String espDBHostname = ConfigHelper.getInstance().getConfig("MSSQL_HOSTNAME");
        String espDBPort = ConfigHelper.getInstance().getConfig("MSSQL_PORT");
        String espDBUsername = ConfigHelper.getInstance().getConfig("MSSQL_USERNAME");
        String espDBPassword = ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD");
        String espDBName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espdbname1");
        String espBackupFile = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espbackupfile1");
        String espDBType = "MSSQL";

        return KDWHelper.restoreEspDatabase(espDBType, espDBHostname, espDBPort, espDBUsername, espDBPassword, espDBName, espBackupFile);

    }

    @Override
    protected boolean prepareLatestDatasets(int testIndex) {

        String espDBHostname = ConfigHelper.getInstance().getConfig("MSSQL_HOSTNAME");
        String espDBPort = ConfigHelper.getInstance().getConfig("MSSQL_PORT");
        String espDBUsername = ConfigHelper.getInstance().getConfig("MSSQL_USERNAME");
        String espDBPassword = ConfigHelper.getInstance().getConfig("MSSQL_PASSWORD");
        String espDBName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espdbname2");
        String espBackupFile = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "espbackupfile2");
        String espDBType = "MSSQL";

        return KDWHelper.restoreEspDatabase(espDBType, espDBHostname, espDBPort, espDBUsername, espDBPassword, espDBName, espBackupFile);

    }

    /*
    @Override
    protected boolean prepareBaseDatasets(int testIndex) {

        // TODO Auto-generated method stub
        // restore map
        // sync latest dataset, restore, isam
        // since it's isam dataset. it may be break by other test. so recover it here
        String sourceUsername = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "basesourceuser");
        String sourcePassword = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "basesourcepassword");
        String sourceHostname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "basesourcehostname");
        String sourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "basesourcerevision");
        String sourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                  testIndex,
                                                                                  "basesourcerestore");

        // restore map
        // sync latest dataset, restore, backup
        String cmd = "";

        // clean env
        cmd = "restinit -a;rm -rf /javaservice/$USER/*; " + "chbase " + sourceRevision + "; " + "setupsql map; "
              + sourceRestore + ";";

        boolean result = OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);

        if (result == false) {
            return false;
        }

        // don't need to restore esp since the database is already there
        return true;
    }

    @Override
    protected boolean prepareLatestDatasets(int testIndex) {
        String sourceUsername = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "sourceuser");
        String sourcePassword = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcepassword");
        String sourceHostname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcehostname");
        String sourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcerevision");
        String sourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                  testIndex,
                                                                                  "sourcerestore");

        // restore map
        // sync latest dataset, restore, backup
        String cmd = "";

        // clean env
        cmd = "restinit -a;rm -rf /javaservice/$USER/*; " + "chbase " + sourceRevision + "; " + "setupsql map; "
              + sourceRestore + ";";

        boolean result = OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);

        if (result == false) {
            return false;
        }

        MSDBHelper msDBHelper = new MSDBHelper();

        // restore esp
        // copy latest dataset to nzhydra, drop if exist, restore
        // drop the old database and backup the new database for ESP
        String espSourceBkDatabase = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                        testIndex,
                                                                                        "espsourcebkdatabase");
        String espSourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                      testIndex,
                                                                                      "espsourcerevision");
        String netmapSql = "EXEC sp_configure 'show advanced options', 1;"
                           + "RECONFIGURE;EXEC sp_configure 'xp_cmdshell', 1;"
                           + "RECONFIGURE;EXEC xp_cmdshell 'NET USE N: \\\\nix\\backup /PERSISTENT:yes';";

        try {
            msDBHelper.executeUpdateSQL(netmapSql);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        String sql = "DROP DATABASE [" + espSourceBkDatabase + "];";

        try {
            msDBHelper.executeUpdateSQL(sql);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        sql = "RESTORE DATABASE [" + espSourceBkDatabase + "] FROM  " + "DISK = N'N:\\" + espSourceRevision
              + "\\testebxawlive.bak' "
              + "WITH  FILE = 1,  MOVE N'espbox2_Data' TO N'E:\\Program Files\\Microsoft SQL Server\\MSSQL12.MSSQLSERVER\\MSSQL\\DATA\\"
              + espSourceBkDatabase + "_Data.mdf',  "
              + "MOVE N'espbox2_Log' TO N'F:\\Program Files\\Microsoft SQL Server\\MSSQL12.MSSQLSERVER\\MSSQL\\Data\\"
              + espSourceBkDatabase + "_Log.ldf',  NOUNLOAD,  STATS = 5";

        try {
            msDBHelper.executeUpdateSQL(sql);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.info(e.getMessage());

            return false;
        }

        return true;
    }
     */

    @Override
    protected List<String> getCheckEtlConvertedCountsProducts(int testIndex) {
        String products = getTestConfigValue(testIndex, "etlconvertedproducts");
        if (products == null) {
            return null;
        }
        return Arrays.asList(products.split(","));
    }

}
