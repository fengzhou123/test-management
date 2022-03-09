package kiwiplan.kdw.testcases;

import java.sql.ResultSet;

import java.util.HashMap;
import java.util.Map;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.DBHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.testcases.base.BaseTestcase;

public class CheckQlikViewCompatibility extends BaseTestcase {
    private Map<String, String> knownIssues;

    public CheckQlikViewCompatibility() {
        super("Fresh Install And Check QlikView Compatibility");
        knownIssues = new HashMap<String, String>();

        String text = "This testcase tests the scnerio of install KDW "
                      + "and then check if the schema of master database is compatibility with QlikView.";

        setTestcaseDescription(text);

        /*
         * Knonw issues:
         *               table_name                              column_name
         *               dwshiftcalendar                 calendar_id
         *               dwshiftsummary                  calendar_id
         *               dwcostcenters                   costcenter_id
         *               dwcostcenterprocesses   costcenter_id
         *               dwdate                                  date_id
         *               datefinancialperiod             date_id
         *               dwjobseriesstep                 job_series_step_id
         *               dwwaste                                 job_series_step_id
         *               dwproductionorders              pcs_order_id
         *               dwshippingdetails               pcs_order_id
         *               dwprogram                               program_id
         *               dwwetendfeedback                program_id
         */
        knownIssues.put("calendar_id", "dwshiftcalendar,dwshiftsummary");
        knownIssues.put("costcenter_id", "dwcostcenters,dwcostcenterprocesses");
        knownIssues.put("date_id", "dwdate,datefinancialperiod");
        knownIssues.put("job_series_step_id", "dwjobseriesstep,dwwaste");
        knownIssues.put("pcs_order_id", "dwproductionorders,dwshippingdetails");
        knownIssues.put("program_id", "dwprogram,dwwetendfeedback");
    }

    private boolean checkQlikViewDupColName(String databaseName) {

        /*
         * select table_name, column_name
         *       from  INFORMATION_SCHEMA.columns
         *       where COLUMN_NAME in
         *       (
         *       SELECT column_name
         *       FROM INFORMATION_SCHEMA.columns
         *       WHERE table_schema IN ('<masterDB name>') AND table_name <> 'dwproductionevents'
         *       GROUP BY column_name
         *       HAVING COUNT(column_name) > 1 )
         *       and table_schema IN ('masterDB name') AND table_name <> 'dwproductionevents'
         *       order by COLUMN_NAME;
         */
        databaseName = databaseName + "_masterDW";

        String sqlCmd = "select table_name, column_name " + "from INFORMATION_SCHEMA.columns where COLUMN_NAME in "
                        + "( SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns " + "WHERE table_schema IN ('"
                        + databaseName
                        + "') AND table_name <> 'dwproductionevents' AND table_name <> 'dwdowntimesplitminutes' "
                        + "GROUP BY column_name HAVING COUNT(column_name) > 1 ) " + "and table_schema IN ('"
                        + databaseName + "') "
                        + "AND table_name <> 'dwproductionevents' AND table_name <> 'dwdowntimesplitminutes' "
                        + "order by COLUMN_NAME";

        try {
            ResultSet rs = DBHelper.getInstance().executeSQL(sqlCmd);

            while (rs.next()) {
                String tableName  = rs.getNString("table_name");
                String columnName = rs.getNString("column_name");

                if (isKnownIssue(columnName, tableName) == false) {
                    this.logger.severe(String.format("Table name: %s, column name: %s", tableName, columnName));

                    return false;
                }
            }
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return true;
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
    }

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {

        // get sub test configurations
        String testname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

        logger.severe("Start the sub testcase : " + testname);

        // fresh install
        String newInstallerName = getLatestBuildName();
        String newRevision      = KDWHelper.getRevFromBuildName(newInstallerName);
        String newInstallerPath = ConfigHelper.getInstance()
                                              .getConfig("BuildRootFolder") + "/" + newRevision + "/"
                                                                            + getLatestBuildName();

        logger.severe("Step1: Fresh install revision: " + newInstallerName);

        if (KDWHelper.kdwFreshInstall(newRevision, hostname, newInstallerPath, username, password, false) == false) {
            logger.severe("Failed to Install KDW revision : " + newInstallerName + " for user: " + username);

            return ERROR_CODE.ERROR;
        }

        String className = getClass().getSimpleName().toLowerCase();
        String dbname    = "kdw_" + newRevision.replace(".", "").replace("-", "") + "_" + className + "_" + testIndex;

        backupLogs(dbname);

        if (KDWHelper.copyKDWDBs(hostname, username, password, dbname) == false) {
            logger.severe("Failed to copy kdw database for user : " + username);

            return ERROR_CODE.ERROR;
        }

        logger.severe("Step2: Check if columns with same name in different table");

        if (checkQlikViewDupColName(dbname) == false) {
            logger.severe("Dup column name check failed: " + newInstallerName);

            return ERROR_CODE.ERROR;
        }

        return ERROR_CODE.OK;
    }

    private boolean isKnownIssue(String columnName, String tableName) {
        boolean known     = false;
        String  tableList = knownIssues.get(columnName);

        if (tableList != null) {
            known = tableList.contains(tableName);
        }

        return known;
    }
}
