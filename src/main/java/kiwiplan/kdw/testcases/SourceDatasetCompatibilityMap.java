package kiwiplan.kdw.testcases;

import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.testcases.base.SourceDatasetCompatibility;
import kiwiplan.kdw.utils.OSUtil;

import java.util.List;

public class SourceDatasetCompatibilityMap extends SourceDatasetCompatibility {
    public SourceDatasetCompatibilityMap() {
        super("MAP: Source datasets Compatibility Test");

        String text =
                "This testcase tests the scnerio of install latest KDW revision "
                        + "and then run ETL against previous revision of source datasets and latest revision. Then, compare the results.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
    }

    @Override
    protected boolean prepareBaseDatasets(int testIndex) {
        String sourceUsername = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "sourceuser");
        String sourcePassword = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcepassword");
        String sourceHostname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcehostname");
        String sourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcebaserevision");
        String sourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcerestore");
        String sourceBkDatabase = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcebkdatabasepre");
        String sourceDatabaseName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcerdatabasename");
        String cmd = "";

        if (!sourceRevision.equalsIgnoreCase("head")) {

            // if it's not head, restore database
            String sourceSQLName = sourceRestore.replaceFirst("r", "t") + ".sql";

            // clean env
            cmd = "restinit -a;rm -rf /javaservice/$USER/*; "

                    // copy the latest dataset from nzodie (hardcode the server name here)
                    // + "scp ssd@nzodie:/mapqa/" + sourceRevision + "/" + sourceSQLName   + " /mapqa/" + sourceRevision + "/; "
                    // chbase and restore the latest dataset
                    + "chbase " + sourceRevision + "; " + "setupsql sql; " + sourceRestore + ";";
            cmd = cmd + "mysqladmin -u kiwisql -pmapadm99 drop --force " + sourceBkDatabase + ";";
            cmd = cmd + "dbcopy " + sourceUsername + " " + sourceBkDatabase;
        } else {

            // drop the old database and backup the new database
            cmd = cmd + "mysqladmin -u kiwisql -pmapadm99 drop --force " + sourceBkDatabase + ";";
            cmd = cmd + "dbcopy " + sourceDatabaseName + " " + sourceBkDatabase;
        }

        return OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);
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
        String sourceBkDatabase = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcebkdatabase");
        String sourceDatabaseName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                testIndex,
                "sourcerdatabasename");
        String cmd = "";

        if (!sourceRevision.equalsIgnoreCase("head")) {

            // if it's not head, restore database
            String sourceSQLName = sourceRestore.replaceFirst("r", "t") + ".sql";

            // clean env
            cmd = "restinit -a;rm -rf /javaservice/$USER/*; "

                    // copy the latest dataset from nzodie (hardcode the server name here)
                    // + "scp ssd@nzodie:/mapqa/" + sourceRevision + "/" + sourceSQLName   + " /mapqa/" + sourceRevision + "/; "
                    // chbase and restore the latest dataset
                    + "chbase " + sourceRevision + "; " + "setupsql sql; " + sourceRestore + ";";
            cmd = cmd + "mysqladmin -u kiwisql -pmapadm99 drop --force " + sourceBkDatabase + ";";
            cmd = cmd + "dbcopy " + sourceUsername + " " + sourceBkDatabase;
        } else {

            // drop the old database and backup the new database
            cmd = cmd + "mysqladmin -u kiwisql -pmapadm99 drop --force " + sourceBkDatabase + ";";
            cmd = cmd + "dbcopy " + sourceDatabaseName + " " + sourceBkDatabase;
        }

        return OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);
    }

    @Override
    protected List<String> getCheckEtlConvertedCountsProducts(int testIndex) {
        // not implemented - pass with no checking
        return null;
    }
}
