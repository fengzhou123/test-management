package kiwiplan.kdw.testcases.base;

import kiwiplan.kdw.core.TestcaseConfigHelper;
import kiwiplan.kdw.utils.OSUtil;

import java.util.List;

public class SourceDatasetCompatibilityMes extends SourceDatasetCompatibility {
    public SourceDatasetCompatibilityMes(String name) {
        super(name);

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
        String sourceJavaRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                       testIndex,
                                                                                       "basejavarevision");
        String sourceJavaRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                      testIndex,
                                                                                      "basejavarestore");

        String javaVersion = (sourceJavaRevision.replace(".", "").compareTo("9541") < 0) ? "8" : "11";

        // map restore
        String cmd = "stop_services; restinit -a;rm -rf /javaservice/$USER/*; chbase " + sourceRevision + "; "
                     + "setupsql sql; " + sourceRestore + ";";

        // temp solution
        cmd = cmd + "cp -f /data/kdw/automation/tools/back/" + sourceUsername + "/kwsql /mapqa/" + sourceUsername
              + "_testing/;";

        // java restore
        cmd = cmd + "chjbase " + sourceJavaRevision + " " + javaVersion + "; " + "restsjava " + sourceJavaRestore + ";";

        // + "start_services";
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
        String sourceDatabaseName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                       testIndex,
                                                                                       "sourcedatabasename");
        String sourceJavaRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                      testIndex,
                                                                                      "javarestore");
        String sourceJavaRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                       testIndex,
                                                                                       "javarevision");

        String javaVersion = (sourceJavaRevision.replace(".", "").compareTo("9541") < 0) ? "8" : "11";

        String cmd = "stop_services; restinit -a;rm -rf /javaservice/$USER/*; ";

        // map restore
        if (sourceRevision.equalsIgnoreCase("head")) {
            cmd = cmd + "mysql -ukiwisql -pmapadm99 -e \"drop database " + sourceUsername + "\"; "
                  + "mysql -ukiwisql -pmapadm99 -e \"drop database " + sourceDatabaseName + "\"; " + "dbcopy nzodie:"
                  + sourceDatabaseName + " " + sourceDatabaseName + ";";
            cmd = cmd + "chbase kiwi_head; setupsql sql; " + sourceRestore + ";";
        } else {

            // if it's not head, restore database
            // chbase and restore the latest dataset
            cmd = cmd + "chbase " + sourceRevision + "; " + "setupsql sql; " + sourceRestore + ";";
        }

        // temp solutioni
        cmd = cmd + "cp -f /data/kdw/automation/tools/back/" + sourceUsername + "/kwsql /mapqa/" + sourceUsername
              + "_testing;";

        // copy latest mes build, there should be only one installer in build folder
        /*
        cmd = cmd + "rm -rf /javaservice/installers/mes-" + sourceJavaRevision + "/*;" + "cp /data/builds/mes-"
              + sourceJavaRevision.substring(0, sourceJavaRevision.indexOf("_")) + "/*"
              + " /javaservice/installers/mes-" + sourceJavaRevision + "/;";
        */
        // java restore
        cmd = cmd + "chjbase " + sourceJavaRevision + " " + javaVersion + "; " + "restsjava " + sourceJavaRestore + "; ";

        // + "start_services";
        return OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);
    }

    @Override
    protected List<String> getCheckEtlConvertedCountsProducts(int testIndex) {
        // not implemented - pass with no checking
        return null;
    }
}
