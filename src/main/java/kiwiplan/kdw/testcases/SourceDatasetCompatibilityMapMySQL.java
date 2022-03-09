package kiwiplan.kdw.testcases;

import kiwiplan.kdw.testcases.base.SourceDatasetCompatibility;
import kiwiplan.kdw.utils.OSUtil;

import java.util.Arrays;
import java.util.List;

public class SourceDatasetCompatibilityMapMySQL extends SourceDatasetCompatibility {

    public SourceDatasetCompatibilityMapMySQL() {
        super("MAP MySQL: Source datasets Compatibility Test");

        String text = "This testcase tests running the latest build of KDW revision specified "
                      + "against the previous and latest versions of MAP MySQL source datasets, "
                      + "then compare the results.";
        setTestcaseDescription(text);
    }

    @Override
    protected boolean prepareBaseDatasets(int testIndex) {
        return this.prepareDatasets(testIndex, "sourcebaserevision", "sourcebkdatabasepre");
    }

    @Override
    protected boolean prepareLatestDatasets(int testIndex) {
        return this.prepareDatasets(testIndex, "sourcerevision", "sourcebkdatabase");
    }

    private boolean prepareDatasets(int testIndex, String sourceRevisionKey, String sourceBackupDatabaseKey) {
        String sourceUsername = getTestConfigValue(testIndex, "sourceuser");
        String sourcePassword = getTestConfigValue(testIndex, "sourcepassword");
        String sourceHostname = getTestConfigValue(testIndex, "sourcehostname");
        String sourceRevision = getTestConfigValue(testIndex, sourceRevisionKey);
        String sourceRestore = getTestConfigValue(testIndex, "sourcerestore");
        String sourceBkDatabase = getTestConfigValue(testIndex, sourceBackupDatabaseKey);

        // setup environment with QA restore scripts
        String cmd = "restinit -a; "
            + "rm -rf /javaservice/$USER/*; "
            + "chbase " + sourceRevision + "; "
            + "setupsql sql; "
            + sourceRestore + "; "
            + "mysqladmin -u kiwisql -pmapadm99 drop --force " + sourceBkDatabase + "; "
            + "dbcopy " + sourceUsername + " " + sourceBkDatabase;
        return OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);
    }

    @Override
    protected List<String> getCheckEtlConvertedCountsProducts(int testIndex) {
        String products = getTestConfigValue(testIndex, "etlconvertedproducts");
        if (products == null) {
            return null;
        }
        return Arrays.asList(products.split(","));
    }
}
