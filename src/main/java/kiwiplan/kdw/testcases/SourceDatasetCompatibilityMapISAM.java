package kiwiplan.kdw.testcases;

import kiwiplan.kdw.testcases.base.SourceDatasetCompatibility;
import kiwiplan.kdw.utils.OSUtil;

import java.util.Arrays;
import java.util.List;

public class SourceDatasetCompatibilityMapISAM extends SourceDatasetCompatibility {

    public SourceDatasetCompatibilityMapISAM() {
        super("MAP ISAM: Source datasets Compatibility Test");

        String text = "This testcase tests running the latest build of KDW revision specified "
                      + "against the previous and latest versions of MAP ISAM source datasets, "
                      + "then compare the results.";
        setTestcaseDescription(text);
    }

    @Override
    protected boolean prepareBaseDatasets(int testIndex) {
        return this.prepareDatasets(testIndex, "sourcebaserevision");
    }

    @Override
    protected boolean prepareLatestDatasets(int testIndex) {
        return this.prepareDatasets(testIndex, "sourcerevision");
    }

    private boolean prepareDatasets(int testIndex, String sourceRevisionKey) {
        String sourceUsername = getTestConfigValue(testIndex, "sourceuser");
        String sourcePassword = getTestConfigValue(testIndex, "sourcepassword");
        String sourceHostname = getTestConfigValue(testIndex, "sourcehostname");
        String sourceRevision = getTestConfigValue(testIndex, sourceRevisionKey);
        String sourceRestore = getTestConfigValue(testIndex, "sourcerestore");

        // setup environment with QA restore scripts
        String cmd = "restinit -a; "
            + "rm -rf /javaservice/$USER/*; "
            + "chbase " + sourceRevision + "; "
            + "setupsql isam; "
            + sourceRestore + ";";       // eg. restrestmap wruatelpasowr
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
