package kiwiplan.kdw.testcases;

import kiwiplan.kdw.testcases.base.SourceDatasetCompatibility;
import kiwiplan.kdw.utils.OSUtil;

import java.util.Arrays;
import java.util.List;

public abstract class SourceDatasetCompatibilityMapMes extends SourceDatasetCompatibility {

    public SourceDatasetCompatibilityMapMes(String name) {
        super(name);
    }

    @Override
    protected boolean prepareBaseDatasets(int testIndex) {
        return this.prepareDatasets(testIndex, "mapsourcebaserevision", "javasourcebaserevision");
    }

    @Override
    protected boolean prepareLatestDatasets(int testIndex) {
        return this.prepareDatasets(testIndex, "mapsourcerevision", "javasourcerevision");
    }

    private boolean prepareDatasets(int testIndex, String sourceRevisionKey, String javaRevisionKey) {
        String sourceUsername = getTestConfigValue(testIndex, "sourceuser");
        String sourcePassword = getTestConfigValue(testIndex, "sourcepassword");
        String sourceHostname = getTestConfigValue(testIndex, "sourcehostname");
        String sourceRevision = getTestConfigValue(testIndex, sourceRevisionKey);
        String sourceRestore = getTestConfigValue(testIndex, "mapsourcerestore");
        String sourceJavaRevision = getTestConfigValue(testIndex, javaRevisionKey);
        String sourceJavaRestore = getTestConfigValue(testIndex, "javasourcerestore");
        String javaVersion = (sourceJavaRevision.replace(".", "").compareTo("9541") < 0) ? "8" : "11";

        // setup environment with QA restore scripts
        String cmd = "stop_services; "
            + "restinit -a; "
            + "rm -rf /javaservice/$USER/*; "
            // restore map
            + "chbase " + sourceRevision + "; "
            + "setupsql sql; "
            + sourceRestore + "; "
            + "cp -f /data/kdw/automation/tools/back/" + sourceUsername + "/kwsql /mapqa/" + sourceUsername + "_testing; "
            // restore java
            + "chjbase " + sourceJavaRevision + " " + javaVersion +"; "
            + sourceJavaRestore + "; ";
//            + "start_services";
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
