package kiwiplan.kdw.testcases;

public class SourceDatasetCompatibilityMapMes1 extends SourceDatasetCompatibilityMapMes {

    public SourceDatasetCompatibilityMapMes1() {
        super("MAP & MES 1 (CWR, OEE, QMS, TSS): Source datasets Compatibility Test");

        String text = "This testcase tests running the latest build of KDW revision specified "
                      + "against the previous and latest versions of MAP & MES (CWR, OEE, QMS, TSS) source datasets, "
                      + "then compare the results.";
        setTestcaseDescription(text);
    }

}
