package kiwiplan.kdw.testcases;

public class SourceDatasetCompatibilityMapMes2 extends SourceDatasetCompatibilityMapMes {

    public SourceDatasetCompatibilityMapMes2() {
        super("MAP & MES 2 (PIC): Source datasets Compatibility Test");

        String text = "This testcase tests running the latest build of KDW revision specified "
                      + "against the previous and latest versions of MAP & MES (PIC) source datasets, "
                      + "then compare the results.";
        setTestcaseDescription(text);
    }

}
