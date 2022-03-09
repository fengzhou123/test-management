package kiwiplan.kdw.testcases;

import kiwiplan.kdw.testcases.base.SourceDatasetCompatibilityMes;

public class SourceDatasetCompatibilityCwr extends SourceDatasetCompatibilityMes {
    public SourceDatasetCompatibilityCwr() {
        super("CWR: Source datasets Compatibility Test");

        String text =
            "This testcase tests the scnerio of install latest KDW revision "
            + "and then run ETL against previous revision of source datasets and latest revision. Then, compare the results.";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
    }
}
