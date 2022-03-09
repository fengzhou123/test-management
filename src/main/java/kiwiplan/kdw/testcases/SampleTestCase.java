package kiwiplan.kdw.testcases;

import java.util.Random;

import kiwiplan.kdw.testcases.base.BaseTestcase;

public class SampleTestCase extends BaseTestcase {
    String errors[] = { "error1", "erro2", "ok" };

    public SampleTestCase() {
        super("Check Update Records Feature");

        String text = "";

        setTestcaseDescription(text);
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
    }

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {
        int    i        = new Random().nextInt(3);
        String selected = errors[i];

        this.logger.severe(selected);
        this.logger.severe(selected);
        this.logger.severe(selected);
        this.logger.severe(selected);
        this.logger.severe(selected);
        this.logger.severe(selected);

        if (selected.equals("ok")) {
            return ERROR_CODE.OK;
        }

        if (this.addError(selected)) {
            return ERROR_CODE.ERROR;
        } else {
            return ERROR_CODE.WARN;
        }
    }
}
