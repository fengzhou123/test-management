package kiwiplan.kdw.batches;

import kiwiplan.kdw.testcases.base.BaseTestcase;

public class TestCaseResult {
    public String                  className;
    public String                  timeElapsed;
    public BaseTestcase.ERROR_CODE tcResult;
    public String                  tcName;
    public int                     tcIndex;
    public long                    tcStartTime;

    public TestCaseResult(String className, String tcName, int index, long startTime) {
        this.className   = className;
        this.tcName      = tcName;
        this.tcIndex     = index;
        this.tcStartTime = startTime;
    }

    public void setTcResult(BaseTestcase.ERROR_CODE tcResult) {
        this.tcResult = tcResult;
    }

    public void setTimeElapsed(String timeElapsed) {
        this.timeElapsed = timeElapsed;
    }
}
