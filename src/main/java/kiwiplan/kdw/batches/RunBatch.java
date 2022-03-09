package kiwiplan.kdw.batches;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import kiwiplan.kdw.core.ConfigHelper;
import kiwiplan.kdw.core.KDWHelper;
import kiwiplan.kdw.core.LogHelper;
import kiwiplan.kdw.testcases.base.BaseTestcase;
import kiwiplan.kdw.utils.MailUtil;
import kiwiplan.kdw.utils.OSUtil;

public class RunBatch {
    public static Logger                     rootLogger         = LogHelper.getRootLogger();
    private static Map<String, BaseTestcase> CLASS_INSTANCE_MAP = new HashMap();

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Provide three parameters: ");
            System.out.println("Batch file name : Including the list of test case to be tested");
            System.out.println("Revision such as 9.10.1 or 8.61.1");

            return;
        }

        String batchFilePath = args[0];
        String rev           = args[1];
        File   batchFile     = new File(batchFilePath);

        if (!batchFile.exists()) {
            System.out.println("cases-ALL file path not exists: " + batchFilePath);
            System.exit(-1);
        }

        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(batchFile.getAbsolutePath() + ".lock", "rw");
            FileChannel      fc               = randomAccessFile.getChannel();

            try (FileLock fileLock = fc.tryLock()) {
                if (null != fileLock) {
                    runSingleInstance(rev, batchFile);
                } else {
                    System.out.println("Only single instance of this process is allowed!!!");
                }
            } catch (OverlappingFileLockException | IOException ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runAutomation(File batchFile, String prevBuildName, String latestBuildName) {
        List<TestCaseResult> testCaseResultList = new ArrayList<>();
        String               latestRev          = KDWHelper.getRevFromBuildName(latestBuildName);

        try {
            List<String> batchLines = FileUtils.readLines(batchFile);

            for (String testcaseName : batchLines) {

                // run testcase for each line in cases-ALL file
                Class        clazz        = null;
                BaseTestcase testcase     = null;
                int          subTestIndex = -1;

                try {
                    testcaseName = testcaseName.trim();

                    if (testcaseName.startsWith("#")) {
                        continue;
                    }

                    int _index = testcaseName.indexOf('@');

                    if (_index > 0) {
                        subTestIndex = Integer.parseInt(testcaseName.substring(_index + 1));
                        testcaseName = testcaseName.substring(0, _index);
                    }

                    testcase = CLASS_INSTANCE_MAP.get(testcaseName);

                    if (testcase == null) {

                        clazz    = Class.forName("kiwiplan.kdw.testcases." + testcaseName);
                        testcase = (BaseTestcase) clazz.newInstance();
                        CLASS_INSTANCE_MAP.put(testcaseName, testcase);
                    }

                    testcase.setPreBuildName(prevBuildName);
                    testcase.setLatestBuildName(latestBuildName);

                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    rootLogger.severe(e.getMessage());

                    continue;
                }

                try {
                    rootLogger.severe("");
                    rootLogger.severe(
                        String.format("============================TEST CASE: %s START===========================",
                                      testcaseName));
                    rootLogger.severe("");

                    List<TestCaseResult> subTestResultList = testcase.runAllSubTests(subTestIndex);

                    rootLogger.severe("");
                    rootLogger.severe(
                        String.format("============================TEST CASE: %s END  ===========================",
                                      testcaseName));
                    rootLogger.severe("");
                    testCaseResultList.addAll(subTestResultList);
                } catch (Exception e) {
                    e.printStackTrace();
                    rootLogger.severe(e.getMessage());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String summaryContent          = RunBatch.getSummaryContent(prevBuildName, latestBuildName, testCaseResultList);
        String successSummaryRecipient = ConfigHelper.getInstance().getConfig("SummaryRecipients");

        MailUtil.sendJavaMail("KDW Automation Result for: " + latestRev,
                              summaryContent,
                              "",
                              successSummaryRecipient,
                              "text/html; charset=utf-8");
    }

    public static void runSingleInstance(String rev, File batchFile) {
        String logFolder  = ConfigHelper.getInstance().getConfig("logFolder");
        File   logFolderF = new File(logFolder);

        if (!logFolderF.exists()) {
            logFolderF.mkdirs();
        }

        String tmpFolder  = ConfigHelper.getInstance().getConfig("tempFolder");
        File   tmpFolderF = new File(tmpFolder);

        if (!tmpFolderF.exists()) {
            tmpFolderF.mkdirs();
        }

        String prevBuildName   = getPrevRevBuildName(rev);
        String latestBuildName = getLatestRevBuildName(rev);

        while (!latestBuildName.endsWith(".sh")) {    // sometimes it fails to read the latest build file info, let it retry
            rootLogger.info(String.format("Latest Build: %s \n", latestBuildName));

            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            latestBuildName = getLatestRevBuildName(rev);
        }

        rootLogger.info(String.format("Previous Build: %s, Latest Build: %s \n", prevBuildName, latestBuildName));

        if (latestBuildName.endsWith(".sh") && (latestBuildName.compareTo(prevBuildName) > 0)) {
            runAutomation(batchFile, prevBuildName, latestBuildName);
            setLastRevBuildName(rev, latestBuildName);
        }
    }

    public static boolean setLastRevBuildName(String rev, String revBuildName) {
        String revFileName = String.format("%s/%s_last.testbuild.txt", OSUtil.BASE_FOLDER, rev);
        File   revFile     = new File(revFileName);

        if (revFile.exists()) {
            try {
                FileUtils.writeStringToFile(revFile, revBuildName);

                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return false;
    }

    public static String getLatestRevBuildName(String rev) {
        String       buildRootDirectory = ConfigHelper.getInstance().getConfig("BuildRootFolder");
        String       alternativeBuildDirectory = ConfigHelper.getInstance().getConfig("AlternativeBuildFolder");
        String       kdwServerName = ConfigHelper.getInstance().getConfig("System.ServerHostName");
        String       kdwUserName   = ConfigHelper.getInstance().getConfig("KDWAUTOUSER");
        String       kdwUserPass   = "mapadm99";
        String       kdwInstallerInBuildRootDirCmd = String.format("basename $(ls -1  %s/%s/kdw* | tail -n1)", buildRootDirectory, rev);
        String       kdwInstallerInAlterBuildDirCmd = String.format("basename $(ls -1  %s/kdw-%s_auto/kdw* | tail -n1)", alternativeBuildDirectory, rev);
        String       buildName     = "notFoundLatestBuildName";
        String       outStr = OSUtil.getRemoteSingleCommandOut(kdwServerName, kdwUserName, kdwUserPass, kdwInstallerInBuildRootDirCmd).toString();

        buildName = (outStr.contains("No such file or directory")) ? "notFoundLatestBuildName" : outStr.substring(1, outStr.length()-1);

        if (buildName.contains("notFoundLatestBuildName")) {

            outStr = OSUtil.getRemoteSingleCommandOut(kdwServerName, kdwUserName, kdwUserPass, kdwInstallerInAlterBuildDirCmd).toString();
            buildName = (outStr.contains("No such file or directory")) ? "notFoundLatestBuildName" : outStr.substring(1, outStr.length()-1);

        }

        System.out.println("////////////////////////buildName " + buildName);
        return buildName;
    }


/*
    public static String getLatestRevBuildName(String rev) {
        String       buildFolder   = ConfigHelper.getInstance().getConfig("BuildRootFolder");
        String       kdwServerName = ConfigHelper.getInstance().getConfig("System.ServerHostName");
        String       kdwUserName   = ConfigHelper.getInstance().getConfig("KDWAUTOUSER");
        String       kdwUserPass   = "mapadm99";
        String       cmd           = String.format("basename $(ls -1  %s/%s/kdw* | tail -n1)", buildFolder, rev);
        String       buildName     = "notFoundLatestBuildName";
        List<String> outList       = OSUtil.getRemoteSingleCommandOut(kdwServerName, kdwUserName, kdwUserPass, cmd);

        for (String line : outList) {
            if (line.startsWith("kdw") && line.endsWith(".sh")) {
                buildName = line;
            }
        }

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~buildName " + buildName);
        return buildName;
    }

 */


    public static String getPrevRevBuildName(String rev) {
        String revFileName = String.format("%s/%s_last.testbuild.txt", OSUtil.BASE_FOLDER, rev);
        File   revFile     = new File(revFileName);

        if (revFile.exists()) {
            try {
                String v = FileUtils.readFileToString(revFile);

                return v.trim();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return "NotFoundPreRev";
    }

    public static String getSummaryContent(String preBuildname, String latestBuildname,
                                           List<TestCaseResult> testCaseResultList) {
        StringBuilder output = new StringBuilder();

        testCaseResultList.sort((o1, o2) -> (int) (o1.tcStartTime - o2.tcStartTime));
        output.append("<!DOCTYPE html><html><head></head><body>\n");
        output.append("<h1>Previous Build : " + preBuildname + "Latest Build : " + latestBuildname
                      + "</h1><br/><br/>\n");
        output.append(
            "<table style=\"width:100%\" border=1 bordercolor=black><tr><th>ClassName</th><th>TestName</th><th>TestResult</th><th>TimeElapsed</th></tr>");

        for (TestCaseResult result : testCaseResultList) {
            String color = " bgcolor=grey";

            switch (result.tcResult) {
            case ERROR :
                color = " bgcolor=red";

                break;

            case OK :
                color = " bgcolor=green";

                break;

            case WARN :
                color = " bgcolor=yellow";

                break;
            }

            String logMsg = "<tr><td>" + result.className + "</td><td>" + result.tcName + "</td><td" + color + ">"
                            + result.tcResult + "</td><td>" + result.timeElapsed + "</td></tr>\n";

            output.append(logMsg);
        }

        output.append("</table>");
        output.append("</body></HTML>");

        return output.toString();
    }
}
