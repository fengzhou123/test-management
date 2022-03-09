package kiwiplan.kdw.testcases.base;

import java.io.File;
import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.text.SimpleDateFormat;

import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;

import org.w3c.dom.Document;

import org.xml.sax.SAXException;

import kiwiplan.kdw.batches.TestCaseResult;
import kiwiplan.kdw.core.*;
import kiwiplan.kdw.tasks.CompareDBs;
import kiwiplan.kdw.utils.MailUtil;
import kiwiplan.kdw.utils.OSUtil;

public abstract class BaseTestcase {
    static Logger                    rootLogger                   = LogHelper.getRootLogger();
    final private String             ATTACHMENT_NAME              = "logattachments";
    protected Logger                 logger                       = null;
    protected String                 testdataPath                 = "";
    protected String                 configFileName               = "";
    protected String                 testcaseName                 = "";
    private String                   testcaseDescription          = "";
    private String                   additionalLogAttachmentFiles = "";
    private String                   preBuildName                 = "";
    private String                   latestBuildName              = "";
    private Map<String, Set<String>> buildErrorMap                = new HashMap<>();
    protected Document               testConfig;
    private String                   localAttachmentPath;
    private String                   remoteTestcaseTempPath;
    private String                   hostname;
    private String                   userName;
    private String                   userPass;
    private String                   testCaseStepsFilePath;

    public enum ERROR_CODE { ERROR, OK, WARN }

    ;
    public BaseTestcase(String testcaseName) {
        this.testcaseName = testcaseName;
        String classname = getClass().getSimpleName().toLowerCase();
        testdataPath   = "testdata-base/testcases/" + classname;
        configFileName = testdataPath + "/" + classname + ".xml";

        try {
            testConfig = TestcaseConfigHelper.loadTestcaseConfig(System.getProperty("user.dir") + "/" + configFileName);
            rootLogger.warning("TEST DESCRIPTION: " + testcaseDescription);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    public boolean addError(String error) {
        String      buildName = this.getLatestBuildName();
        Set<String> errList   = buildErrorMap.get(buildName);

        if (errList == null) {
            errList = new HashSet<>();
            buildErrorMap.put(buildName, errList);
        }

        if (errList.contains(error)) {
            return false;
        }

        errList.add(error);

        return true;
    }

    public void backupLogs(String logFolerName) {
        String userName        = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "user");
        String password        = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "password");
        String installLogFile  = "/javaservice/" + userName + "/java/sites/" + userName + "/*/logs/install.log.txt";
        String etlLogFile      = "/javaservice/" + userName + "/java/pentaho/" + userName
                                 + "/current/kiwiplan/logs/*.*";
        String confFile        = "/javaservice/" + userName + "/java/pentaho/" + userName
                                 + "/current/kiwiplan/conf/kp_*.xml";
        String attachmentFiles = installLogFile + " " + etlLogFile + " " + confFile + " "
                                 + additionalLogAttachmentFiles;
        String targetFolder = ATTACHMENT_NAME + "/" + logFolerName;
        String cmd          = "cd " + this.remoteTestcaseTempPath + "; mkdir -p " + targetFolder + "; cp -rf "
                              + attachmentFiles + " " + targetFolder;

        OSUtil.runRemoteCommand(hostname, userName, password, cmd);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long   estimatedTime = System.currentTimeMillis() - startTime;
        long   second        = (estimatedTime / 1000) % 60;
        long   minute        = (estimatedTime / (1000 * 60)) % 60;
        long   hour          = (estimatedTime / (1000 * 60 * 60)) % 24;
        String timeElapsed   = String.format("%02d:%02d:%02d", hour, minute, second);

        System.out.println(timeElapsed);
    }

    public void postSubTest(int subTestIndex) {
        DBHelper.getInstance().closeDBConnection();
    }

    public void preSubTest(int subTestIndex) {
        String classname = getClass().getSimpleName().toLowerCase();

        this.hostname = ConfigHelper.getInstance().getConfig("System.ServerHostName");
        this.userName = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "user");
        this.userPass = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "password");

        String userHome = OSUtil.getRemoteUserHome(hostname, userName, userPass);

        this.remoteTestcaseTempPath = userHome + "/temp_" + this.getLatestBuildName() + "/" + classname;

        String cmd = "rm -rf " + remoteTestcaseTempPath + "; mkdir -p " + remoteTestcaseTempPath;

        OSUtil.runRemoteCommand(hostname, userName, userPass, cmd);
        logger.severe("");

        String localHostName = "localhost";

        try {
            InetAddress inetAddress = InetAddress.getLocalHost();

            localHostName = inetAddress.getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        logger.severe("Current  build name: " + this.getLatestBuildName());
        logger.severe("Previous build name: " + this.getPreBuildName());
        logger.severe("Local hostname : " + localHostName);
        logger.severe("Current local directory : " + OSUtil.BASE_FOLDER);
        logger.severe("Current remote hostname : " + this.hostname);
        logger.severe("Current remote username : " + this.userName);
        logger.severe("Current remote home directory : " + OSUtil.getRemoteUserHome(this.hostname,
                                                                                                          this.userName,
                                                                                                          this.userPass));
        logger.severe("\n\n");
    }

    public void prepareAttachments() {
        backupLogs("failureLogConf");

        String userName = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "user");
        String password = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "password");
        String hostname = ConfigHelper.getInstance().getConfig("System.ServerHostName");

//
//      String installLogFile = "/javaservice/" + userName + "/java/sites/" + userName + "/*/logs/install.log.txt";
//      String etlLogFile = "/javaservice/" + userName + "/java/pentaho/" + userName + "/current/kiwiplan/logs/*.txt";
//
//      String confFile = "/javaservice/" + userName + "/java/pentaho/" + userName + "/current/kiwiplan/conf/kp_*.xml";
//
//      String attachmentFiles = installLogFile + " " + etlLogFile + " " + confFile + " " + additionalLogAttachmentFiles;
        String cmd = "cd " + this.remoteTestcaseTempPath;    // mkdir " + ATTACHMENT_NAME + "; cp -rf " + attachmentFiles + " " + ATTACHMENT_NAME;

        cmd = cmd + "; tar cvfz " + ATTACHMENT_NAME + ".tar.gz " + ATTACHMENT_NAME + "/*; chmod -R 777 *";
        OSUtil.runRemoteCommand(hostname, userName, password, cmd);

        try {
            File tmpFile = File.createTempFile("kdwauto.logattachment.", ".tar.gz");

            OSUtil.scpToLocal(tmpFile.getAbsolutePath(),
                              this.remoteTestcaseTempPath + '/' + ATTACHMENT_NAME + ".tar.gz",
                              hostname,
                              userName,
                              password);
            localAttachmentPath = tmpFile.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String printDiffLines(List<String> diffLines) {
        StringBuilder diffOut = new StringBuilder();

        if (this.hasDiffLines(diffLines)) {
            diffOut.append("\n----------------------------------------------------------------DIFF----------\n");

            for (String line : diffLines) {
                diffOut.append(line + '\n');
            }

            diffOut.append("--------------------------------------------------------------------------------\n");
            this.logger.severe(diffOut.toString());
        }

        return diffOut.toString();
    }

    public List<TestCaseResult> runAllSubTests(int subTestIndex) {

        List<TestCaseResult> subTestResultList = new ArrayList<>();
        String               newInstallerName  = getLatestBuildName();

//      String newRevision = KDWHelper.getRevFromBuildName(newInstallerName);
        String failDetailRecipient = ConfigHelper.getInstance().getConfig("FailsDetailRecipients");
        int    testCount           = TestcaseConfigHelper.getTestCount(testConfig);

        for (int testIndex = 0; testIndex < testCount; testIndex++) {
            if ((subTestIndex >= 0) && (subTestIndex != testIndex)) {
                continue;
            }

            String subTestName = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "testname");

            this.setUpSubTestcaseLogger(testIndex);
            preSubTest(testIndex);

            String         testName  = newInstallerName + " TestCase : " + this.getClass().getSimpleName() + "/"
                                       + subTestName;
            long           startTime = System.currentTimeMillis();
            TestCaseResult subResult = new TestCaseResult(this.getClass().getSimpleName(),
                                                          subTestName,
                                                          testIndex,
                                                          startTime);
            ERROR_CODE code = runSubTest(this.hostname, this.userName, this.userPass, testIndex);

            subResult.setTcResult(code);
            logger.info("RESULT: " + code.name() + " for " + testName);

            if (code == ERROR_CODE.ERROR) {
                String subject = "FAILED: " + testName;

//              logger.severe(subject);
                prepareAttachments();
                MailUtil.sendJavaMail(subject,
                                      this.getStepLogDetailFileContent(),
                                      getLocalLogAttachments(),
                                      failDetailRecipient,
                                      "text/plain; charset=utf-8");
            }

            postSubTest(testIndex);

            long   elapsedTime    = System.currentTimeMillis() - startTime;
            long   second         = (elapsedTime / 1000) % 60;
            long   minute         = (elapsedTime / (1000 * 60)) % 60;
            long   hour           = (elapsedTime / (1000 * 60 * 60));
            String timeElapsedStr = String.format("%02d:%02d:%02d", hour, minute, second);

            subResult.setTimeElapsed(timeElapsedStr);
            subTestResultList.add(subResult);
        }

        return subTestResultList;
    }

    protected abstract ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex);

    protected void setAdditionalLogAttachmentFiles(String attachments) {
        additionalLogAttachmentFiles = attachments;
    }

    public boolean hasDiffLines(List<String> outLines) {
        for (String line : outLines) {
            if ((line != null) && (line.startsWith("<") || line.startsWith(">"))) {
                return true;
            }
        }

        return false;
    }

    public String getLatestBuildName() {
        return latestBuildName;
    }

    public void setLatestBuildName(String latestBuildName) {
        this.latestBuildName = latestBuildName;
    }

    public String getLocalLogAttachments() {
        return localAttachmentPath;
    }

    public String getPreBuildName() {
        return preBuildName;
    }

    public void setPreBuildName(String preBuildName) {
        this.preBuildName = preBuildName;
    }

    protected String getRemoteTestcaseTempPath() {
        return remoteTestcaseTempPath;
    }

    public String getStepLogDetailFileContent() {
        File f = new File(testCaseStepsFilePath);

        try {
            return FileUtils.readFileToString(f);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "FileNotFoundError";
    }

    protected void setTestcaseDescription(String testcaseDescription) {
        this.testcaseDescription = testcaseDescription;
    }

    private void setUpSubTestcaseLogger(int testIndex) {
        String className = this.getClass().getSimpleName().toLowerCase();
        String logFolder = ConfigHelper.getInstance().getConfig("logFolder");
        String rev       = KDWHelper.getRevFromBuildName(this.getLatestBuildName());

        this.testCaseStepsFilePath = OSUtil.BASE_FOLDER + "/" + logFolder + "/" + className + '_' + rev + '_'
                                     + testIndex + ".log";

//      this.testCaseStepsFilePath = SystemHelper.BASE_FOLDER + "/" + logFolder + "/" + className + '_' + testIndex + "_steps.log";
        Logger tcLogger = Logger.getLogger(this.getClass().getName());
        String logLevel = ConfigHelper.getInstance().getConfig("loglevel");

        tcLogger.setLevel(Level.parse(logLevel));

        Handler[] handlers = tcLogger.getHandlers();

        for (Handler handler : handlers) {
            tcLogger.removeHandler(handler);
            handler.close();
        }

        try {
            Handler          fh           = new FileHandler(testCaseStepsFilePath);
            SimpleDateFormat formatter    = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
            Formatter        logFormatter = new SimpleFormatter() {
                public String format(LogRecord record) {
                    String formattedDate = formatter.format(new Date(record.getMillis()));

                    return String.format("%s %s%s", formattedDate, record.getMessage(), "\n");
                }
            };

            fh.setFormatter(logFormatter);
            tcLogger.setLevel(Level.parse(logLevel));
            tcLogger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }

        CompareDBs.getInstance().setLogger(tcLogger);
        this.logger = tcLogger;
    }

    public boolean prepareSourceMapAndMesDatabases(String sourceHostname, String sourceUsername, String sourcePassword, String sourceDBType,
                                                   String mapSourceRevision, String mapsourceRestore, String mesSourceRevision,
                                                   String mesJavaVersion, String mesSourceRestore) {

        String sourceDBTypeSetupCmd = (sourceDBType.equalsIgnoreCase("ISAM")) ? "setupsql isam" : "setupsql mysql";

        // setup environment with QA restore scripts
        String cmd = "stop_services; "
                + "restinit -a; "
                + "rm -rf /javaservice/$USER/*; "
                + "chbase " + mapSourceRevision + "; "
                + sourceDBTypeSetupCmd + "; "
                + mapsourceRestore + "; "
                + "chjbase " + mesSourceRevision + " " + mesJavaVersion + "; "
                + mesSourceRestore + "; ";
//                + "start_services";

        return OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);

    }


}
