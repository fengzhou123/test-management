package kiwiplan.kdw.utils;

import java.io.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.SCPInputStream;
import ch.ethz.ssh2.SCPOutputStream;
import ch.ethz.ssh2.Session;

import kiwiplan.kdw.core.LogHelper;

public class OSUtil {
    public static String               MYSQL_JAR_FILE_PATH = "scripts/mysql-connector-java-5.1.46-bin.jar";
    static Logger                      rootLogger          = LogHelper.getRootLogger();
    private static Map<String, String> userHomeMap         = new ConcurrentHashMap<>();
    public final static String         BASE_FOLDER         = System.getProperty("user.dir");

    public static void main(String[] args) {

//      List<String> cmdList = new ArrayList<>();
//      cmdList.add("sshpass");
//      cmdList.add("-p");
//      cmdList.add("mapadm99");
//      cmdList.add("scp");
//      cmdList.add("/data/kdw/automation/kdw-9.10.1/testdata/testcases/ipdailyregression/kp_sites_n1_n_base.xml");
//      cmdList.add("java09@nzkdwserver:/javaservice/java09/java/pentaho/java09/current/kiwiplan/conf/kp_sites_n1_n.xml");
//
//      try {
//          int rc = OSUtil.runOsCommand(cmdList, null, 5);
//      } catch (Exception e) {
//          e.printStackTrace();
//      }
//      String localFileName = "/home/mao.li/KDW-Automation/kdwauto-maven/pom.xml";
//      String remoteFileName = "/mapqa/java09_testing/pom.xml";
//
//      int rc = scpToRemote(localFileName, remoteFileName, "nzkdwserver", "java09", "mapadm99");
//      System.out.println(rc);
//
//      localFileName = "/home/mao.li/pom.xml";
//      rc = scpToLocal(localFileName, remoteFileName, "nzkdwserver", "java09", "mapadm99");
//      System.out.println(rc);
//      List<String> cmdList = new ArrayList<>();
//      cmdList.add("whoami");
//      cmdList.add("base");
//      cmdList.add("env");
//      cmdList.add("restsjava -f kdw_auto");
//      cmdList.add("echo $?");
//
        String remoteHost = "nzsctestml";
        String userName   = "java01";
        String userPass   = "mapadm99";

//      String home       = OSUtil.getRemoteUserHome(remoteHost, userName, userPass);
//
//      System.out.println("$HOME: " + home);
//      ch.ethz.ssh2.Connection conn = getSsh2Connection(remoteHost, userName, userPass);
//      OSUtil.runSshBashCmdList(conn, cmdList);
//      StringBuilder buffer = new StringBuilder(10);
//
//
//      for(char c: "abcdefghijklmn".toCharArray()){
//        buffer.append(c);
//        System.out.printf("%s, %d \n", buffer, buffer.length());
//        if(buffer.length() >= 10){
//            buffer.deleteCharAt(0);
//        }
//      }
        String cmd =
            "diff $PENTAHODIR/kiwiplan/conf/temp_working.sql $PENTAHODIR/../../../sites/java01/current/template/kp-etl-core/sqlsrc/kp-etl-core-9.10.1-INSTALL.sql";
        List<String> commandOut = getRemoteSingleCommandOut(remoteHost, userName, userPass, cmd);

        System.out.println(commandOut);
    }

    public static int runLocalCommand(List<String> cmdList, File workDir, int timeOutInMin) {
        rootLogger.info(cmdList.toString());

        ProcessBuilder pb = new ProcessBuilder(cmdList);

        pb.redirectErrorStream(true);
        pb.directory(workDir);

        try {
            Process      process = pb.start();
            StreamThread st      = new StreamThread("process.reader", process.getInputStream());

            st.start();
            process.waitFor(timeOutInMin, TimeUnit.MINUTES);

            List<String> outlines = st.getOutLines();

            for (String s : outlines) {
                rootLogger.info(s);
            }

            return process.exitValue();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static boolean runRemoteCommand(String hostname, String username, String password, String systemCommand) {

        // if (!systemCommand.endsWith("echo $?")) {
        systemCommand += "; export RC=$?; echo $RC; echo $RC; echo $RC";

        // }
        rootLogger.info(String.format("%s@%s> %s", username, hostname, systemCommand));

        ArrayList<String> cmdList = new ArrayList<>();

        cmdList.add(systemCommand);

        ch.ethz.ssh2.Connection     conn          = getSsh2Connection(hostname, username, password);
        FixedSizeLinkedList<String> fixedSizeList = new FixedSizeLinkedList<>(10);

        runSshBashCmdList(conn, cmdList, fixedSizeList);

        boolean findEcho$ = false;

        for (String line : fixedSizeList) {
            if (line.endsWith("echo $RC")) {
                findEcho$ = true;

                continue;
            }

            if (findEcho$ ){
                if(line.equals("0") || line.equals("") || line == null)
                return true;
                else
                    rootLogger.severe("result = " + line);
            }
        }

        return false;
    }

    public static void runSshBashCmd(ch.ethz.ssh2.Connection con, String command,
                                     FixedSizeLinkedList<String> outLinesList) {
        List<String> cmdList = new ArrayList<>();

        cmdList.add(command);
        cmdList.add("");
        runSshBashCmdList(con, cmdList, outLinesList);
    }

    public static void runSshBashCmdList(ch.ethz.ssh2.Connection con, List<String> cmdList,
                                         FixedSizeLinkedList<String> outLinesList) {
        try {
            if (cmdList.size() == 1) {
                String   singleLine = cmdList.get(0);
                String[] subCmds    = singleLine.split(";");

                cmdList.clear();

                for (String subCmd : subCmds) {
                    subCmd = subCmd.trim();

                    if (subCmd.length() > 0) {
                        cmdList.add(subCmd);
                    }
                }
            }

//          rootLogger.info(con.getHostname() + ": " + cmdList.toString());
            Session session = con.openSession();

            session.requestPTY("vt100");
            session.startShell();

            try (InputStream stdOutIn = session.getStdout();
                OutputStream stdInOut = session.getStdin();
                InputStream stdErrin = session.getStderr()) {
                SSHStreamReaderThread stdOutReader = new SSHStreamReaderThread("stdOUT", stdOutIn, stdInOut);

                stdOutReader.start();

                SSHStreamReaderThread stdErrReader = new SSHStreamReaderThread("stdErr", stdErrin, stdInOut);

                stdErrReader.start();
                stdOutReader.wait4LoginPrompt();

                FixedSizeLinkedList<String> list    = new FixedSizeLinkedList(10);
                boolean                     timeOut = false;

                for (String cmd : cmdList) {
                    if (cmd.trim().equals("")) {
                        rootLogger.info("cmd skip> " + cmd);

                        continue;
                    }

                    list.clear();
                    rootLogger.info("cmd> " + cmd);
                    stdOutReader.exeCMD(cmd);

                    if(con.getHostname().equals("nzvader") && cmd.contains("chjbase")){
                        stdOutReader.exeCMD("export JAVA_HOME=/opt/jdk-11.0.7");
                    }

                    long startT = System.currentTimeMillis();

                    while (!stdOutReader.isEndOfCommand()) {
                        long nowT = System.currentTimeMillis();

                        if ((nowT - startT) >= 1000 * 60 * 60) {
                            rootLogger.severe("Execution of command {" + cmd + "} has exceeded 60 mins. break!");
                            timeOut = true;

                            break;
                        }

                        String lineInQueue = stdOutReader.outputQueue.poll();

                        while (lineInQueue != null) {
                            outLinesList.appendObject(lineInQueue);

//                          rootLogger.info("\tout> " + lineInQueue);
                            list.appendObject(lineInQueue);
                            lineInQueue = stdOutReader.outputQueue.poll();
                            startT = System.currentTimeMillis();
                        }

                        lineInQueue = stdErrReader.outputQueue.poll();

                        while (lineInQueue != null) {
                            outLinesList.appendObject(lineInQueue);

//                          rootLogger.info("\terr> " + lineInQueue);
                            list.appendObject(lineInQueue);
                            lineInQueue = stdErrReader.outputQueue.poll();
                            startT = System.currentTimeMillis();
                        }
                    }

                    for (String line : list) {
                        rootLogger.info("\tout> " + line);
                    }

                    if (timeOut) {
                        rootLogger.severe("Stop processing any more commands. break!");

                        break;
                    }
                }
            }

            session.close();
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int scpToLocal(String localFileName, String remoteFileName, String remoteHost, String userName,
                                 String userPass) {
        try {
            File                    localFile      = new File(localFileName);
            OutputStream            out            = FileUtils.openOutputStream(localFile);
            ch.ethz.ssh2.Connection con            = getSsh2Connection(remoteHost, userName, userPass);
            SCPClient               scpClient      = con.createSCPClient();
            SCPInputStream          scpInputStream = scpClient.get(remoteFileName);

            IOUtils.copy(scpInputStream, out);
            scpInputStream.close();
            out.close();
            con.close();

            return 0;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static int scpToRemote(String localFileName, String remoteFileName, String remoteHost, String userName,
                                  String userPass) {
        try {
            int                     lastSlashIndex  = remoteFileName.lastIndexOf('/');
            String                  remoteFileN     = remoteFileName.substring(lastSlashIndex + 1);
            String                  remoteFileD     = remoteFileName.substring(0, lastSlashIndex);
            File                    localFile       = new File(localFileName);
            FileInputStream         in              = FileUtils.openInputStream(localFile);
            ch.ethz.ssh2.Connection con             = getSsh2Connection(remoteHost, userName, userPass);
            SCPClient               scpClient       = con.createSCPClient();
            SCPOutputStream         scpOutputStream = scpClient.put(remoteFileN,
                                                                    localFile.length(),
                                                                    remoteFileD,
                                                                    "0775");

            IOUtils.copy(in, scpOutputStream);
            in.close();
            scpOutputStream.close();
            con.close();

            return 0;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static String getLocalTempDir() {
        return System.getProperty("user.dir") + "/temp";
    }

    public static List<String> getRemoteSingleCommandOut(String hostname, String username, String password,
                                                         String cmdWithComma) {
        rootLogger.info(String.format("%s@%s> %s", username, hostname, cmdWithComma));

        ArrayList<String> cmdList = new ArrayList<>();

        cmdList.add(cmdWithComma);

        ch.ethz.ssh2.Connection     conn         = getSsh2Connection(hostname, username, password);
        FixedSizeLinkedList<String> outLinesList = new FixedSizeLinkedList<>(-1);

        runSshBashCmd(conn, cmdWithComma, outLinesList);

        int cmdLastLineIndex = 0;

        for (int i = 0; i < outLinesList.size(); i++) {
            String line = outLinesList.get(i);

            if (((i == 0) && line.endsWith(cmdWithComma)) || ((i > 0) && cmdWithComma.endsWith(line))) {
                cmdLastLineIndex = i;

                break;
            }
        }

        for (int i = 0; i <= cmdLastLineIndex; i++) {
            if (outLinesList.size() > 0) {
                outLinesList.remove(0);
            }
        }

        return outLinesList;
    }

    public static String getRemoteUserHome(String hostname, String username, String password) {
        String mapKey   = hostname + username + password;
        String mapValue = userHomeMap.get(mapKey);

        if (mapValue != null) {
            return mapValue;
        }

        List<String> outList = OSUtil.getRemoteSingleCommandOut(hostname, username, password, "echo $EVALHOME");

        while (outList.size() <= 0) {
            outList = OSUtil.getRemoteSingleCommandOut(hostname, username, password, "echo $EVALHOME");

            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        mapValue = outList.get(0);
        userHomeMap.put(mapKey, mapValue);
        rootLogger.severe("USER HOME for " + username + " is: " + mapValue);

        return mapValue;
    }

    public static ch.ethz.ssh2.Connection getSsh2Connection(String hostname, String username, String password) {
        ch.ethz.ssh2.Connection conn = null;

        try {
            conn = new ch.ethz.ssh2.Connection(hostname);
            conn.connect();

            if (conn.authenticateWithPassword(username, password)) {
                return conn;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static class StreamThread extends Thread {
        InputStream  inputStream = null;
        List<String> outLines    = new ArrayList<>();

        public StreamThread(String name, InputStream in) {
            this.setName(name);
            this.inputStream = in;
        }

        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;

                while ((line = br.readLine()) != null) {
                    outLines.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public List<String> getOutLines() {
            return outLines;
        }
    }
}
