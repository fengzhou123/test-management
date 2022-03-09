package kiwiplan.kdw.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;

import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.codec.binary.Hex;

import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class SSHStreamReaderThread extends Thread {
    final private static char   LF          = '\n';
    final private static char   CR          = '\r';
    public Queue<String>        outputQueue = new ConcurrentLinkedQueue<>();
    private PushbackInputStream inputStream;
    private OutputStream        outputStream;
    private String              prompt;
    private boolean             endOfCommand;

    public SSHStreamReaderThread(String name, InputStream in, OutputStream out) {
        this.setName(name);
        this.inputStream  = new PushbackInputStream(new StreamGobbler(in));
        this.outputStream = out;
    }

    public synchronized void exeCMD(final String cmd) {

//      LogHelper.logTestcaseAll(LogHelper.LOGLEVEL_INFO, "SSH SHELL CMD: " + cmd);
        this.setEndOfCommand(false);

        try {
            this.outputStream.write(cmd.getBytes());
            this.outputStream.write(LF);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String                  remoteHost = "nzkdwserver";
        String                  userName   = "java09";
        String                  userPass   = "mapadm99";
        ch.ethz.ssh2.Connection con        = OSUtil.getSsh2Connection(remoteHost, userName, userPass);

        try {
            Session session = con.openSession();

            session.requestPTY("vt100");
            session.startShell();

            try (InputStream stdOutIn = session.getStdout();
                OutputStream os = session.getStdin();
                InputStream stdErrin = session.getStderr()) {
                SSHStreamReaderThread utilOutIn = new SSHStreamReaderThread("stdOUT", stdOutIn, os);

                utilOutIn.start();

                SSHStreamReaderThread utilErrIn = new SSHStreamReaderThread("stdErr", stdErrin, os);

                utilErrIn.start();
                utilOutIn.wait4LoginPrompt();

                List<String> cmdList = new ArrayList<>();

                cmdList.add("whoami");
                cmdList.add("base");
                cmdList.add("kdwcleanuninstall");

//              cmdList.add("restsjava -f kdw_auto");
                cmdList.add("echo $?");

                for (String cmd : cmdList) {
                    utilOutIn.exeCMD(cmd);

                    while (!utilOutIn.isEndOfCommand()) {
                        utilOutIn.sleepInSeconds(1);

                        String lineInQueue = utilOutIn.outputQueue.poll();

                        while (lineInQueue != null) {
                            System.out.println("\tREAD BACK: " + lineInQueue);
                            lineInQueue = utilOutIn.outputQueue.poll();
                        }
                    }
                }

                System.out.println("EO CMD");
            }

            session.close();
            con.close();
            System.out.println("EOF");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        boolean       eol        = true;
        StringBuilder lineBuffer = new StringBuilder();

        while (true) {
            char ch;

            try {
                int i = this.inputStream.read();

                if (i == -1) {
                    break;
                }

                ch = (char) i;

//              if (ch == '\uffff') break;
            } catch (IOException e) {
                e.printStackTrace();

                break;
            }

            switch (ch) {
            case CR :
            case LF :
                if (!eol) {
                    String line = lineBuffer.toString();

                    outputQueue.offer(line);
                    lineBuffer.setLength(0);
                }

                eol = true;

                break;

            default :
                if (eol) {
                    eol = false;
                }

                lineBuffer.append(ch);

                break;
            }

            if (lineBuffer.toString().equals(this.prompt) || lineBuffer.toString().equals("\u001b[98m" + this.prompt)) {

//              System.out.printf("STREAM READER Line buffer is: [%s]\n", Hex.encodeHexString(lineBuffer.toString().getBytes(/* charset */)));
                this.setEndOfCommand(true);
            }
        }
    }

    public void sleepInSeconds(int seconds) {
        try {
            this.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void wait4LoginPrompt() {
        sleepInSeconds(3);
        this.getLastLine();

        while (true) {
            this.exeCMD("");
            sleepInSeconds(1);

            String first = this.getLastLine();

            if (first != null) {
                this.exeCMD("");
                sleepInSeconds(1);

                String second = this.getLastLine();

                if ((first != null) && (second != null) && first.equals(second)) {
                    this.setPrompt(first);

                    break;
                }
            }
        }
    }

    public boolean isEndOfCommand() {
        return this.endOfCommand;
    }

    public void setEndOfCommand(boolean endOfCommand) {
        this.endOfCommand = endOfCommand;
    }

    public synchronized String getLastLine() {
        String line     = this.outputQueue.poll();
        String prevLine = line;

        while (line != null) {
            line = this.outputQueue.poll();

            if (line != null) {
                prevLine = line;
            }
        }

        return prevLine;
    }

    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }
}
