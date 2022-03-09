package kiwiplan.kdw.core;

import java.io.IOException;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.logging.*;

public class LogHelper {
    public static boolean rootLoggerInit = false;

    public static void main(String[] args) {}

    public static Logger getRootLogger() {
        if (rootLoggerInit) {
            return Logger.getLogger("");
        }

        String logFolder       = ConfigHelper.getInstance().getConfig("logFolder");
        String summaryFilePath = System.getProperty("user.dir") + "/" + logFolder + "/summary.log";
        Logger rootLogger      = Logger.getLogger("");

        rootLogger.setLevel(Level.ALL);

        try {
            FileHandler      fh           = new FileHandler(summaryFilePath);
            SimpleDateFormat formatter    = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
            Formatter        logFormatter = new SimpleFormatter() {
                public String format(LogRecord record) {
                    String formattedDate = formatter.format(new Date(record.getMillis()));

                    return String.format("%s %s%s", formattedDate, record.getMessage(), "\n");
                }
            };
            String logLevel = ConfigHelper.getInstance().getConfig("loglevel");

            fh.setLevel(Level.parse(logLevel));
            rootLogger.addHandler(fh);

            for (Handler handler : rootLogger.getHandlers()) {
                handler.setFormatter(logFormatter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        rootLoggerInit = true;

        return rootLogger;
    }
}
