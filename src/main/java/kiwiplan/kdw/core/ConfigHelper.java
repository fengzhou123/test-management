package kiwiplan.kdw.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigHelper {
    private static final String OVERALLTESTCONFIG_FILE_NAME = "conf/testdata-base.properties";
    private static ConfigHelper singleton                   = new ConfigHelper();
    private Properties          overallTestConfig;

    private ConfigHelper() {
        if (overallTestConfig == null) {
            overallTestConfig = new Properties();

            try {
                overallTestConfig.load(new FileInputStream(OVERALLTESTCONFIG_FILE_NAME));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String interpolateKeys(String rawString) {
        String       regExp       = "\\[(\\S*?)\\]";
        Pattern      pattern      = Pattern.compile(regExp);
        List<String> vairableList = new ArrayList<>();
        Matcher      m            = pattern.matcher(rawString);

        while (m.find()) {
            vairableList.add(m.group(1));
        }

        for (String key : vairableList) {
            String keyV = getInstance().getConfig(key);

            rawString = rawString.replace("[" + key + "]", keyV);
        }

        return rawString;
    }

    public String getConfig(String key) {
        return overallTestConfig.getProperty(key);
    }

    /* Static 'instance' method */
    public static ConfigHelper getInstance() {
        return singleton;
    }
}
