package kiwiplan.kdw.core;

import java.io.File;
import java.io.IOException;

import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

public class TestcaseConfigHelper {
    static Logger rootLogger = LogHelper.getRootLogger();

    public static Document loadTestcaseConfig(String fileName)
            throws ParserConfigurationException, SAXException, IOException {
        File                   xmlFile    = new File(fileName);
        DocumentBuilderFactory dbFactory  = DocumentBuilderFactory.newInstance();
        DocumentBuilder        dBuilder   = dbFactory.newDocumentBuilder();
        Document               testConfig = dBuilder.parse(xmlFile);

        testConfig.getDocumentElement().normalize();

        return testConfig;
    }

    public static void main(String[] args) {
        String s = "a[KDWAUTOSRCUSER_1]....[KDWAUTOSRCUSER_2]b";

        ConfigHelper.interpolateKeys(s);
    }

    public static int getTestCount(Document testConfig) {
        if (testConfig == null) {
            return -1;
        }

        NodeList nTestList = testConfig.getElementsByTagName("Test");

        return nTestList.getLength();
    }

    public static String getTestcaseConfigValue(Document testConfig, String nodeName) {
        String nodeValue = "";

        if (testConfig == null) {
            return "";
        }

        NodeList nCaseList = testConfig.getElementsByTagName(nodeName);

        // assert(nList instanceof Element);
        assert(nCaseList.getLength() == 1);
        nodeValue = nCaseList.item(0).getTextContent();

        String newValue;

        if (nodeValue.indexOf('[') >= 0) {
            newValue = ConfigHelper.interpolateKeys(nodeValue);
        } else {
            newValue = nodeValue;
        }

        rootLogger.info(String.format("Test Case node [%s] value: [%s]", nodeName, newValue));

        return newValue;
    }

    public static String getTestcaseSubTestConfigValue(Document testConfig, int configIndex, String nodeName) {
        String nodeValue = "";

        if (testConfig == null) {
            return "";
        }

        NodeList nTestList = testConfig.getElementsByTagName("Test");

        assert(nTestList.getLength() > configIndex);
        nodeValue = ((Element) nTestList.item(configIndex)).getElementsByTagName(nodeName).item(0).getTextContent();

        String newValue;

        if (nodeValue.indexOf('[') >= 0) {
            newValue = ConfigHelper.interpolateKeys(nodeValue);
        } else {
            newValue = nodeValue;
        }

        rootLogger.info(String.format("SubTest [%d] node [%s] value: [%s]", configIndex, nodeName, newValue));

        return newValue;
    }
}
