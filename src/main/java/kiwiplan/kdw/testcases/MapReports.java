package kiwiplan.kdw.testcases;

import java.io.File;
import java.io.IOException;

import java.sql.ResultSet;

import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

import kiwiplan.kdw.core.*;
import kiwiplan.kdw.tasks.CompareDBs;
import kiwiplan.kdw.testcases.base.BaseTestcase;
import kiwiplan.kdw.utils.OSUtil;

public class MapReports extends BaseTestcase {
    String         sourceFilePath     = OSUtil.BASE_FOLDER + "/temp/mapreports";
    private String mapReportDBName    = "kdw_kdw9101_mapreport_0";
    private String kdwMapReportDBName = "kdw_kdw9101_kdwmapreport_0";

    public MapReports() {
        super("MAP Report");

        String text =
            "Map report Test. Generate Map report using source dataset and generate Map report using KDW result, then compare them.";

        setTestcaseDescription(text);
    }

    boolean compareTwoMapReports() {

        // get table list
        String sql = "SELECT * FROM inf" + "ormation_schema.tables where table_schema='" + this.kdwMapReportDBName
                     + "'";
        List<String> tableNameList = new ArrayList<>();

        try (ResultSet tableRS = DBHelper.getInstance().executeSQL(sql);) {
            while (tableRS.next()) {
                String tableName = tableRS.getString("TABLE_NAME");

                tableNameList.add(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            for (String tableName : tableNameList) {

//              String tableName = tableRS.getString("TABLE_NAME");
                Map<String, String>    toleranceMap    = new HashMap<String, String>();
                Map<String, String>    starlenMap      = new HashMap<String, String>();
                List<String>           naturalkeysList = new Vector<String>();
                Document               dom;
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder        db  = dbf.newDocumentBuilder();

                dom = db.parse(new File(OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + "mapkdwreportsmapping/"
                                        + tableName + ".xml"));

                NodeList document       = dom.getChildNodes();
                Node     mapping        = document.item(0);
                NodeList mappingConfigs = mapping.getChildNodes();

                for (int i = 0; i < mappingConfigs.getLength(); i++) {
                    Node mappingConfig = mappingConfigs.item(i);

                    if (mappingConfig.getNodeName().equals("columnmapping")) {
                        String mapName = mappingConfig.getAttributes().getNamedItem("mapname").getTextContent().trim();
                        String kdwName = mappingConfig.getAttributes().getNamedItem("kdwname").getTextContent().trim();

                        if (!kdwName.isEmpty()) {
                            String tolerance = "0";
                            Node   node      = mappingConfig.getAttributes().getNamedItem("tolerance");

                            if (node != null) {
                                tolerance = node.getTextContent().trim();
                            }

                            if (!tolerance.equalsIgnoreCase("0")) {
                                toleranceMap.put(mapName, tolerance);
                            }

                            Node starlenNode = mappingConfig.getAttributes().getNamedItem("starlen");

                            if (starlenNode != null) {
                                starlenMap.put(mapName, starlenNode.getTextContent().trim());
                            }
                        }
                    } else if (mappingConfig.getNodeName().equals("naturalkeys")) {
                        String          naturalkeys = mappingConfig.getTextContent().trim();
                        StringTokenizer st          = new StringTokenizer(naturalkeys, ",");

                        while (st.hasMoreElements()) {
                            naturalkeysList.add(st.nextToken());
                        }
                    }
                }

                // put kdwMapReportDBName as the first parameter since it's columns are subset of mapReportDBName
                this.logger.severe(String.format("Comparing Tables: %s on both databases: %s vs %s",
                                                 tableName,
                                                 this.kdwMapReportDBName,
                                                 this.mapReportDBName));

                if (CompareDBs.getInstance()
                              .compareNormalDBsTableWithSameSchemaWithRoundAndStarconvet(this.kdwMapReportDBName,
                                                                                         this.mapReportDBName,
                                                                                         tableName,
                                                                                         naturalkeysList,
                                                                                         toleranceMap,
                                                                                         starlenMap) == false) {
                    this.logger.severe(String.format("Error to compare table: %s on both databases: %s vs %S",
                                                     tableName,
                                                     this.kdwMapReportDBName,
                                                     this.mapReportDBName));

                    return false;
                } else {
                    this.logger.info(String.format("Success to compare table: %s on both databases: %s vs %S",
                                                   tableName,
                                                   this.kdwMapReportDBName,
                                                   this.mapReportDBName));
                }
            }
        } catch (ParserConfigurationException pce) {
            System.out.println(pce.getMessage());
        } catch (SAXException se) {
            System.out.println(se.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    boolean generateMapReportFile(MapReportHelper mapReportHelper, int testIndex) {
        String sourceUsername = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "sourceuser");
        String sourcePassword = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcepassword");
        String sourceHostname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcehostname");
        String mapreportDate = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                  testIndex,
                                                                                  "mapreportdate");
        String togglefile = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "togglefile");

        // here I use the table as the script name to generate the map report
        String shellScript = "rm -f *LS; cp /data/kdw/automation/tools/back/" + sourceUsername + "/kwsql .;"
                             + "cp /data/kdw/automation/tools/genmapreport/tog/" + togglefile + " .;"
                             + "/data/kdw/automation/tools/genmapreport/" + mapReportHelper.getOutputTableName()
                             + ".sh " + mapreportDate;

        shellScript += "; find .  -regex \".*\\.LS\" | xargs readlink -f;";

        List<String> remoteCommandOut = OSUtil.getRemoteSingleCommandOut(sourceHostname,
                                                                         sourceUsername,
                                                                         sourcePassword,
                                                                         shellScript);
        List<String> LSFileList = new ArrayList<>();

        for (String line : remoteCommandOut) {
            if (line.endsWith("LS")) {
                LSFileList.add(line);
            }
        }

        if (LSFileList.size() != 1) {
            this.logger.severe("Error: could not find LS file on the remote server after generating the MAP report!");
            this.logger.severe("Error: count of LS files: " + LSFileList.size());

            return false;
        }

        for (String remoteF : LSFileList) {
            int    index         = remoteF.lastIndexOf('/');
            String fileName      = remoteF.substring(index + 1);
            String localFileName = sourceFilePath + '/' + fileName;

            OSUtil.scpToLocal(localFileName, remoteF, sourceHostname, sourceUsername, sourcePassword);
        }

        return true;
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
        Document               dom;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder        db;

        try {
            db  = dbf.newDocumentBuilder();
            dom = db.parse(new File(OSUtil.BASE_FOLDER + "/" + "testdata/testcases/mapreports" + "/"
                                    + "mapkdwreportsmapping/" + "csc_hc_production_report_mapping" + ".xml"));

            NodeList document       = dom.getChildNodes();
            Node     mapping        = document.item(0);
            NodeList mappingConfigs = mapping.getChildNodes();

            for (int i = 0; i < mappingConfigs.getLength(); i++) {
                Node mappingConfig = mappingConfigs.item(i);

                if (mappingConfig.getNodeName().equals("columnmapping")) {
                    String kdwName = mappingConfig.getAttributes().getNamedItem("kdwname").getTextContent().trim();

                    if (!kdwName.isEmpty()) {
                        String tolerance = "0";
                        Node   node      = mappingConfig.getAttributes().getNamedItem("tolerance");

                        if (node != null) {
                            tolerance = node.getTextContent().trim();
                            System.out.println(tolerance);
                        }
                    }
                } else if (mappingConfig.getNodeName().equals("naturalkeys")) {
                    String naturalkeys = mappingConfig.getTextContent().trim();

                    System.out.println(naturalkeys);
                }
            }
        } catch (ParserConfigurationException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SAXException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    boolean populateMapReport(MapReportHelper mapReportHelper, int testIndex) {
        String installerName = getLatestBuildName();
        String revision      = KDWHelper.getRevFromBuildName(installerName);

        this.mapReportDBName = "kdw_" + revision.replace(".", "").replace("-", "") + "_mapreport_" + testIndex;

        if (mapReportHelper.populateReportOutputTable(mapReportDBName) == false) {
            return false;
        }

        return true;
    }

    @Override
    public void postSubTest(int testIndex) {
        CompareDBs.setColumnFilterList("");
        CompareDBs.setTableFilterList("");
        super.postSubTest(testIndex);
    }

    @Override
    public void preSubTest(int testIndex) {

        // TODO Auto-generated method stub
        super.preSubTest(testIndex);

        String tableFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "tablefilterlist");
        String columnFilterList = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                     testIndex,
                                                                                     "columnfilterlist");

        CompareDBs.setColumnFilterList(columnFilterList);
        CompareDBs.setTableFilterList(tableFilterList);
    }

    public ERROR_CODE runSubTest(String hostname, String username, String password, int testIndex) {

        /**
         * 1, Source Dataset preparing:
         * login to server (nzvader)
         * restore dataset
         * chbase 8.20
         * setupsql sql
         * restipbowling
         * 2, Generate Map report file:
         * Prepare toggle files for each report.
         * Current:
         * Have IPs toggle files. Will use IPs ones
         * The mapping between reports and toggle files: toggleFileMapping.txt
         * Todo:
         * Replace the toggle file with IP version programmatically. (Kyle)
         * Create command files to generate report files:
         * Eg: Put commands (specify the period): csc00->h->c-> into .test file and run : csc00 - i .test
         * Todo: generate the command files for each reports. (Andre)
         * Got the file (like P529471_1.LS) somewhere
         * Current: We have some output files for development: P529471_1.LS
         * Todo: figure out the location of the reports generated by the commands. (Andre)
         * 3, Populate Map report data into mysql database:
         * Current: Albert's tool, and config file for csc_hc_production_report
         * Todo:
         * 1, Integrate it into KDW automation framework (Kyle)
         * 2, Optimize the configure file for csc_hc_production_report (Kyle)
         * 3, Generate all the configure files for all reports after the one for csc_hc_production_report finalized (Andre)
         * 4, Get KDW result:
         * run KDW against the source dataset for the time period specified in section #2
         * 5, Make Map report based on KDW result
         * Current:
         * Has Tonys report.
         * Has analyse result from Andre about the table structure mapping and column data mapping for most of the reports.
         * Todo:
         * 1, use Tony's report to generate database and table against KDW result. (Kyle)
         * 2, figure out the mapping between Map report and Tonys report ( table structure and column data ) and create configuration files.
         * a)     CSC-HC. Try to use configuration to control the column renaming and data converting. (Kyle)
         * b)     Other reports configuration files. (Andre)
         * 3, Use the configure file to generate the database and tables. (Kyle)
         * 6, Compare with the two reports
         *
         */

        // 1, source dataset prepare
        this.logger.severe("Step 1: prepare source dataset");

        if (sourceDatasetPrepare(testIndex) == false) {
            return ERROR_CODE.ERROR;
        }

        if (testEveryMapReports(testIndex) == false) {
            return ERROR_CODE.ERROR;
        }

        return ERROR_CODE.OK;
    }

    boolean sourceDatasetPrepare(int testIndex) {
        String sourceUsername = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig, testIndex, "sourceuser");
        String sourcePassword = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcepassword");
        String sourceHostname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcehostname");
        String sourceRevision = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                   testIndex,
                                                                                   "sourcerevision");
        String sourceRestore = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                  testIndex,
                                                                                  "sourcerestore");
        String cmd = "";

        // clean env
        cmd = "restinit -a;rm -rf /javaservice/$USER/*; "

        // chbase and restore the latest dataset
        + "chbase " + sourceRevision + "; " + "setupsql sql; " + sourceRestore + ";";

        return OSUtil.runRemoteCommand(sourceHostname, sourceUsername, sourcePassword, cmd);
    }

    private boolean testEveryMapReports(int testIndex) {
        String mapReportConfigPath = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + "mapreportsconf/";
        String kdwReportConfigPath = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + "mapkdwreportsmapping/";
        String tonyReportSqlPath   = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + "tonyreportsqls/";
        File   path                = new File(mapReportConfigPath);

        if (!path.exists()) {

            // path did not exist
            this.logger.severe("MAP report config path " + mapReportConfigPath + " does not exist!");

            return false;
        }

        File sourceFilePathF = new File(sourceFilePath);

        try {
            FileUtils.deleteDirectory(sourceFilePathF);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileUtils.forceMkdir(sourceFilePathF);
        } catch (IOException e) {
            e.printStackTrace();

            return false;
        }

        // get script file list
        for (final File fileEntry : path.listFiles()) {
            if (fileEntry.isDirectory()) {
                continue;
            }

            MapReportHelper mapReportHelper = new MapReportHelper(sourceFilePath,
                                                                  mapReportConfigPath + "/" + fileEntry.getName());

            // 2, Generate Map report file
            this.logger.severe("Step 2: Generate Map report file");

            if (generateMapReportFile(mapReportHelper, testIndex) == false) {
                return false;
            }

            // 3, Populate Map report data into mysql database
            this.logger.severe("Step 3: Populate Map report data into mysql database");

            if (populateMapReport(mapReportHelper, testIndex) == false) {
                return false;
            }

            // 4, Get KDW result
            this.logger.severe("Step 4: Get KDW result");

            if (getKDWResult(testIndex) == false) {
                return false;
            }

            KDWMapReportHelper kdwMapReportHelper = new KDWMapReportHelper();

            kdwMapReportHelper.loadConfig(tonyReportSqlPath,
                                          kdwReportConfigPath + "/" + fileEntry.getName().replace("config", "mapping"));

            // 5, Make Map report based on KDW result
            this.logger.severe("Step 5: Make Map report based on KDW result");

            if (getMapReportWithKDWResult(kdwMapReportHelper, testIndex) == false) {
                return false;
            }

            // 6, Compare the two reports
            this.logger.severe("Step 6: Compare the two reports");

            if (compareTwoMapReports() == false) {
                return false;
            }
        }

        return true;
    }

    boolean getKDWResult(int testIndex) {

        // run ETL
        boolean bResult        = true;
        String  hostname       = ConfigHelper.getInstance().getConfig("System.ServerHostName");
        String  username       = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "user");
        String  password       = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "password");
        String  runStartDate   = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "runstartdate");
        String  runEndDate     = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "runenddate");
        String  period         = runStartDate + " " + runEndDate;
        String  siteconfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                    testIndex,
                                                                                    "siteconfigname");
        String kpsitefile    = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + siteconfigname + ".xml";
        String etlconfigname = TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                  testIndex,
                                                                                  "etlconfigname");
        String kpetlfile        = OSUtil.BASE_FOLDER + "/" + testdataPath + "/" + etlconfigname + ".xml";
        String newInstallerName = getLatestBuildName();
        String newRevision      = KDWHelper.getRevFromBuildName(newInstallerName);
        String newInstallerPath = ConfigHelper.getInstance()
                                              .getConfig("BuildRootFolder") + "/" + newRevision + "/"
                                                                            + getLatestBuildName();

        if (KDWHelper.kdwFreshInstall(newRevision, hostname, newInstallerPath, username, password, false) == false) {
            this.logger.severe("Failed to Install KDW revision : " + newInstallerPath + " for user: " + username);
            bResult = false;

            return bResult;
        }

        if (KDWHelper.kdwRunETL(newRevision, hostname, username, password, kpsitefile, kpetlfile, period) == false) {
            this.logger.severe("Failed to run ETL " + period);    // $NON-NLS-1$
            bResult = false;
            this.logger.severe("Test Step Failed ");

            return bResult;
        }

        // check result
        if (KDWHelper.kdwCheckETLResult(hostname, username, password) == false) {
            this.logger.severe("KDW ETL running failed. Period: " + period + " Installer: " + newInstallerPath);
            bResult = false;
            this.logger.severe("Test Step Failed ");

            return bResult;
        }

        return bResult;
    }

    boolean getMapReportWithKDWResult(KDWMapReportHelper kdwMapReportHelper, int testIndex) {
        String installerName = getLatestBuildName();
        String revision      = KDWHelper.getRevFromBuildName(installerName);

        this.kdwMapReportDBName = "kdw_" + revision.replace(".", "").replace("-", "") + "_kdwmapreport_" + testIndex;

        String sql4LoadReportData = kdwMapReportHelper.generateSql4Report(kdwMapReportDBName);
        String username           = TestcaseConfigHelper.getTestcaseConfigValue(testConfig, "user");
        String sourceDB           = username + "_masterDW";
        String sql                = "use " + sourceDB + ";";

        // below variables come from IP's UAT scripts. Some scripts use same name variable but different meanning.
        // The scripts were updated accordingly. And original scripts are save in the "original" folder
        sql = sql + "SET @EnterReportDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                    testIndex,
                                                                                                    "EnterReportDate") + "';";
        sql = sql + "SET @EnterReportPeriod = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                      testIndex,
                                                                                                      "EnterReportPeriod") + "';";    // Some script set the value as date, like '2014-03-01'
        sql = sql + "SET @EnterNoShiftCode = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                     testIndex,
                                                                                                     "EnterNoShiftCode") + "';";
        sql = sql + "SET @PlantDataset = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                 testIndex,
                                                                                                 "PlantDataset") + "';";
        sql = sql + "SET @Period1StartDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                     testIndex,
                                                                                                     "Period1StartDate") + "';";
        sql = sql + "SET @Period1EndDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                   testIndex,
                                                                                                   "Period1EndDate") + "';";
        sql = sql + "SET @ReportStartDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                    testIndex,
                                                                                                    "ReportStartDate") + "';";
        sql = sql + "SET @ReportEndDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                  testIndex,
                                                                                                  "ReportEndDate") + "';";
        sql = sql + "SET @EnterStartDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                   testIndex,
                                                                                                   "EnterStartDate") + "';";
        sql = sql + "SET @EnterEndDate = '" + TestcaseConfigHelper.getTestcaseSubTestConfigValue(testConfig,
                                                                                                 testIndex,
                                                                                                 "EnterEndDate") + "';";
        sql = sql + "create database if not exists " + kdwMapReportDBName + ";";
        sql = sql + sql4LoadReportData;
        System.out.println(sql);

        try {
            DBHelper.getInstance().executeSQL(sql);
        } catch (Exception e) {

            // TODO Auto-generated catch block
            e.printStackTrace();

            return false;
        }

        return true;
    }
}
