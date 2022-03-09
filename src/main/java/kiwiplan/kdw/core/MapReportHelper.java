package kiwiplan.kdw.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

//updated from Albert's code
public class MapReportHelper {
    private static final int extendOfColumnSize = 2;
    private static Logger    rootLogger         = LogHelper.getRootLogger();

    // get from config
    private String reportName;
    private String sourceFilePath;
    private String sourceFileName;
    private String reportConfigFile;
    private String DBNAME;
    private String outputTableName;

    // interval data structure
    private ArrayList<HashMap<String, String>> reportConfig;
    private ArrayList<String>                  reportRawLines;
    private ArrayList<String>                  reportFilteredLines;
    private ArrayList<HashMap<String, String>> reportRows;

    public MapReportHelper(String sourceFilePath, String reportConfigFile) {
        this.sourceFilePath      = sourceFilePath;
        this.reportName          = "unknown";
        this.sourceFileName      = "unknown";
        this.reportConfigFile    = reportConfigFile;
        this.reportConfig        = new ArrayList<HashMap<String, String>>();
        this.outputTableName     = "unknown";
        this.reportRawLines      = new ArrayList<String>();
        this.reportFilteredLines = new ArrayList<String>();
        this.reportRows          = new ArrayList<HashMap<String, String>>();
        this.readReportConfig(this.reportConfigFile);
    }

    private boolean createOutputTable() {
        String sqlCreateDatabase = "CREATE SCHEMA IF NOT EXISTS `" + this.DBNAME + "`";
        String sqlDropTable      = "use " + this.DBNAME + "; DROP TABLE IF EXISTS `" + this.outputTableName + "`";
        String sqlCreateTable    = "use " + this.DBNAME + "; CREATE TABLE `" + this.outputTableName
                                   + "` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,";

        for (HashMap<String, String> columnConfig : this.reportConfig) {
            if (!columnConfig.get("output").equals("yes")) {
                continue;
            }

            String name       = columnConfig.get("name");
            String type       = columnConfig.get("type");
            int    width      = Integer.parseInt(columnConfig.get("width"));
            String columnType = "unknown";

            if (type.equals("time")) {
                columnType = "TIME";
            } else if (type.equals("int") || type.equals("int_comma")) {
                columnType = "INT(11)";
            } else if (type.equals("string")) {

                // For Map report, we only use string as the type.
                // I did not remove other types from code just in case used in
                // future
                int columnSize = width + MapReportHelper.extendOfColumnSize;

                columnType = "VARCHAR(" + Integer.toString(columnSize) + ")";
            } else if (type.equals("decimal")) {
                int columnSize = width + MapReportHelper.extendOfColumnSize - 1;

                columnType = "DECIMAL(" + Integer.toString(columnSize) + ",2)";
            }

            sqlCreateTable = sqlCreateTable + "`" + name + "` " + columnType + " DEFAULT NULL,";
        }

        sqlCreateTable = sqlCreateTable + "PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8";

        // System.out.println(sqlDropTable);
        // System.out.println(sqlCreateTable);
        try {
            DBHelper.getInstance().executeUpdateSQL(sqlCreateDatabase);
            DBHelper.getInstance().executeUpdateSQL(sqlDropTable);
            DBHelper.getInstance().executeUpdateSQL(sqlCreateTable);
        } catch (Exception e) {
            e.printStackTrace();

            return false;
        }

        return true;
    }

    private boolean filterReportRawLines() {
        if (this.reportName.equals("CSC HC Production Report")) {
            for (String currentLine : this.reportRawLines) {
                if ((currentLine.length() != 0)
                        && (" ".equals(currentLine.charAt(0)) || Character.isDigit(currentLine.charAt(0)))) {
                    this.reportFilteredLines.add(currentLine);

                    // System.out.println(currentLine);
                }
            }
        } else if (this.reportName.equals("unknown")) {
            this.reportFilteredLines = this.reportRawLines;
        }

        return true;
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
    }

    private boolean outputreportRows() {
        String sql = "INSERT INTO `" + this.DBNAME + "`.`" + this.outputTableName + "` (";

        for (HashMap<String, String> columnConfig : this.reportConfig) {
            if (!columnConfig.get("output").equals("yes")) {
                continue;
            }

            String name = columnConfig.get("name");

            sql = sql + "`" + name + "`,";
        }

        sql = sql.substring(0, sql.length() - 1);
        sql = sql + ") VALUES (";

        for (HashMap<String, String> columnConfig : this.reportConfig) {
            if (!columnConfig.get("output").equals("yes")) {
                continue;
            }

            sql = sql + "?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ")";

        // System.out.println(sql);
        Connection        conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DBHelper.getInstance().getDBConnection(this.DBNAME);
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);

            for (HashMap<String, String> currentRow : this.reportRows) {
                int j = 1;

                for (int i = 0; i < this.reportConfig.size(); i++) {
                    HashMap<String, String> columnConfig = this.reportConfig.get(i);

                    if (!columnConfig.get("output").equals("yes")) {
                        continue;
                    }

                    String name  = columnConfig.get("name");
                    String value = currentRow.get(name);

                    stmt.setString(j, value);
                    j++;
                }

                stmt.executeUpdate();
            }

            conn.commit();
        } catch (SQLException se) {
            se.printStackTrace();

            return false;
        } catch (Exception e) {
            e.printStackTrace();

            return false;
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();

                return false;
            }

            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();

                return false;
            }
        }

        return true;
    }

    private boolean parseReportFilteredLines() {
        for (String currentLine : this.reportFilteredLines) {
            HashMap<String, String> currentRow = new HashMap<String, String>();

            for (HashMap<String, String> columnConfig : this.reportConfig) {
                if (!columnConfig.get("output").equals("yes")) {
                    continue;
                }

                String name          = columnConfig.get("name");
                int    startpos      = Integer.parseInt(columnConfig.get("startpos")) - 1;
                int    width         = Integer.parseInt(columnConfig.get("width"));
                String type          = columnConfig.get("type");
                String currentColumn = currentLine.substring(startpos, startpos + width).trim();

                if (type.equals("time") || type.equals("int") || type.equals("decimal")) {
                    currentRow.put(name,
                                   (currentColumn.length() != 0)
                                   ? currentColumn
                                   : null);
                } else if (type.equals("string")) {
                    currentRow.put(name, currentColumn);
                } else if (type.equals("int_comma")) {
                    currentRow.put(name,
                                   (currentColumn.length() != 0)
                                   ? currentColumn.replace(",", "")
                                   : null);
                }
            }

            reportRows.add(currentRow);

            // System.out.println(currentRow);
        }

        return true;
    }

    public boolean populateReportOutputTable(String dbName) {
        this.DBNAME = dbName;

        if (this.readSourceConfig(this.reportConfigFile) == false) {
            return false;
        }

        if (this.createOutputTable() == false) {
            return false;
        }

        if (this.readReportRawLines(sourceFileName) == false) {
            return false;
        }

        if (this.filterReportRawLines() == false) {
            return false;
        }

        if (this.parseReportFilteredLines() == false) {
            return false;
        }

        if (this.outputreportRows() == false) {
            return false;
        }

        return true;
    }

    private boolean readReportConfig(String reportConfigFile) {
        Document               dom;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        try {
            DocumentBuilder db = dbf.newDocumentBuilder();

            dom = db.parse(new File(reportConfigFile));

            NodeList document      = dom.getChildNodes();
            Node     report        = document.item(0);
            NodeList reportConfigs = report.getChildNodes();

            for (int i = 0; i < reportConfigs.getLength(); i++) {
                Node reportConfig = reportConfigs.item(i);

                if (reportConfig.getNodeName().equals("reportinfo")) {
                    this.reportName = reportConfig.getAttributes().getNamedItem("reportname").getTextContent().trim();

                    String outputTableNameC = reportConfig.getAttributes()
                                                          .getNamedItem("outputtablename")
                                                          .getTextContent()
                                                          .trim();

                    if (outputTableNameC.length() != 0) {
                        this.outputTableName = outputTableNameC;
                    } else {
                        this.outputTableName = this.reportName.toLowerCase().replace(" ", "_") + "_map";
                    }
                } else if (reportConfig.getNodeName().equals("column")) {
                    HashMap<String, String> columnConfig = new HashMap<String, String>();

                    columnConfig.put("name", reportConfig.getAttributes().getNamedItem("name").getTextContent().trim());
                    columnConfig.put("startpos",
                                     reportConfig.getAttributes().getNamedItem("startpos").getTextContent().trim());
                    columnConfig.put("width",
                                     reportConfig.getAttributes().getNamedItem("width").getTextContent().trim());
                    columnConfig.put("type", reportConfig.getAttributes().getNamedItem("type").getTextContent().trim());
                    columnConfig.put("output",
                                     reportConfig.getAttributes().getNamedItem("output").getTextContent().trim());
                    this.reportConfig.add(columnConfig);
                }
            }

            // System.out.println(this.reportConfig);
        } catch (ParserConfigurationException pce) {
            System.out.println(pce.getMessage());

            return false;
        } catch (SAXException se) {
            System.out.println(se.getMessage());

            return false;
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());

            return false;
        }

        /*
         * catch (TransformerException tfe) {
         * tfe.printStackTrace();
         *   }
         */
        return true;
    }

    private boolean readReportRawLines(String reportFile) {
        BufferedReader br = null;

        try {
            String currentLine;

            br = new BufferedReader(new FileReader(reportFile));

            while ((currentLine = br.readLine()) != null) {
                this.reportRawLines.add(currentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();

            return false;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();

                return false;
            }
        }

        return true;
    }

    private boolean readSourceConfig(String reportConfigFile) {
        File path = new File(this.sourceFilePath);

        if (!path.exists()) {

            // path did not exist
            rootLogger.severe("Map Report Source Folder did not exist : " + this.sourceFilePath);

            return false;
        }

        assert(path.listFiles().length == 1);

        for (final File fileEntry : path.listFiles()) {
            this.sourceFileName = this.sourceFilePath + "/" + fileEntry.getName();
        }

        return true;
    }

    public String getDBNAME() {
        return DBNAME;
    }

    public String getOutputTableName() {
        return outputTableName;
    }
}
