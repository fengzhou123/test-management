package kiwiplan.kdw.core;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

public class KDWMapReportHelper {
    private String                  tonyReportName;
    private String                  mapReportTableName;
    private String                  tonyReportSqlPath;
    private HashMap<String, String> mapKDWMapping;

    public KDWMapReportHelper() {
        this.tonyReportName     = "";
        this.mapReportTableName = "";
        this.mapKDWMapping      = new HashMap<>();
    }

    public String generateSql4Report(String targetDB) {
        String createTableSql = "drop table if exists `" + targetDB + "`.`" + this.mapReportTableName + "`;";

        createTableSql += "create table `" + targetDB + "`.`" + this.mapReportTableName + "` (";

        String valuesSql = " (select ";
        String insertSql = "insert into `" + targetDB + "`.`" + this.mapReportTableName + "` (";

        for (String key : this.mapKDWMapping.keySet()) {
            createTableSql += "`" + key + "` varchar(255)" + ",";
            insertSql      += "`" + key + "`" + ",";
            valuesSql      += mapKDWMapping.get(key) + ",";
        }

        valuesSql      = valuesSql.substring(0, valuesSql.length() - 1);
        insertSql      = insertSql.substring(0, insertSql.length() - 1) + ")";
        createTableSql = createTableSql.substring(0, createTableSql.length() - 1);

        String finalSql = "";

        // remove the last ,
        createTableSql = createTableSql + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

        String tonyReportSql;

        try {
            tonyReportSql = new String(Files.readAllBytes(Paths.get(this.tonyReportSqlPath + "/" + tonyReportName
                                                                    + ".sql")));

            if (tonyReportSql.endsWith(";")) {
                tonyReportSql = tonyReportSql.substring(0, tonyReportSql.length() - 1);
            }

            valuesSql = valuesSql + " from (" + tonyReportSql + ") as tmp);";

            String insertDataSql = insertSql + valuesSql;

            finalSql = createTableSql + insertDataSql;
        } catch (IOException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return finalSql;
    }

    public boolean loadConfig(String sqlPath, String mappingConfigFile) {
        Document               dom;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        this.tonyReportSqlPath = sqlPath;

        try {
            DocumentBuilder db = dbf.newDocumentBuilder();

            dom = db.parse(new File(mappingConfigFile));

            NodeList document       = dom.getChildNodes();
            Node     mapping        = document.item(0);
            NodeList mappingConfigs = mapping.getChildNodes();

            for (int i = 0; i < mappingConfigs.getLength(); i++) {
                Node mappingConfig = mappingConfigs.item(i);

                if (mappingConfig.getNodeName().equals("mappinginfo")) {
                    this.tonyReportName = mappingConfig.getAttributes()
                                                       .getNamedItem("tonyreportname")
                                                       .getTextContent()
                                                       .trim();
                    this.mapReportTableName = mappingConfig.getAttributes()
                                                           .getNamedItem("mapreporttablename")
                                                           .getTextContent()
                                                           .trim();
                } else if (mappingConfig.getNodeName().equals("columnmapping")) {
                    String mapName = mappingConfig.getAttributes().getNamedItem("mapname").getTextContent().trim();
                    String kdwName = mappingConfig.getAttributes().getNamedItem("kdwname").getTextContent().trim();

                    if (!kdwName.isEmpty()) {
                        mapKDWMapping.put(mapName, kdwName);
                    }
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

        return true;
    }

    public static void main(String[] args) {

        // TODO Auto-generated method stub
    }

//  public void setTonyReportName(String tonyReportName) {
//          this.tonyReportName = tonyReportName;
//  }
//
//  public void setMapReportTableName(String mapReportTableName) {
//          this.mapReportTableName = mapReportTableName;
//  }
//
//  public void setMapKDWMapping(HashMap<String, String> mapKDWMapping) {
//          this.mapKDWMapping = mapKDWMapping;
//  }
    public HashMap<String, String> getMapKDWMapping() {
        return mapKDWMapping;
    }

    public String getMapReportTableName() {
        return mapReportTableName;
    }

    public String getTonyReportName() {
        return tonyReportName;
    }
}
