package kiwiplan.kdw.core;

import java.io.File;
import java.io.IOException;

import java.util.*;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;

public class KDWSchemaHelper {
    private static final String                      KDWSCHEMA_FILE_NAME_PREFIX = "";
    static Logger                                    rootLogger                 = LogHelper.getRootLogger();
    private static Map<String, List<KDWTableSchema>> revSchemaMap               = new HashMap<>();

    private static boolean loadKDWSchema(String revision)
            throws ParserConfigurationException, SAXException, IOException {
        List<KDWTableSchema> revTableSchemaList = revSchemaMap.get(revision);

        if ((revTableSchemaList == null)) {
            revTableSchemaList = new ArrayList<>();

            File                   xmlFile   = new File("kdwschema/" + KDWSCHEMA_FILE_NAME_PREFIX + revision + ".xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder        dBuilder  = dbFactory.newDocumentBuilder();
            Document               schemaDoc = dBuilder.parse(xmlFile);

            schemaDoc.getDocumentElement().normalize();

            NodeList tableList = schemaDoc.getElementsByTagName("table");

            for (int tableIndex = 0; tableIndex < tableList.getLength(); tableIndex++) {
                KDWTableSchema tableSchema = new KDWTableSchema();
                Node           table       = tableList.item(tableIndex);
                NamedNodeMap   attr        = table.getAttributes();
                Node           name        = attr.getNamedItem("name");

                if (name == null) {
                    rootLogger.severe("Schema loading failed: no table name attribute specified for one table. ");

                    return false;
                }

                tableSchema.tablename = name.getNodeValue();

//              rootLogger.info(revision + "/tablename: " + tableSchema.tablename);
                Node comp = attr.getNamedItem("comparetable");

                if (comp == null) {
                    rootLogger.severe("Schema loading failed: no comparetable attribute specified for one table. ");

                    return false;
                }

                tableSchema.comparetable = comp.getNodeValue().equals("true");

//              rootLogger.info(revision + "/comparetable: " + tableSchema.comparetable);
                NodeList naturalKeys     = table.getChildNodes();
                int      naturalKeyIndex = 0;

                for (int childNodeIndex = 0; childNodeIndex < naturalKeys.getLength(); childNodeIndex++) {
                    Node naturalKey = naturalKeys.item(childNodeIndex);

                    if (!naturalKey.getNodeName().equalsIgnoreCase("naturalkey")) {
                        continue;
                    }

                    naturalKeyIndex++;

                    NamedNodeMap keyattr = naturalKey.getAttributes();
                    NaturalKey   key     = new NaturalKey();
                    Node         columns = keyattr.getNamedItem("columns");

                    if (columns == null) {
                        rootLogger.severe(
                            "Schema loading failed: Table's natural key should has columns attribute specified.");

                        return false;
                    }

                    key.columns = columns.getNodeValue();

//                  rootLogger.info("\tNatural Key columns: " + key.columns);
                    Node filter = keyattr.getNamedItem("filter");

                    if (filter != null) {
                        key.filter = filter.getNodeValue();
                    }

                    if ((naturalKeyIndex > 1) && (key.filter.isEmpty())) {
                        rootLogger.severe(
                            "Schema loading failed: Table has multiple naturalkey for different condition, but no filter specified.");

                        return false;
                    }

//                  rootLogger.info("\tNatural Key filter: " + key.filter);
                    Node filtercolumns = keyattr.getNamedItem("filtercolumns");

                    if (filtercolumns != null) {
                        key.filtercolumns = filtercolumns.getNodeValue();
                    }

//                  rootLogger.info("\tNatural Key filtercolumns: " + key.filtercolumns);
                    Node precompare = keyattr.getNamedItem("precompare");

                    if (precompare != null) {
                        key.precompare = precompare.getNodeValue();
                    }

//                  rootLogger.info("\tNatural Key precompare: " + key.precompare);
                    tableSchema.naturalKeyConf.add(key);
                }

                revTableSchemaList.add(tableSchema);
            }

            revSchemaMap.put(revision, revTableSchemaList);
        }

        return true;
    }

    public static void main(String[] args) {
        try {
            KDWSchemaHelper.loadKDWSchema("7.90.2");
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

        try {
            KDWSchemaHelper.loadKDWSchema("7.90.2");
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

        KDWSchemaHelper.showKDWSchema();

        String revision  = "7.90.2";
        String tableName = "dwwaste";

        System.out.println(KDWSchemaHelper.getUniqueKey(revision, tableName));

        try {
            String[][] naturalKey = KDWSchemaHelper.getTableNaturalKeys(revision, tableName);

            System.out.println("Natural Key of : " + tableName);

            for (int iCount = 0; iCount < naturalKey.length; iCount++) {
                assert(naturalKey[iCount].length == 2);
                System.out.println("Filter: " + naturalKey[iCount][0]);
                System.out.println("Natural Key: " + naturalKey[iCount][1]);
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    // public static boolean validateSchemaConfig(String revision) {
    // return true;
    // }
    private static void showKDWSchema() {
        if (revSchemaMap == null) {
            rootLogger.info("No schema loaded. Please load schema first.");

            return;
        }

        for (String key : revSchemaMap.keySet()) {
            List<KDWTableSchema> revSchemaList = revSchemaMap.get(key);

            for (KDWTableSchema tableSchema : revSchemaList) {
                rootLogger.info("Table Name: " + tableSchema.tablename);
                rootLogger.info("Comparetable: " + tableSchema.comparetable);
                rootLogger.info("Natural Keys: ");

                for (int naturalKeyIndex = 0; naturalKeyIndex < tableSchema.naturalKeyConf.size(); naturalKeyIndex++) {
                    NaturalKey naturalKey = tableSchema.naturalKeyConf.get(naturalKeyIndex);

                    naturalKey.print();
                }
            }
        }
    }

    public static boolean isTableComparetable(String revision, String tableName) {
        try {
            KDWSchemaHelper.loadKDWSchema(revision);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
            rootLogger.severe("Loading KDW scheam Failed");

            return false;
        }

        List<KDWTableSchema> revSchemaList = revSchemaMap.get(revision);

        for (KDWTableSchema tableSchema : revSchemaList) {
            if (tableSchema.tablename.equalsIgnoreCase(tableName)) {
                return tableSchema.comparetable;
            }
        }

        return false;
    }

    /*
     * Return a String array like:
     * filter1, naturalkey1
     * filter2, naturalkey2
     * filter3, naturalkey3
     */
    public static String[][] getTableNaturalKeys(String revision, String tableName)
            throws ParserConfigurationException, SAXException, IOException {
        String[][] naturalKeys = {};
        boolean    schemaload  = false;

        schemaload = loadKDWSchema(revision);

        if (schemaload != true) {
            throw new ParserConfigurationException();
        }

        List<KDWTableSchema> revSchemaList = revSchemaMap.get(revision);

        for (KDWTableSchema tableSchema : revSchemaList) {
            if (tableSchema.tablename.equalsIgnoreCase(tableName)) {
                naturalKeys = new String[tableSchema.naturalKeyConf.size()][2];

                for (int naturalKeyIndex = 0; naturalKeyIndex < tableSchema.naturalKeyConf.size(); naturalKeyIndex++) {
                    NaturalKey key = tableSchema.naturalKeyConf.get(naturalKeyIndex);

                    naturalKeys[naturalKeyIndex][0] = key.filter;
                    naturalKeys[naturalKeyIndex][1] = key.columns;
                }

                return naturalKeys;
            }
        }

        return naturalKeys;
    }

    public static String getUniqueKey(String revision, String tableName) {
        String  uniqueKey  = "";
        boolean schemaload = false;

        try {
            schemaload = loadKDWSchema(revision);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

        if (schemaload != true) {
            rootLogger.severe("Failed to load KDW schema");

            return uniqueKey;
        }

        boolean              foundTable    = false;
        List<KDWTableSchema> revSchemaList = revSchemaMap.get(revision);

        for (KDWTableSchema tableSchema : revSchemaList) {
            if (tableSchema.tablename.equalsIgnoreCase(tableName)) {
                foundTable = true;

                String filterColumns = "";

                assert(tableSchema.naturalKeyConf.size() > 1);

                NaturalKey key = tableSchema.naturalKeyConf.get(0);

                uniqueKey     = key.columns;
                filterColumns = key.filtercolumns;

                for (int naturalKeyIndex = 1; naturalKeyIndex < tableSchema.naturalKeyConf.size(); naturalKeyIndex++) {
                    key       = tableSchema.naturalKeyConf.get(naturalKeyIndex);
                    uniqueKey = uniqueKey + "," + key.columns;

                    if ((!key.filtercolumns.isEmpty()) && (!filterColumns.contains(key.filtercolumns))) {
                        filterColumns = filterColumns + "," + key.filtercolumns;
                    }
                }

                String[] uniqueStrings = uniqueKey.split(",");

                uniqueKey = uniqueStrings[0];

                for (int sIndex = 1; sIndex < uniqueStrings.length; sIndex++) {
                    if (!uniqueKey.contains(uniqueStrings[sIndex])) {
                        uniqueKey = uniqueKey + "," + uniqueStrings[sIndex];
                    }
                }

                if (!filterColumns.isEmpty()) {
                    uniqueKey = uniqueKey + "," + filterColumns;
                }

                return uniqueKey;
            }
        }

        if (foundTable == false) {
            rootLogger.warning("getUniqueKey: Did not find matched table in kdwschema config for table: " + tableName);
        }

        return "";
    }

    static class KDWTableSchema {
        String           tablename;
        boolean          comparetable;
        List<NaturalKey> naturalKeyConf;

        public KDWTableSchema() {
            tablename      = "";
            comparetable   = true;
            naturalKeyConf = new ArrayList<>();
        }
    }


    static class NaturalKey {
        String filtercolumns;
        String filter;
        String precompare;
        String columns;

        public NaturalKey() {
            filtercolumns = "";
            filter        = "";
            precompare    = "";
            columns       = "";
        }

        void print() {
            rootLogger.info("\tcolumns: " + columns);
            rootLogger.info("\tfilter: " + filter);
            rootLogger.info("\tfiltercolumns: " + filtercolumns);
            rootLogger.info("\tprecompare: " + precompare);
        }
    }
}
