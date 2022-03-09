package kiwiplan.kdw.core;

import java.io.*;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kiwiplan.kdw.batches.EspDatabaseRestoration;
import kiwiplan.kdw.tasks.MSCompareDBs;
import kiwiplan.kdw.tasks.MysqlCompareDBs;
import org.apache.commons.io.FileUtils;

import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import kiwiplan.kdw.utils.OSUtil;

public class KDWHelper {

	// steps sequence of fresh installation rev 7.90.2
	static final int STEP_INSTALL_BASE_DIRECTORY = 1;
	static final int STEP_INSTALL_SITE_IDENTIFIER = STEP_INSTALL_BASE_DIRECTORY + 1;

	// private static boolean mod_ut = false;
	static final int STEP_INSTALL_INSTALL_TYPE = STEP_INSTALL_SITE_IDENTIFIER + 1;
	static final int STEP_INSTALL_INSTALL_MODE = STEP_INSTALL_INSTALL_TYPE + 1;
	static final int STEP_INSTALL_MULTI_INSTALL = STEP_INSTALL_INSTALL_MODE + 1;
	static final int STEP_INSTALL_ADMIN_ACCOUNT = STEP_INSTALL_MULTI_INSTALL + 1;
	static final int STEP_INSTALL_DB_CONFIG = STEP_INSTALL_ADMIN_ACCOUNT + 1;
	static final int STEP_INSTALL_DB_SUPER_USER = STEP_INSTALL_DB_CONFIG + 1;
	static final int STEP_INSTALL_DB_PRINCIPAL_USER = STEP_INSTALL_DB_SUPER_USER + 1;
	static final int STEP_INSTALL_GENERAL_CONFIG = STEP_INSTALL_DB_PRINCIPAL_USER + 1;
	static final int STEP_INSTALL_SHELL_CONFIG = STEP_INSTALL_GENERAL_CONFIG + 1;
	static final int STEP_INSTALL_ENV_CONFIG = STEP_INSTALL_SHELL_CONFIG + 1;
	static final int STEP_INSTALL_NETWORK_CONFIG = STEP_INSTALL_ENV_CONFIG + 1;
	static final int STEP_INSTALL_OPT_NETWORK_CONFIG = STEP_INSTALL_NETWORK_CONFIG + 1;
	static final int STEP_INSTALL_MEM_CONFIG = STEP_INSTALL_OPT_NETWORK_CONFIG + 1;
	static final int STEP_INSTALL_DB_MASTER_CONFIG = STEP_INSTALL_MEM_CONFIG + 1;
	static final int STEP_INSTALL_DEL_RECORDS_TRACKING = STEP_INSTALL_DB_MASTER_CONFIG + 1;
	static final int STEP_INSTALL_EVENT_CONFIG = STEP_INSTALL_DEL_RECORDS_TRACKING + 1;
	static final int STEP_INSTALL_PHASERUN_LOG = STEP_INSTALL_EVENT_CONFIG + 1;
	static final int STEP_INSTALL_LOG_LEVEL = STEP_INSTALL_PHASERUN_LOG + 1;
	static final int STEP_INSTALL_PRE_POST_CONFIG = STEP_INSTALL_LOG_LEVEL + 1;
	static final int STEP_INSTALL_REPORTING_DAYS = STEP_INSTALL_PRE_POST_CONFIG + 1;
	static final int STEP_INSTALL_PROD_REPORTING_DAYS = STEP_INSTALL_REPORTING_DAYS + 1;
	static final int STEP_INSTALL_ETL_CONFIG = STEP_INSTALL_PROD_REPORTING_DAYS + 1;
	static final int STEP_INSTALL_MEASUREMENT_CONFIG = STEP_INSTALL_ETL_CONFIG + 1;
	static final int STEP_INSTALL_MASTER_TIMEZONE = STEP_INSTALL_MEASUREMENT_CONFIG + 1;
	static final int STEP_INSTALL_CUSTOMERSCOPE = STEP_INSTALL_MASTER_TIMEZONE + 1;
	static final int STEP_INSTALL_FAILURE_EMAIL_CONFIG = STEP_INSTALL_MEASUREMENT_CONFIG + 1;
	static final int STEP_INSTALL_DB_WORKING_CONFIG = STEP_INSTALL_FAILURE_EMAIL_CONFIG + 1;
	static final int STEP_INSTALL_LEGACY_MAP_ETL = STEP_INSTALL_DB_WORKING_CONFIG + 1;
	static final int STEP_INSTALL_INSTALL_RESULT = STEP_INSTALL_LEGACY_MAP_ETL + 1;
	static final int STEP_INSTALL_INITIAL_CONFIGURE = STEP_INSTALL_INSTALL_RESULT + 1;

	public static final String kdwInstallationCode = "Installation";
	public static final String kdwUpgradeCode = "Upgrade";
	/**
	 * Queries to run to verify data has been converted for the specific products.  A count > 0 indicates a successful conversion.
	 */
	private static final Map<String, String> ETL_CONVERTED_COUNT_QUERIES = new HashMap<String, String>() {{
		put("csc", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwcorrugatorproductionorders");
		put("pcs", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwproductionorders WHERE production_order_record_origin = 'PCS'");
		put("rss", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwpaperinventory");
		put("ult", "SELECT SUM(ult.c) AS cnt FROM (SELECT COUNT(*) AS c FROM ${USERNAME}_masterDW.dwinventorymovement UNION SELECT COUNT(*) AS c FROM ${USERNAME}_masterDW.dwinventorylevel) ult");
		put("cwr", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwcwrwasterecord");
		put("qms", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwquality");
		put("oee", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwoeerecord");
		put("tss", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwtssvisit");
		put("pic", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwpicpurchaseorder");
		put("mms", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwinventorytransactionheader where invtranshead_source_system = 'MMS'");
		put("esp", "SELECT COUNT(*) AS cnt FROM ${USERNAME}_masterDW.dwepdproductdesign");
	}};

	private static final String INSTALLER_ROOTPATH = "/javaservice/installers/";
	static Logger rootLogger = LogHelper.getRootLogger();

	public static boolean copyKDWDBs(String hostname, String username, String password, String label) {
		boolean result = true;
		switch (DBHelper.databaseType) {
			case DBHelper.DATABASE_MSSQL :
				String old_working = ConfigHelper.getInstance().getConfig("KDWAUTOUSER") + "_workingDW";
				String new_working = label + "_workingDW";
				String old_master = ConfigHelper.getInstance().getConfig("KDWAUTOUSER") + "_masterDW";
				String new_master = label + "_masterDW";
				result = copyMSSQLDB(old_working, new_working);
				result = result && copyMSSQLDB(old_master, new_master);
				break;
			case DBHelper.DATABASE_MYSQL :
				result = copyMySQLDBs(hostname, username, password, label);
				break;
		}
		return result;
	}

	public static boolean copyMySQLDBs(String hostname, String username, String password, String label) {
		String cmd = "DBName=" + label + ";";

		cmd += "echo drop DB \"$DBName\"_masterDW;";
		cmd += "mysqladmin -f -ukiwisql -pmapadm99 drop \"$DBName\"_masterDW;";
		cmd += "echo drop DB \"$DBName\"_workingDW;";
		cmd += "mysqladmin -f -ukiwisql -pmapadm99 drop \"$DBName\"_workingDW;";
		cmd += "echo copy \"$USER\"_masterDW as \"$DBName\"_masterDW;";
		cmd += "dbcopy \"$USER\"_masterDW \"$DBName\"_masterDW;";
		cmd += "echo copy \"$USER\"_workingDW as \"$DBName\"_workingDW;";
		cmd += "dbcopy \"$USER\"_workingDW \"$DBName\"_workingDW;";
		rootLogger.info("Backup dbs as : " + label + "_masterDW/_workingDW");
		return OSUtil.runRemoteCommand(hostname, username, password, cmd);
	}

	public static boolean copyMSSQLDB(String olddb, String newdb) {
		MSDBHelper msdbHelper = new MSDBHelper();
		String deletesql = String.format("EXECUTE master.dbo.xp_delete_file 0,'E:\\kdw_db_backup\\%s.bak'", olddb);
		String backsql = String.format("BACKUP DATABASE %s TO DISK = 'E:\\kdw_db_backup\\%s.bak'", olddb, olddb);
		String dropsql = String.format("drop database if exists %s", newdb);
		String restoresql = String.format("RESTORE DATABASE %s FROM DISK = 'E:\\kdw_db_backup\\%s.bak' WITH FILE = 1, MOVE '%s' TO 'E:\\kdw_db_backup\\%s.mdf', MOVE '%s_log' TO 'E:\\kdw_db_backup\\%s_Log.ldf', NOUNLOAD, STATS = 5", newdb, olddb, olddb, newdb, olddb, newdb);

		try {
			msdbHelper.executeUpdateSQL(deletesql);
		} catch (Exception e) {
			rootLogger.info("backup file doesn't exist");
		}
		try {
			msdbHelper.executeUpdateSQL(backsql);
			msdbHelper.executeUpdateSQL(dropsql);
			msdbHelper.executeUpdateSQL(restoresql);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private static String createFormattedFile(String localRawFilePath) {
		File newLocalFile = new File(OSUtil.getLocalTempDir(), "formattedFile.txt");

		try {
			String fileContent = FileUtils.readFileToString(new File(localRawFilePath));
			String formattedContent = ConfigHelper.interpolateKeys(fileContent);

			FileUtils.writeStringToFile(newLocalFile, formattedContent);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return newLocalFile.getAbsolutePath();
	}

	private static boolean errorMsgOccur(String line, BufferedWriter writer) {
		boolean errorOccur = false;

		line = line.toLowerCase();

		if (line.contains("Hit Enter to continue...".toLowerCase())) {
			try {
				writer.write("\n");
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}

			errorOccur = true;
		}

		return errorOccur;
	}

	public static boolean kdwCheckETLResult(String hostname, String username, String password) {
		String sql = "SELECT log_success FROM " + username + "_masterDW.dwsiterunlog where log_success != 1";
		String sql2 = "SELECT task_id FROM " + username + "_masterDW.sch_task_history where task_status != 'SUCCESS'";
		boolean bResult = true;
		ResultSet rsSiteRunLog = null;

		try {
			rsSiteRunLog = DBHelper.getInstance().executeSQL(sql);
			if (rsSiteRunLog.next()) {
				rootLogger.info("run failed");
				bResult = false;
			}
		} catch (Exception e) {
		}
		try {
			rsSiteRunLog = DBHelper.getInstance().executeSQL(sql2);
			if (rsSiteRunLog.next()) {
				rootLogger.info("task failed, check sch_task_history");
				bResult = false;
			}
		} catch (Exception e) {
		}

		return bResult;
	}

	public static boolean kdwCheckETLConvertedCounts(String username, List<String> products) {
		if (products == null || products.isEmpty()) {
			return true;
		}
		rootLogger.info("Checking converted counts for products: " + products);
		for (String product : products) {
			String query = ETL_CONVERTED_COUNT_QUERIES.get(product.toLowerCase());
			if (query == null) {
				rootLogger.warning("No count query defined for product \"" + product + "\", skipping check...");
				continue;
			}
			query = query.replaceAll("\\$\\{USERNAME\\}", username);
			ResultSet resultSet = null;
			try {
				resultSet = DBHelper.getInstance().executeSQL(query);
				if (!resultSet.next()) {
					rootLogger.severe("ETL converted count check query for product \"" + product + "\" returns no results.");
					return false;
				}
				int count = resultSet.getInt("cnt");
				if (count == 0) {
					rootLogger.severe("No data converted for product \"" + product + "\".");
					return false;
				}
				else {
					rootLogger.info(count + " rows of data converted for product \"" + product + "\".");
				}
			}
			catch (Exception e) {
				rootLogger.log(Level.SEVERE, "Error checking ETL converted count for product \"" + product + "\", query failed: " + query, e);
				return false;
			}
			finally {
				if (resultSet != null) {
					try {
						resultSet.close();
					}
					catch (Exception e) {
						rootLogger.log(Level.SEVERE, "Failed to close result set", e);
					}
				}
			}
		}
		return true;
	}

	public static List<String> kdwCompareSchema(String hostname, String username, String password, String dbname1,
			String dbname2, String testcaseTempPath) {
		List<String> diffLines = new ArrayList<>();

		// mysqldump --skip-comments --skip-extended-insert --no-data -u kiwisql
		// -pmapadm99 dbname1 > sqldump1.sql
		// mysqldump --skip-comments --skip-extended-insert --no-data -u kiwisql
		// -pmapadm99 dbname2 > sqldump2.sql
		// diff sqldump1.sql sqldump2.sql > schemadiff
		// check difference is empty
		String dumpFileName1 = testcaseTempPath + "/" + dbname1 + ".sql";
		String dumpFileName2 = testcaseTempPath + "/" + dbname2 + ".sql";
		String sqlDumpcmd = "mysqldump --skip-comments --skip-extended-insert --no-data";
		String excludeAutoIncr = " | sed 's/ AUTO_INCREMENT=[0-9]*\\b//'";

		// remove the mb4 syntex. There are some different between 8.10 and other
		// revisions.
		// it's utf8mb4 in 8.10 and utf8 in other revisions
		String removeMB4 = " | sed 's/utf8mb4/utf8/'";

		// --ignore-table=database.table1 --ignore-table=database.table2
		// use script: | sed 's/ AUTO_INCREMENT=[0-9]*\b//' to remove
		// auto_incremental. The option : --skip-auto-increment is not a real
		// one.
		String cmd1 = sqlDumpcmd + " -u " + "kiwisql" + " -p" + "mapadm99" + " " + dbname1 + excludeAutoIncr
				+ removeMB4;

		// cmd = cmd + "";
		// cmd = cmd + ;
		cmd1 = cmd1 + " > " + dumpFileName1 + ";";
		OSUtil.runRemoteCommand(hostname, username, password, cmd1);

		String cmd2 = sqlDumpcmd + " -u " + "kiwisql" + " -p" + "mapadm99" + " " + dbname2 + excludeAutoIncr
				+ removeMB4;

		// cmd = cmd + "";
		// cmd = cmd + ;
		cmd2 = cmd2 + " > " + dumpFileName2 + ";";
		OSUtil.runRemoteCommand(hostname, username, password, cmd2);

		String sortedDumpFileName1 = dumpFileName1 + ".sorted";
		String sortedDumpFileName2 = dumpFileName2 + ".sorted";
		String sortCmd = "rm -rf " + sortedDumpFileName1 + " " + sortedDumpFileName2 + ";";

		sortCmd = sortCmd + "sort " + dumpFileName1 + " > " + sortedDumpFileName1 + ";";

		// remove the last , for each line
		sortCmd = sortCmd + "sed -i 's/,$//' " + sortedDumpFileName1 + ";";
		sortCmd = sortCmd + "sort " + dumpFileName2 + " > " + sortedDumpFileName2 + ";";
		sortCmd = sortCmd + "sed -i 's/,$//' " + sortedDumpFileName2 + ";";
		OSUtil.runRemoteCommand(hostname, username, password, sortCmd);

		String diffCmd = "diff " + sortedDumpFileName1 + " " + sortedDumpFileName2;

		// return 0 if diff same, so runRemoteCommand will return true
		// bResult = SystemHelper.runRemoteCommand(hostname, username, password,
		// diffCmd);
		diffLines = OSUtil.getRemoteSingleCommandOut(hostname, username, password, diffCmd);

		// write the diff info into file for logging purpose
		diffCmd = "cd " + testcaseTempPath + ";diff " + sortedDumpFileName1 + " " + sortedDumpFileName2 + " > "
				+ dbname1 + ".vs." + dbname2 + ".diff";

		// return 0 if diff same, so runRemoteCommand will return true
		OSUtil.runRemoteCommand(hostname, username, password, diffCmd);

		return diffLines;
	}

	public static boolean kdwFreshInstall(String revision, String hostname, String installerpath, String username,
			String password, boolean isBaseRevision) {

		rootLogger.severe("Fresh Install with: " + installerpath);
		kdwPreFreshInstall(revision, hostname, installerpath, username, password);

		boolean bResult = true;
		String cmd = "";

		String kdwProfile = "kdw_auto";

		String javaVersion = (revision.replace(".", "").compareTo("9541") < 0) ? "8" : "11";

		if (DBHelper.databaseType.equals(DBHelper.DATABASE_MSSQL)) {
			kdwProfile = "kdw_mssql";
			String drop_w_sql = "drop database if exists " + username + "_workingDW";
			String drop_m_sql = "drop database if exists " + username + "_masterDW";
			try {
				MSDBHelper.getInstance().executeUpdateSQL(drop_w_sql);
				MSDBHelper.getInstance().executeUpdateSQL(drop_m_sql);
			} catch (SQLException throwables) {
				throwables.printStackTrace();
				return false;
			}
		}
		if (isBaseRevision) {
			String temp = installerpath.substring(0, installerpath.lastIndexOf("/"));
			// String temp = installerpath;
			while (temp.endsWith("/")) {
				temp = temp.substring(0, temp.length() - 1);
			}

			temp = temp.substring(temp.lastIndexOf("/"));
			temp = temp.substring(temp.indexOf("-") + 1);
			if (temp.startsWith("/"))
				temp = temp.substring(1);

			String jbase = temp;

			cmd = "restinit -a;rm -rf /javaservice/$USER/*; chjbase " + jbase + " " + javaVersion;
			cmd = cmd + "; restsjava -f " + kdwProfile;
		} else {
			String revisionNumber = revision.substring(revision.indexOf("-") + 1, revision.length());
			String jbase = revisionNumber + "_auto";
			String targetFolder = "kdw-" + revision + "_auto";
			String installerTargetFolder = INSTALLER_ROOTPATH + targetFolder;

			cmd = "restinit -a;rm -rf /javaservice/$USER/*; chjbase " + jbase + " " + javaVersion;
			cmd = cmd + "; mkdir " + installerTargetFolder;
			cmd = cmd + "; rm -rf " + installerTargetFolder + "/*";
			cmd = cmd + "; cp " + installerpath + " " + installerTargetFolder;
			cmd = cmd + "; chmod -R 777 " + installerTargetFolder;
			cmd = cmd + "; restsjava -f " + kdwProfile;
		}

		bResult = OSUtil.runRemoteCommand(hostname, username, password, cmd);

		if (bResult) {
			kdwPostFreshInstall(revision, hostname, installerpath, username, password);
		}

//      else {
//          rootLogger.warning("Failed to install KDW, will give it another chance!!!");
//          bResult = OSUtil.runRemoteCommand(hostname, username, password, cmd);
//          kdwPostFreshInstall(revision, hostname, installerpath, username, password);
//      }
		return bResult;
	}

	public static boolean kdwUpgradeInstall(String revision, String hostname, String installerpath, String username,
			String password) {
		rootLogger.severe("Upgrade Install with: " + installerpath);
		kdwPreUpgradeInstall(revision, hostname, installerpath, username, password);

		boolean bResult = true;
		String cmd = "";

			String temp = installerpath.substring(0, installerpath.lastIndexOf("/"));
			while (temp.endsWith("/")) {
				temp = temp.substring(0, temp.length() - 1);
			}

			temp = temp.substring(temp.lastIndexOf("/"));
			temp = temp.substring(temp.indexOf("-") + 1);
			if (temp.startsWith("/"))
				temp = temp.substring(1);

			String jbase = temp;

			cmd = "restinit -a;chjbase " + jbase;
			cmd = cmd + "; restsjava " + "kdw_auto";

		bResult = OSUtil.runRemoteCommand(hostname, username, password, cmd);

		if (bResult) {
			kdwPostFreshInstall(revision, hostname, installerpath, username, password);
		}

		return bResult;
	}

	public static boolean kdwPostFreshInstall(String revision, String hostname, String installerpath, String username,
			String password) {
		String remoteJarFilePath = scpLocalScriptToRemoteHome(hostname, username, password, OSUtil.MYSQL_JAR_FILE_PATH);
		String localScriptPath = "scripts/postinstall";
		String remoteScriptPath = scpLocalScriptToRemoteHome(hostname, username, password, localScriptPath);
		String cmd = remoteScriptPath + " " + revision;

		cmd += "; rm -rf $PENTAHODIR/kiwiplan/dist/mysql*; cp " + remoteJarFilePath
				+ " $PENTAHODIR/kiwiplan/dist/; ls $PENTAHODIR/kiwiplan/dist/mysql*";

		// String localJarPath =
		// "/home/mao.li/kdw-9.10.1/etl/kp-etl-core/target/kp-etl-core-9.10.1.jar";
		// String remoteJarPath =
		// "/qa/src/javaservices/java02/java/sites/java02/9.10.1082/dist/kp-etl-core-9.10.1.jar";
		// OSUtil.scpToRemote(localJarPath, remoteJarPath, hostname,username, password);
		return OSUtil.runRemoteCommand(hostname, username, password, cmd);
	}

	public static boolean kdwPostUpgradeInstall(String revision, String hostname, String installerpath, String username,
			String password) {
		String remoteJarFilePath = scpLocalScriptToRemoteHome(hostname, username, password, OSUtil.MYSQL_JAR_FILE_PATH);
		String localScriptPath = "scripts/postupgrade";
		String remoteScriptPath = scpLocalScriptToRemoteHome(hostname, username, password, localScriptPath);
		String cmd = remoteScriptPath + " " + revision;

		cmd += "; rm -rf $PENTAHODIR/kiwiplan/dist/mysql*; cp " + remoteJarFilePath + " $PENTAHODIR/kiwiplan/dist/; ";

		return OSUtil.runRemoteCommand(hostname, username, password, cmd);
	}

	public static boolean kdwPreFreshInstall(String revision, String hostname, String installerpath, String username,
			String password) {
		String localScriptPath = "scripts/preinstall";
		String remoteScriptPath = scpLocalScriptToRemoteHome(hostname, username, password, localScriptPath);
		String cmd = remoteScriptPath + " " + revision;

		return OSUtil.runRemoteCommand(hostname, username, password, cmd);
	}

	public static boolean kdwPreUpgradeInstall(String revision, String hostname, String installerpath, String username,
			String password) {
		String localScriptPath = "scripts/preupgrade";
		String remoteScriptPath = scpLocalScriptToRemoteHome(hostname, username, password, localScriptPath);
		String cmd = remoteScriptPath + " " + revision;

		// String localJarPath =
		// "/home/mao.li/kdw-9.10.1/etl/kp-etl-core/target/kp-etl-core-9.10.1.jar";
		// String remoteJarPath =
		// "/qa/src/javaservices/java02/java/sites/java02/9.10.1082/dist/kp-etl-core-9.10.1.jar";
		// OSUtil.scpToRemote(localJarPath, remoteJarPath, hostname,username, password);
		return OSUtil.runRemoteCommand(hostname, username, password, cmd);
	}


	public static boolean areAllTasksProcessed(String revision, String hostname, String username, String password, String dbType,
											   String dbHostname, String dbPort, String dbUsername, String dbPassword) {

		Connection conn = null;

		try {

			conn = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);
			String tasksSql = "select count(*) as numOfRecords from " + username + "_masterDW.sch_task_queue";
			ResultSet tasksRS = EspDatabaseRestoration.executeSQL(conn, tasksSql);

			// if there is no record in the sch_task_queue table, returns false.
			if (tasksRS.getString("numOfRecords").equalsIgnoreCase("0")) {

				return false;

			}

			EspDatabaseRestoration.startSchedulerAndExecutor(hostname, username, password);

			long startTime = System.currentTimeMillis();
			int maxRunTimeInMilliSec = 1200000;

			while ((System.currentTimeMillis() - startTime) < maxRunTimeInMilliSec) {

				tasksRS = EspDatabaseRestoration.executeSQL(conn, tasksSql);

				if (tasksRS.getString("numOfRecords").equalsIgnoreCase("0")) {

					return true;

				}

				Thread.sleep(5000);

			}

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			try {

				if(conn != null) conn.close();

			} catch (SQLException se) {

				se.printStackTrace();

			}

		}

		return false;

	}

	public static boolean areAllTasksExecuted(String revision, String hostname, String username, String password, String kdwSiteIdentifier,
											  String dbType, String dbHostname, String dbPort, String dbUsername, String dbPassword, String masterDBName) {

		Connection conn = null;

		try {

			System.out.println("/////////////////////////////////dbType: " + dbType);

			conn = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);

			String tasksSql = (dbType.equalsIgnoreCase("MYSQL")) ? "select count(*) as numOfRecords from " + masterDBName + ".sch_task_queue":
					"select count(*) as numOfRecords from [" + masterDBName + "].[dbo].[sch_task_queue]";
			ResultSet taskRS = EspDatabaseRestoration.executeSQL(conn, tasksSql);

			taskRS.next();

			// if there is no record in the sch_task_queue table, returns false.
			if (taskRS.getString("numOfRecords").equalsIgnoreCase("0")) {

				return false;

			}

//			String kdwInstallationDir = "/javaservice/" + username + "/java/pentaho/" + kdwSiteIdentifier + "/current/kiwiplan/";
			String kdwInstallationDir = "/javaservice/" + username + "/services/etl/" + kdwSiteIdentifier + "/current/kiwiplan/";
//			String existingSchedulerExecutorKillCmd = kdwInstallationDir + "bin/listRunningKDWServices.sh | grep -E '" + username + ".*8445' | cut -d' ' -f1 | xargs kill -9";
			String appSchedulerPortChangeCmd = "sed -i 's/8443/8245/g' " + kdwInstallationDir + "conf/application_scheduler.properties";
			String appExecutorPortChangeCmd = "sed -i 's/8443/8245/g' " + kdwInstallationDir + "conf/application_executor.properties";
			String schedulerStartCmd = kdwInstallationDir + "bin/startScheduler.sh";
			String executorStartCmd = kdwInstallationDir + "bin/startExecutor.sh";

			// Kill the existing scheduler and executor processes started by previous ETL run.
//			OSUtil.runRemoteCommand(hostname, username, password, existingSchedulerExecutorKillCmd);

			// Change the port in application_scheduler.properties and application_executor.properties from 8443 to 8445 to avoid conflicts with any running KDW processes.
			OSUtil.runRemoteCommand(hostname, username, password, appSchedulerPortChangeCmd);
			OSUtil.runRemoteCommand(hostname, username, password, appExecutorPortChangeCmd);

			// Start scheduler and executor
			OSUtil.runRemoteCommand(hostname, username, password, schedulerStartCmd);
			Thread.sleep(30000);
			OSUtil.runRemoteCommand(hostname, username, password, executorStartCmd);

			long startTime = System.currentTimeMillis();
			int maxRunTimeInMilliSec = 3600000;

			while ((System.currentTimeMillis() - startTime) < maxRunTimeInMilliSec) {

				taskRS = EspDatabaseRestoration.executeSQL(conn, tasksSql);
				taskRS.next();

				if (taskRS.getString("numOfRecords").equalsIgnoreCase("0")) {

					return true;

				}

				Thread.sleep(5000);

			}

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			try {

				if(conn != null) conn.close();

			} catch (SQLException se) {

				se.printStackTrace();

			}

		}

		return false;

	}


	/**
	 *
	 * Below methods are relative to KDW running
	 */
	public static boolean kdwRunETL(String revision, String hostname, String username, String password,
									String localRawKpSiteFilePath, String localRawEtlFile, String period) {

		String localFormatedSiteFile = createFormattedFile(localRawKpSiteFilePath);
		String remoteKpSiteFile = String.format("/javaservice/%s/java/pentaho/%s/current/kiwiplan/conf/kp_sites.xml",
				username, username);
		int rc = OSUtil.scpToRemote(localFormatedSiteFile, remoteKpSiteFile, hostname, username, password);

		if (rc != 0) {
			return false;
		}

		String localFormatedetlFile = createFormattedFile(localRawEtlFile);
		String remoteEtlFile = String.format("/javaservice/%s/java/pentaho/%s/current/kiwiplan/conf/kp_etl.xml",
				username, username);

		rc = OSUtil.scpToRemote(localFormatedetlFile, remoteEtlFile, hostname, username, password);

		if (rc != 0) {
			return false;
		}

		String dataInitCmd = "cd $PENTAHODIR/kiwiplan/bin;./dwdate_initialization.sh";
		List<String> dataInitCmdResult = OSUtil.getRemoteSingleCommandOut(hostname, username, password, dataInitCmd);

		for (String line : dataInitCmdResult) {
			if (line.contains("Finished with errors")) {
				return false;
			}
		}

		String cmd = "cd $PENTAHODIR/kiwiplan/;";
		cmd = cmd + "./bin/load_schedule_configure.sh kiwiplan/conf;";
		List<String> stringList = OSUtil.getRemoteSingleCommandOut(hostname, username, password, cmd);

		for (String line : stringList) {
			if (line.contains("Finished with errors")) {
				return false;
			}
		}

		cmd = "cd $PENTAHODIR/kiwiplan/;";
		cmd += "./bin/" + ConfigHelper.getInstance().getConfig("KDWETLBatch") + " " + period;

		stringList = OSUtil.getRemoteSingleCommandOut(hostname, username, password, cmd);

		for (String line : stringList) {
			if (line.contains("Finished with errors")) {
				return false;
			}
		}

		String revisionConvertedStr = revision.replace(".", "");

		if (revisionConvertedStr.compareTo("9721") >= 0) {

			String dbHostname = ConfigHelper.getInstance().getConfig("Database.DBServer");
			String dbPort = ConfigHelper.getInstance().getConfig("Database.DBPort");
			String dbUsername = ConfigHelper.getInstance().getConfig("Database.DBUser");
			String dbPassword = ConfigHelper.getInstance().getConfig("Database.DBPasswd");
			String dbType = ConfigHelper.getInstance().getConfig("Database.DBTYPE");
			String masterDBName = username + "_masterDW";

			return areAllTasksExecuted(revision, hostname, username, password, username, dbType, dbHostname, dbPort, dbUsername, dbPassword, masterDBName);

		}

		return true;
	}


	public static boolean kdwSetupForeignKey(String revision, String hostname, String username, String password) {
		String localScriptPath = "scripts/" + revision + "/foreignkeys";
		String remoteScriptPath = scpLocalScriptToRemoteHome(hostname, username, password, localScriptPath);
		String cmd = "mysql -ukiwisql -pmapadm99 \"$USER\"_masterDW < " + remoteScriptPath;

		return OSUtil.runRemoteCommand(hostname, username, password, cmd);
	}

	/*
	public static boolean kdwUpgradeInstall_old(String revision, String hostname, String installerpath, String username,
			String password) {
		rootLogger.severe("Upgrade Install with: " + installerpath);
		kdwPreUpgradeInstall(revision, hostname, installerpath, username, password);

		boolean bResult = true;

		try {
			Connection conn = new Connection(hostname);

			conn.connect();

			// Authenticate
			boolean isAuthenticated = conn.authenticateWithPassword(username, password);

			if (isAuthenticated == false) {
				throw new IOException("Authentication failed.");
			}

			Session sess = conn.openSession();

			// String installer =
			// installerpath.substring("/data/build/".length(),
			// installerpath.length());
			String remoteKDWLicenceFilePath = String.format(
					"/qa/src/javaservices/%s/java/sites/%s/current/conf/licence/KDW.licence", username, username);
			String cmd = "source /etc/profile; source /mapqa/bin/.evprofile; ";

			cmd = cmd + "cd $HOME; ";

			// cmd = cmd + "cp "+SystemHelper.BASE_FOLDER+"/*.properties .; ";
			cmd = cmd + "cp " + remoteKDWLicenceFilePath + " $HOME/;";
			// add additional parameter to go pass the configure page
			cmd = cmd
					+ "cat /mapqa/java09_testing/920upgrade.properties >> $INSTALLDIR/conf/recentparametervalues.properties;";
			cmd = cmd + installerpath;
			cmd = cmd + " -s " + username + " /javaservice/" + username;
			sess.execCommand(cmd);

			InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(sess.getStdin()));
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			String line = null;
			int step = 0;
			int timeout = 60;

			while (true) {
				line = br.readLine();

				if (line == null) {
					if (timeout-- > 0) {
						Thread.sleep(10);

						continue;
					}

					break;
				}

				rootLogger.info(line);

				if (errorMsgOccur(line, writer)) {
					rootLogger.severe("Install/Upgrade met error: " + line);
					bResult = false;

					continue;
				}

				// === Products to install/upgrade ===
				if (line.contains("=== Products to install/upgrade ===")) {
					step = STEP_INSTALL_INSTALL_TYPE;
				}

				if ((step == STEP_INSTALL_INSTALL_TYPE) && (line.contains("Next:N"))) {
					writer.write("\n");
					writer.flush();

					continue;
				}
				if ((step == STEP_INSTALL_INSTALL_TYPE) && (line.contains("default=n>"))) {
					writer.write("n\n");
					writer.flush();

					continue;
				}

				// === Initial Configuration ===
				if (line.contains("Installation Configuration")) {
					step = STEP_INSTALL_INITIAL_CONFIGURE;
				}

				if ((step == STEP_INSTALL_INITIAL_CONFIGURE) && (line.contains("Initial Configuration"))) {
					writer.write("\n\n\n");
					writer.flush();
					writer.write("yes\n");
					writer.flush();

					continue;
				}

				if (line.contains("Choose a scope of customer number")) {
					step = STEP_INSTALL_CUSTOMERSCOPE;
				}

				if ((step == STEP_INSTALL_CUSTOMERSCOPE)) {
					writer.write("\n");
					writer.flush();

					continue;
				}

				if (line.contains("Master database MySQL server timezone configuration")) {
					step = STEP_INSTALL_MASTER_TIMEZONE;
				}

				if ((step == STEP_INSTALL_MASTER_TIMEZONE) && (line.contains("Current values"))) {
					writer.write("y\n");
					writer.flush();

					continue;
				}

				if ((step == STEP_INSTALL_MASTER_TIMEZONE)
						&& (line.contains("Please enter the master database MySQL server timezone"))) {
					writer.write("Pacific/Auckland\n");
					writer.flush();

					continue;
				}

				if ((step == STEP_INSTALL_MASTER_TIMEZONE) && (line.contains("Input summary"))) {
					writer.write("\n");
					writer.flush();

					continue;
				}

				// Install result
				// KDW - Kiwiplan Data Warehouse Successful
				// same step number for below steps
				//
				if (line.contains("Installation completed successfully:")
						|| line.contains("One or more products failed to install:")) {
					step = STEP_INSTALL_INSTALL_RESULT;
				}

				if ((step == STEP_INSTALL_INSTALL_RESULT) && (line.contains("Successful"))) {
					bResult = true;
				}

				if ((step == STEP_INSTALL_INSTALL_RESULT) && (line.contains("Failed"))) {
					bResult = false;
				}

				//
				if ((step == STEP_INSTALL_INSTALL_RESULT) && (line.contains("Exit:E"))) {
					writer.write("E\n");
					writer.flush();

					continue;
				}
			}

			br.close();
			writer.close();
			rootLogger.info("ExitCode: " + sess.getExitStatus());
			sess.close();
			conn.close();
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(2);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(2);
		}

		if (bResult) {
			bResult = kdwPostUpgradeInstall(revision, hostname, installerpath, username, password);
		}

		return bResult;
	}
	 */

	public static void main(String[] args) {

		// if (args.length < 4) {
		// LogHelper.log(LogHelper.LOGLEVEL_INFO,
		// "==============================================================================================");
		// LogHelper.log(LogHelper.LOGLEVEL_INFO, "need 4 parameters:");
		// LogHelper.log(LogHelper.LOGLEVEL_INFO,
		// "hostname, username, password, installerpath");
		// LogHelper.log(LogHelper.LOGLEVEL_INFO,
		// "Example: nzvader java04 mapadm99
		// /javaservice/installers/kdw-7.90.2_curr/kdw-7.90.2-113.sh");
		// LogHelper.log(LogHelper.LOGLEVEL_INFO,
		// "==============================================================================================");
		// System.exit(2);
		// }
		//
		// String hostname = args[0];
		// String username = args[1];
		// String password = args[2];
		// String installerpath = args[3];
		// LogHelper.initializeLog("KDWHelper");
		// LogHelper.truncate();
		// String hostname = "nzvader";
		// String username = "java13";
		// String password = "mapadm99";
//      String installerpath = "/javaservice/installers/kdw-7.90.1_p6released/kdw-7.90.1-266.sh";
		//// String installerpath =
		//// "/javaservice/installers/kdw-7.90.2_released/kdw-7.90.2-283.sh";
		// String installerpath =
		//// "/javaservice/installers/kdw-7.90_released/kdw-7.90-15.sh";
		// String upgradepath = "/data/builds/kdw-8.0/kdw-8.0-261.sh";
		//
		//// LogHelper.truncate();
		//
		// String sysBaseFolder =
		//// ConfigHelper.getInstance().getConfig("System.BaseFolder");
		// String logFolder = ConfigHelper.getLogConfig().getProperty("logFolder");
		//
		//// deal with summary log
		// LogHelper.initializeSummaryLog(sysBaseFolder + "/" + logFolder);
		// LogHelper.initializeTestcaseLog(new CompSchema4FreshUpgrade());
		//
		//// deal with temp folder
		//// SystemHelper.initializeTempFolder();
		// KDWHelper.kdwFreshInstall("kdw-7.90", hostname, installerpath, username,
		//// password, true);
		// KDWHelper.kdwUpgradeInstall("kdw-8.0", hostname, upgradepath, username,
		//// password);
//       String installerpath = "/javaservice/installers////kdw-8.10_released/////kdw-8.10-89.sh";
//       String temp = installerpath.substring(0, installerpath.lastIndexOf("/"));
//       while (temp.endsWith("/")) {
//       temp = temp.substring(0, temp.length() - 1);
//       }
//       temp = temp.substring(temp.lastIndexOf("/"));
//       temp = temp.substring(temp.indexOf("-") + 1);
//       String jbase = temp;
//       System.out.println(jbase);
		// KDWHelper.kdwCompareSchema(hostname, username,
		// password, dbname1, dbname2);
		String name = "kdw-9.10.1122.sh\n";
		String rev = getRevFromBuildName(name);

		System.out.print(rev);
	}

	public static String scpLocalScriptToRemoteHome(String hostname, String username, String password,
			String localScriptPath) {
		String localFilePath = System.getProperty("user.dir") + "/" + localScriptPath;
		File localFile = new File(localFilePath);

		if (!localFile.exists()) {
			return null;
		}

		String localFileName = localFile.getName();
		String remoteFileName = OSUtil.getRemoteUserHome(hostname, username, password) + "/" + localFileName;

		OSUtil.scpToRemote(localFilePath, remoteFileName, hostname, username, password);

		return remoteFileName;
	}

	public static boolean isNaturalKey(String revision, String tableName, String columnName) {
		return KDWSchemaHelper.getUniqueKey(revision, tableName).contains(columnName);
	}

	/**
	 *
	 * Natural Key of KDW tables As for 23/05/2014, the doc: https://docs.google.
	 * com/a/kiwiplan.net.nz/document/d/110kZB0fC66zpveLFRcusDsC0b12o
	 * -9hL9PUC6jXe1so/edit
	 */

	/**
	 *
	 * End of natural key definition
	 */

	/**
	 *
	 * Below methods are relative to analyzing KDW ETL results
	 */
	public static boolean isPCSEnabled(String dbName) throws Exception {
		boolean pcsEnabled = true;
		String sql = "SELECT has_pcs FROM " + dbName + ".dwsiteconfig";

		// String sql = "DESCRIBE 'FACTRY'";
		ResultSet rs = DBHelper.getInstance().executeSQL(sql);

		while (rs.next()) {
			String enabled = rs.getString("has_pcs");

			if (enabled.equals("0")) {
				pcsEnabled = false;
			}
		}

		return pcsEnabled;
	}

	public static long getRecordNumber(String dbName, String tableName) {
		return DBHelper.getInstance().getRecordNumber(dbName, tableName);
	}

	public static String getRevFromBuildName(String buildName) {
		Pattern pattern = Pattern.compile("kdw-([0-9]+\\.[0-9]+\\.*[0-9]).*");
		Matcher matcher = pattern.matcher(buildName.trim());
		String revision = "";

		if (matcher.matches()) {
			revision = matcher.group(1);
		}

		rootLogger.info("Revision Number got from build name: " + buildName + " is : " + revision);

		return revision;
	}

//  public static boolean isRevLaterThan7702(String dbName) {
//      boolean isRevLaterThan7702 = true;
//      String  sql                = "SELECT level_step_number FROM " + dbName    // $NON-NLS-1$
//                                   + ".dwinventorylevel";                       // $NON-NLS-1$
//
//      // String sql = "DESCRIBE 'FACTRY'";
//      try {
//          DBHelper.getInstance().executeSQL(sql);
//      } catch (Exception e) {
//
//          // e.printStackTrace();
//          return false;
//      }
//
//      return isRevLaterThan7702;
//  }

	public static boolean restoreEspDatabase (String dbType, String dbHostname, String dbPort, String dbUsername,
											  String dbPassword, String espDBName, String bakFileLocation) {

		boolean espDatabaseRestored = true;
		Connection connection = null;

		try {

			connection = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);

			System.out.println("Drop the database " + espDBName + ", if it exists.");

			EspDatabaseRestoration.removeDatabase(connection, espDBName);

			System.out.println("Restore the database " + espDBName);

			String dbRestoreSql = "restore database " + espDBName + " from disk = '" + bakFileLocation + "'" +
									" with move 'espbox2_Data' to 'E:\\Data\\" + espDBName + "'" +
									", move 'espbox2_Log' to 'F:\\Logs\\" + espDBName + "', replace";

			EspDatabaseRestoration.executeUpdate(connection, dbRestoreSql);

		} catch (Exception e) {

			e.printStackTrace();
			espDatabaseRestored = false;

		} finally {

			try {

				if(connection != null) connection.close();

			} catch (SQLException se) {

				se.printStackTrace();

			}

		}

		return espDatabaseRestored;

	}

	public static boolean killExistingSchedulerAndExecutorProcesses(String kdwHostname, String kdwUsername, String kdwPassword,
																	String siteIdentifier) {

		String kdwInstallationDir = "/javaservice/" + kdwUsername + "/service/etl/" + siteIdentifier;

		String existingSchedulerExecutorKillCmd = "ps -ef | grep -i /javaservice/" + kdwUsername + "/services/etl/" + siteIdentifier + " | grep -v grep | awk '{print $2}' | xargs kill -9";
		String existingProcessesOnSpecificPortKillCmd = "/sbin/lsof -t -i:8245 | xargs kill -9";

		// Kill the existing scheduler and executor processes started by previous ETL run and any processes related to the scheduler port.
		OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, existingSchedulerExecutorKillCmd);

		// Kill the existing processes related to the scheduler port.
		OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, existingProcessesOnSpecificPortKillCmd);

		return true;

	}


	public static boolean removeExistingKdwDBAndInstallation (String kdwHostname, String kdwUsername, String kdwPassword,
														   String dbType, String dbHostname, String dbPort, String dbUsername,
														   String dbPassword, String siteIdentifier) {

		Connection connection = null;
		boolean existingKdwDBAndInstallationRemoved = true;

		try {

			connection = EspDatabaseRestoration.getConnection(dbType, dbHostname, dbPort, dbUsername, dbPassword);
			String kdwMasterDBName = siteIdentifier + "_master_datawarehouse";
			String kdwWorkingDBName = siteIdentifier + "_working_datawarehouse";

			EspDatabaseRestoration.removeDatabase(connection, kdwMasterDBName);
			EspDatabaseRestoration.removeDatabase(connection, kdwWorkingDBName);

			String existingKdwInstallDirRemovalCmd = "rm -rf /javaservice/" + kdwUsername + "/service/etl/" + siteIdentifier + ";" +
														"rm -rf /javaservice/" + kdwUsername + "/service/sites/" + siteIdentifier;

			existingKdwDBAndInstallationRemoved = OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, existingKdwInstallDirRemovalCmd) &&
					killExistingSchedulerAndExecutorProcesses(kdwHostname, kdwUsername, kdwPassword, siteIdentifier);

		} catch (Exception e) {

			e.printStackTrace();
			existingKdwDBAndInstallationRemoved = false;

		} finally {

			try {

				if (connection != null) connection.close();

			} catch (SQLException se) {

				se.printStackTrace();

			}
		}

		return existingKdwDBAndInstallationRemoved;

	}


	public static boolean installOrUpgradeKDW (String kdwHostname, String kdwUsername, String kdwPassword, String baseDirectory,
											   String installerDirectory, String siteIdentifier, String dbType, String dbHostname,
											   String dbPort, String dbUsername, String dbPasswordEncrypted,
											   String jenkinsHostname, String jenkinsUsername, String jenkinsPassword, String javaHome,
											   String kdwIntallOrUpgradeRevision, String kdwInstallOrUpgradeBuild,
											   String operationType) {

		String kdwOperationCmd = "mkdir -p /javaservice/" + kdwUsername + "/services;" +
				"export BASE_DIRECTORY=" + baseDirectory + ";" +
				"export INSTALLER_DIRECTORY=" + installerDirectory + ";" +
				"export SITE_IDENTIFIER=" + siteIdentifier + ";export DATABASE_BRAND=" + dbType + ";" +
				"export DATABASE_HOST=" + dbHostname + ";export DATABASE_PORT=" + dbPort + ";" +
				"export DATABASE_USERNAME=" + dbUsername + ";export DATABASE_PASSWORD=" + dbPasswordEncrypted + ";" +
				"export JENKINS_SERVER=" + jenkinsHostname + ";export JENKINS_USERNAME=" + jenkinsUsername + ";" +
				"export JENKINS_PASSWORD=" + jenkinsPassword + ";export JAVA_HOME=" + javaHome + ";" +
				"cd /javaservice/java23/kdw_auto_upgrade_scripts/kdw-auto-install-upgrade;";

		if (operationType.equalsIgnoreCase("installation")) {

			kdwOperationCmd += "./install -p kdw -r " + kdwIntallOrUpgradeRevision + " -b " + kdwInstallOrUpgradeBuild;

		} else {

			kdwOperationCmd += "./upgrade -p kdw -r " + kdwIntallOrUpgradeRevision + " -b " + kdwInstallOrUpgradeBuild;

		}

		return OSUtil.runRemoteCommand(kdwHostname, kdwUsername, kdwPassword, kdwOperationCmd);

	}

	public static boolean copyMysqlConnectorAndKdwConfigurationFiles(String kdwHostname, String kdwUsername, String kdwPassword,
																	 String kdwSiteIdentifier, String localRawKpSiteFile,
																	 String localMysqlConnectorFile) {

		String localFormatedSiteFile = createFormattedFile(localRawKpSiteFile);
		String remoteKpSiteFile = String.format("/javaservice/%s/services/etl/%s/current/kiwiplan/conf/kp_sites.xml", kdwUsername, kdwSiteIdentifier);

		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>localFormatedSiteFile");
		System.out.println(localFormatedSiteFile);

		int rc = OSUtil.scpToRemote(localFormatedSiteFile, remoteKpSiteFile, kdwHostname, kdwUsername, kdwPassword);

		if (rc != 0) {
			return false;
		}

		String remoteMysqlConnectorFile = String.format("/javaservice/%s/services/etl/%s/current/lib/mysql-connector-java-5.1.48.jar",
				kdwUsername, kdwSiteIdentifier);

		rc = OSUtil.scpToRemote(localMysqlConnectorFile, remoteMysqlConnectorFile, kdwHostname, kdwUsername, kdwPassword);

		if (rc != 0) {
			return false;
		}

		return true;
	}

	public static boolean invokeDwdateInitAndLoadScheduleConfigure(String kdwHostname, String kdwUsername, String kdwPassword,
																   String kdwSiteIdentifier, String sourceDBType) {

		String mapDBTypeAlterCmd = (sourceDBType.equalsIgnoreCase("ISAM")) ?
				"sed -i s/\\<map-db-type\\>MYSQL/\\<map-db-type\\>ISAM/ /javaservice/" + kdwUsername + "/services/etl/" + kdwSiteIdentifier + "/current/kiwiplan/conf/kp_etl.xml;" :
				"";

		String cmd = mapDBTypeAlterCmd + "cd /javaservice/" + kdwUsername + "/services/etl/" + kdwSiteIdentifier + "/current/kiwiplan/bin;" +
				"./dwdate_initialization.sh;" + "./load_schedule_configure.sh kiwiplan/conf";
		List<String> stringList = OSUtil.getRemoteSingleCommandOut(kdwHostname, kdwUsername, kdwPassword, cmd);
		boolean result = true;

		for (String line : stringList) {
			if (line.contains("Finished with errors")) {
				return false;
			}
		}

		return true;

	}

	public static boolean runKdwEtl(String revision, String kdwHostname, String kdwUsername, String kdwPassword, String kdwSiteIdentifier, String period,
									String dbType, String dbHostname, String dbPort, String dbUsername, String dbPassword) {

		String cmd = "cd /javaservice/" + kdwUsername + "/services/etl/" + kdwSiteIdentifier + "/current/kiwiplan/bin;" +
				"./" + ConfigHelper.getInstance().getConfig("KDWETLBatch") + " " + period;

		List<String> stringList = OSUtil.getRemoteSingleCommandOut(kdwHostname, kdwUsername, kdwPassword, cmd);

		for (String line : stringList) {
			if (line.contains("Finished with errors")) {
				return false;
			}
		}

		String revisionConvertedStr = revision.replace(".", "");

		if (revisionConvertedStr.compareTo("9721") >= 0) {

			String masterDBName = kdwSiteIdentifier + "_master_datawarehouse";

			return areAllTasksExecuted(revision, kdwHostname, kdwUsername, kdwPassword, kdwSiteIdentifier, dbType,
					dbHostname, dbPort, dbUsername, dbPassword, masterDBName);

		}

		return true;
	}


	public static boolean checkKdwEtlResult (String kdwDBType, String kdwDBHostname, String kdwDBPort,
											 String kdwDBUsername, String kdwDBPassword, String kdwSiteIdentifier) {

		String runLogCheckSqlQuery = (kdwDBType.equalsIgnoreCase("MYSQL")) ? "SELECT log_success FROM " + kdwSiteIdentifier + "_master_datawarehouse.dwsiterunlog where log_success != 1":
				"SELECT log_success FROM [" + kdwSiteIdentifier + "_master_datawarehouse].[dbo].[dwsiterunlog] where log_success != 1";

		String taskHistoryCheckSqlQuery = (kdwDBType.equalsIgnoreCase("MYSQL")) ? "SELECT task_id FROM " + kdwSiteIdentifier + "_master_datawarehouse.sch_task_history where task_status != 'SUCCESS'":
				"SELECT task_id FROM [" + kdwSiteIdentifier + "_master_datawarehouse].[dbo].[sch_task_history] where task_status != 'SUCCESS'";

		boolean bResult = true;
		ResultSet rsSiteRunLog = null;
		Connection connection = null;

		try {

			connection = EspDatabaseRestoration.getConnection(kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword);
			rsSiteRunLog = EspDatabaseRestoration.executeSQL(connection, runLogCheckSqlQuery);

			if (rsSiteRunLog.next()) {

				rootLogger.info("run failed");
				bResult = false;

			}

			rsSiteRunLog = EspDatabaseRestoration.executeSQL(connection, taskHistoryCheckSqlQuery);

			if (rsSiteRunLog.next()) {

				rootLogger.info("task failed, check sch_task_history");
				bResult = false;

			}

		} catch (Exception e) {

			e.printStackTrace();
			return false;

		} finally {

			try {

				if(connection != null) connection.close();

			} catch (SQLException se) {

				se.printStackTrace();

			}

		}

		System.out.println("ETL result check returns " + bResult);

		return bResult;

	}


	public static boolean installAndConfigureKdwRunEtlAndCheckResults(String hostname, String username, String password, String kdwDBType,
															   String kdwDBHostname, String kdwDBPort, String kdwDBUsername, String kdwDBPassword,
															   String kdwDBPasswordEncrypted, String kdwBaseDirectory,
															   String kdwInstallerDirectory, String kdwSiteIdentifier, String installerJenkinsHostname,
															   String installerJenkinsUsername, String installerJenkinsPassword,
															   String kdwJavaHome, String kdwRevision, String kdwBuild, String kpSitesFile,
															   String mySqlConnectorFile, String etlPeriod, String sourceDBType) {

		//Clean up existing KDW databases and installation
		rootLogger.severe("Clean up existing KDW databases and installation");

		if (KDWHelper.removeExistingKdwDBAndInstallation(hostname, username, password, kdwDBType, kdwDBHostname, kdwDBPort,
				kdwDBUsername, kdwDBPassword, kdwSiteIdentifier) == false) {
			rootLogger.severe("Failed to remove existing KDW databases and installation.");

			return false;
		}

		// fresh install base build
		rootLogger.severe("KDW install revision: " + kdwRevision + " build: " + kdwBuild);

		if (KDWHelper.installOrUpgradeKDW(hostname, username, password, kdwBaseDirectory, kdwInstallerDirectory, kdwSiteIdentifier,
				kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPasswordEncrypted, installerJenkinsHostname,
				installerJenkinsUsername, installerJenkinsPassword, kdwJavaHome, kdwRevision, kdwBuild, kdwInstallationCode) == false) {
			rootLogger.severe("Failed to Install KDW revision : " + kdwRevision + " build: " + kdwBuild);

			return false;
		}

		// Copy MYSQL connector and kp_sites.xml to the remote machine.
		rootLogger.severe("Copy MYSQL connector and kp_sites.xml to the remote machine.");

		if (KDWHelper.copyMysqlConnectorAndKdwConfigurationFiles(hostname, username, password, kdwSiteIdentifier, kpSitesFile, mySqlConnectorFile) == false) {

			rootLogger.severe("Failed to copy MYSQL connector, kp_sites_n1_n.xml and kp_etl.xml files to the remote machine.");    // $NON-NLS-1$

			return false;

		}

		// Invoke load_schedule_configure.sh to load sites and datasets information to database.
		rootLogger.severe("Invoke dwdate_initialization and load_schedule_configure scripts.");

		if (KDWHelper.invokeDwdateInitAndLoadScheduleConfigure(hostname, username, password, kdwSiteIdentifier, sourceDBType) == false) {
			rootLogger.severe("Failed to invoke load_schedule_configure.sh to load sites and datasets information to database.");    // $NON-NLS-1$
			rootLogger.severe("Test Step Failed ");

			return false;
		}

		// run ETL to get historical data
		rootLogger.severe("Run ETL: " + etlPeriod);

		if (KDWHelper.runKdwEtl(kdwRevision, hostname, username, password, kdwSiteIdentifier, etlPeriod,
				kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword) == false) {
			rootLogger.severe("Failed to run first time of ETL " + etlPeriod);    // $NON-NLS-1$
			rootLogger.severe("Test Step Failed ");

			return false;
		}

		// check result
		rootLogger.severe("Check if ETL succeeded or not");

		if (KDWHelper.checkKdwEtlResult(kdwDBType, kdwDBHostname, kdwDBPort, kdwDBUsername, kdwDBPassword, kdwSiteIdentifier) == false) {
			rootLogger.severe("KDW ETL running failed. Period: " + etlPeriod);

			return false;
		}

		return true;


	}

}
