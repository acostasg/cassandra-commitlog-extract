package agent;

import java.util.*;
import java.io.IOException;

import org.apache.log4j.Logger;

public class Main {

    public static Map<String,Object> exporterConf;

	protected static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            logger.info("Usage: <runner-script.sh> <exporter name> <override ETL length in seconds (optional)>");
            return;
        }

        logger.info("Loading config files...");
        logger.debug("Mode debug/verbose is [ON] [This option write much information on screen.]");
        
        ConfManager.init(args[0]);

        //JDBCManager.singleton();

        String exporterName = args[1];
        logger.info("Launching exporter '" + exporterName + "'");

        exporterConf = ConfManager.getExporter(exporterName);
        
        //status cron csv
        if (exporterConf.containsKey("checkCronCSV") && ((Boolean)exporterConf.get("checkCronCSV")).booleanValue()) {
            blockedCron();
        }

        if (args.length >= 3) {
            Integer period = Integer.parseInt(args[2]);
            exporterConf.put("commitLogPeriod", period);
        }

        if (exporterConf.containsKey("initPreload") && ((Boolean)exporterConf.get("initPreload")).booleanValue()) {
            logger.info("Preloading some ID mappings from Cassandra...");
            Preload.init();
        }

        if (exporterConf.containsKey("initCommitLogReader") && ((Boolean)exporterConf.get("initCommitLogReader")).booleanValue()) {
            String commitLogFolder = (String)exporterConf.get("commitLogFolder");
            int binSize = ((Integer)exporterConf.get("commitLogBinSize")).intValue();
            Object periodO = exporterConf.get("commitLogPeriod");
            long period = (long)(((Integer)periodO).intValue());
            int idBlockSize = ((Integer)exporterConf.get("idBlockSize")).intValue();
            ConfManager.setGlobal("idBlockSize", ""+idBlockSize);
            logger.info("Reading commit logs from "+commitLogFolder+", bin size " + binSize +
                ", id block size " + idBlockSize + ", period " + period + " seconds");
            CommitLogReader r = new CommitLogReader(binSize);
            r.readCommitLogDirectory(commitLogFolder, period);
        }

        if (exporterConf.containsKey("postgresBulkMode") && ((Boolean)exporterConf.get("postgresBulkMode")).booleanValue()) {
            ConfManager.setGlobal("postgresBulkMode", "true");
        } else {
            ConfManager.setGlobal("postgresBulkMode", "false");
        }

        List<Flow> flows = new LinkedList<Flow>();
        List<Thread> running = new LinkedList<Thread>();

        String readerClassName = (String)exporterConf.get("reader");
        List<String> writersClassNames = (List<String>)exporterConf.get("writers");

        Map<String,Object> filterConf = (Map<String,Object>)exporterConf.get("filter");
        Filter filter = null;
        if (filterConf != null) {
            filter = new Filter(filterConf);
        }

        logger.info("Loading flow definitions...");
        List<List<String>> flowDefinitions = (List<List<String>>)exporterConf.get("flows");

        // output is shared and multithreaded, init the output manager
        logger.info("Init output manager...");
        OutputManager.init(writersClassNames, filter);

        // create a flow for every table listing inside the flow definitions, and launch them
        logger.info("Start flows...");
        for (List<String> flowTables : flowDefinitions) {
            Flow flow = new Flow(flowTables, readerClassName);
            Thread t = new Thread(flow);
            t.start();
            logger.info("Flow started!");
            running.add(t);
            flows.add(flow);
        }

        // monitor the threads and wait for them to die
        Monitor m = new Monitor(flows);
        for (Thread t : running) {
            t.join();
            logger.info("Flow ended");
        }

        logger.info("Ending monitoring");
        m.end();
        
        logger.info("All flows ended, closing output manager");
        OutputManager.closeAll();
        
        //rename file
        flows.get(0).close();
        
        CSVReaderStatus.setStatus(CSVReaderStatus.statusFree);

    }
    
	private static void blockedCron() throws Exception {
		switch (CSVReaderStatus.getStatus()) {
			case CSVReaderStatus.statusBlocked:
				 	logger.info("BLOCKED WAITING: another similarly cron still running.");
				 	System.exit(0);
				 	break;
			case CSVReaderStatus.statusError:
					logger.info("FATAL ERROR: Checking status because before itineration FAILED, the process is BLOCKED.");
					System.exit(0);
					break;
			}
		CSVReaderStatus.setStatus(CSVReaderStatus.statusBlocked);
	}
}
