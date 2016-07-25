package agent;

import java.io.File;
import java.util.*;

import org.apache.log4j.Logger;

public class CSVDumper implements RowGatherer {
    private CSVReaderManager csvReaderManager;

    protected static Logger logger = Logger.getLogger(CommitLogReader.class.getName());

    public void init(String table) throws Exception {

        Map<String,Object> connexions = ConfManager.getConnection((String)Main.exporterConf.get("configCSVDumper"));
        Map<String,Object> conf = ConfManager.getTable(table);
        int confBlockSize = ((Integer)conf.get("blockSize")).intValue();

        //path
        String path;
        if (connexions.containsKey("path")) {
            path = (String)connexions.get("path");
        } else {
            path = "./";
        }

        logger.debug("** Table init "+ table +"  dump: " + conf.toString() + " from path " + path +"**");

        //Discard in seconds
        long time;
        if (Main.exporterConf.containsKey("commitLogPeriod")) {
        	time = (Integer)Main.exporterConf.get("commitLogPeriod");
        } else {
        	time = 300;
        }

        csvReaderManager = new CSVReaderManager(table, path , confBlockSize + 1, time);
    }

    public void reset() throws Exception {
    	csvReaderManager.reset();
    }

    public RowBlock read() throws Exception {
        return csvReaderManager.readSequential();
    }

    public void close() throws Exception {
    	csvReaderManager.close();
    }
    
    public void renameCSVFile() throws Exception {
    	//change name post-processed csv file
    	if (csvReaderManager.getFileProcessed() != null){
	    	File filediriden = csvReaderManager.getFileProcessed();
	    	File newfilediriden = new File(filediriden.getAbsolutePath()+"_processed");
	    	filediriden.renameTo(newfilediriden);
    	}
    }

}
