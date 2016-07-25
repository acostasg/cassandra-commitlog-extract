package agent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.lang.Exception;
import java.lang.String;
import java.sql.Timestamp;
import java.util.Map;
import au.com.bytecode.opencsv.*;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

public class CSVReaderStatus {
	
    protected static Logger logger = Logger.getLogger(CSVReaderStatus.class.getName()); 

    public static final char statusFree = '0';
    public static final char statusBlocked = '1';
    public static final char statusError = '2';

	private static au.com.bytecode.opencsv.CSVWriter writer;

	private static CSVReader csvReader;
	
	public static char getStatus() throws Exception {

        String fullPathFile = CSVReaderStatus.getUrlStatusFile();

        File fileStatus = new File(fullPathFile);
        
        char status = statusBlocked;
        
        if(fileStatus.isFile() && fileStatus.canRead()) {
            logger.debug("Reader file status: " + fileStatus.getAbsolutePath());

        	FileReader fileReader = new FileReader(fileStatus);
        	csvReader = new CSVReader(fileReader,',');
        	
            String[] confLine;
            while ((confLine = csvReader.readNext()) != null) {
            	if (confLine.length>2){
            		throw new Exception("FATAL ERROR. No check status:" + fileStatus.getAbsolutePath());
               }
            	status = confLine[0].charAt(0);
            	
            }

            logger.info("Get status cron: " + status );
            
            csvReader.close();
            fileReader.close();
            
            return status;
        } else {
        	return CSVReaderStatus.statusBlocked;
        }
	}
	

	/*
	 * Save status actual cron:
	 * 
	 *  char statusFree = '0';
	 *  char statusBlocked = '1';
	 *  char statusError = '2';
	 * 
	 */
	public static void setStatus(char statusChar) throws IOException{

        String fullPathFile = CSVReaderStatus.getUrlStatusFile();
        java.util.Date date = new java.util.Date();
        Timestamp timestamp = new Timestamp(date.getTime());

        BufferedWriter out = new BufferedWriter(new java.io.FileWriter(fullPathFile));
        writer = new au.com.bytecode.opencsv.CSVWriter(out);
        String[] values = {String.valueOf(statusChar), timestamp.toString()};
        writer.writeNext(values);
        out.close();
        writer.close();
	}

    /**
     * get path to csv file status
     * @return string
     */
    private static String getUrlStatusFile(){

        if (Main.exporterConf.containsKey("configCSVDumper")){
            Map<String,Object> confCSV = ConfManager.getConnection((String)Main.exporterConf.get("configCSVDumper"));
            return (String)confCSV.get("statusPath");
        }

        if (Main.exporterConf.containsKey("defaultStatusPath")){
            return (String)Main.exporterConf.get("defaultStatusPath");
        }

        logger.error("Error no config path status file for this instance configuration");
        return "status/status.conf";
    }
}
