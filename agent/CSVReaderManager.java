package agent;

import java.lang.Object;

import agent.CSVReaderStatus;
import org.apache.log4j.Logger;
import org.apache.commons.io.*;
import org.postgresql.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;

import au.com.bytecode.opencsv.*;

import java.io.*;
import java.sql.BatchUpdateException;
import java.util.*;
import java.util.regex.Pattern;
import java.lang.reflect.Method;


// multi purpose reader class used by all CSVReader
public class CSVReaderManager {
	
    protected static Logger logger = Logger.getLogger(CommitLogReader.class.getName()); 

	private String table;
    private File file;
    private CSVReader csvReader;
	private String lastLine;
	private int blockSize;
	private String path;

	public CSVReaderManager(String table, String path, int blockSize, long rollbackDeltaSeconds ) throws Exception {
		logger.info("** INIT READER CSV "+ table +" **");
        this.table = table;
        this.blockSize = blockSize;
        this.path = path;
        reset();
        getCSVfile(this.path,rollbackDeltaSeconds);
	}

    public void reset() throws Exception {
    	lastLine = "";
    	blockSize = 0;
    } 
    
    private static Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("cassandra_.*.csv");

    private static boolean possibleCSVFile(String filename) {
        return COMMIT_LOG_FILE_PATTERN.matcher(filename).matches();
    }

    private void getCSVfile(final String directory, long rollbackDeltaSeconds) throws Exception
    {

        File[] files = new File(directory).listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                if (!CSVReaderManager.possibleCSVFile(name)) {
                    return false;
                }

                return true;
            }
        });

        if (files == null || files.length == 0) {
            logger.info("No csv files found in "+ directory +" skipping replay");
            return;
        }

        // order for last file
        Arrays.sort(files);

        //only last file
        this.file = files[0];

        logger.info("Accepted file , " + this.file.getName().toString() + " csv");
    }

	@SuppressWarnings("unchecked")
	private RowBlock processResult(RowBlock block, File file ) throws Exception{

         FileReader fileReader = new FileReader(file);

         if (!file.canRead()){
        	 extracted(file);
         }

         this.csvReader = new CSVReader(fileReader,',','"');

         logger.info("Scanning CSV " + file.getAbsoluteFile());

         String[] nextLine;         
         while ((nextLine = csvReader.readNext()) != null) {
        	 
         	if (nextLine.length<=1){
            	continue;
            }

        	//the second field of csv [1] is the RowKey of cassandra column
            String line = nextLine[1];
            lastLine = line;
            
            if (nextLine[0].toString().trim().equalsIgnoreCase(table.toString().trim()) && nextLine[1]!=null && !nextLine[1].toString().isEmpty()){

            	ObjectMapper mapper = new ObjectMapper();

                byte[] decoded = Base64.decode(nextLine[1]);

                Map<String,String> jsonColumns = mapper.readValue(decoded, Map.class);
                
                //logger.debug("** dumper ["+jsonColumns.toString()+ "**");
                
                agent.Row row = new agent.Row(jsonColumns.get("RowKey"));
                row.columns = jsonColumns;                
	            
	            block.rows.add(row);
            } else {
                //logger.debug("** RowKey Invalid RowKey "+ nextLine[1].toString() +" : CF "+ nextLine[0].toString() +" not "+ table.toString()+ " **");
            	continue;
            }
        }        
        return block;
    }

	private void extracted(File file) throws Exception {
		throw new Exception("The csv file ["+file.getAbsoluteFile()+"] is BLOCKED. ", new Throwable(Flow.NOERROR));
	}

    public RowBlock readSequential() throws Exception {

    	if (this.file == null){
    		throw new Exception("Not csv file(s) in path" + this.path, new Throwable(Flow.NOERROR));
    	}

    	RowBlock csvRows = new RowBlock(table);
    	processResult(csvRows, file);

    	//return null;
        return csvRows;
    }    

    public void close() throws Exception {    	
        csvReader.close();
        logger.debug("** FINSIH block "+ blockSize +". READED CF "+ table +" **");
    }
    
    public File getFileProcessed(){
    	return this.file;
    }

}
