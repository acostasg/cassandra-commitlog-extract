package agent;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.util.*;

import org.apache.log4j.Logger;

public class Flow implements Runnable {
    protected static Logger logger = Logger.getLogger(Flow.class.getName());

    private static final String classCSVDumper = "agent.CSVDumper";
    public static final String NOERROR = "noError";
    public static final String NOERRORNOCHANGE = "noErrornoChange";

    private RowGatherer reader = null;

    private int lastBlockSize = 0;

    private List<String> tableNames;
    private String currentTableName;

    private Class readerClass;

    private boolean end;

    public Flow(List<String> tableNames, String readerClassName)
            throws Exception {
        this.tableNames = tableNames;
        readerClass = Class.forName(readerClassName);
        end = false;
    }

    public int checkAndClearLastBlockSize() {
        int r = 0;
        synchronized (this) {
            r = lastBlockSize;
            lastBlockSize = 0;
        }
        return r;
    }

    public String banner() {
        return currentTableName;
    }

    public boolean isFinished() {
        return end;
    }

    public void run() {

        try {

            for (String tableName : tableNames) {
                logger.info("Start: " + tableName);
                currentTableName = tableName;

                checkAndClearLastBlockSize();

                reader = (RowGatherer) readerClass.newInstance();
                reader.init(tableName);

                while (true) {
                    RowBlock block = reader.read();
                    if (block.rows.isEmpty()) {
                        break;
                    }

                    OutputManager.write(block);

                    synchronized (this) {
                        lastBlockSize += block.rows.size();
                        //debug trace
                        logger.info("increment last block Size:" + lastBlockSize);
                    }

                    //csv dumper no blockSize, it's only for cassandra
                    if (readerClass.getName() == classCSVDumper) break;

                }
                OutputManager.commit(tableName);

                reader.close();
                logger.info("End: " + tableName);
            }

        } catch (Throwable t) {
            if (t.getCause() != null && t.getCause().getMessage() == Flow.NOERROR) {
                //this.unlockCron( CSVReaderStatus.statusFree);
                logger.info(t.getMessage().toString());
            } else if (t.getCause() != null && t.getCause().getMessage() == Flow.NOERRORNOCHANGE) {
                logger.info(t.getMessage().toString());
            } else {
                this.unlockCron(CSVReaderStatus.statusError);
                logger.error("Exception in flow :", t);
            }
        } finally {
            end = true;
        }

    }

    private void unlockCron(char status) {
        if (readerClass.getName() == classCSVDumper) {
            // unlock cron
            try {
                CSVReaderStatus.setStatus(status);
            } catch (IOException e) {
                logger.info(e.getMessage());
            }
        }
    }

    public void close() throws Exception {
        if (reader != null) reader.renameCSVFile();
    }

}
