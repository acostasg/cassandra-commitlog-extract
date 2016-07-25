package agent;

import java.io.*;
import java.util.*;

public class OutputManager {

    private static OutputManager instance = null;

    private Map<String, LinkedList<RowWriter>> writers;

    private List<Class> writersClasses;
    private Filter filter;
    private static long dateUniqueCSV;

    private OutputManager(List<String> writersClassNames, Filter filter) throws Exception {
        writers = new HashMap<String, LinkedList<RowWriter>>();
        writersClasses = new LinkedList<Class>();
        for (String className : writersClassNames) {
            writersClasses.add(Class.forName(className));
        }
        this.filter = filter;
    }

    public static void init(List<String> writersClassNames, Filter filter) throws Exception {
        OutputManager.instance = new OutputManager(writersClassNames, filter);
        //for single model csv file text
        java.util.Date date = new java.util.Date();
        long timeStamp = date.getTime();
        OutputManager.dateUniqueCSV = timeStamp;
    }

    private List<RowWriter> touchWriters(String table) throws Exception {
        LinkedList<RowWriter> returnWriters = null;
        synchronized (this) {
            if (!writers.containsKey(table)) {
                returnWriters = new LinkedList<RowWriter>();
                for (Class klass : writersClasses) {
                    RowWriter writer = (RowWriter) klass.newInstance();
                    writer.init(table);
                    writer.execute(OutputManager.dateUniqueCSV);
                    returnWriters.add(writer);
                }
                writers.put(table, returnWriters);
            } else {
                returnWriters = writers.get(table);
            }
        }
        return returnWriters;
    }

    private void closeAllI() throws Exception {
        synchronized (this) {
            for (Map.Entry<String, LinkedList<RowWriter>> kv : writers.entrySet()) {
                List<RowWriter> cwriters = kv.getValue();
                for (RowWriter writer : cwriters) {
                    synchronized (writer) {
                        writer.commit();
                        writer.close();
                    }
                }
            }
        }
    }

    public static void write(RowBlock block) throws Exception {
        List<RowWriter> writers = OutputManager.instance.touchWriters(block.table);
        for (RowWriter writer : writers) {
            synchronized (writer) {
                writer.write(block, OutputManager.instance.filter);
            }
        }
    }

    public static void close(String table) throws Exception {
        List<RowWriter> writers = OutputManager.instance.touchWriters(table);
        for (RowWriter writer : writers) {
            synchronized (writer) {
                writer.commit();
                writer.close();
            }
        }
    }

    public static void commit(String table) throws Exception {
        List<RowWriter> writers = OutputManager.instance.touchWriters(table);
        for (RowWriter writer : writers) {
            synchronized (writer) {
                writer.commit();
            }
        }
    }

    public static void closeAll() throws Exception {
        OutputManager.instance.closeAllI();
    }

}
