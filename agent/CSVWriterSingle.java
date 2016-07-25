package agent;

import java.util.*;

import org.apache.log4j.Logger;
import org.postgresql.util.Base64;

import com.fasterxml.jackson.databind.ObjectMapper;

// follows http://tools.ietf.org/html/rfc4180

public class CSVWriterSingle implements RowWriter {

    protected static Logger logger = Logger.getLogger(CSVWriterSingle.class.getName());

    private long fileSize;
    private boolean printColumnsNow;
    private ArrayList<String> columns;
    private String table;
    private List<String> writersMultiplePaths;
    private List<FileWriter> writersFiles;
    private Map<String, Object> connection;

    public void init(String table) throws Exception {

        logger.debug("** INIT CSVWriterSingle, it's create only csv. **");

        this.table = table;

        connection = ConfManager.getConnection("csv-single");

        Map<String, Object> conf = ConfManager.getConnection("csv-single");

        this.printColumnsNow = true;
        if (conf.containsKey("header")) {
            this.printColumnsNow = ((Boolean) conf.get("header")).booleanValue();
        }

        this.fileSize = 8 * 1024 * 1024;
        if (conf.containsKey("fileSize")) {
            try {
                this.fileSize = ((Long) conf.get("fileSize")).longValue();
            } catch (ClassCastException e) {
                this.fileSize = ((Integer) conf.get("fileSize")).intValue();
            }
        }


        try {
            writersMultiplePaths = getPaths();
        } catch (Exception e) {
            writersMultiplePaths.set(0, "./");
        }

        //init
        writersFiles = new ArrayList<FileWriter>();

        this.columns = null;

        Map<String, Object> ct = (Map<String, Object>) ConfManager.getTable(table);
        this.printColumnsNow = false;

        if (ct.containsKey("fixedColumns")) {
            LinkedHashMap<String, String> cols = (LinkedHashMap<String, String>) ct.get("fixedColumns");
            this.columns = new ArrayList<String>();
            this.columns.add("id");
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                this.columns.add(kv.getKey());
            }
            //this.printColumnsNow = true;
        }

    }


    private List<String> getPaths() throws Exception {
        List<String> list = (List<String>) connection.get("path");
        return list;
    }

    /**
     * This method is only for single csv
     */
    public void execute(long timestamp) throws Exception {

        Map<String, Object> connection = ConfManager.getConnection("csv-single");

        //extension node
        String extension;
        if (connection.containsKey("extension")) {
            extension = (String) connection.get("extension");
        } else {
            extension = "1";
        }

        //object writers for multiple paths
        for (String path : this.writersMultiplePaths) {
            logger.debug(" ***************FILE->" + path + "cassandra_" + timestamp + "_" + extension + ".csv");
            this.writersFiles.add(new FileWriter(path + "cassandra_" + timestamp + "_" + extension, "csv", this.fileSize));
        }

        logger.debug("** HEADER STATUS " + Boolean.toString(this.printColumnsNow) + " **");

        if (this.printColumnsNow) {
            writeColumns();
        }
    }

    private void writeColumns() throws Exception {
        boolean first = true;
        StringBuilder sb = new StringBuilder("");
        Map<String, String> oracleCols = Utils.validOracleColumnNames(new HashSet(columns));
        for (String column : columns) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            column = oracleCols.get(column);
            sb.append(column);
        }
        sb.append("\n");


        //object writers for multiple paths
        for (FileWriter writer : writersFiles) {
            writer.write(sb.toString());
        }

        logger.debug(" Write column " + sb.toString() + "");
    }

    public void write(RowBlock block, Filter filter) throws Exception {

        StringBuffer sb = new StringBuffer("");

        for (Row row : block.rows) {

            if (columns == null) {
                Set<String> sc = block.columnNames();
                sc.remove("id");
                columns = new ArrayList<String>(sc);
                columns.add(0, "id");
                writeColumns();
            }

            if (filter != null && !filter.isValid(row) && !filter.tableNotFilter(this.table)) {
                continue;
            }

            row.columns.put("RowKey", row.key);
            ObjectMapper mapper = new ObjectMapper();
            String value = mapper.writeValueAsString(row.columns);
            value = escape(value);
            String encoded = Base64.encodeBytes(value.getBytes("UTF-8"));
            //value = escape(value);

            sb.append("\"" + table + "\",\"" + encoded + "\"\n");
        }

        //object writers for multiple paths
        for (FileWriter writer : writersFiles) {
            writer.write(sb.toString());
        }

    }

    public void close() throws Exception {
        //object close for multiple paths
        for (FileWriter writer : writersFiles) {
            writer.close();
        }
    }

    public void commit() throws Exception {
    }

    private String escape(String v) throws Exception {
        v = v.replace("!!!Groupalia_Mapper_Manager.JSON!!!", "");
        //v = Utils.deFuxMore(v);
        //v = Utils.fixEncoding(v);
        //v = StringEscapeUtils.escapeCsv(v);
        return v;
    }

}
