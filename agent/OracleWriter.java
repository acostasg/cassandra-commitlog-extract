package agent;

import java.util.*;
import java.io.*;
import java.sql.*;

import org.apache.commons.lang.StringEscapeUtils;

import org.apache.log4j.Logger;

public abstract class OracleWriter {
    protected static Logger logger = Logger.getLogger(Main.class.getName());


    protected String table;

    protected List<String> mergeTempTable() {
    	
        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");
        Map<String,String> oracleCols = Utils.validOracleColumnNames(cols.keySet());
        
        List<String> r = new LinkedList<String>();

        StringBuilder sb = new StringBuilder("");
        
        //semaforo
        //sb = new StringBuilder("UPDATE sem_tab SET DUMMY='x'");
        //r.add(sb.toString());
        
        //duplicate
        //sb = new StringBuilder("delete \"" + table + "_Temp\" where (\"id\", \"autoUpdatedOn\") not in (select \"id\", max(\"autoUpdatedOn\") from \"" + table + "_Temp\" group by \"id\")\n");
        //r.add(sb.toString());

        sb = new StringBuilder();
        sb.append("MERGE INTO \"" + table + "\" USING \"" + table + "_Temp\" ON (\"" + table + "_Temp\".\"id\"=\"" + table + "\".\"id\")\n");

        sb.append("WHEN NOT MATCHED THEN INSERT (\"id\"");
        for (Map.Entry<String, String> kv : cols.entrySet()) {
            String name = kv.getKey();
            name = oracleCols.get(name);
            sb.append(",\""+ name +"\"");
        }

        sb.append(")\nVALUES (\"" + table + "_Temp\".\"id\"");
        for (Map.Entry<String, String> kv : cols.entrySet()) {
            String name = kv.getKey();
            name = oracleCols.get(name);
            sb.append(",\"" + table + "_Temp\".\""+name+"\"");
        }

        sb.append(")\nWHEN MATCHED THEN UPDATE SET ");
        boolean first = true;
        for (Map.Entry<String, String> kv : cols.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            String name = kv.getKey();
            name = oracleCols.get(name);
            sb.append("\"" + table + "\".\""+name+"\"=\"" + table + "_Temp\".\""+name+"\"");
        }

        r.add(sb.toString());

        sb = new StringBuilder("TRUNCATE TABLE \"" + table + "_Temp\"");
        r.add(sb.toString());
        
        //log DEBUG
        logger.debug("MERGE SQL:"+ r.toString());

        return r;
    }

    private PreparedStatement fixedPstmt = null;
    protected PreparedStatement touchStatement(JDBCWriter w) throws Exception {
        if (fixedPstmt == null) {
            Connection conn = w.getConnection();

            Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
            Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");
            Map<String,String> oracleCols = Utils.validOracleColumnNames(cols.keySet());

            List<String> r = new LinkedList<String>();

            StringBuilder sb = new StringBuilder("");
            sb.append("INSERT INTO \"" + table + "_Temp\" (\"id\"");
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                String name = kv.getKey();
                name = oracleCols.get(name);
                sb.append(",\""+ name +"\"");
            }
            sb.append(") VALUES (?");
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                sb.append(",?");
            }
            sb.append(")\n");
            
            //log DEBUG
            logger.debug("TOUCHSTATMENT SQL:"+ sb.toString());

            fixedPstmt = conn.prepareStatement(sb.toString());
        }
        return fixedPstmt;
    }

    // insert row by row version with JDBC prepared statement
    protected void insertsJDBC(JDBCWriter w, RowBlock block, Filter filter) throws Exception {
        long elapsed = System.currentTimeMillis();

        PreparedStatement pstmt = touchStatement(w);

        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");
        //Map<String,String> oracleCols = Utils.validOracleColumnNames(cols.keySet());

        List<String> r = new LinkedList<String>();

        for (Row row : block.rows) {
            String key = row.key;

            if (filter != null && !filter.isValid(row) && !filter.tableNotFilter(this.table) ) {
                continue;
            }

            if (key.length() > 255) {
                key = key.substring(0, 255);
            }

            pstmt.setString(1, escapeSoft(key));
            int i = 2;
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                String name = kv.getKey();
                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                pstmt.setString(i, escapeSoft(value));
                i++;
            }
            pstmt.addBatch();
        }

        pstmt.executeBatch();
        w.getConnection().commit();
        elapsed = System.currentTimeMillis() - elapsed;
        logger.info("Oracle batching of " + block.rows.size() + " prepared inserts took " + elapsed + "ms");
    }

    // insert row by row version
    protected List<String> inserts(RowBlock block, Filter filter) throws Exception {

        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");
        Map<String,String> oracleCols = Utils.validOracleColumnNames(cols.keySet());

        List<String> r = new LinkedList<String>();

        for (Row row : block.rows) {
            StringBuilder sb = new StringBuilder("");

            String key = row.key;

            if (filter != null && !filter.isValid(row) && !filter.tableNotFilter(this.table) ) {
                continue;
            }

            if (key.length() > 255) {
                key = key.substring(0, 255);
            }

            sb.append("INSERT INTO \"" + table + "_Temp\" (\"id\"");
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                String name = kv.getKey();
                name = oracleCols.get(name);
                sb.append(",\""+ name +"\"");
            }

            sb.append(") VALUES (");
            escape(key, sb);
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                sb.append(",");
                String name = kv.getKey();
                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                escape(value, sb);
            }
            sb.append(")\n");
            r.add(sb.toString());
        }

        
        //log DEBUG
        logger.debug("INSERT SQL:"+ r.toString());
        return r;
    }


/*
    // insert all version
    protected List<String> inserts(RowBlock block, Filter filter) throws Exception {

        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");

        List<String> r = new LinkedList<String>();

        StringBuilder top = new StringBuilder("");

        top.append("INSERT ALL \n");

        for (Row row : block.rows) {
            StringBuilder sb = new StringBuilder("");

        	String key = row.key;

            if (filter != null && !filter.isValid(row)) {
                continue;
            }

            if (key.length() > 90) {
                key = key.substring(0, 90);
            }

            sb.append(" INTO \"" + table + "_Temp\" (\"id\"");
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                String name = kv.getKey();
                sb.append(",\""+ name +"\"");
            }

            sb.append(") VALUES (");
            escape(key, sb);
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                sb.append(",");
                String name = kv.getKey();
                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                escape(value, sb);
            }
            sb.append(")\n");
            top.append(sb.toString());

            //r.add(sb.toString());
        }

        top.append("SELECT * FROM dual\n");
        r.add(top.toString());

        return r;
    }
*/

/*
    // merge into by row version
    protected List<String> inserts(RowBlock block, Filter filter) throws Exception {

        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");

        List<String> r = new LinkedList<String>();

        for (Row row : block.rows) {
            StringBuilder sb = new StringBuilder("");

            String key = row.key;

            if (filter != null && !filter.isValid(row)) {
                continue;
            }

            if (key.length() > 90) {
                key = key.substring(0, 90);
            }

            sb.append("MERGE INTO \"" + table + "\" USING dual ON (\"id\"=");
            escape(key, sb);
            sb.append(")\nWHEN NOT MATCHED THEN INSERT (\"id\"");
            StringBuilder sbd = new StringBuilder("");
            escape(key, sbd);

            for (Map.Entry<String, String> kv : cols.entrySet()) {
                sb.append(",");
                sbd.append(",");

                String name = kv.getKey();
                sb.append("\""+ name +"\"");

                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                escape(value, sbd);
            }
            sb.append(") VALUES (");
            sb.append(sbd.toString());
            sb.append(")\nWHEN MATCHED THEN UPDATE SET ");

            boolean first = true;
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                String name = kv.getKey();
                sb.append("\""+ name +"\"=");

                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                escape(value, sb);
            }
            r.add(sb.toString());
        }

        return r;
    }
*/

    private void escape(String v, StringBuilder sb) throws Exception {
        if (v.length() > 2900) {
            v = v.substring(0, 2900);
        }
        v = Utils.deFuxOracle(v);
        sb.append("'");
        sb.append(Utils.deFuxMore(StringEscapeUtils.escapeSql(v)));
        sb.append("'");
    }

    private String escapeSoft(String v) throws Exception {
        if (v.length() > 2900) {
            v = v.substring(0, 2900);
        }
        return Utils.deFuxOracle(v);
    }

}
