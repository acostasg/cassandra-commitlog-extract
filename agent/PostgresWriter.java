package agent;

import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

public abstract class PostgresWriter {
	
    protected String table;
    protected boolean bulkMode;
    private String tempNameTable;

    protected List<String> initTempTable() throws Exception {
    	
    	if ( tempNameTable == null || tempNameTable.isEmpty() ){
    		Random random = new Random() ;
            Integer randomNumber = random.nextInt(99999) + 2;
    		
    		Long startTime = System.nanoTime();
        	tempNameTable = table+"_"+startTime.toString()+"_"+randomNumber.toString();
    	}

        List<String> r = new LinkedList<String>();
        r.add("CREATE TEMPORARY TABLE \""+ tempNameTable +"_Temp\" (id varchar(255) NOT NULL, attributes hstore) WITHOUT OIDS");
        r.add("ALTER TABLE \""+ tempNameTable +"_Temp\" ADD PRIMARY KEY (\"id\")");
        return r;
    }

    protected List<String> insertsToTemp(RowBlock block, Filter filter) throws Exception {
    	
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

            sb.append("INSERT INTO \""+tempNameTable+"_Temp\" (\"id\", \"attributes\") VALUES ('");
            sb.append(StringEscapeUtils.escapeSql(Utils.deFuxMore(Utils.deFux(key))));
            sb.append("', '");

            boolean first = true;

            StringBuffer sbr = new StringBuffer("");
            for (Map.Entry<String, String> kv : row.columns.entrySet()) {
                String value = kv.getValue();
                if (value == null) {
                    continue;
                }
                String column = kv.getKey();

                if (!first) {
                    sbr.append(",");
                }
                try {
                    escape(column, sbr);
                    sbr.append("=>");
                    escape(value, sbr);
                } catch (Exception e) {
                    
                }
                first = false;
            }
            sb.append(StringEscapeUtils.escapeSql(sbr.toString()));
            sb.append("')");
            r.add(sb.toString());
        }

        return r;
    }


    protected List<String> mergeTempTable(){
        List<String> r = new LinkedList<String>();
        StringBuilder sb = new StringBuilder("");
        r.add("ANALYZE \""+ tempNameTable +"_Temp\"");
        sb.append("WITH upsert AS (UPDATE \""+table+"\" orig SET attributes=temp.attributes FROM \""+tempNameTable+"_Temp\" temp WHERE orig.id=temp.id RETURNING orig.id)\n");
        sb.append("INSERT INTO \""+table+"\" SELECT temp2.id,temp2.attributes FROM \""+tempNameTable+"_Temp\" temp2 WHERE temp2.id NOT IN (SELECT u.id FROM upsert u)");
        r.add(sb.toString());
        r.add("TRUNCATE TABLE \""+ tempNameTable +"_Temp\"");
        return r;
    }

    // do not use the non-bulk mode of the following
    private void outStartBlock(StringBuffer sb) {
        if (bulkMode) {
            sb.append("COPY \"" + table + "\" FROM STDIN (DELIMITER '^');\n");
        } else {
            sb.append("WITH new_values (nid, nattributes) AS (VALUES\n");
        }
    }

    private void outNewRow(StringBuffer sb) {
        if (bulkMode) {
            sb.append("\n");
        } else {
            sb.append(",\n");
        }
    }

    private void outStartRow(StringBuffer sb, String key) {
        if (bulkMode) {
            sb.append(key);
            sb.append("^");
        } else {
            sb.append("('");
            sb.append(key);
            sb.append("','");
        }
    }

    private void outEndRow(StringBuffer sb, String value) {
        if (bulkMode) {
            sb.append(value);
        } else {
            sb.append(value);
            sb.append("'::hstore)");
        }
    }

    private void outEndBlock(StringBuffer sb) {
        if (bulkMode) {
            sb.append("\n\\.\n\n");
        } else {
            sb.append("),\n"+
                "upsert AS (\n"+
                "    UPDATE \"" + table + "\"\n"+
                "        SET id = nid,\n"+
                "            attributes = nattributes\n"+
                "    FROM new_values\n"+
                "    WHERE id = nid\n"+
                "    RETURNING \"" + table + "\".*\n"+
                ")\n"+
                "INSERT INTO \"" + table + "\" (id, attributes)\n"+
                "SELECT nid, nattributes\n"+
                "FROM new_values\n"+
                "WHERE NOT EXISTS (SELECT 1 \n"+
                "                  FROM upsert up \n"+
                "                  WHERE up.id = new_values.nid);\n\n");
        }
    }

    protected String inserts(RowBlock block, Filter filter) throws Exception {

        StringBuffer sb = new StringBuffer("");

        outStartBlock(sb);

		boolean firstRow = true;
        for (Row row : block.rows) {

        	String key = row.key;

            if (filter != null && !filter.isValid(row) && !filter.tableNotFilter(this.table) ) {
                continue;
            }

        	if (firstRow) {
        		firstRow = false;
        	} else {
                outNewRow(sb);
        	}

            if (key.length() > 255) {
                key = key.substring(0, 255);
            }
            outStartRow(sb, StringEscapeUtils.escapeSql(Utils.deFuxMore(Utils.deFux(key))));

			boolean first = true;

			StringBuffer sbr = new StringBuffer("");
	        for (Map.Entry<String, String> kv : row.columns.entrySet()) {
	            String value = kv.getValue();
	            if (value == null) {
	                continue;
	            }
	            String column = kv.getKey();

	            if (!first) {
	                sbr.append(",");
	            }
	            try {
	                escape(column, sbr);
	                sbr.append("=>");
	                escape(value, sbr);
	            } catch (Exception e) { }
	            first = false;
	        }

            outEndRow(sb, StringEscapeUtils.escapeSql(sbr.toString()));
        }

        outEndBlock(sb);

        return sb.toString();
    }

    private void escape(String v, StringBuffer sb) throws Exception {
        if (v.length() > 2000) {
            v = v.substring(0, 2000);
        }
        v = Utils.deFux(v);
        sb.append("\"");
        sb.append(Utils.deFuxMore(Utils.hstoreEscape(v)));
        sb.append("\"");
    }

}
