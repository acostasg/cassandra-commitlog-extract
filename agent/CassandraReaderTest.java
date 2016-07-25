package agent;


// https://github.com/rantav/hector
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.model.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.*;
import me.prettyprint.hector.api.query.*;
import me.prettyprint.hector.api.exceptions.*;

import org.apache.cassandra.thrift.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

import java.awt.Color;
import java.io.*;
import java.util.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;




import java.nio.ByteBuffer;
import java.util.List;

// multi purpose reader class used by all cassandra readers: dumpers, ID gatherers, preloaders
public class CassandraReaderTest{
	
	protected static Logger logger = Logger.getLogger(Main.class.getName());

	private String table;
    private String[] columns;
	private String lastKey;
	private int blockSize;
	private CassandraManager manager;
    private RangeSlicesQuery<String, String, String> rangeSlicesQuery;
    private MultigetSliceQuery<String, String, String> multigetSliceQuery;
    
    
    

	public CassandraReaderTest(String table, String[] columns, int blockSize) throws Exception {
		this.manager = CassandraManager.singleton();
        this.table = table;
        this.columns = columns;
        this.blockSize = blockSize;
        reset();
	}

    public void reset() throws Exception {
    	lastKey = "";
        StringSerializer stringSerializer = StringSerializer.get(); 
        Keyspace keyspace = manager.getKeyspace();

        rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);            
        rangeSlicesQuery.setColumnFamily(table);
        rangeSlicesQuery.setKeys(lastKey, "");
        rangeSlicesQuery.setRowCount(blockSize);
        if (columns != null) {
            rangeSlicesQuery.setColumnNames(columns);
        } else {
            rangeSlicesQuery.setRange("", "", false, 100000);
        }

        multigetSliceQuery = HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);            
        multigetSliceQuery.setColumnFamily(table);
        //multigetSliceQuery.setKeys(
        if (columns != null) {
            multigetSliceQuery.setColumnNames(columns);
        } else {
            multigetSliceQuery.setRange("", "", false, 100000);
        }
    }
    

    private RowBlock processResult(Rows<String, String, String> orderedRows) {
        RowBlock block = new RowBlock(table);
        
        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");
        
        for (me.prettyprint.hector.api.beans.Row<String, String, String> cassandraRow : orderedRows) {
            String key = cassandraRow.getKey();
            // "" will never be a valid ID in our model, so this is safe for the first iteration too
            if (key.equals(lastKey)) {
                continue;
            }
            lastKey = key;

            agent.Row row = new agent.Row(key);

            List columns = cassandraRow.getColumnSlice().getColumns();
            
            int n = 0;
            for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
                HColumn column = (HColumn)iterator.next();
                
                
                
                
                if(cols.get(column.getName().toString()) != null && cols.get(column.getName().toString()).equals("Integer"))
                {
                	//Integer value = Character.codePointAt(column.getValue().toString(), 0);
                	//row.columns.put(column.getName().toString(), value.toString());
                	
                	
                	//if(value.equals(0))
                	if(key.equals("descuentos/zaragoza/sastre|es"))
                	{
                		
                		
                		Integer value2 = Character.codePointAt(column.getValue().toString(), 0);
                		
                		
                		
                		//int codepoint = column.getValue().toString().codePointAt(0);
                		
                		//byte[] foobyte = column.getValue().toString().getBytes();
                		
                		/*
                		try{
	                		String s = column.getValue().toString();
	                        System.out.println(s);
	                        byte[] b = new byte[s.length()];
	                        for (int i = 0; i < b.length; i++) {
	                            int cp = s.codePointAt(i);
	                            b[i] = (byte) cp;
	                            System.out.print((byte) cp + " ");
	                        }
	                        System.out.println();
	                        System.out.println(new String(b, "UTF-8"));
	                        System.out.println(new String(b, "US-ASCII"));
	                        System.out.println(new String(b, "ISO-8859-1"));
                		} catch (UnsupportedEncodingException e) { }
                		*/
                	    
                		/*String strImport = column.getValue().toString();
                	    strImport = strImport.replaceAll("\uFFFD", "");
                	    
                	    System.out.println(strImport);*/
                		
                		logger.info(
                				"key: " + key +
                    			" - value1: " + column.getValue().toString().length() + 
                    			" - value2: " + value2
                    	);
                		
                		
                		
                		/*
                		//String unicodeString   = new String("\u0048" + "\u0065" + "\u006C" + "\u1D18"); //"Help"
                		String unicodeString   = new String(column.getValue().toString());
                		byte[] utf8Bytes       = null;
                		String convertedString = null;
                		try {
                			System.out.println(unicodeString);
                			utf8Bytes       = unicodeString.getBytes("UTF-8");
                			convertedString = new String(utf8Bytes,  "UTF-8");
                			
                			System.out.println(convertedString); //same as the original string
                			
                		}
                		catch (UnsupportedEncodingException e)
                		{   e.printStackTrace();
                		}
                		*/
                		
                		/*
                		try {
	                		String str = column.getValue().toString();
	                		byte[] utf8 = str.getBytes("UTF-8");
	                		byte[] utf16 = str.getBytes("UTF-16");
	                		byte[] utf32 = str.getBytes(Charset.forName("UTF-32"));
	                		byte[] win31 = str.getBytes(Charset.forName("Windows-31j"));
	                		
                			
                			
	                		
	                		//System.out.println(str);
	                	} catch (UnsupportedEncodingException e) {
	            			
	            		}
                		*/
                		
                		/*try {
	                		
	                	    byte[] utf8 = column.getValue().toString().getBytes("ASCII");
	
	                	    // Convert from UTF-8 to Unicode
	                	    String stringb = new String(utf8, "UTF-8");
	                	    
	                	    System.out.println(Arrays.toString("\u0080".getBytes("UTF-8")));
	                	    
	                	    logger.info(
	                    			" - value2: " + stringb
	                    	);
	                	    
                		} catch (UnsupportedEncodingException e) {
                			
                		}*/
                		
                		
                		/*logger.info(
                    			" - value2: " + value2
                    	);*/
                	}
                	
                	//String strImport = "For some reason my double quotes were lost.";
                	
                	
                	
                	/*logger.info(
                			"key: " + key +
                			" - col: " + column.getName().toString() +
                			" - orig: " + column.getValue().toString() +
                			" - value: " + value.toString()
                	);*/
                	
                }
                else
                {
                	//logger.info("ERROR -------> ");
                	//row.columns.put(column.getName().toString(), column.getValue().toString());
                }
                
                
                
                //logger.info("TESSSSTTT -------> " + cols.get("sssssaaa"));
                
                /*if(cols.get(column.getName().toString()).equals("Integer"))
                {
                	Integer value = Character.codePointAt(column.getValue().toString(), 0);
                	row.columns.put(column.getName().toString(), value.toString());
                	
                }
                else
                {
                	row.columns.put(column.getName().toString(), column.getValue().toString());
                }*/
                //row.columns.put(column.getName().toString(), column.getValue().toString());
                
                n++;
            }
            // skip empty rows
            if (n == 0) {
                continue;
            }

            block.rows.add(row);
        }

        return block;
    }

    public RowBlock readMulti(Iterable<String> i) throws Exception {
        multigetSliceQuery.setKeys(i);
        Rows<String, String, String> orderedRows = multigetSliceQuery.execute().get();
        return processResult(orderedRows);
    }

    public RowBlock readSequential() throws Exception {
        rangeSlicesQuery.setKeys(lastKey, "");
		OrderedRows<String, String, String> orderedRows = rangeSlicesQuery.execute().get();
        return processResult(orderedRows);
    }
    
    
    
    


    

}
